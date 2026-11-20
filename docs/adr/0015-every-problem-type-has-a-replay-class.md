# ADR 0015: every problem type has a replay class; a retryable refusal is not stored

- Status: Accepted (2026-11-20)
- Owner decision: fleet rule of 2026-11-18 (`.claude/rules/fleet/idempotency-and-outbox.md`; the standard: warehouse-harness-template `docs/idempotency-replay-classes.md`)
- Supersedes: the "a 4xx answer is stored and replayed" sentence of ADR 0008 (its Decision text stands as the record of 2026-11-01; this ADR is the dated pointer forward)

## Context

A request retried under the same `Idempotency-Key` was answered from the store whenever the store held an answer for that key, whatever the first answer was: a 2xx and any 4xx were stored (the 4xx by `finishRejected`, on its own, best effort), only a 5xx was never stored. A stored refusal that a retry could pass pins a false answer for the life of the row: a `409 concurrent-modification` or a `404 case-not-found` replayed to a caller that did the right thing and retried.

## Decision

Every problem type this service can answer has exactly one replay class, declared on its catalogue entry (`problem.class` in `internal/adapters/inbound/http/errors.go`):

| Slug | Class | Why |
| --- | --- | --- |
| `malformed-request-body` | FINAL | the body is not valid JSON for the operation (decodeJSON, the NUL guard's three variants share the slug) |
| `invalid-case-id` | FINAL | the path parameter's own shape |
| `invalid-source-type` | FINAL | the path parameter's own shape |
| `invalid-status` | FINAL | a GET query value; request-only (classified for completeness) |
| `invalid-severity` | FINAL | a field of the request body |
| `invalid-queue` | FINAL | a field of the request body |
| `invalid-assignee` | FINAL | a field of the request body |
| `invalid-actor` | FINAL | the X-Actor header, part of the request |
| `invalid-resolution` | FINAL | fields of the request body |
| `invalid-policy` | FINAL | fields of the request body |
| `invalid-limit` | FINAL | a GET query value; request-only (classified for completeness) |
| `invalid-cursor` | FINAL | a GET query value; request-only (classified for completeness) |
| `unknown-query-parameter` | FINAL | a GET query value; request-only (classified for completeness) |
| `case-not-found` | RETRYABLE | a case opens only from a consumed fact (ADR 0001; `POST /exceptions` is the permanent 405), so the missing case is a fact not consumed yet — it can arrive moments later |
| `case-already-resolved` | FINAL | RESOLVED is terminal and nothing moves it back (the status machine of ADR 0002); the refusal can never clear by retrying |
| `concurrent-modification` | RETRYABLE | the optimistic version guard lost to a live competing writer (case_repository.go: version-guarded upsert, RowsAffected 0); re-fetching and retrying is exactly the advertised cure |
| `internal-error` | RETRYABLE | a 5xx is never stored, whatever the class |
| `idempotency-key-required` | FINAL | the request itself carries no key |
| `idempotency-key-invalid` | FINAL | the request's key is not 1–128 printable ASCII |
| `idempotency-key-reused` | FINAL | a stored answer never changes (no TTL, no purge), so the conflict never clears by retrying |
| `not-found` | FINAL | the router's route table is static; a function of the request alone |
| `method-not-allowed` | FINAL | the route table is static; `POST /exceptions` is deliberately a permanent 405 (ADR 0001) |
| `dependency-unavailable` | RETRYABLE | the readiness 503; a 5xx is never stored |
| `invalid-report-range` | FINAL | the reports router's query range; request-only (the reports router serves GETs only — no keyed route; classified for completeness) |
| `report-store-error` | RETRYABLE | the reports router's 500; a 5xx is never stored |

- FINAL and OUTCOME answers are stored and replayed; a RETRYABLE answer is NOT stored: the key is released, a retry under the same key executes again, and a different body under the released key is a NEW request (not `idempotency-key-reused`).
- The middleware reads the class (`writeProblem` marks the capturing writer with it) and holds no list of slugs and no status ranges beyond 2xx and 5xx. An answer with no class (a handler writing a status by hand) is released like RETRYABLE — the fail-safe direction.
- There is **no OUTCOME entry, and none can exist in this store**: the middleware runs the handler inside one unit of work and rolls the whole thing back on any refusal (`serveFresh` returns `errRollback` on code ≥ 400), storing the 4xx answer separately afterwards (`finishRejected`). No committed state can back a refusal: every keyed use case refuses before its `atomically(...)` (commands.go: load, domain command, save+publish). The class stays in the catalogue and the middleware (`stored()` accepts OUTCOME) so a future design change is a one-entry flip, proven by the template suite.
- A concurrent duplicate under the same key sees nothing claimed: both run in parallel units of work. The duplicate that finishes 2xx (or a storable 4xx) first wins the key row; the RETRYABLE one simply stores nothing. A later retry finds the winner's answer.
- RELEASED means the same key may be tried again by whoever sent it. No consumer-side retry behaviour is added here: callers driven by people must surface the refusal; automation retrying is a separate PR.

## Store design (verified from code, 2026-11-20)

Port-based (`ports.IdempotencyStore`: `Lookup`, `Save`), no savepoint, a 4xx stored separately:

- `Idempotency.Middleware` wraps the POSTs and the PUT (`server.go`'s idempotency group).
- `Store.Lookup` first: a hit with the same fingerprint replays (byte-identical, `Idempotency-Replayed: true`); a hit with another fingerprint is `409 idempotency-key-reused`; nothing runs.
- A new key runs the handler INSIDE `UoW.Do` (one Postgres transaction; the repositories join it through `pgtx`):
  - 2xx: `Save` + re-`Lookup` in the same transaction; a lost race (`errLostRace`) rolls back and replays the winner.
  - any ≥ 400: `errRollback` — the unit of work (the handler's writes) is rolled back; the answer is then stored on its own by `finishRejected` (best effort, separate `Save`) only when its class is FINAL/OUTCOME. A 5xx, a RETRYABLE or an unclassified answer is flushed unstored: the key is released exactly like the 5xx branch.
- Stores: `postgres/idempotency_store.go` (`SELECT` by key, no age predicate; `INSERT ... ON CONFLICT (key) DO NOTHING`) and the `memory` double used by the middleware's unit tests. The release decision lives in the middleware, so both stores honour it unchanged.
- **TTL/retention: none.** No age predicate on `Lookup`, no purge of `idempotency_keys` anywhere in the codebase — rows live forever. Recorded as an open point (fleet rule: a 24-hour window should be enforced by a purge and an age check); the fix shape is order-sourcing #84 / last-mile-delivery #86. Not fixed in this PR.

## Consequences

- The two RETRYABLE problem types (`concurrent-modification`, `case-not-found`) stop being replayed as stored refusals; a caller that retries the same key now executes again. No consumer of this service sends a deterministic key (verified: no outbound HTTP client exists in this service — CLAUDE.md rule 6, "frozen local copies, no live lookup"), so no consumer change is required; callers of a person-driven surface must surface the refusal.
- `apis/openapi.yaml` descriptions now say which refusals replay; `docs/docs/api-reference/rest` regenerated.
- A test holds this ADR's table equal to the catalogue (`TestTheADRDeclaresTheClassOfEveryProblem`); the harness template suite (`idempotency_replay_classes_test.go`) proves every class behaviour per entry, and the real-Postgres proofs of both RETRYABLE types live in `cmd/api/idempotency_classes_integration_test.go`.
