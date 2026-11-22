# ADR 0015: every problem type has a replay class; a retryable refusal is not stored

- Status: Accepted (2026-11-20); amended 2026-11-21 (in-flight duplicates re-read the key before an unstored refusal); amended 2026-11-22 (a 24 h TTL and an hourly purge — the retention open point, order-sourcing #84 / last-mile-delivery #86)
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
- A concurrent duplicate under the same key sees nothing claimed: both run in parallel units of work. The duplicate that finishes 2xx (or a storable 4xx) first wins the key row. Before answering an unstored refusal (RETRYABLE, unclassified or 5xx) the middleware **re-reads the key**: a found answer is replayed (another body: `idempotency-key-reused`), so N in-flight copies are one run and N-1 replays (see the amendment below).
- RELEASED means the same key may be tried again by whoever sent it. No consumer-side retry behaviour is added here: callers driven by people must surface the refusal; automation retrying is a separate PR.

## Store design (verified from code, 2026-11-20)

Port-based (`ports.IdempotencyStore`: `Lookup`, `Save`), no savepoint, a 4xx stored separately:

- `Idempotency.Middleware` wraps the POSTs and the PUT (`server.go`'s idempotency group).
- `Store.Lookup` first: a hit with the same fingerprint replays (byte-identical, `Idempotency-Replayed: true`); a hit with another fingerprint is `409 idempotency-key-reused`; nothing runs.
- A new key runs the handler INSIDE `UoW.Do` (one Postgres transaction; the repositories join it through `pgtx`):
  - 2xx: `Save` + re-`Lookup` in the same transaction; a lost race (`errLostRace`) rolls back and replays the winner.
  - any ≥ 400: `errRollback` — the unit of work (the handler's writes) is rolled back; the answer is then stored on its own by `finishRejected` (best effort, separate `Save`) only when its class is FINAL/OUTCOME. A 5xx, a RETRYABLE or an unclassified answer is not stored: the key is released exactly like the 5xx branch. Before flushing it, `finishRejected` re-reads the key (`Store.Lookup`, outside the rolled-back transaction) and replays a found answer instead; a failed re-read is logged at WARN and the request's own refusal goes out.
- Stores: `postgres/idempotency_store.go` (`SELECT` by key, no age predicate; `INSERT ... ON CONFLICT (key) DO NOTHING`) and the `memory` double used by the middleware's unit tests. The release decision lives in the middleware, so both stores honour it unchanged.
- **TTL/retention: none.** No age predicate on `Lookup`, no purge of `idempotency_keys` anywhere in the codebase — rows live forever. Recorded as an open point (fleet rule: a 24-hour window should be enforced by a purge and an age check); the fix shape is order-sourcing #84 / last-mile-delivery #86. Not fixed in this PR.

## Consequences

- The two RETRYABLE problem types (`concurrent-modification`, `case-not-found`) stop being replayed as stored refusals; a caller that retries the same key now executes again. No consumer of this service sends a deterministic key (verified: no outbound HTTP client exists in this service — CLAUDE.md rule 6, "frozen local copies, no live lookup"), so no consumer change is required; callers of a person-driven surface must surface the refusal.
- `apis/openapi.yaml` descriptions now say which refusals replay; `docs/docs/api-reference/rest` regenerated.
- A test holds this ADR's table equal to the catalogue (`TestTheADRDeclaresTheClassOfEveryProblem`); the harness template suite (`idempotency_replay_classes_test.go`) proves every class behaviour per entry, and the real-Postgres proofs of both RETRYABLE types live in `cmd/api/idempotency_classes_integration_test.go`.

## Amendment (2026-11-21): an in-flight duplicate replays, it does not re-run

Fleet audit 2026-11-21 (warehouse-harness-template `docs/idempotency-replay-classes.md`, "In-flight duplicates"). The key `Lookup` runs before the unit of work and the case row lock is taken INSIDE the use case, by the version-guarded upsert of `postgres/case_repository.go` (`ON CONFLICT ... WHERE exception_cases.version = excluded.version - 1`). A same-key, same-body copy of a triage, assignment or resolution that arrived while the original held the row passed the `Lookup`, loaded the case at the old version, queued on the row lock and, once the original committed its change and its key row, matched 0 rows: `409 concurrent-modification`, RETRYABLE, not stored. The client got a refusal for a command that had succeeded (reproduced over real Postgres through the router: `cmd/api` `TestAnInFlightDuplicateTriageReplaysTheOriginalOverPostgres`, which parks the original after its Save, waits for the duplicate's `pg_stat_activity` Lock wait and releases).

**Decision.** `finishRejected` re-reads the key before answering any unstored refusal (RETRYABLE, unclassified or 5xx):

- found, same fingerprint: the stored answer is replayed with `Idempotency-Replayed: true`;
- found, another fingerprint: `409 idempotency-key-reused`;
- not found: the request's own refusal goes out and the key stays released (the ordinary RETRYABLE path);
- the re-read fails: logged at WARN (`idempotency: re-read before an unstored refusal failed`) and the request's own refusal goes out.

Re-reading there is equivalent to re-reading under the lock: the refusal already rolled back every write, and the re-read runs after the duplicate's transaction ended, so it sees the original's committed key row. A re-read at the START of the unit of work would not do: the unit takes no lock at begin, so under READ COMMITTED that read runs before the original commits and finds nothing.

The re-read is the same `Lookup` as the up-front check, so it honours the same replay window. This store has none yet (the TTL/retention open point above): an answer 24 h old and one a microsecond older both replay through either path, and `TestTheReReadBeforeAnUnstoredRefusalAgreesWithTheUpFrontLookupAt24Hours` pins that the two paths agree. When the retention fix adds an age bound to `Lookup`, both paths get it at once.

Tests: `internal/adapters/inbound/http/idempotency_inflight_test.go` (replay, 5xx, other body, the 24 h agreement, the WARN), the harness template's `TestConcurrentDuplicateReplaysTheOriginalsStoredAnswer` re-rendered into `idempotency_replay_classes_test.go`, and the Postgres probe above.

## Amendment (2026-11-22): a 24-hour TTL and an hourly purge — a stored answer stops living forever

This resolves the open point of the store design above ("TTL/retention: none") with the fleet pattern already merged in order-sourcing (#84) and last-mile-delivery (#86). **This is a behaviour change, the bigger one of the wave: until today a stored answer replayed FOREVER; from this change a key is remembered for 24 hours.**

**Decision.**

- **One TTL constant**, `repository.IdempotencyTTL = 24h` (the fleet default), shared by every reader and writer of the window — the age guard, the Save refresh and the purge — so the read check, the write and the sweep can never disagree about which rows are live.
- **Age guard on the replay read.** `Lookup` selects `created_at > now() - TTL`, so a row older than the TTL is invisible: it can never answer a replay between expiry and the sweep. Every reader of the port shares it — the up-front check, the post-save re-read and the `finishRejected` re-read (the 2026-11-21 amendment predicted this: "both paths get it at once"; `TestTheReReadBeforeAnUnstoredRefusalAgreesWithTheUpFrontLookupAt24Hours` now pins agreement in BOTH directions: inside the window both replay, past it both execute).
- **Save refreshes an expired row.** `ON CONFLICT (key) DO UPDATE ... WHERE created_at <= now() - TTL`, with `created_at` set to the NEW answer's stamp. First-writer-wins is unchanged WITHIN the window (a live row is never overwritten; a concurrent loser still replays the winner); past it the key is free again, and the same key and body EXECUTES a second time while a different body under it becomes a NEW request instead of `409 idempotency-key-reused`.
- **Bounded background purge.** `PurgeExpired(ctx, limit)` joins `ports.IdempotencyStore` (implemented by the Postgres store and the memory double; the port contract grew `repocontract.IdempotencyRetention`). `cmd/api` runs it at start and then hourly, in batches of 500: `ORDER BY created_at LIMIT n FOR UPDATE SKIP LOCKED`, the age test repeated on the DELETE itself, no advisory lock, and it never waits on a row a request holds — a row whose lock it cannot take is skipped. A purge that deletes an expired row just before a Save only turns the refresh into a plain INSERT; an expired row is invisible to `Lookup`, so deleting it changes nothing a request can observe. Like the SLA sweep the worker only ever logs: a failed pass is retried by the next tick.

**The replay-class premise, re-examined.** The Decision table's rationale for `idempotency-key-reused` (FINAL) was "a stored answer never changes (no TTL, no purge), so the conflict never clears by retrying." The TTL removes the "no TTL, no purge" half, so the premise is re-stated: WITHIN the 24-hour window the stored answer is still immutable and the conflict still cannot clear by retrying — FINAL stands. PAST the window the row is gone and the key is free: a retry is not a "cleared conflict" but a first execution (and the 409 itself is never stored — `replay()` answers it before any storage). No other class changes: a FINAL answer is stored and replayed for 24 h (previously forever), a RETRYABLE answer is still never stored, a 5xx still releases the key.

**Consequences.**

- A caller must treat the key as unique per logical request for 24 hours (a UUID per submit: no practical collision). After 24 h the same key and body executes again — its effects run a second time — and the same key with a different request is no longer a 409.
- `idempotency_keys` stops growing: the hourly purge deletes every row the read guard already ignores, oldest first, bounded per statement.
- `apis/openapi.yaml` descriptions now state the 24-hour window (the parameter, the info block, the 409 problem); `docs/docs/api-reference/rest` regenerated.
- Tests: the Postgres proofs in `cmd/api/idempotency_retention_integration_test.go` (an expired key executes again, the purge deletes expired keeps live, one batch per pass, a held row is skipped not waited for, requests racing the purge), the contract suite `repocontract.IdempotencyRetention` on both stores, the middleware unit tests in `internal/adapters/inbound/http/idempotency_retention_test.go`, and the worker loop tests in `cmd/api/retention_test.go`.
