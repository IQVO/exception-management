# ADR 0008: Adopt the fleet REST API guideline

## Status

Accepted (2026-11-01). Applies the warehouse REST API guideline (nouns not
verbs, idempotent `POST`/`PATCH`, `409` key reuse, `Idempotency-Replayed`,
keyset paging) to this context. Builds on ADR 0001 (the REST surface and the
deliberate absence of `POST /exceptions`) and ADR 0007 (the read use cases the
MCP and A2A adapters share with REST).

## Context

The 2026-10-30 portfolio assessment found three command-URL styles across
the fleet, two spellings of the replay header, key reuse answered `422` in
most services and unpaginated lists. This context had verb sub-paths
(`/triage`, `/assign`, `/resolve`), a `POST /exceptions` that answered `405`
*before* the idempotency middleware (so it was the one unkeyed `POST`), and
two bare-array lists (`GET /exceptions`, `GET /exception-policies`). The
guideline is enforced by the Spectral `rest-guideline` rule
(`.spectral/functions/restGuideline.js`, rules R1-R8) in the `api-lint` job.

## Decision

Hard cutover, no aliases, no toggles, `operationId`s unchanged:

1. **Routes are nouns (R1, R2).** `POST /exceptions/{caseId}/triage` becomes
   `/triage-decisions`, `/assign` becomes `/assignments`, `/resolve` becomes
   `/resolutions`. Statuses and bodies do not change. `TestRetiredRoutesAreGone`
   (and its Postgres twin) pins the old paths at 404/405.
2. **Every `POST` is idempotent (R3-R6, R8).** `POST /exceptions` joins the
   route-scoped idempotency group: the key is demanded first (`400
   idempotency-key-required`), the permanent `405` is stored and replayed.
   The middleware adds `Idempotency-Replayed: true` on every replay (and only
   on a replay), answers `409 idempotency-key-reused` when the same key
   arrives with another method, path or body (the key is global, the
   fingerprint is method + path + SHA-256 of the body), and answers `400
   idempotency-key-invalid` for a key over 128 characters or not printable
   ASCII. A `4xx` is stored, a `5xx` is rolled back with the unit of work and
   never stored; concurrent same-key requests are serialised by the key's
   primary key. The `PUT /exception-policies/{sourceType}` stays keyed: each
   accepted declaration appends a version, so a retry without the key would
   not be idempotent.
3. **Lists are keyset pages (R7).** `GET /exceptions` (newest opened first,
   ordered by `(opened_at, case_id)` descending) and `GET /exception-policies`
   (by source type) answer `{items, nextCursor}` with `limit` (1-500, default
   100) and `cursor`. The cursor is the fleet `page` package's base64url JSON
   of the last row's sort key and id (copied unchanged to
   `internal/adapters/inbound/http/page`). Both repositories (Postgres and
   in-memory) implement `ListPage`; the shared `repocontract` suite walks the
   pages with rows sharing an `opened_at`. Invalid `limit` is `400
   invalid-limit`, an undecodable cursor `400 invalid-cursor`, an undeclared
   query parameter (`after`, `offset`, `nextToken`, ...) `400
   unknown-query-parameter`.
4. **MCP and A2A keep their tool schemas and complete data.** The shared use
   cases `ListOpenExceptions.Execute` and `ListExceptionPolicies.Execute` walk
   the keyset pages in-process (500 rows per page) and return every row; REST
   uses their `Page` methods.

### Exemptions

None. Both collections are paginated; no operation carries
`x-pagination-exempt`.

### Consumers changed

The exceptions web remote (`web/src/api.ts`, `web/src/types.ts` and their
tests) calls the new routes, reads `items` and follows `nextCursor`; every
`POST` already minted a fresh `Idempotency-Key` per submission. Outside this
repo, `e2e-tests` (`returns_test.go`) calls the renamed routes and is updated
in its own PR. No outbound adapter of this service calls a REST list.

## Consequences

- Breaking by design: new URLs, a new list envelope, a key now required on
  `POST /exceptions`. It deploys together with the REST cutover set.
- `docs/docs/api-reference/rest` is regenerated from `apis/openapi.yaml`.
