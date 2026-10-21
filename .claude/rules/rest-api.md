---
paths:
  - "internal/adapters/inbound/http/**"
  - "apis/openapi*.yaml"
  - "apis/openapi/**"
---

# REST API (inbound adapter)

Route -> use case, kept in sync with `apis/openapi.yaml`.

- `GET  /exceptions?status=&queue=&site=&sourceType=` -> `ListOpenExceptions` (200 + `[]`, never 404 on a list)
- `GET  /exceptions/{caseId}`                          -> `GetException`
- `POST /exceptions/{caseId}/triage`                   -> `TriageException` (200; body `{severity, queue}`)
- `POST /exceptions/{caseId}/assign`                   -> `AssignException` (200; body `{assignee}`)
- `POST /exceptions/{caseId}/resolve`                  -> `ResolveException` (200; body `{resolutionCode, rootCause, note}`; 409 `case-already-resolved`)
- `PUT  /exception-policies/{sourceType}`              -> `DeclareExceptionPolicy` (201)
- `GET  /exception-policies`                           -> reads
- `GET  /healthz`, `GET /readyz` (drain gate + Postgres ping + Kafka metadata), `GET /metrics`

There is deliberately no `POST /exceptions`: every case comes from a consumed fact (ADR 0001).

## Conventions

- Error shape: RFC 7807 `application/problem+json`; problem types are kebab-case and listed in the OpenAPI spec.
- Every POST/PUT requires an `Idempotency-Key` header (middleware before the handler; a replay returns the stored response).
- A NUL byte in a path is 404, in a body 400 `malformed-request-body` (Postgres rejects U+0000).
- Auth: none. The fleet-wide REST+MCP bearer layer was reverted 2026-09-11; `TestNoAuthMiddlewareReintroduced` keeps it that way.
- No handler calls a sibling context over REST or MCP.
