---
id: overview
title: API overview
sidebar_label: API overview
slug: /api-reference
sidebar_position: 1
---

# API overview

| Interface | Source of truth | Notes |
| --- | --- | --- |
| REST | `apis/openapi.yaml` (OpenAPI 3.0.3, version 1.0.0) | The pages under *REST* are generated from the spec with `npm run gen-api-docs`; do not edit them by hand. Servers declared in the spec: `http://localhost:8080` (local) and `http://localhost:8000/api/exception-management` (Kong in the kind cluster). |
| Events | `apis/asyncapi.yaml` | Summarised on the [event catalogue](/docs/api-reference/events); every field on [Domain events](/docs/ddd/domain-events). |
| MCP | `internal/adapters/inbound/mcp` | `cmd/mcp`, Streamable HTTP on `:8090` at `/mcp`: read-only tools `list_open_exceptions`, `get_exception`, `get_exception_policies`, `get_sla_report`. Writes stay on REST. See [MCP tools](/docs/mcp/tools). |

## Endpoints

| Tag | Endpoint | Use case | Success |
| --- | --- | --- | --- |
| Exceptions | `GET /exceptions` | `ListOpenExceptions` | `200`, one keyset page `{items, nextCursor}`, never 404 on a list |
| Exceptions | `POST /exceptions` | — | **405 by design**: every case is a consumed fact (ADR 0001), and the 405 body says so; keyed like every `POST` |
| Exceptions | `GET /exceptions/{caseId}` | `GetException` | `200`, the case |
| Exceptions | `POST /exceptions/{caseId}/triage-decisions` | `TriageException` | `200`, body `{severity, queue}` |
| Exceptions | `POST /exceptions/{caseId}/assignments` | `AssignException` | `200`, body `{assignee}` |
| Exceptions | `POST /exceptions/{caseId}/resolutions` | `ResolveException` | `200`, body `{resolutionCode, rootCause, note?}`; `409 case-already-resolved` when terminal |
| Exception policies | `GET /exception-policies` | `ListExceptionPolicies` | `200`, one keyset page `{items, nextCursor}` of the latest version per source type |
| Exception policies | `PUT /exception-policies/{sourceType}` | `DeclareExceptionPolicy` | `201`, the new policy version |
| Health | `GET /healthz`, `GET /readyz`, `GET /metrics` | — | `200` / `503` ([Runbook](/docs/operations/runbook)) |

`GET /exceptions` filters: `status` (`OPEN`, `TRIAGED`, `ASSIGNED`, `RESOLVED`; omitted = every status,
open and resolved alike — resolve activity is part of the record), `queue`, `site`, `sourceType`.
Both lists take `limit` (1–500, default 100) and `cursor` (the previous page's `nextCursor`, absent on
the first page); repeat the filters on every page request.

## Conventions

- **Bodies.** Strict JSON: unknown fields, trailing data and a malformed body are `400
  malformed-request-body`. JSON names are camelCase on REST and snake_case in event `data`.
- **`Idempotency-Key`** is required on every `POST` (the three case commands and the keyed `405` of
  `POST /exceptions`) and on `PUT /exception-policies/{sourceType}`. A missing key is
  `400 idempotency-key-required` (`400 idempotency-key-invalid` when it is over 128 characters or not
  printable ASCII); the same key and the same request (method, path, body) replays the stored answer
  (4xx included) with `Idempotency-Replayed: true`; the same key with a different request is
  `409 idempotency-key-reused`. A 5xx is never stored.
- **Pagination.** `limit` outside 1–500 is `400 invalid-limit`, an undecodable `cursor` is
  `400 invalid-cursor` and an undeclared query parameter (`after`, `offset`, …) is
  `400 unknown-query-parameter`.
- **Errors** are RFC 7807 `application/problem+json` with
  `type = https://errors.exception-management.warehouse-systems.dev/<slug>`. The slugs are
  `malformed-request-body`, `invalid-case-id`, `invalid-source-type`, `invalid-status`,
  `invalid-severity`, `invalid-queue`, `invalid-assignee`, `invalid-resolution`, `invalid-policy`,
  `case-not-found`, `case-already-resolved`, `concurrent-modification`, `not-found`,
  `method-not-allowed`, `idempotency-key-required`, `idempotency-key-invalid`,
  `idempotency-key-reused`, `invalid-limit`, `invalid-cursor`, `unknown-query-parameter`,
  `dependency-unavailable` and `internal-error`. Causes and fixes are on
  [Troubleshooting](/docs/operations/troubleshooting).
- **NUL guard.** A NUL byte in a path is 404, in a body `400 malformed-request-body` (Postgres
  rejects U+0000).
- **Auth**: none. The fleet removed REST and MCP bearer auth on 2026-09-11 and
  `TestNoAuthMiddlewareReintroduced` fails CI if auth middleware returns. The router installs no
  CORS middleware.
- **No case creation.** There is deliberately no `POST /exceptions` that opens a case: every case
  starts from a consumed fact (ADR 0001), and the spec pins the 405 so the decision is visible on
  the wire.
