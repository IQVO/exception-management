---
id: tools
title: MCP tools
sidebar_label: MCP tools
sidebar_position: 1
---

# MCP tools

`cmd/mcp` serves a **read-only** Model Context Protocol surface over the same read use cases the
REST adapter uses (`ListOpenExceptions`, `GetException`, `ListExceptionPolicies`,
`GetSLAReport`). It is a second, independent deployable (Helm chart component `mcp`, **disabled by
default**), so an agent host can answer questions about the caseload without touching the write
path.

| | |
| --- | --- |
| Transport | Streamable HTTP at `MCP_ADDR` (default `:8090`), paths `/` and `/mcp` (the ops-agent's `*_MCP_ENDPOINT` convention); no stdio, no SSE |
| Health | `GET /healthz` open (JSON status) |
| Auth | none — fleet decision 2026-09-11; access control is the in-cluster ClusterIP boundary |
| Writes | impossible: no write use case is wired, so this binary can never insert into the outbox; it also never starts the relay, never runs the consumers or the SLA sweeper, never dials Kafka |
| Runtime | real Postgres only (`DATABASE_URL` required; no in-memory mode) |
| Env | `DATABASE_URL`, `MIGRATIONS_DATABASE_URL` (direct DSN for boot migrations; default `DATABASE_URL`), `MCP_ADDR` (`:8090`), `LOG_LEVEL` |

## Tools

Every tool is annotated read-only, idempotent and closed-world. `TestToolSurface`
(`internal/adapters/inbound/mcp/governance_test.go`) fails the build on any write-verb tool name,
and the exact registry is pinned by a golden file
(`internal/adapters/inbound/mcp/testdata/tool_registry.golden.json`).

### `list_open_exceptions`

List the exception cases, newest opened first, narrowed by the optional filters. Each case carries
its dedup identity (`source_type`, `source_subject`, `site_id`), `status` (`OPEN`, `TRIAGED`,
`ASSIGNED`, `RESOLVED`), `severity`, `queue`, `occurrence_count` (how often the same problem
happened), the `escalated` flag, `assignee` when one exists, the SLA clock (`sla_minutes`,
`sla_due_at`), the frozen `source_payload` of the opening fact and the `audit` trail.

- Input: `status` (optional), `queue` (optional), `site_id` (optional), `source_type` (optional) —
  omitted means no narrowing; `status` omitted means every status, resolve activity is part of the
  record
- Errors: `invalid-status` (unknown status value)

### `get_exception`

Read one case by id: everything `list_open_exceptions` carries per case, plus the resolution
(`resolution_code`, `root_cause`, `note`, `resolved_at`) once resolved — manual or auto, the latter
also carrying `auto_resolve_rule_id`, `correlated_type` and `correlated_event_id`.

- Input: `case_id` (string, must start with `case-`, e.g. `case-1b4e28ba`)
- Errors: `invalid-case-id` (blank), `case-not-found` (unknown id)

### `get_exception_policies`

List the exception policy declared for every source type, ordered by source type: the latest
`version`, the `severity` and `queue` a new case of that type opens with, `sla_minutes`, and
`auto_resolve` — `null` when the type has no rule, else `{on_type, within_minutes}` (the correlated
CloudEvents type and the window within which it auto-resolves the case).

- Input: none

### `get_sla_report`

Read how the open caseload sits against its SLA clocks at `generated_at`: `open_cases`, `breached`
(open past their `sla_due_at`), `escalated` (cases the SLA sweeper flagged) and the open counts
`by_severity` and `by_queue`.

- Input: none

A handler error becomes an `isError` tool result carrying the error text — never a transport
failure.

## Example call

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "get_sla_report",
    "arguments": {}
  }
}
```

```json
{
  "generated_at": "2026-10-21T09:30:00Z",
  "open_cases": 12,
  "breached": 2,
  "escalated": 2,
  "by_severity": {"LOW": 1, "MEDIUM": 6, "HIGH": 4, "CRITICAL": 1},
  "by_queue": {"cpt-missed": 7, "count-discrepancy": 3, "network-ack": 2}
}
```

## Who consumes this

`warehouse-ops-agent` is the intended consumer (its wiring is **roadmap, not built yet** — no
`*_MCP_ENDPOINT` for this service exists on the agent today). Writes (triage, assign, resolve,
declare policy) stay on REST and have no MCP path by design — the agent stays zero-write.

Source: `cmd/mcp/main.go`, `cmd/mcp/router.go`,
`internal/adapters/inbound/mcp/{server,tools}.go`.
