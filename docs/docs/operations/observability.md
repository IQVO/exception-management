---
id: observability
title: Observability
sidebar_label: Observability
sidebar_position: 3
---

# Observability

What signals exist today, and what deliberately does not.

## Logs

Structured JSON (`slog`) on stdout, one line per event, level from `LOG_LEVEL`. The lines that
matter to an operator:

- Boot: configuration problems are reported all at once, each naming its variable.
- `readiness: dependency is not ready` (WARN) with the dependency name — what `/readyz` is failing
  on, seen in the logs before it is seen on the probe.
- Consumer skips: an invalid CloudEvent is WARN-logged and the offset committed past; a fact for an
  **undeclared source type** is WARN-logged (`no policy declared for the source type`) and
  committed past — that log line is the signal of a missing policy, not an incident.
- Relay and sweeper: attempt failures, DLQ writes, sweep escalations.

## Metrics

`GET /metrics` (Prometheus text exposition) currently carries the process presence gauge:

```
# HELP exception_management_up The exception-management process is up.
# TYPE exception_management_up gauge
exception_management_up 1
```

The full registry (case counters by status/queue, escalation counts, outbox depth, consumer lag)
is a planned, fleet-wide instrumentation step — **roadmap, not built**: no prometheus client is in
the module yet. Until then, the operational truth is the SLA report read (`get_sla_report` over
MCP, [MCP tools](/docs/mcp/tools)) and the logs.

## Health endpoints

| Endpoint | Answers | Backed by |
| --- | --- | --- |
| `GET /healthz` | `200 {"status":"ok"}` | the process being up — nothing else, by design |
| `GET /readyz` | `200 {"status":"ready"}` / `503 {"status":"not_ready"}` (draining) / `503` problem+json naming the failing dependencies | the drain gate + Postgres ping + Kafka broker metadata; cached 3 s, one round bounded at 2 s |
| `GET /metrics` | the exposition above | — |

`cmd/mcp` serves only `GET /healthz` (open) beside the MCP handler.

## What is deliberately absent

- **No auth logs** — there is no auth middleware to log (fleet decision 2026-09-11).
- **No distributed tracing** — no OTel SDK is wired; correlation across contexts is the CloudEvents
  `id` chain (source fact id frozen on the case, correlated event id on auto-resolve).
- **No analytics projector** — ADR 0004 declares a `warehouse.exception-management.analytics`
  mirror of every published type, but the projector is **roadmap, not built**; no consumer exists.
- **No dashboards in-repo** — the fleet-wide metrics step above is the prerequisite.

## The SLA report

The one aggregate the domain itself serves: at `generated_at`, the open caseload against its
clocks — `open_cases`, `breached` (open past `sla_due_at`), `escalated` (flagged by the sweeper)
and the open counts `by_severity` / `by_queue`. Served by the `get_sla_report` MCP tool; there is
no REST path.
