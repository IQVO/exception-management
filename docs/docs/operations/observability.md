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

`GET /metrics` (Prometheus text exposition) on the REST router is a **hand-written handler**
(`handleMetrics` in `internal/adapters/inbound/http/metrics.go`, routed in `server.go`). It writes a
constant string, so the process exposes exactly **one series**, the presence gauge:

```
# HELP exception_management_up The exception-management process is up.
# TYPE exception_management_up gauge
exception_management_up 1
```

No Prometheus client library is in `go.mod` and no other series is evidenced in the code: the case
counters by status/queue, escalation counts, outbox depth and consumer lag are **not built**. The chart
can switch on Kong's `prometheus` KongPlugin (`kongPlugins.prometheus`, default off in `values.yaml`),
which gives per-route request metrics at the gateway, not from this process. Until in-process metrics
exist, the operational truth is the SLA report read (`get_sla_report` over MCP,
[MCP tools](/docs/mcp/tools)), the analytics reports below and the logs.

## Health endpoints

| Endpoint | Answers | Backed by |
| --- | --- | --- |
| `GET /healthz` | `200 {"status":"ok"}` | the process being up — nothing else, by design |
| `GET /readyz` | `200 {"status":"ready"}` / `503 {"status":"not_ready"}` (draining) / `503` problem+json naming the failing dependencies | the drain gate + Postgres ping + Kafka broker metadata; cached 3 s, one round bounded at 2 s |
| `GET /metrics` | the exposition above | — |

`cmd/mcp` serves only `GET /healthz` (open) beside the MCP handler.

## What is deliberately absent

- **No auth logs** — there is no auth middleware to log (fleet decision 2026-09-11).
- **No in-process distributed tracing or OTel metrics** — no OpenTelemetry SDK setup is evidenced:
  `go.mod` lists `go.opentelemetry.io/otel` and `otel/sdk`, but non-test code imports only the OTel
  API, in `internal/adapters/inbound/a2a/skills.go` (one `a2a.skill <name>` span per skill call through
  the global tracer), and no `TracerProvider`, `MeterProvider` or exporter is created in `cmd/` or
  `internal/` (the SDK is imported only by `skills_test.go`). With no provider set those spans are
  not exported. The chart can switch on Kong's `opentelemetry` KongPlugin (`kongPlugins.opentelemetry`,
  default off) for gateway-level traces. Correlation across contexts is the CloudEvents `id` chain
  (source fact id frozen on the case, correlated event id on auto-resolve).
- **No dashboards in-repo** — not evidenced; the in-process metrics step above is the prerequisite.

## Analytics projector and reports

Built (ADR 0006), separate from the OLTP service and its database:

- `cmd/exceptions-projector` consumes the topic `warehouse.exception-management.analytics` under the
  required `ANALYTICS_CONSUMER_GROUP`. It reads seven types (`ExceptionOpened`, `ExceptionTriaged`,
  `ExceptionAssigned`, `ExceptionEscalated`, `ExceptionResolved`, `ExceptionAutoResolved`,
  `ExceptionPolicyDeclared`; `analyticsKinds` in `internal/adapters/inbound/kafka/analytics_consumer.go`)
  and acknowledges and ignores any other type. It is the only writer of the **analytical** database
  (`ANALYTICS_DATABASE_URL`, separate from the OLTP one): at boot it applies the embedded migration
  `analytics/migrations/0001_exception_analytics`, then in one transaction per event it records the
  CloudEvents id in `analytics_processed_events` (a replay is a no-op) and upserts `exception_case_facts`
  (one row per case, earliest time per milestone) or `exception_policy_facts` (one row per source type
  and policy version). An event the database can never accept is rejected for dead-lettering instead of
  retried forever. It serves only `GET /healthz` and `GET /readyz` on `ADMIN_ADDR` (`:8091`), no `/metrics`.
- `cmd/exceptions-reports` is the read-only reader over a read-only pool: `GET /reports/root-cause-frequency`,
  `/reports/mean-time-to-resolve`, `/reports/sla-breach-rate`, `/reports/freshness` and `/healthz` on `:8092`.
  It never opens the OLTP database or Kafka.

What is **not** evidenced: any consumer of `warehouse.exception-management.analytics` other than this
projector, and any metric or trace emitted by the projector or the reports reader.

## The SLA report

The one aggregate the domain itself serves: at `generated_at`, the open caseload against its
clocks — `open_cases`, `breached` (open past `sla_due_at`), `escalated` (flagged by the sweeper)
and the open counts `by_severity` / `by_queue`. Served by the `get_sla_report` MCP tool; there is
no REST path.
