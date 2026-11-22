---
id: observability
title: Observability
sidebar_label: Observability
sidebar_position: 3
---

# Observability

What signals exist, how to read them, and what deliberately does not. The service follows the fleet
observability standard ([ADR 0014](/docs/adr/0014-adopt-the-fleet-observability-standard)): every
binary pushes traces and metrics over OTLP/gRPC to the Collector, and the logs carry the trace ids.

## Where the signals go

All five binaries (`cmd/api`, `cmd/mcp`, `cmd/a2a`, `cmd/exceptions-projector`,
`cmd/exceptions-reports`) call `telemetry.Setup` at boot
(`internal/adapters/outbound/telemetry`). It installs a tracer provider and a meter provider that
export over OTLP/gRPC to `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`; the chart sets the
in-cluster Collector), starts the Go runtime metrics, and installs the W3C trace-context propagator.
Export is lazy and non-blocking: a Collector that is down or absent costs telemetry, never boot or
availability. The variables are in [Configuration](/docs/operations/configuration); the service
names are `exception-management` (api), `exception-management-mcp`, `exception-management-a2a`,
`exception-management-projector` and `exception-management-reports`, with `deployment.environment.name`
from `ENVIRONMENT` and `service.version` from `SERVICE_VERSION`.

## Logs

Structured JSON (`slog`) on stdout, one line per event, level from `LOG_LEVEL`. A line written with a
context that carries a span also carries `trace_id` and `span_id`: the request lines and the lines
the REST handlers write while serving a request. Lines written without one (boot, consumers, relay,
sweeper, projector) carry neither. The lines that matter to an operator:

- Request: one `http request` line per request on the REST, reports, MCP and A2A routers with
  `method`, `route` (the matched chi pattern, never the raw path), `status`, `duration_ms`, `bytes` and
  `request_id`; INFO below 400, WARN for 4xx, ERROR for 5xx. `/healthz` and `/readyz` are not logged.
- Boot: configuration problems are reported all at once, each naming its variable; `telemetry
  configured` names the service and the OTLP endpoint.
- `readiness: dependency is not ready` (WARN) with the dependency name — what `/readyz` is failing
  on, seen in the logs before it is seen on the probe.
- Consumer skips: an invalid CloudEvent is WARN-logged and the offset committed past; a fact for an
  **undeclared source type** is WARN-logged (`no policy declared for the source type`) and
  committed past — that log line is the signal of a missing policy, not an incident.
- Relay and sweeper: attempt failures, DLQ writes, sweep escalations.

## Traces

`otelchi` starts one server span per request on the REST, reports, MCP and A2A routers, named after the
route pattern (`/exceptions/{caseId}`, not `/exceptions/case-8a1f`). An incoming `traceparent` is
honoured. The A2A adapter adds one `a2a.skill <name>` span per skill call
(`internal/adapters/inbound/a2a/skills.go`), which the A2A binary now exports. The consumers, the relay
and the projector start no span. The trace does **not** continue across Kafka: see below.

## Metrics

OpenTelemetry metrics, pushed over OTLP (they are not served by this process). The Prometheus names
are what the Collector's Prometheus exporter is expected to publish (dots become underscores,
counters gain `_total`, a unit of `s` becomes `_seconds`); the Collector configuration lives in
warehouse-infra and was not read for this page.

| Instrument | Type, unit | Attributes | Emitted by | Expected Prometheus series |
| --- | --- | --- | --- | --- |
| `http.server.request.duration` | histogram, `s` | `http.method`, `http.scheme`, `http.route` | api, reports, mcp, a2a | `http_server_request_duration_seconds_bucket`, `_sum`, `_count` |
| `exception.cases.opened` | counter, `{case}` | `outcome`, `exception.severity`, `reason` | api | `exception_cases_opened_total` |
| Go runtime metrics | per `contrib/instrumentation/runtime` | — | all five binaries | `go_*` |

`exception.cases.opened` counts every consumed fact the open-or-touch decision handles
(`OpenOrTouchException`, the only path that opens a case):

| `outcome` | Meaning | Other attributes |
| --- | --- | --- |
| `opened` | no case owned the dedup key; a new one opened | `exception.severity` of the case |
| `touched` | a case owned the key; its occurrence went up | `exception.severity` |
| `replayed` | the opening event of the owning case again; the idempotent no-op | `exception.severity` |
| `rejected` | nothing opened or touched | `reason`: `policy-undeclared`, `invalid-fact`, `concurrent-modification`, `open-case-exists` or `internal-error` |

A rising `policy-undeclared` means a producer emits a source type nobody declared a policy for (a
configuration gap, the same condition as the consumer WARN above); a rising `internal-error` or
`concurrent-modification` means the service itself is failing to take facts in. The counter measures
decisions, not commits: a decision inside a unit of work that later fails to commit is counted again
when the message is redelivered.

`GET /metrics` on the REST router is unchanged and unrelated: a **hand-written handler**
(`handleMetrics` in `internal/adapters/inbound/http/metrics.go`) that writes a constant string, so it
still exposes exactly one series, the presence gauge `exception_management_up`. The chart can switch on
Kong's `prometheus` KongPlugin (`kongPlugins.prometheus`, default off), which gives per-route request
metrics at the gateway.

## Health endpoints

| Endpoint | Answers | Backed by |
| --- | --- | --- |
| `GET /healthz` | `200 {"status":"ok"}` | the process being up — nothing else, by design |
| `GET /readyz` | `200 {"status":"ready"}` / `503 {"status":"not_ready"}` (draining) / `503` problem+json naming the failing dependencies | the drain gate + Postgres ping + Kafka broker metadata; cached 3 s, one round bounded at 2 s |
| `GET /metrics` | the presence gauge above | — |

`cmd/mcp` and `cmd/a2a` serve `GET /healthz` (open) beside their handlers; the request-duration
histogram still measures it, the request log skips it.

## What is deliberately absent

- **No auth logs** — there is no auth middleware to log (fleet decision 2026-09-11).
- **No trace propagation across Kafka.** The outbox table has no headers column and a change to carry
  the trace context would need a schema migration, which ADR 0014 declines. A trace ends at the outbox
  write; correlation across contexts is the CloudEvents `id` chain (source fact id frozen on the case,
  correlated event id on auto-resolve).
- **No business counters beyond `exception.cases.opened`.** Case counts by status or queue, escalation
  counts, outbox depth and consumer lag are not evidenced in the code.
- **No dashboards in-repo.** The metric names above are what a dashboard is built from; the
  dashboards belong to warehouse-infra.

## Analytics projector and reports

Built (ADR 0013), separate from the OLTP service and its database:

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
  retried forever. It serves only `GET /healthz` and `GET /readyz` on `ADMIN_ADDR` (`:8091`), no
  `/metrics`, and exports the Go runtime metrics and trace-correlated logs, no request metrics.
- `cmd/exceptions-reports` is the read-only reader over a read-only pool: `GET /reports/root-cause-frequency`,
  `/reports/mean-time-to-resolve`, `/reports/sla-breach-rate`, `/reports/freshness` and `/healthz` on `:8092`.
  It never opens the OLTP database or Kafka, and emits the request-duration histogram and server spans
  like the other HTTP binaries.

What is **not** evidenced: any consumer of `warehouse.exception-management.analytics` other than this
projector, and any projector-specific metric (events projected, projection lag beyond
`/reports/freshness`).

## The SLA report

The one aggregate the domain itself serves: at `generated_at`, the open caseload against its
clocks — `open_cases`, `breached` (open past `sla_due_at`), `escalated` (flagged by the sweeper)
and the open counts `by_severity` / `by_queue`. Served by the `get_sla_report` MCP tool; there is
no REST path.
