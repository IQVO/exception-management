# ADR 0014: adopt the fleet observability standard

## Status

Accepted (2026-11-14).

## Context

Until this ADR the service had logs and health endpoints and nothing else (the
old `docs/docs/operations/observability.md` said so). Counted from the tree
before this change:

- `go.opentelemetry.io/otel` and `otel/sdk` were direct dependencies, but no
  `TracerProvider`, `MeterProvider` or exporter was created anywhere in `cmd/`
  or `internal/`; non-test code imported only the OTel API, in
  `internal/adapters/inbound/a2a/skills.go` (one `a2a.skill <name>` span per
  skill call, which no provider exported).
- No HTTP request metrics, no request log, no trace-correlated log line, no
  business counter. `GET /metrics` is a hand-written handler that writes one
  constant gauge (`exception_management_up`).
- The chart had no `otel.*` values and rendered none of the OpenTelemetry
  variables.

The fleet has a written standard for this: Tier 1 (identical everywhere:
`telemetry.Setup` to the Collector over OTLP/gRPC with runtime metrics, the
middleware order `RequestID`, `otelchi`, `otelchimetric`, `RequestLogger`,
`Recoverer`, `/healthz`) and Tier 2 (a naming and attribute convention for
business counters). The references are inventory-storage's ADR 0016 (the
WMS reference implementation) and wes-work-planning's ADR 0013, read from
their `origin/develop`. This service was the open gap.

## Decision

Adopt the standard, with these local choices.

1. **One `telemetry` adapter**, `internal/adapters/outbound/telemetry`: `Setup`
   (`telemetry.go`) and the trace-correlating slog handler `WithTraceContext`
   (`slogotel.go`) are copied verbatim from inventory-storage, tests included.
   An empty OTLP endpoint falls back to `DefaultEndpoint` (`localhost:4317`) and
   still installs real providers; export is lazy and non-blocking, so a missing
   Collector costs telemetry, never availability or boot. Nothing under
   `internal/domain` or `internal/application` imports the adapter.
2. **One `cmd/*/telemetry.go` per binary**, for every one of the five binaries
   in `cmd/`: `api`, `mcp`, `a2a`, `exceptions-projector`, `exceptions-reports`.
   There is no separate relay binary: the outbox relay runs inside `cmd/api`.
   Each reads `OTEL_SERVICE_NAME` (default per binary: `exception-management`,
   `-mcp`, `-a2a`, `-projector`, `-reports`), `OTEL_EXPORTER_OTLP_ENDPOINT`,
   `SERVICE_VERSION`, and `Setup` reads `ENVIRONMENT`. The loggers wrap their
   JSON handler with `WithTraceContext`.
3. **HTTP middleware, in the standard order**, on the REST router, the reports
   router and the `cmd/mcp` and `cmd/a2a` routers: `RequestID`, `otelchi`
   (server span named after the chi route pattern, never the raw path),
   `otelchimetric.NewServerRequestDuration` (`http.server.request.duration`,
   labelled with `http.route`), `RequestLogger`, then the panic recoverer. The
   REST and reports routers keep their own `recoverPanic` in the recoverer slot,
   because it answers the RFC 7807 `internal-error` problem the contract
   documents; the MCP and A2A routers use chi's `Recoverer`. `/healthz` is
   unchanged. `RequestLogger` lives in its own package,
   `internal/adapters/inbound/requestlog`, so that `cmd/mcp` never imports a path
   containing `inbound/http` (the fleet's MCP read-only rule; this repository's
   own fitness tests constrain `internal/adapters/inbound/mcp` and do not check
   `cmd/mcp` imports, so the package split is the guard). It logs
   the route pattern, not the raw path, and skips `/healthz` and `/readyz`. The
   projector is not an HTTP server of business traffic (its admin port serves
   probes only), so it gets `Setup` and trace-correlated logs, no middleware.
4. **Tier 2: one counter, on `OpenOrTouchException`.** That use case is the
   aggregate's primary command: every case opens from a consumed fact
   ([ADR 0001](./0001-exception-management-bounded-context.md); `POST /exceptions`
   is a permanent 405), and `OpenOrTouchException` is the only path that opens
   one. Triage, assignment and resolution act on a case that already exists, so
   they are secondary. The port is `ports.CaseMetrics`; the implementation is
   `internal/adapters/outbound/telemetry.CaseMetrics`; the use case calls it
   once per `Execute` and a use case built without one records nothing (nil-safe
   no-op default).
   - Instrument: `exception.cases.opened`, a counter, unit `{case}`. It follows
     `<context>.<noun>.<verb>`; the Prometheus series is expected to be
     `exception_cases_opened_total`.
   - Attributes: `outcome` is `opened` (a new case), `touched` (an existing
     case's occurrence went up), `replayed` (the opening event again, the
     idempotent no-op) or `rejected`. `exception.severity` (`LOW`, `MEDIUM`,
     `HIGH`, `CRITICAL`) is set on every decided case. `reason` is set only on
     `rejected`, and is the problem slug: `policy-undeclared`, `invalid-fact`,
     `concurrent-modification` (the slug of the REST catalogue),
     `open-case-exists`, `internal-error` (also the REST slug). The consumer
     path has no HTTP problem catalogue of its own, so the slugs it needs beyond
     the REST ones are named here.
   - The attribute set is bounded: nothing identifying a case, a subject or a
     source type is an attribute.
   - What is counted is the decision, at the end of `Execute`. When the call runs
     inside an enclosing unit of work (the consumers wrap it in `ConsumeFact`)
     that later fails to commit, the decision was already counted and its
     redelivery is counted again. The counter measures decisions taken, not
     commits.
5. **Chart.** `otel.enabled`, `otel.endpoint`, `otel.serviceName` and
   `otel.serviceVersion`, plus `environment`, render `ENVIRONMENT`,
   `SERVICE_VERSION`, `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_SERVICE_NAME` on
   every Deployment that runs a binary (api, mcp, a2a, projector, reports; there
   is no CronJob). `serviceVersion` defaults to empty and falls back to
   `image.tag`, then the chart's `appVersion`. The `extraEnv` list of every
   component refuses the four names. `otel.enabled=false` only stops the chart
   rendering the endpoint and the service name: the binaries then use their
   built-in defaults, so telemetry is dropped, not switched off in code.
   `charts/exception-management/tests/render_test.sh` asserts all of it, scans
   `cmd/*/telemetry.go` for the names the binaries read, and checks that every
   variable a component Deployment renders is documented in
   `docs/docs/operations/configuration.md`.

### Deviations from the reference, and what was not done

- **No trace propagation across Kafka.** The reference stamps `traceparent` and
  `tracestate` into the outbox row where it is appended. Here the `outbox`
  table (`migrations/0001_exception_management.up.sql`) has the columns `id`,
  `event_id`, `aggreg_type`, `aggreg_id`, `payload`, `occurred_at`, `attempts`,
  `next_attempt_at` and `created_at`: no headers column, and
  `internal/adapters/outbound/kafka/encoder.go` builds only the content-type
  header at encode time. Carrying the trace context would need a schema
  migration, which this change does not add. A trace therefore ends at the
  outbox write; the inbound consumers start no span either. The CloudEvents
  encoder goldens are untouched.
- **The `http://` literal in `normalizeEndpoint` is kept as in the reference.**
  No fitness test or lint rule in this repository rejects URL literals
  (`internal/architecture` and `scripts/harness/repo_lint.py` hold none), so no
  `net/url` workaround was needed.
- **No `otelchimetric.NewServerActiveRequests`.** ADR 0016 notes that
  inventory-storage's routers also add it; the Tier 1 baseline it defines, and
  this change, name only the duration histogram.
- **`GET /metrics` is not touched.** It is part of the REST contract
  (`apis/openapi.yaml`) and still writes only the presence gauge. The OpenTelemetry
  metrics travel over OTLP to the Collector, not through that route.
- **`golang.org/x/net` is bumped to v0.62.0.** The OTLP/gRPC dependencies pull
  an older one that the `vuln` job flags; `make vuln` is part of this change's
  gates.

## Consequences

- A request to the api, the reports reader, the MCP server or the A2A agent
  produces an exported `http.server.request.duration` and a server span, and
  its log line carries the same `trace_id` and `span_id`. An inbound
  `traceparent` is honoured because `Setup` installs the W3C propagator.
- Go runtime metrics come from every binary, the projector included.
- The `a2a.skill <name>` spans that used to go nowhere are now exported.
- Lines written without a span-carrying context (consumers, relay, sweeper,
  projector) carry no `trace_id` or `span_id`.
- The dashboards in `warehouse-infra` are not part of this change; the metric
  names to build them from are in the observability page.
- Cross-context correlation stays the CloudEvents `id` chain until the outbox
  can carry headers.
