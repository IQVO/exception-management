---
id: configuration
title: Configuration
sidebar_label: Configuration
sidebar_position: 1
---

# Configuration

All configuration is environment variables, read once at boot; a missing mandatory variable is a
boot error naming it (every problem is reported at once, before a connection opens or the listener
binds). There is no config file and no `*_MODE` toggle anywhere — the chart refuses to render one.

## `cmd/api`

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `DATABASE_URL` | yes | — | Postgres DSN. Real Postgres only; no in-memory mode |
| `KAFKA_BROKERS` | yes | — | comma-separated broker list. Real Kafka only |
| `HTTP_ADDR` | no | `:8080` | REST listen address |
| `LOG_LEVEL` | no | `info` | `debug` \| `info` \| `warn` (`warning` accepted) \| `error`, JSON to stdout |
| `OUTBOX_RELAY_INTERVAL` | no | `1s` | Go duration or plain seconds; malformed/non-positive falls back with a WARN |
| `OUTBOX_MAX_ATTEMPTS` | no | `20` | times one outbox row may fail before the relay excludes it ([ADR 0005](/docs/adr/0005-outbox-relay-attempt-cap-and-dlq)); `0` = unlimited |
| `SLA_SWEEP_INTERVAL` | no | `1m` | SLA sweeper tick; malformed/non-positive falls back with a WARN (a typo never silently disables it) |
| `SHUTDOWN_DRAIN_DELAY` | no | `5s` | wait after `/readyz` flips before the listener closes; `0` disables |

### The consumer-group variables

All eleven are **mandatory** even though each names a group the binary's consumers use — the boot
check fails closed so a deployment cannot go live missing a pinned group id. They are stable names,
fixed in `.claude/rules/integration-events.md`; renaming one in a running environment re-reads the
topic from the start.

| Variable | Consumes |
| --- | --- |
| `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.fulfillment.events` |
| `WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.work-planning.events` |
| `NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.network-fulfillment.events` |
| `ORDER_CORRELATION_CONSUMER_GROUP` | `warehouse.order-management.events` (auto-resolve correlation) |
| `INVENTORY_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.inventory.events` |
| `HELD_STOCK_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.inventory.events` — the topic's second group, held-stock facts (ADR 0012) |
| `RETURNS_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.returns-processing.events` |
| `CARTON_DISCREPANCY_CONSUMER_GROUP` | `warehouse.inbound-receiving.events` (ADR 0006) |
| `YARD_MANAGEMENT_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.yard-management.events` (ADR 0010) |
| `DOCK_SCHEDULING_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.dock-scheduling.events` (ADR 0010) |
| `OUTBOUND_SHIPPING_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.outbound-shipping.events` (ADR 0011) |

## `cmd/mcp`

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `DATABASE_URL` | yes | — | same Postgres as `cmd/api` |
| `MIGRATIONS_DATABASE_URL` | no | `DATABASE_URL` | direct (non-pooled) DSN used only for the boot migrations — golang-migrate's session-scoped `pg_advisory_lock` cannot pass through PgBouncer transaction pooling |
| `MCP_ADDR` | no | `:8090` | Streamable HTTP listen address (paths `/` and `/mcp`, plus open `GET /healthz`) |
| `LOG_LEVEL` | no | `info` | as above |

The MCP binary reads **no** Kafka variable: it never dials Kafka, never starts the relay, the
consumers or the sweeper.

## `cmd/a2a`

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `DATABASE_URL` | yes | — | same Postgres as `cmd/api` |
| `MIGRATIONS_DATABASE_URL` | no | `DATABASE_URL` | direct (non-pooled) DSN used only for the boot migrations, as for `cmd/mcp` |
| `A2A_AGENT_URL` | yes | — | absolute http(s) URL of this agent's JSON-RPC endpoint, advertised on the Agent Card (the chart defaults it to the in-cluster Service DNS `http://<release>-a2a.<ns>.svc.cluster.local:8093/`); the binary refuses to boot without it |
| `A2A_ADDR` | no | `:8093` | listen address (JSON-RPC at `/` and `/a2a`, the card at `/.well-known/agent-card.json`, open `GET /healthz`) |
| `LOG_LEVEL` | no | `info` | as above |

Like the MCP binary, the A2A agent is read-only and reads **no** Kafka variable: it never dials Kafka,
never starts the relay, the consumers or the sweeper ([ADR 0007](/docs/adr/0007-a2a-inbound-adapter)).

## `cmd/exceptions-projector`

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | yes | — | the analytical Postgres DSN (direct, the projector runs the embedded analytical migrations through it); the projector never opens the OLTP database |
| `KAFKA_BROKERS` | yes | — | comma-separated broker list |
| `ANALYTICS_CONSUMER_GROUP` | yes | — | the projector's fixed consumer group, no default in the binary |
| `ADMIN_ADDR` | no | `:8091` | `/healthz` and `/readyz` listen address |
| `LOG_LEVEL` | no | `info` | as above |

## `cmd/exceptions-reports`

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | yes | — | the analytical Postgres DSN, opened through a read-only pool; the reader never opens the OLTP database or Kafka |
| `HTTP_ADDR` | no | `:8092` | reports API listen address |
| `LOG_LEVEL` | no | `info` | as above |

## Observability variables (all five binaries)

Every binary reads the same four variables; the chart renders them on every component that runs a
binary (see [Observability](/docs/operations/observability) and
[ADR 0014](/docs/adr/0014-adopt-the-fleet-observability-standard)). None is required: with nothing
set the binary exports to `localhost:4317`, which drops the telemetry when no Collector listens, and
never blocks or fails to boot.

| Variable | Default | Meaning |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | Collector OTLP/gRPC endpoint, `host:port` or a full URL (`https://` keeps TLS) |
| `OTEL_SERVICE_NAME` | per binary: `exception-management` (api), `exception-management-mcp`, `exception-management-a2a`, `exception-management-projector`, `exception-management-reports` | `service.name` of every span and metric |
| `SERVICE_VERSION` | `dev` | `service.version` |
| `ENVIRONMENT` | `local` | `deployment.environment.name` (read by `telemetry.Setup`) |

## Helm

`charts/exception-management/values.yaml` maps every key 1:1 to an env var
(`config.outboxRelayInterval` → `OUTBOX_RELAY_INTERVAL`, `config.slaSweepInterval` →
`SLA_SWEEP_INTERVAL`, …) and the chart refuses to render without `database.url` /
`database.existingSecret` and `kafka.brokers`. The observability variables come from
`environment` (`ENVIRONMENT`) and `otel.*` (`otel.endpoint` → `OTEL_EXPORTER_OTLP_ENDPOINT`,
`otel.serviceName` → `OTEL_SERVICE_NAME` with `-mcp`, `-a2a`, `-projector` or `-reports` appended per
component, `otel.serviceVersion` → `SERVICE_VERSION`, falling back to `image.tag` and then the chart's
`appVersion`); `otel.enabled=false` stops the chart rendering the endpoint and the service name. An
`extraEnv` entry naming one of the four is refused at render time. The MCP server is a separate
component, **disabled by default**:

```yaml
mcp:
  enabled: true   # component=mcp, Service <fullname>-mcp:8090
a2a:
  enabled: true   # component=a2a, Service <fullname>-a2a:8093 (A2A_AGENT_URL defaults to its DNS name)
```

Probe wiring and the shutdown budget are on the [Runbook](/docs/operations/runbook) page.
