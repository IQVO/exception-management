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

### The six consumer-group variables

All six are **mandatory** even though each names a group the binary's consumers use — the boot
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
| `RETURNS_EXCEPTIONS_CONSUMER_GROUP` | `warehouse.returns-processing.events` |

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

## Helm

`charts/exception-management/values.yaml` maps every key 1:1 to an env var
(`config.outboxRelayInterval` → `OUTBOX_RELAY_INTERVAL`, `config.slaSweepInterval` →
`SLA_SWEEP_INTERVAL`, …) and the chart refuses to render without `database.url` /
`database.existingSecret` and `kafka.brokers`. The MCP server is a separate component, **disabled
by default**:

```yaml
mcp:
  enabled: true   # component=mcp, Service <fullname>-mcp:8090
a2a:
  enabled: true   # component=a2a, Service <fullname>-a2a:8093 (A2A_AGENT_URL defaults to its DNS name)
```

Probe wiring and the shutdown budget are on the [Runbook](/docs/operations/runbook) page.
