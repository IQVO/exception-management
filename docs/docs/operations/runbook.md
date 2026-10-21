---
id: runbook
title: Runbook
sidebar_label: Runbook
sidebar_position: 2
---

# Runbook

How to deploy and operate exception-management. Everything is real Postgres and real Kafka; there
is no degraded mode to fall into.

## Deploy

The service ships as the `exception-management` Helm chart (`charts/exception-management`):
`cmd/api` as the main Deployment, `cmd/mcp` as an optional second component (default off). In the
local kind cluster the image is built from the checkout and side-loaded
(`kind load docker-image`, `pullPolicy: IfNotPresent`); the render fails with a named value when
`database.*` or `kafka.brokers` is missing — that is deliberate.

```bash
helm upgrade --install exception-management charts/exception-management \
  --set database.existingSecret=exception-management-db \
  --set kafka.brokers=redpanda-bootstrap:9092 \
  --set mcp.enabled=true
```

Topics are never created through an admin API: the outbox relay's writer auto-creates
`warehouse.exception-management.events` (and its `.dlq`) on first write, the fleet convention.
Consumed topics are the producers' concern; a topic whose producer has not written yet shows as
consumer retry logs, not as unreadiness.

## Migrations

Embedded in the binary, run at boot, idempotent (golang-migrate's advisory lock makes concurrent
starts safe — both `cmd/api` and `cmd/mcp` run them, so either can boot first against a fresh
database). Behind PgBouncer transaction pooling, set `MIGRATIONS_DATABASE_URL` to a direct DSN for
that one step; the runtime pool keeps using `DATABASE_URL`
([Configuration](/docs/operations/configuration)).

## Probes

| Probe | Path | Behaviour |
| --- | --- | --- |
| startup | `/healthz` | ~60 s window (`periodSeconds: 2` × `failureThreshold: 30`) for the Postgres dial + migrations |
| liveness | `/healthz` | process-up only; never flipped by shutdown or dependencies — a slow Postgres must not get the pod killed |
| readiness | `/readyz` | drain gate **and** Postgres ping **and** a Kafka broker metadata answer (reachability only, never topics); the verdict is cached 3 s so probes cannot hammer either dependency |

A Kafka outage takes the pod out of the Service (not down); a shutdown flips readiness to
`not_ready` first so traffic drains before the listener closes.

## Graceful shutdown

Sequence, worst case sequential: `SHUTDOWN_DRAIN_DELAY` (default 5 s) + HTTP drain (10 s) + outbox
relay drain (10 s) + inbound consumers drain (15 s) + consumer close (5 s) ≈ 45 s. The chart sets
`terminationGracePeriodSeconds: 50` and refuses a shorter budget if you raise the drain delay.

## Routine operations

- **Declare/change a policy** — `PUT /exception-policies/{sourceType}` with an `Idempotency-Key`.
  A policy is append-only versions: a change is a new version, and new cases open under it; open
  cases keep the SLA they opened with.
- **Watch the queues** — `GET /exceptions?status=OPEN&queue=…`, or the SLA report via the MCP tool
  ([MCP tools](/docs/mcp/tools)).
- **Redrive a DLQ** — see below.

## Outbox relay

Every published event is claimed from the outbox by the relay
(`OUTBOX_RELAY_INTERVAL`, default 1 s, batches per row) — nothing is published from a handler
directly ([ADR 0005](/docs/adr/0005-outbox-relay-attempt-cap-and-dlq)). A row that keeps failing
is excluded after `OUTBOX_MAX_ATTEMPTS` (default 20) so a poison row can never block the relay,
and a row whose fact the encoder refuses is dead-lettered at once (below). A crash between
publish and mark re-delivers the row **byte-identically** — same CloudEvents `id` — so consumers
dedupe; at-least-once is the contract.

## Dead-letter topics

| Topic | Written by | Cause | Redrive |
| --- | --- | --- | --- |
| `<consumed-topic>.dlq` | the inbound consumer | a consumed message that can never apply (payload no wire shape) — written at once with `x-dlq-source-topic` / `x-dlq-cause` headers, offset committed past | fix the producer or the policy, then replay the payload manually; the CloudEvents `id` ledger makes a replay safe |
| `warehouse.exception-management.events.dlq` | the outbox relay | an outbox row whose fact the encoder refuses, or a row that exhausted `OUTBOX_MAX_ATTEMPTS` (default 20) — headers carry `x-dlq-error`, `x-dlq-outbox-row`, `x-dlq-aggregate` | the row is parked, not retried; after fixing, republish from the outbox payload |

An invalid CloudEvent (not envelope-shaped) is never dead-lettered — it is WARN-logged and the
offset committed past; a poison message only reaches the DLQ when it *is* a CloudEvent whose
payload can never apply.

## Scaling

`replicaCount` may exceed one safely: cases dedupe on `(source_type, source_subject, site_id)` +
the CloudEvents `id` ledger inside the row transaction, the relay claims batches per row, and
consumer groups rebalance. Keep group ids fixed — a renamed group re-reads from the start.
