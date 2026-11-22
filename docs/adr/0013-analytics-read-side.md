# ADR 0013: the analytics read side

## Status

Accepted (2026-11-14, recorded retroactively). The read side was built on
2026-10-21 (commits `c1697be` and `769e924`) and has been cited as "ADR 0006"
from the code, the Helm chart, `apis/asyncapi.yaml` and the docs since then. No
ADR was written for it, and the number 0006 went to the carton-discrepancy
consumer ([ADR 0006](./0006-carton-discrepancy-consumer-and-routing.md),
2026-10-24). This is the record those citations meant; they now point here.

## Context

The service publishes seven kinds of fact: the six exception-case facts
(`ExceptionOpened`, `ExceptionTriaged`, `ExceptionAssigned`,
`ExceptionEscalated`, `ExceptionResolved`, `ExceptionAutoResolved`) and
`ExceptionPolicyDeclared`. Questions about the history of the cases (which root
causes recur, how long cases take to resolve, how many breach their SLA) are
answered from those facts, not from the OLTP database. Constraints:
[ADR 0003](./0003-local-copies-and-consumed-contracts.md) (no live call to a
sibling context) and [ADR 0004](./0004-cloudevents-envelope-and-type-catalogue.md)
(CloudEvents 1.0, two streams: `events` and `analytics`). The fleet already had
this shape; `internal/analytics/report/report.go` cites it as the same decision
as pick-face-replenishment ADR 0008.

## Decision

An additive analytical read side, built from this service's own published facts.
Three processes, one writer:

1. **An analytics twin of every published fact.** The outbox relay encodes each
   persisted fact into two CloudEvents: the integration message on
   `warehouse.exception-management.events` and its twin on
   `warehouse.exception-management.analytics`. They share one CloudEvents `id`
   (minted once at enqueue, so a relay retry republishes the same id) and carry
   byte-identical payloads; only the `dataschema` stream segment differs
   (`urn:warehouse:exception-management:{events,analytics}:<Event>:v1`).
   `internal/adapters/outbound/kafka/encoder.go` (`Encode`) does this. The
   analytics topic is not an integration contract: the only consumer is this
   service's own projector (ADR 0004).
2. **The projector is the only writer.** `cmd/exceptions-projector` consumes the
   analytics topic under a fixed, env-supplied consumer group
   (`ANALYTICS_CONSUMER_GROUP`, no default, a boot error when unset). It applies
   the embedded analytical migrations at boot, deduplicates on the CloudEvents
   `id` in `analytics_processed_events` (a replay is a no-op) and upserts
   `exception_case_facts` and `exception_policy_facts`. Each milestone column
   keeps the time of the earliest matching event, so arrival order never
   matters. It serves only `/healthz` and `/readyz` (`ADMIN_ADDR`, `:8091`),
   never opens the OLTP database and calls no sibling context.
3. **A separate analytical database.** `ANALYTICS_DATABASE_URL` (a direct DSN)
   names a database apart from the OLTP one; its schema lives in
   `analytics/migrations/`. It is a derived copy, not a source of truth: it can
   be dropped and rebuilt by replaying the topic under a new consumer group.
4. **A read-only reports binary.** `cmd/exceptions-reports` answers, from the
   analytical database only and through a read-only pool,
   `GET /reports/root-cause-frequency`, `/reports/mean-time-to-resolve`,
   `/reports/sla-breach-rate` and `/reports/freshness` (plus `/healthz`) on
   `HTTP_ADDR` (`:8092`). It writes nothing and opens neither the OLTP database
   nor Kafka. It has no auth layer: access control is the in-cluster boundary.
5. **The read model is isolated.** `internal/analytics/report` imports no other
   internal package, so the OLTP domain cannot leak into the projection; the
   architecture test keeps that region closed. The analytical store has an
   in-memory and a Postgres implementation, run against one contract test so the
   two cannot drift.
6. **Off by default in the chart.** `analytics.enabled` is `false`, so the
   default render is unchanged. Enabling it renders the projector Deployment
   (no Service, replica count fixed, not autoscaled) and the reports Deployment
   with its Service. The chart refuses to render without a DSN source
   (`analytics.url` or `analytics.existingSecret`) or without a consumer group.

## Consequences

- Reporting reads never touch OLTP data or OLTP credentials: the projector has
  none, and the reports pool is read-only.
- The analytical database can be rebuilt at any time by replaying the topic
  under a new consumer group.
- Every published fact is encoded and relayed twice, and a second database and
  two more processes have to be run when the read side is on.
- The projector keeps per-process consumer state, so it is deliberately not
  horizontally scaled.
- The reports are eventually consistent with the OLTP state;
  `/reports/freshness` exposes the lag.
- This ADR records a decision that was built first. Where the code, chart or
  docs said "ADR 0006" in the sense of the read side, they now say ADR 0013;
  ADR 0006 remains the carton-discrepancy consumer.
