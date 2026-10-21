---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
sidebar_position: 4
---

# Troubleshooting

## Process and boot

| Symptom | Cause | Fix |
| --- | --- | --- |
| exits at boot: `DATABASE_URL is required` | the only runtime is real Postgres | set `DATABASE_URL` (chart: `database.url` or `database.existingSecret`) |
| exits at boot: `KAFKA_BROKERS is required` | same rule for Kafka | set `KAFKA_BROKERS` (chart: `kafka.brokers`) |
| exits at boot naming a `*_CONSUMER_GROUP` variable | one of the six pinned group ids is missing (fail-closed by design) | set all six — see [Configuration](/docs/operations/configuration); never rename one in a running environment |
| CrashLoop with `run migrations` error | migrations could not run; behind PgBouncer the session-scoped advisory lock cannot pass | set `MIGRATIONS_DATABASE_URL` to a direct DSN |
| helm render fails naming a value | the chart refuses a render without a database source / brokers / with a `*_MODE` env | supply the named value; there are no modes to enable |

## Readiness

| Symptom | Cause | Fix |
| --- | --- | --- |
| `/readyz` 503 `dependency-unavailable` naming `postgres` | Postgres unreachable | check the DSN / the secret; liveness stays 200, so the pod is not killed |
| `/readyz` 503 naming `kafka` | no broker answered a metadata request | check `KAFKA_BROKERS`; a *topic* missing is never a readiness failure — only broker reachability is |
| `/readyz` 503 `{"status":"not_ready"}` without dependencies | the drain gate: shutdown flipped it | expected during rollovers; traffic drains for `SHUTDOWN_DRAIN_DELAY` (default 5 s) |
| consumer retry logs for one topic, readiness fine | the topic's producer has not written yet / topic absent | nothing to fix here; the producer owns topic creation |

## "No case opened"

| Symptom | Cause | Fix |
| --- | --- | --- |
| fact consumed, no case, WARN `no policy declared for the source type` | the source type has no declared `ExceptionPolicy` — the most common gap; the fact is committed past, not dead-lettered | `PUT /exception-policies/{sourceType}` with severity/queue/slaMinutes; re-fire the fact (new CloudEvents `id`) to open the case |
| two anomalies became one case | same dedup key `(source_type, source_subject, site_id)` — by design | that *is* the design: one case per problem; `occurrence_count` says how often |
| auto-resolve did not fire | correlated fact arrived outside `within_minutes` of `last_seen_at` (edge inclusive), or the orders do not match (correlation is order-level), or no `auto_resolve` on the policy | check the policy version the case opened under; resolve manually in doubt |
| resolved case received the fact again, stayed resolved | a fact after resolution touches (occurrence + 1) and never reopens — v1 has no reopen | by design; open questions belong on a new incident |

## REST errors

| Status / slug | Cause | Fix |
| --- | --- | --- |
| `405 method-not-allowed` on `POST /exceptions` | by design: every case starts from a consumed fact | produce the fact on its topic ([Quickstart](/docs/overview/quickstart)) |
| `400 idempotency-key-required` | a write without the header | send `Idempotency-Key` on triage/assign/resolve/PUT policy |
| `409 idempotency-key-reused` | same key replayed with a *different* body | the first answer is the stored one; use a new key only for a new intent |
| `400 malformed-request-body` | bad JSON, unknown field, trailing data — strict by design; also a NUL byte | send exactly the contract's body |
| `409 case-already-resolved` | resolve (or a state change) on a terminal case | resolution is terminal; a recurring problem is a *touch*, reopen is not in v1 |
| `409 concurrent-modification` | two writers raced the aggregate version | re-read the case, reapply the intent |
| `503 dependency-unavailable` | Postgres failed during the request | check the database; the write was not applied |

## Kafka and the outbox

| Symptom | Cause | Fix |
| --- | --- | --- |
| messages pile up in `<topic>.dlq` | payloads that can never apply (see [Runbook](/docs/operations/runbook#dead-letter-topics)) | read the `x-dlq-*` headers; fix producer/policy, then replay — the `id` ledger makes it safe |
| events on the wire stop | relay excluded a poison row after `OUTBOX_MAX_ATTEMPTS` (default 20) attempts, or Kafka was down through the budget | the row is parked in the DLQ, not lost; after the fix, republish from the outbox payload |
| duplicate events on the wire after a crash | the relay republishes an uncertain batch — byte-identically, same CloudEvents `id` | nothing to fix: consumers dedupe on `id` |
| consumer group re-reads the topic from the start | a group id was renamed | restore the previous `*_CONSUMER_GROUP` value |
