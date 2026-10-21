# ADR 0005: Outbox relay — delete-on-publish, attempt cap, dead-letter topic

## Status

Accepted (2026-10-21).

## Context

ADR 0004 pinned what leaves the service and in which envelope; the outbox
table (migration 0001) pins how the rows wait: one row per domain fact with
`event_id` (the CloudEvents id, minted once at enqueue), the fact's payload
envelope, and the retry state (`attempts`, `next_attempt_at`). This ADR
decides how the relay drains those rows (`internal/adapters/outbound/outbox`
+ `postgres.OutboxStore` + `kafka.RelaySink`), following the fleet's relay
convention (inventory-storage `outbox_relay.go`, then pick-face-
replenishment / slotting-optimization's package split) and the gap-8 / D12
attempt-cap rule enforced by the architecture sensor
`TestOutboxRelaysPassAttemptCap`.

## Decision

### Delivery semantics

- One pass claims up to 100 due rows (`next_attempt_at <= now()`, `attempts`
  below the cap) in id order under `FOR UPDATE SKIP LOCKED`, hands each row
  to the Kafka sink one at a time, and **deletes a row once its send
  returned nil, in the same transaction** — the shipped schema has no
  `published_at` marker, so removal is the sent signal. Delivery is
  at-least-once; consumers dedupe on the persisted CloudEvents `id`, which a
  retry republishes byte-identically.
- The first send error stops the pass at that row, commits what already
  succeeded, records `attempts + 1` and pushes `next_attempt_at` one
  exponential backoff step forward (`2^attempts` seconds, capped at five
  minutes). Rows behind the failure stay for the next pass.

### Per-key ordering trade-off (deliberate)

Strict per-key FIFO holds only within a pass. A row held by backoff — like a
row excluded by the cap — is **overtaken by due rows behind it, including
rows of the same key** (same case id, so same partition). One poison row
must not stop the whole relay; consumers of `warehouse.exception-management.
events` therefore tolerate intra-case reordering of *different* facts during
a failure window. Escalated/Resolved keep their domain ordering guarantees
inside the aggregate; no v1 consumer orders strictly on arrival.

### Attempt cap

`OUTBOX_MAX_ATTEMPTS` (cmd default **20**, `0` = unlimited) caps how many
times a row may fail before the claim excludes it (`attempts < cap`). The
failure that reaches the cap is logged once at ERROR with the row id, event
type and key, and the row **stays in the outbox** with its `attempts` for
inspection; an operator re-arms it after fixing the cause with
`UPDATE outbox SET attempts = 0, next_attempt_at = now() WHERE id = ...`.
Every `cmd/*` that builds a relay passes `WithMaxAttempts` inline
(architecture fitness test `TestOutboxRelaysPassAttemptCap`).

### Dead-letter topic

Deterministic poison — a row whose fact has **no wire shape** (an event the
service does not publish, a payload that is not the publisher's envelope, or
one that fails CloudEvents validation) — can never be fixed by retrying. The
sink publishes it RAW to **`warehouse.exception-management.events.dlq`**
(the fleet pattern: the integration topic + `.dlq`) in the same write path:
key = the aggregate id, value = the row's payload bytes as held (jsonb
rendering), headers `content-type: application/json` plus `x-dlq-error`,
`x-dlq-outbox-row`, `x-dlq-aggregate`, `x-dlq-failed-at`. A successful DLQ
write disposes of the row (delete, like a publish); a failed DLQ write is an
ordinary send failure (attempt recorded, retried) — poison is never dropped
on a broker hiccup, and a cancelled context never dead-letters (shutdown
leaves the row unpublished). Unknown events yield no message and no error
from the encoder; the DLQ decision belongs to the relay's sink, which sees
the whole row.

### Wiring

`cmd/api` (this service's composition root) wires the relay only for now —
pool + embedded migrations at boot, `postgres.NewOutboxStore`,
`kafka.NewRelaySink` (acks=all, Hash balancer, auto-topic-creation, no
writer-level topic: each message names its own), `OUTBOX_RELAY_INTERVAL`
(default 1s). The publisher, REST surface and consumers are later build
steps on the same binary.

## Consequences

- The outbox table stays small by itself: only failed/capped rows remain.
- Post-mortems of lost-looking events check the DLQ topic and the outbox
  table (`attempts > 0`), not the log alone.
- The per-key reorder window is bounded by one backoff step (or the cap);
  documented here so the twin services (returns-processing, wave 3) inherit
  the same rule knowingly.
- `next_attempt_at` makes a capped row's total retry lifetime long
  (exponential to 5-minute steps); the re-arm SQL is the runbook's fast path.
