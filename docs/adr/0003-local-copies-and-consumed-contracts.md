# ADR 0003: local copies and the consumed contracts

## Status

Accepted (2026-10-21).

## Context

Every case starts from a fact another context published. The fleet rule
(context-boundaries) forbids a live REST or MCP call between contexts, so
exception-management keeps what it needs from its producers locally, the same
conformist stance as the rest of the fleet. This ADR lists what is consumed,
the local-copy rule and the version / dedup semantics, verified against each
producer's `origin/develop` `apis/asyncapi.yaml` on 2026-10-21 (payloads are
restated in this repo's `apis/asyncapi.yaml`, never imported).

Unlike an event-fed read-model context (e.g. pick-face-replenishment's bin
levels), this service replicates NO producer state. The only local copies are
per-case and per-event:

| local copy | fed by | rule |
|---|---|---|
| the case's frozen source payload | the source fact that opened it | written once at open, never changed, never re-fetched (ADR 0002) |
| the open-case dedup key index | every consumed source fact | exactly one open case per (source type, source subject, site); a repeat touches, never opens |
| the consumed-event id ledger | every consumed message | dedupe on the CloudEvents `id`, claimed in the SAME transaction as the effect |
| policy versions | `PUT /exception-policies/{sourceType}` (local command, not a copy) | append-only versions |

The `OrderRepromised` correlation is not a copy either: it reads this
service's OWN open cases (by the work-unit-ref reversal of the case's source
`order_ref`) and matches `order_id`. No order state is kept.

## Decision

### Consumed contracts

| producer | topic | full type | used for | status |
|---|---|---|---|---|
| fulfillment-execution | `warehouse.fulfillment.events` | `com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed` | case source (dedup key subject = task id); `order_ref` feeds the auto-resolve correlation | Verified |
| fulfillment-execution | `warehouse.fulfillment.events` | `com.warehouse.wes.fulfillment-execution.package.PackageDiverted` | case source (subject = package id) | Pending the producer's promotion PR (analytics-only today) |
| fulfillment-execution | `warehouse.fulfillment.events` | `com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected` | case source (subject = package id) | Pending the producer's promotion PR (analytics-only today) |
| wes-work-planning | `warehouse.work-planning.events` | `com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached` | case source (subject = path id; `data` carries only `path_id` by the producer's design) | Verified |
| network-fulfillment | `warehouse.network-fulfillment.events` | `com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk` | case source (subject = networkRef; siteId feeds the dedup key) | Verified |
| network-fulfillment | `warehouse.network-fulfillment.events` | `com.warehouse.wes.network-fulfillment.networkorder.OrphanedHoldCancelled` | case source (subject = networkRef; siteId feeds the dedup key) | Verified |
| order-management | `warehouse.order-management.events` | `com.warehouse.wes.order-management.order.OrderRepromised` | correlation only — NEVER a case source; the correlated fact of the auto-resolve rule | Verified |
| returns-processing | `warehouse.returns-processing.events` | `com.warehouse.wms.returns-processing.returnreceipt.UnexpectedReturnReceived` | case source (subject = receipt id; site_id feeds the dedup key) | Pinned consumer-first (producer ships in wave 3) |
| inventory-storage | `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected` | case source (subject = bin id) | Pending the producer's promotion PR (analytics-only today) |
| inventory-storage | `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.stock.ItemUnlocated` | case source (subject = stock unit id) | Pending the producer's promotion PR (analytics-only today) |

### Version / dedup semantics

- **No versioned payloads among the sources.** None of the consumed types
  carries a monotonic `version` field, so there is nothing to keep-highest:
  the dedup key alone decides open-vs-touch, and the CloudEvents `id` ledger
  alone decides process-once.
- **Dedupe on the CloudEvents `id`**: the id is claimed in the SAME database
  transaction as the effect (open, touch or auto-resolve); a replayed id is a
  no-op that still commits the Kafka offset.
- **Re-firing sources are by design.** `TaskCPTMissed` re-fires every sweep
  tick, and the network-fulfillment sweeps re-fire while their condition
  holds: every repeat after the first TOUCHES the open case (occurrence + 1,
  `lastSeenAt`), exactly the behaviour ADR 0002 fixes.
- **Correlation window.** The auto-resolve rule counts a correlated fact only
  when it arrives within `within_minutes` of the case's LAST OCCURRENCE and
  its subject matches the case's source through the work-unit formula; a
  mismatch is a silent no-op (the case stays open), never an error.

### Consumer rules

- Dispatch on the FULL `type`; ignore unknown types; skip (WARN, commit past)
  anything that is not a valid CloudEvent.
- Consumer group ids come from env, never string literals at the call site:
  `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP`, `WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP`,
  `NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP`, `ORDER_CORRELATION_CONSUMER_GROUP`,
  `RETURNS_EXCEPTIONS_CONSUMER_GROUP`, `INVENTORY_EXCEPTIONS_CONSUMER_GROUP`
  (a missing group env is a boot error).
- Commit the Kafka offset only after the effect and the id claim commit
  (`FetchMessage` + `CommitMessages`).
- Events leave only through the transactional outbox (fleet rule); nothing is
  published from a consumer path directly.

### No live lookup

exception-management has no outbound HTTP or MCP client to a sibling. An
unknown or pending source simply never arrives; its cases never open, and
nothing is faked. The ops-agent reads cases through the read-only MCP surface
and writes nothing.

## Consequences

- Until a promotion PR merges (or returns-processing ships), its types never
  arrive: those edges are dashed `planned / not built` in the docs, never
  simulated.
- Planned, not built (ADR 0001): workforce-management `PathUnderstaffed` and
  network-inventory-planning `TransferStuckDetected` live on analytics topics
  only today; manual case creation, associate paging and labor-cost
  correlation stay excluded from v1.
- Six consumer groups' worth of lag is the cost of keeping this service
  independent of its producers' availability.
