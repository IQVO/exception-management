# ADR 0004: CloudEvents envelope and type catalogue

## Status

Accepted (2026-10-21).

## Context

Every Kafka message in the fleet is a CloudEvents 1.0 event in structured mode
(fleet rule `.claude/rules/fleet/cloudevents.md`, warehouse-docs
`docs/strategic-design/event-standard-cloudevents.md`). This ADR is the
catalogue of every type exception-management publishes or consumes;
`TestEventCatalogueMatchesContract` checks it against `apis/asyncapi.yaml`.

## Decision

### Envelope

- `specversion` `1.0`; `id` a UUID minted once per domain event and persisted
  with the outbox row (stable across relay retries; consumers dedupe on it);
  `source` `/warehouse/exception-management`; `subject` the case id
  (`case-<uuid>`), or the source type for the policy event; `time` the
  occurred-at instant in UTC; `datacontenttype` `application/json`;
  `dataschema`
  `urn:warehouse:exception-management:<events|analytics>:<EventName>:v1`.
- Kafka value = the JSON event format; header
  `content-type: application/cloudevents+json; charset=UTF-8`; Kafka key = the
  subject, so all events of one case stay ordered on one partition.
- `data` is snake_case; optional fields are omitted when unset; timestamps are
  RFC 3339 UTC.
- Built and decoded only through `internal/adapters/kafka/cloudevents`.

### Published

`<entity>` is the raising aggregate, lowercase, no separators:
`exceptioncase` or `exceptionpolicy`. The stream column says where the type
goes: `events` is `warehouse.exception-management.events` (the integration
contract), `analytics` is `warehouse.exception-management.analytics` (this
service's own projector only, not an integration contract). The analytics
mirror of every type below ships with the projector in a later build step of
this service; it is declared here so the stream set does not change under the
consumers later.

| type | stream | raised when | `data` |
|---|---|---|---|
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionOpened` | events + analytics | a source fact opens a case (first occurrence of the dedup key) | `{case_id, source_type, source_subject, site_id (nullable), severity, queue, opened_at, sla_due_at, source_event_id, source_payload (object, frozen)}` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionTriaged` | events + analytics | severity/queue set by an operator or `system` | `{case_id, severity, queue, actor, at}` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionAssigned` | events + analytics | an owner takes the case | `{case_id, assignee, actor, at}` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionEscalated` | events + analytics | the SLA sweeper sets the breach flag | `{case_id, sla_due_at, at}` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionResolved` | events + analytics | an operator records resolution code + root cause (terminal) | `{case_id, resolution_code, root_cause, note (nullable), actor, at}` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionAutoResolved` | events + analytics | a policy auto-resolve rule fires on a correlated fact | `{case_id, rule_id, correlated_type, correlated_event_id, at}` |
| `com.warehouse.wes.exception-management.exceptionpolicy.ExceptionPolicyDeclared` | events + analytics | a policy version is declared | `{source_type, version, severity, queue, sla_minutes, auto_resolve (nullable)}` |

Payload rules:

- `ExceptionPolicyDeclared` has subject and key = the source type (the full
  CloudEvents type string of the governed source fact); every other type has
  subject and key = the case id.
- `severity` is `LOW`, `MEDIUM`, `HIGH` or `CRITICAL`.
- `ExceptionOpened.source_payload` is the frozen copy of the consumed source
  fact's `data`: it never changes after open and is never re-fetched
  (ADR 0002).
- `ExceptionResolved.note` is null when the operator recorded no remark.
- `ExceptionAutoResolved.correlated_type` and `correlated_event_id` are the
  full CloudEvents type and id of the fact that satisfied the rule.
- Analytics copies carry the same `type` and the same `id` per occurrence,
  with `dataschema`
  `urn:warehouse:exception-management:analytics:<EventName>:v1`.
- A breaking payload change is a new `.v2` type and dataschema, never a
  mutation of v1.

### Consumed (ADR 0003)

| type | topic | consumer group env | status |
|---|---|---|---|
| `com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed` | `warehouse.fulfillment.events` | `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` | v1 (verified on the producer's contract) |
| `com.warehouse.wes.fulfillment-execution.package.PackageDiverted` | `warehouse.fulfillment.events` | `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` | pending the producer's promotion PR (analytics-only today) |
| `com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected` | `warehouse.fulfillment.events` | `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` | pending the producer's promotion PR (analytics-only today) |
| `com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached` | `warehouse.work-planning.events` | `WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP` | v1 (verified) |
| `com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk` | `warehouse.network-fulfillment.events` | `NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` | v1 (verified) |
| `com.warehouse.wes.network-fulfillment.networkorder.OrphanedHoldCancelled` | `warehouse.network-fulfillment.events` | `NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` | v1 (verified) |
| `com.warehouse.wes.order-management.order.OrderRepromised` | `warehouse.order-management.events` | `ORDER_CORRELATION_CONSUMER_GROUP` | v1 (verified; correlation only, never a case source) |
| `com.warehouse.wms.returns-processing.returnreceipt.UnexpectedReturnReceived` | `warehouse.returns-processing.events` | `RETURNS_EXCEPTIONS_CONSUMER_GROUP` | pinned consumer-first (producer ships in wave 3) |
| `com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected` | `warehouse.inventory.events` | `INVENTORY_EXCEPTIONS_CONSUMER_GROUP` | pending the producer's promotion PR (analytics-only today) |
| `com.warehouse.wms.inventory-storage.stock.ItemUnlocated` | `warehouse.inventory.events` | `INVENTORY_EXCEPTIONS_CONSUMER_GROUP` | pending the producer's promotion PR (analytics-only today) |

Planned, not built (analytics-topic only today, no consumer yet):
workforce-management `PathUnderstaffed`, network-inventory-planning
`TransferStuckDetected`.

Every other type on those topics is ignored. A message that is not a valid
CloudEvent is logged and skipped, never retried forever. Payloads are restated
from the producers' own contracts in `apis/asyncapi.yaml` and never imported.

## Consequences

- Consumers need nothing but this catalogue and `apis/asyncapi.yaml`; there is
  no shared Go code across repos.
- The `analytics` stream is not an integration contract: the only consumer is
  this service's own projector (a later build step); other contexts keep
  reading `warehouse.exception-management.events`.
- Until a pending promotion PR merges, its types simply never arrive; the
  contract above is pinned consumer-first and fixed here in a follow-up PR if
  a producer's final contract differs.
