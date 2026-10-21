---
paths:
  - "internal/adapters/**/kafka/**"
  - "internal/adapters/outbound/events/**"
  - "apis/asyncapi*"
---

# Cross-service integration events (Kafka)

This service PUBLISHES on `warehouse.exception-management.events` (integration)
and `warehouse.exception-management.analytics` (its own projector only), and
CONSUMES the exception-class facts of six sibling contexts into local, frozen
copies on the case. It never calls a sibling over REST or MCP. Type prefix
`com.warehouse.wes.exception-management.`, source `/warehouse/exception-management`.

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message this service produces or consumes (integration
`warehouse.<ctx>.events` AND analytics `warehouse.<ctx>.analytics`) is a
CloudEvents 1.0 event in structured content mode. This is a hard fleet rule,
not a preference — there is nothing to "choose" here:

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone
  fleet-wide). `internal/architecture/fitness_test.go`'s
  `TestNoEventEnvelopeToggleOrFlatEnvelope` fails CI on any of them.
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  (latest v2) via ONE helper package,
  `internal/adapters/kafka/cloudevents/` — copy it from this template's
  `templates/cloudevents/cloudevents.go.tmpl` (+ its test) and change only
  the per-repo constants. Transport stays `segmentio/kafka-go` (no sdk-go
  protocol/client packages, no hand-rolled CloudEvent structs).
- Every produced message carries the Kafka header
  `content-type: application/cloudevents+json; charset=UTF-8`
  (`cloudevents.ContentTypeHeader()`), next to the W3C trace headers
  (`traceparent`/`tracestate` stay in headers, never duplicated into
  extension attributes). Message key = aggregate id, `kafkago.Hash{}`
  balancer.
- Required attributes: `specversion=1.0`; `id` (UUID v4 minted ONCE per
  domain event and persisted with the outbox row, so redelivery carries
  the same id); `source=/warehouse/<repo>`; `type`; `subject` (aggregate
  instance id, never empty); `time` (domain occurred-at, UTC);
  `datacontenttype=application/json`;
  `dataschema=urn:warehouse:<repo>:<events|analytics>:<EventName>:v<N>`.
  No custom extension attributes without an ADR.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`
  (this repo: `com.warehouse.wes.exception-management`). The SAME `type` names the
  occurrence on both the integration and the analytics topic; `dataschema`
  names the payload shape. Breaking payload change => new `.v2` type + new
  dataschema version, never mutate an existing one.
- Consumers decode with `cloudevents.Decode` (validates), dispatch on the
  FULL `type` string (never a short name or suffix match), ignore unknown
  types, read `time`/`subject` from attributes and the payload via
  `DataAs`, dedupe on `id`, and DLQ/skip — WARN log + commit past, never
  crash, never block the partition, never fall back to parsing a legacy
  shape — anything that fails CloudEvents validation.
- Tests: a golden exact-JSON test per published `type` (all attributes +
  the `content-type` header); a legacy-flat-message-rejected test per
  consumer; Kafka integration tests via testcontainers only.

Full standard, subdomain table and the fleet's cross-service type
catalogue live in the warehouse-docs repository (the CloudEvents event
standard page). Record this repo's own envelope decisions as ADRs under
`docs/adr/`.

### Published types

| `type` | topic(s) | `subject` / key | `dataschema` |
| --- | --- | --- | --- |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionOpened` | `warehouse.exception-management.events` (+analytics) | caseId | `urn:warehouse:exception-management:events:ExceptionOpened:v1` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionTriaged` | same | caseId | `urn:warehouse:exception-management:events:ExceptionTriaged:v1` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionAssigned` | same | caseId | `urn:warehouse:exception-management:events:ExceptionAssigned:v1` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionEscalated` | same | caseId | `urn:warehouse:exception-management:events:ExceptionEscalated:v1` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionResolved` | same | caseId | `urn:warehouse:exception-management:events:ExceptionResolved:v1` |
| `com.warehouse.wes.exception-management.exceptioncase.ExceptionAutoResolved` | same | caseId | `urn:warehouse:exception-management:events:ExceptionAutoResolved:v1` |
| `com.warehouse.wes.exception-management.exceptionpolicy.ExceptionPolicyDeclared` | same | sourceType | `urn:warehouse:exception-management:events:ExceptionPolicyDeclared:v1` |

### Consumed types

| `type` | topic | producer | v1 |
| --- | --- | --- | --- |
| `com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed` | `warehouse.fulfillment.events` | fulfillment-execution | yes |
| `com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached` | `warehouse.work-planning.events` | wes-work-planning | yes |
| `com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk` | `warehouse.network-fulfillment.events` | network-fulfillment | yes |
| `com.warehouse.wes.network-fulfillment.networkorder.OrphanedHoldCancelled` | `warehouse.network-fulfillment.events` | network-fulfillment | yes |
| `com.warehouse.wes.order-management.order.OrderRepromised` | `warehouse.order-management.events` | order-management (correlation for auto-resolve only) | yes |
| `com.warehouse.wms.returns-processing.returnreceipt.UnexpectedReturnReceived` | `warehouse.returns-processing.events` | returns-processing | wave 3 |
| `com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected` | `warehouse.inventory.events` | inventory-storage | after its promotion PR |
| `com.warehouse.wms.inventory-storage.stock.ItemUnlocated` | `warehouse.inventory.events` | inventory-storage | after its promotion PR |
| `com.warehouse.wes.fulfillment-execution.package.PackageDiverted` | `warehouse.fulfillment.events` | fulfillment-execution | after its promotion PR |
| `com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected` | `warehouse.fulfillment.events` | fulfillment-execution | after its promotion PR |

Planned, not built (analytics-topic only today): workforce-management `PathUnderstaffed`, network-inventory-planning `TransferStuckDetected`.

Consumer groups (env, no default, boot error when unset): `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP`, `WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP`, `NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP`, `ORDER_CORRELATION_CONSUMER_GROUP`, `INVENTORY_EXCEPTIONS_CONSUMER_GROUP`, `RETURNS_EXCEPTIONS_CONSUMER_GROUP`.

## Consumer group id

If this service consumes Kafka: state where the consumer group id comes
from. It MUST be env-configurable, never a hardcoded string literal --
`internal/architecture/fitness_test.go`'s
TestKafkaConsumerGroupNeverHardcodedInline enforces this (a real incident:
wes-work-planning's hardcoded group id let a locally-run e2e-tests process
silently collide with the live in-cluster Deployment's consumer group on
the shared fleet Kafka broker).

If this consumer replays from FirstOffset on every start to build an
in-memory read model (rather than resuming from a committed offset), the
group id must additionally be UNIQUE PER PROCESS INSTANCE (hostname+PID+
timestamp), not just configurable -- see HARNESS.md's Kafka section for
why a shared group breaks that pattern specifically.
