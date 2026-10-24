---
id: domain-events
title: Domain events
sidebar_label: Domain events
sidebar_position: 20
description: Every event exception-management publishes and consumes, with its full payload — the field-level companion to the event catalogue.
---

# Domain events

Every Kafka message is a CloudEvents 1.0 event in **structured mode**: the
Kafka value is the whole JSON envelope, header `content-type:
application/cloudevents+json; charset=UTF-8`. Envelopes are built and
decoded only in `internal/adapters/kafka/cloudevents` (ADR 0004). The
contract is `apis/asyncapi.yaml`; the summary table is the
[event catalogue](/docs/api-reference/events).

## Published

Integration topic `warehouse.exception-management.events`, `source`
`/warehouse/exception-management`, `subject` and Kafka key = the case id
(the source type for the policy event), `dataschema`
`urn:warehouse:exception-management:events:<EventName>:v1`. A
`warehouse.exception-management.analytics` mirror of every type is
declared in ADR 0004 but **not built yet** (roadmap: the projector).

| Event | Raised when |
| --- | --- |
| `ExceptionOpened` | a source fact opens a case (first occurrence of the dedup key) |
| `ExceptionTriaged` | severity/queue set by an operator or `system` |
| `ExceptionAssigned` | an owner takes the case |
| `ExceptionEscalated` | the SLA sweeper sets the breach flag |
| `ExceptionResolved` | an operator records resolution code + root cause (terminal) |
| `ExceptionAutoResolved` | a policy auto-resolve rule fires on a correlated fact |
| `ExceptionPolicyDeclared` | a policy version is declared (subject and key = the source type) |

Touch raises nothing: the occurrence count and the audit trail are the
record.

### Payloads (`data`, snake_case, optional fields omitted when unset)

`ExceptionOpened`
`{case_id, source_type, source_subject, site_id (nullable), severity, queue, opened_at, sla_due_at, source_event_id, source_payload (object, frozen)}`

`ExceptionTriaged`
`{case_id, severity, queue, actor, at}`

`ExceptionAssigned`
`{case_id, assignee, actor, at}`

`ExceptionEscalated`
`{case_id, sla_due_at, at}`

`ExceptionResolved`
`{case_id, resolution_code, root_cause, note (nullable), actor, at}`

`ExceptionAutoResolved`
`{case_id, rule_id, correlated_type, correlated_event_id, at}`

`ExceptionPolicyDeclared`
`{source_type, version, severity, queue, sla_minutes, auto_resolve (nullable: {on_type, within_minutes})}`

`severity` is `LOW`, `MEDIUM`, `HIGH` or `CRITICAL`. The CloudEvents `id`
is a UUID minted once at enqueue and persisted with the outbox row — a
relay retry republishes byte-identically, consumers dedupe on it. A
breaking payload change is a new `.v2` type and dataschema, never a
mutation of v1.

## Consumed

Nine case sources plus one correlation fact, each under its pinned
consumer group. Dispatch is on the FULL type; every other type on those
topics is ignored; anything that is not a valid CloudEvent is logged and
skipped (WARN, commit past).

| Type (prefix `com.warehouse.`) | Topic | Dedup subject | Notes |
| --- | --- | --- | --- |
| `wes.fulfillment-execution.task.TaskCPTMissed` | `warehouse.fulfillment.events` | task id | Re-fires every sweep tick by design; `order_ref` = work unit `{order_id}-line-{line_no}` feeds the correlation |
| `wes.fulfillment-execution.package.PackageDiverted` | `warehouse.fulfillment.events` | package id | Promoted to the integration topic by the producer (PR #234, merged 2026-10-21) |
| `wes.fulfillment-execution.package.WeightDiscrepancyDetected` | `warehouse.fulfillment.events` | package id | Same promotion |
| `wes.work-planning.workpool.BacklogThresholdBreached` | `warehouse.work-planning.events` | path id | `data` carries only `path_id` (the producer's contract) |
| `wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk` | `warehouse.network-fulfillment.events` | networkRef | Re-fires while the condition holds; siteId feeds the dedup key |
| `wes.network-fulfillment.networkorder.OrphanedHoldCancelled` | `warehouse.network-fulfillment.events` | networkRef | Same |
| `wms.returns-processing.returnreceipt.UnexpectedReturnReceived` | `warehouse.returns-processing.events` | receipt id (+ line) | One case per receipt line; site_id feeds the dedup key |
| `wms.inbound-receiving.receipt.CartonDiscrepancyRaised` | `warehouse.inbound-receiving.events` | receipt id + sscc or `-` + kind | One case per (receipt, sscc, kind); UnlabelledCarton/CartonOverLimit route to receiving-lead, every other kind to problem-solve (ADR 0006) |
| `wms.inventory-storage.bin.DiscrepancyDetected` | `warehouse.inventory.events` | bin id | Promoted by the producer (PR #239, merged 2026-10-21) |
| `wms.inventory-storage.stock.ItemUnlocated` | `warehouse.inventory.events` | stock unit id | Same promotion |
| `wes.order-management.order.OrderRepromised` | `warehouse.order-management.events` | — | **Correlation only, never a case source**: the correlated fact of the auto-resolve rule |

**Not consumed (roadmap):** workforce-management `PathUnderstaffed`,
network-inventory-planning `TransferStuckDetected` — analytics-topic-only
today, promotion pending on the producers.

None of the consumed types carries a version field: the dedup key alone
decides open-vs-touch and the CloudEvents `id` ledger alone decides
process-once (ADR 0003).

## Example: ExceptionOpened

```json
{
  "specversion": "1.0",
  "id": "0f4a2a2e-1f6b-4a1e-9d0c-6f2f2c9b7a11",
  "source": "/warehouse/exception-management",
  "type": "com.warehouse.wes.exception-management.exceptioncase.ExceptionOpened",
  "subject": "case-1b4e28ba-2b11-53ba-bc5f-4a5d7e8f9a01",
  "time": "2026-10-21T09:15:04.122Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:exception-management:events:ExceptionOpened:v1",
  "data": {
    "case_id": "case-1b4e28ba-2b11-53ba-bc5f-4a5d7e8f9a01",
    "source_type": "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed",
    "source_subject": "task-8a1f",
    "site_id": null,
    "severity": "HIGH",
    "queue": "cpt-missed",
    "opened_at": "2026-10-21T09:15:04.122Z",
    "sla_due_at": "2026-10-21T09:45:04.122Z",
    "source_event_id": "6c1f0e8a-0a3d-4bfa-9b6e-7d2c1f5a3e22",
    "source_payload": {
      "task_id": "task-8a1f",
      "order_ref": "wu-8a1f",
      "task_type": "PICK",
      "cpt": "2026-09-30T14:55:00Z"
    }
  }
}
```

(The envelope shape and every field name are pinned by
`apis/asyncapi.yaml` and checked by the golden exact-JSON tests per type;
the values above are an illustration of the shape, not a captured
message.)
