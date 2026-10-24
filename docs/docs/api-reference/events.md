---
id: events
title: Event catalogue
sidebar_label: Event catalogue
sidebar_position: 99
---

# Event catalogue

Summary of `apis/asyncapi.yaml` (AsyncAPI 3.0.x). The file is the authority for payload fields;
[ADR 0004](/docs/adr/0004-cloudevents-envelope-and-type-catalogue) is the type catalogue and
[Domain events](/docs/ddd/domain-events) carries the field-level detail.

## Envelope

Every message, published or consumed, is a **CloudEvents 1.0** event in structured content mode
with the Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.

| Attribute | Value |
| --- | --- |
| `specversion` | `1.0` |
| `id` | UUID minted once per domain event at enqueue and persisted with the outbox row; consumers dedupe on it |
| `source` | `/warehouse/exception-management` |
| `type` | `com.warehouse.wes.exception-management.<entity>.<EventName>` with `<entity>` `exceptioncase` or `exceptionpolicy` |
| `subject` | the case id (the source type for the policy event) — also the Kafka key |
| `time` | domain occurred-at, UTC |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:exception-management:events:<EventName>:v1` |

A breaking payload change gets a new `.v2` type and dataschema; an existing one is never mutated.

## Published

Topic `warehouse.exception-management.events`, written by the outbox relay only
([ADR 0005](/docs/adr/0005-outbox-relay-attempt-cap-and-dlq)). An analytics mirror topic
(`warehouse.exception-management.analytics`) is declared **roadmap, not built** — the projector
does not exist yet and no consumer is wired.

| Type suffix | Raised | Payload (`data`) |
| --- | --- | --- |
| `exceptioncase.ExceptionOpened` | a source fact opens a case (first occurrence of the dedup key) | `case_id`, `source_type`, `source_subject`, `site_id` (nullable), `severity`, `queue`, `opened_at`, `sla_due_at`, `source_event_id`, `source_payload` (object, frozen) |
| `exceptioncase.ExceptionTriaged` | `POST /exceptions/{caseId}/triage` | `case_id`, `severity`, `queue`, `actor`, `at` |
| `exceptioncase.ExceptionAssigned` | `POST /exceptions/{caseId}/assign` | `case_id`, `assignee`, `actor`, `at` |
| `exceptioncase.ExceptionEscalated` | the SLA sweeper flags a breach | `case_id`, `sla_due_at`, `at` |
| `exceptioncase.ExceptionResolved` | `POST /exceptions/{caseId}/resolve` (terminal) | `case_id`, `resolution_code`, `root_cause`, `note` (nullable), `actor`, `at` |
| `exceptioncase.ExceptionAutoResolved` | a policy auto-resolve rule fires on the correlated fact | `case_id`, `rule_id`, `correlated_type`, `correlated_event_id`, `at` |
| `exceptionpolicy.ExceptionPolicyDeclared` | `PUT /exception-policies/{sourceType}` (subject and key = the source type) | `source_type`, `version`, `severity`, `queue`, `sla_minutes`, `auto_resolve` (nullable: `{on_type, within_minutes}`) |

**Touch raises nothing**: the occurrence count and the audit trail are the record — a repeat fact
is visible on the case, not on the wire. Outbox rows the encoder refuses, or that exhaust
`OUTBOX_MAX_ATTEMPTS`, are dead-lettered to `warehouse.exception-management.events.dlq`
([Runbook](/docs/operations/runbook#dead-letter-topics)).

## Consumed

Nine case sources plus one correlation fact, each under its pinned consumer group. Dispatch is on
the FULL type; every other type on those topics is ignored (commit past).

| Type | Topic | Consumer group env |
| --- | --- | --- |
| `com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed` | `warehouse.fulfillment.events` | `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` |
| `com.warehouse.wes.fulfillment-execution.package.PackageDiverted` | `warehouse.fulfillment.events` | `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` |
| `com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected` | `warehouse.fulfillment.events` | `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` |
| `com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached` | `warehouse.work-planning.events` | `WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP` |
| `com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk` | `warehouse.network-fulfillment.events` | `NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` |
| `com.warehouse.wes.network-fulfillment.networkorder.OrphanedHoldCancelled` | `warehouse.network-fulfillment.events` | `NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` |
| `com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected` | `warehouse.inventory.events` | `INVENTORY_EXCEPTIONS_CONSUMER_GROUP` |
| `com.warehouse.wms.inventory-storage.stock.ItemUnlocated` | `warehouse.inventory.events` | `INVENTORY_EXCEPTIONS_CONSUMER_GROUP` |
| `com.warehouse.wms.returns-processing.returnreceipt.UnexpectedReturnReceived` | `warehouse.returns-processing.events` | `RETURNS_EXCEPTIONS_CONSUMER_GROUP` |
| `com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised` | `warehouse.inbound-receiving.events` | `CARTON_DISCREPANCY_CONSUMER_GROUP` |
| `com.warehouse.wes.order-management.order.OrderRepromised` | `warehouse.order-management.events` | `ORDER_CORRELATION_CONSUMER_GROUP` — **correlation only, never a case source** |

**Roadmap, not consumed:** workforce-management `PathUnderstaffed` and
network-inventory-planning `TransferStuckDetected` — analytics-topic-only on the producers today;
promotion pending.

Processing rules: dedupe on the CloudEvents `id` in the same transaction as the effect; commit the
offset only after success; anything that is not a valid CloudEvent is WARN-logged and skipped; a
fact whose source type has no declared policy is WARN-logged and committed past
([Troubleshooting](/docs/operations/troubleshooting#no-case-opened)); a payload that can never
apply dead-letters to `<topic>.dlq` at once.
