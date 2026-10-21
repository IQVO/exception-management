# ADR 0001: exception-management as the owner of the problem-solve lifecycle

## Status

Accepted (2026-10-20).

## Context

The fleet already detects operational anomalies and publishes them as facts:
`TaskCPTMissed` (fulfillment-execution), `BacklogThresholdBreached`
(wes-work-planning), `AcknowledgementDeadlineAtRisk` and
`OrphanedHoldCancelled` (network-fulfillment), `DiscrepancyDetected` and
`ItemUnlocated` (inventory-storage), `PackageDiverted` and
`WeightDiscrepancyDetected` (fulfillment-execution), `PathUnderstaffed`
(workforce-management), `TransferStuckDetected` (network-inventory-planning).
None of them has an owner after it is raised: no triage, no assignee, no SLA,
no resolution, no root cause. fulfillment-execution's own ADR 0033 says of the
problem-solve area that "the only signal was the domain events". On the floor
this work is called **Problem Solve**; in the fleet it has no model.

A case lifecycle is not the producers' concern (they must stay the single
writer of their own aggregates) and not the console's (a screen is not a
system of record). It is a context of its own, consumer-first: every case
starts from a fact another context published.

## Decision

Introduce `exception-management` as a new bounded context. It owns the
**ExceptionCase** (one open case per dedup key, a fixed status machine, an
SLA clock, an audit trail of every transition) and the **ExceptionPolicy**
(per source type: severity, queue, SLA, declarative auto-resolve rule). It
never writes to a producer; a resolution is a fact other contexts may read.

### Bounded context canvas

| Field | Value |
|---|---|
| Name | exception-management |
| Purpose | Give every detected anomaly a single owner with a lifecycle (open, triage, assign, resolve, root cause), an SLA clock and dedup, so humans and the ops-agent work cases instead of raw events. |
| Strategic classification | **Supporting subdomain** today; candidate Core once auto-resolution policies and agent recommendations run against it (operational accuracy at scale). |
| Tier / CloudEvents subdomain | **WES**, subdomain `wes` (it shapes operator flow; it holds no stock, layout or master data). Type prefix `com.warehouse.wes.exception-management.`, source `/warehouse/exception-management`. Entity segments: `exceptioncase`, `exceptionpolicy`. |
| Domain roles | System of record for cases and policies. Conformist to every producer (local frozen copies of the source payload). Open Host Service for case facts. |
| Inbound communication | v1 sources on integration topics: `TaskCPTMissed`, `BacklogThresholdBreached`, `AcknowledgementDeadlineAtRisk`, `OrphanedHoldCancelled`, `OrderRepromised` (correlation only), `UnexpectedReturnReceived` (returns-processing). After the producer promotions (wave P1): `DiscrepancyDetected`, `ItemUnlocated`, `PackageDiverted`, `WeightDiscrepancyDetected`. Planned, not built: `PathUnderstaffed`, `TransferStuckDetected`. REST commands from operators and the console. No live REST/MCP call to a sibling, ever. |
| Outbound communication | `ExceptionOpened`, `ExceptionTriaged`, `ExceptionAssigned`, `ExceptionEscalated`, `ExceptionResolved`, `ExceptionAutoResolved`, `ExceptionPolicyDeclared` on `warehouse.exception-management.events` (ADR 0004 catalogue), analytics copies; REST reads; read-only MCP tools for warehouse-ops-agent. |
| Ubiquitous language | Problem solve, case, source fact, dedup key, occurrence, queue, severity, SLA clock, escalation, resolution code, root cause, auto-resolve rule. |
| Business decisions | Exactly one open case per dedup key (source type, source subject, site): a repeated fact touches the open case (occurrence count, last seen) instead of opening another; status machine `OPEN -> TRIAGED -> ASSIGNED -> RESOLVED`, `ESCALATED` is a flag, not a state; `RESOLVED` needs a resolution code and is terminal; every transition records its actor (operator, policy id or `system`); the case holds a frozen copy of the source payload, never a reference to re-fetch; auto-resolve is a declarative policy rule ("close when `<type>` arrives with the same subject within N minutes"), never agent-initiated. |
| Assumptions | `TaskCPTMissed.order_ref` and `PackageManifested.order_ref` carry WES's per-line work unit id `{order_id}-line-{line_no}`, not order-management's `order_id` (order-management ADR 0018 learned this): the CPT <-> `OrderRepromised` correlation reverses that frozen formula and fails soft (case stays open) on no match. The SLA sweeper is a bounded ticker that reports facts and sets the flag; it never blocks work. |
| Verification | Mutation gate over every domain package; golden exact-JSON test per published type; godog scenarios: fact -> open, repeated fact -> touch, triage/assign/resolve, SLA breach -> escalated, `OrderRepromised` -> auto-resolved; an end-to-end test on real Postgres + Kafka driven only through Kafka and REST. |

### Context map

| Edge | Pattern | Direction |
|---|---|---|
| fulfillment-execution, wes-work-planning, network-fulfillment, inventory-storage, returns-processing, order-management -> exception-management | Conformist (frozen local copies of their Published Language) | upstream -> us |
| workforce-management, network-inventory-planning -> exception-management | planned / not built (analytics-topic sources, promotion pending) | — |
| exception-management -> warehouse-console (`exceptions_mfe`), warehouse-ops-agent (read-only MCP) | REST / MCP reads | us -> readers |
| exception-management -> analytics projector | own topic | internal |

### Explicitly excluded (v1)

- Manual case creation (`POST /exceptions`): every case is a fact; a manual lane reopens "who may assert an anomaly" and is a v2 decision.
- Agent-initiated writes (warehouse-ops-agent stays zero-write; it reads cases and recommends).
- Paging or notifying associates (external channel, would need an ACL).
- Correlating cases to labor cost (labor-performance may read `ExceptionResolved` later).
- A generic workflow engine: the state machine is fixed and small on purpose.
- Any live REST or MCP lookup of a sibling context at request time.

## Consequences

- Anomalies stop being log noise: each has an owner, an age, a queue and an outcome, and the ops-agent gets an aggregate to reason over.
- Producers whose facts live only on analytics topics are promoted by their own PRs (wave P1); until then those edges are dashed `planned / not built`, never faked.
- This context runs on real Postgres and real Kafka only (fleet rule); the shipped binary has no in-memory path.

## References

- Fleet plan: `~/warehouse-systems/.hermes/plans/2026-10-20_231017-returns-processing-and-exception-management.md` (decision log, pinned contracts, waves).
- Precedents: order-management ADR 0018 (`order_ref` work-unit formula), fulfillment-execution ADR 0025 (`SweepCPTMisses`: a sweeper reports facts), inventory-storage ADR 0013 (event-fed local copies).
