---
id: ubiquitous-language
title: Ubiquitous language
sidebar_label: Ubiquitous language
sidebar_position: 9
---

# Ubiquitous language

The words this context uses, exactly as the code uses them
(`internal/domain`, `.claude/rules/domain-model.md`, ADR 0001/0002). Every
term maps to a named thing in the model; when a term stops mapping, the
model or the term is wrong.

| Term | Meaning |
| --- | --- |
| **Problem Solve** | The operational work this context owns: taking an anomaly someone detected and driving it to a root cause. On the floor this is the name of the job; in the fleet it had no model before ADR 0001. |
| **ExceptionCase (case)** | One owned anomaly with a lifecycle: the frozen source fact, the dedup key, triage, an owner, an SLA clock, occurrences and the resolution. Aggregate root (`internal/domain/exceptioncase`). |
| **Source fact** | The producer's event that opened (or touched) a case. Its payload is frozen on the case at open and never re-fetched. |
| **Source type / source subject** | The full CloudEvents `type` and `subject` of the source fact. Two of the three dedup key parts. |
| **Dedup key** | (source type, source subject, site). At most ONE open case exists per key; a repeated fact touches the open case. |
| **Occurrence** | One arrival of the source fact. The case counts them (`occurrenceCount`) and keeps `lastSeenAt`. Re-firing sources (`TaskCPTMissed` re-fires every sweep tick) are by design. |
| **Touch** | The effect of a repeated fact on the open case: occurrence + 1, `lastSeenAt` advanced, one TOUCHED audit entry, no event on the wire. |
| **Queue** | The work queue a case sits in after triage (from the policy default or an operator). |
| **Severity** | `LOW`, `MEDIUM`, `HIGH` or `CRITICAL`. Set by the policy at open, changeable by an operator at triage. |
| **SLA clock** | `slaDueAt = openedAt + policy.slaMinutes`, computed once at open from the policy version then current. Breach sets the escalated flag. |
| **Escalation** | The flag the SLA sweeper sets when the clock breaches. It is never a state and never a blocker: an escalated case is still triaged, assigned and resolved. |
| **Status** | `OPEN`, `TRIAGED`, `ASSIGNED`, `RESOLVED` — one direction only. `RESOLVED` requires a resolution code and is terminal. |
| **Actor** | Who made a transition: an operator id, a policy id or `system`. Every audit entry records one. |
| **Resolution code / root cause** | The mandatory pair recorded at resolve. The root cause is the analytic payload of the whole context. |
| **Auto-resolve rule** | Declarative policy data: "close when `<on type>` arrives with the same subject within `<within minutes>` of the last occurrence". Data, not code; the ops-agent reads it but never fires it. |
| **Correlated fact** | The event that satisfied an auto-resolve rule (for example `OrderRepromised` closing a `TaskCPTMissed` case). Recorded on the case with its full type and id. |
| **Work unit ref** | WES's per-line work unit id `{order_id}-line-{line_no}` (order-management ADR 0018). `shared.ParseWorkUnitRef` reverses it for the correlation. |
| **ExceptionPolicy** | The per-source-type handling policy: default severity, queue, SLA minutes, optional auto-resolve rule. Versioned and append-only. Aggregate (`internal/domain/exceptionpolicy`). |
| **Policy version** | Every `PUT /exception-policies/{sourceType}` appends the next version; history is never rewritten. A new version governs cases opened after its declaration. |
| **Handling policy** | Synonym for ExceptionPolicy used in the REST spec's tag names ("Exception policies"). |
| **Outbox** | The transactional table every domain fact is written to in the same transaction as the aggregate; the relay drains it to Kafka (ADR 0005). |
| **Dead letter** | A row whose fact has no wire shape, published raw to `warehouse.exception-management.events.dlq` with `x-dlq-*` headers. |
| **Consumer group** | One of the six pinned Kafka groups (one per source topic family + the correlation group), read from env, never a string literal. |
