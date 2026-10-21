---
id: aggregate-design-canvas
title: Aggregate design canvas
sidebar_label: Aggregate design canvas
sidebar_position: 14
description: The ddd-crew Aggregate Design Canvas applied to the ExceptionCase and ExceptionPolicy aggregates — invariants, boundaries, consistency rules and lifecycle.
---

# Aggregate design canvas

Two aggregates, both pure Go under `internal/domain` (no I/O, no clock
reads; they import nothing outside `internal/domain`). ADR 0002 is the
decision record.

## ExceptionCase (`internal/domain/exceptioncase`)

**Aggregate root.** One owned anomaly.

| Canvas section | Value |
| --- | --- |
| **Identity** | `case_id` (`case-<uuid>`, minted by `system.CaseIDs` at open — the real IDGen, never a test double in `cmd/`). |
| **Natural key (dedup)** | (source type, source subject, site) — the dedup key; at most one OPEN case per key (partial unique index `uq_exception_cases_one_open_per_key` backs the rule across cases). |
| **Contains** | The frozen source payload (deep copy, JSONB), the dedup key parts, status, severity, queue, assignee, escalated flag, occurrence count, `openedAt`/`lastSeenAt`, the SLA clock (`slaMinutes`, `slaDueAt`), the resolution (code, root cause, note) or the auto-resolution evidence (rule id, correlated type + event id), and the audit trail (exactly one entry per accepted transition). |
| **Does not contain** | Any producer state (no order copy, no task copy, no stock level — ADR 0003), any reference to re-fetch, any clock or randomness. |
| **Invariants** | One direction only `OPEN -> TRIAGED -> ASSIGNED -> RESOLVED`; `ESCALATED` is a flag; `RESOLVED` requires resolution code + root cause and is terminal (`ErrCaseClosed`, 409 at the edge, the aggregate raises nothing); occurrence >= 1 and `lastSeenAt >= openedAt`; the frozen payload never changes after open; the aggregate refuses to open a second case for a key it holds open. |
| **Consistency** | Invariants enforced immediately inside the aggregate; the cross-case one-open-per-key rule enforced by the use case plus the partial unique index (the loser of a race gets `ErrOpenCaseExists`); persistence guarded by optimistic concurrency (`version`, +1 per accepted transition, `WHERE version = excluded.version - 1`). |
| **Lifecycle** | Open (from a consumed fact) → maybe Touch many times → Triage / Assign / reassign in any order while unresolved → Resolve (operator) or AutoResolve (policy rule) terminal. A Touch after resolution counts the occurrence and never reopens. |
| **Commands** | `Open`, `Touch`, `Triage`, `Assign`, `Reassign`, `Resolve`, `AutoResolve`, `BreachesAt(now)` (pure query). |
| **Events raised** | `ExceptionOpened`, `ExceptionTriaged`, `ExceptionAssigned`, `ExceptionEscalated`, `ExceptionResolved`, `ExceptionAutoResolved` — plain structs with `OccurredAt()`, no CloudEvents knowledge; `Touch` raises nothing on the wire. |
| **Cross-aggregate rule** | None at the aggregate level by design: policies are read at open (use case), so a case never holds a policy reference; a new policy version governs cases opened after its declaration. |

## ExceptionPolicy (`internal/domain/exceptionpolicy`)

**Aggregate root.** The handling policy of ONE source type.

| Canvas section | Value |
| --- | --- |
| **Identity** | (source type, version) — versioned, append-only: `Declare` creates version 1, `Redeclare` returns the NEXT version as a new value, the receiver is never mutated, history is never rewritten. |
| **Contains** | Severity, queue, `slaMinutes`, the optional `AutoResolveRule{OnType, WithinMinutes}` (data, never code), `declaredAt`. |
| **Invariants** | Source type is a full CloudEvents type and non-blank; severity valid; queue non-blank; `SLAMinutes >= 1`; `Version >= 1`; `WithinMinutes >= 1` when the rule exists. |
| **Consistency** | The version is part of the primary key; two writers racing the same next version collide into `ErrConcurrentModification` (409 at the edge). |
| **Lifecycle** | Append-only version chain; no update in place, no delete. |
| **Commands** | `Declare`, `Redeclare`; the pure matcher `Matches(correlatedType, subject, case, now)` evaluates the rule (same subject, inclusive window `now <= lastSeenAt + withinMinutes`). |
| **Events raised** | `ExceptionPolicyDeclared` (subject and Kafka key = the source type). |
| **Why the rule is data** | An operator declares it through `PUT /exception-policies/{sourceType}` without a deploy, every version is auditable, and the ops-agent may READ it and recommend but never execute it (ADR 0002). The rule id carries the derived deterministic form `<sourceType>-><onType>`. |

## Shared kernel (`internal/domain/shared`)

Value objects only: `DedupKey`, `Actor`, severity, `CaseID`/`PolicyID`
types, and `ParseWorkUnitRef` — the reversal of WES's frozen work-unit
formula `{order_id}-line-{line_no}` (order-management ADR 0018) used by
the correlation.
