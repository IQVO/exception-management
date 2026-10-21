---
id: eventstorming
title: EventStorming
sidebar_label: EventStorming
sidebar_position: 16
description: Big-picture EventStorming view — the domain events this context owns (orange), the facts it consumes (pink), the commands (blue) and the policies (purple) that connect them.
---

# EventStorming

Big-picture view in the EventStorming colour convention: **domain events we
publish** (orange), **facts we consume** (pink), **commands** (blue),
**policies / automated rules** (purple), **read models** (green). Time
flows down; one lane per aggregate.

```mermaid
flowchart TB
    subgraph SOURCES["Facts consumed (pink) - six consumer groups"]
        direction LR
        F1["TaskCPTMissed<br/>(re-fires each sweep)"]
        F2["PackageDiverted"]
        F3["WeightDiscrepancyDetected"]
        F4["BacklogThresholdBreached"]
        F5["AcknowledgementDeadlineAtRisk"]
        F6["OrphanedHoldCancelled"]
        F7["UnexpectedReturnReceived"]
        F8["DiscrepancyDetected"]
        F9["ItemUnlocated"]
        R1["OrderRepromised<br/>(correlation only)"]
    end

    P1["Policy: one open case per<br/>dedup key - repeat touches"]
    P2["Policy: SLA sweeper<br/>(bounded ticker)"]
    P3["Policy: auto-resolve rule<br/>(data, from ExceptionPolicy)"]

    subgraph CASELANE["ExceptionCase lane"]
        E1["ExceptionOpened"]
        E2["ExceptionTriaged"]
        E3["ExceptionAssigned"]
        E4["ExceptionEscalated"]
        E5["ExceptionResolved"]
        E6["ExceptionAutoResolved"]
    end

    subgraph POLLANE["ExceptionPolicy lane"]
        C1["PUT /exception-policies/{sourceType}"]
        E7["ExceptionPolicyDeclared"]
    end

    T1["Triage (operator)"]
    T2["Assign (operator)"]
    T3["Resolve (operator)"]

    F1 --> P1
    F2 --> P1
    F3 --> P1
    F4 --> P1
    F5 --> P1
    F6 --> P1
    F7 --> P1
    F8 --> P1
    F9 --> P1
    P1 --> E1
    R1 --> P3
    P3 --> E6
    T1 --> E2
    T2 --> E3
    T3 --> E5
    P2 --> E4
    C1 --> E7
    E1 --> P2
```

## The timeline in words

| Step | Event | Caused by |
| --- | --- | --- |
| 1 | `ExceptionOpened` | First occurrence of a dedup key: a consumed fact (policy: open-or-touch under the declared handling policy). |
| — | *(no event)* | A repeated fact TOUCHES (occurrence + 1, `lastSeenAt`): the audit trail is the record, nothing hits the wire. |
| 2 | `ExceptionPolicyDeclared` | `PUT /exception-policies/{sourceType}` appends a policy version. |
| 3 | `ExceptionTriaged` / `ExceptionAssigned` | Operator commands (severity/queue; owner), each with the operator as actor. |
| 4 | `ExceptionEscalated` | Policy: the SLA sweeper finds `now` past `slaDueAt` on an unflagged open case — once per case. |
| 5a | `ExceptionResolved` | Operator records resolution code + root cause (terminal). |
| 5b | `ExceptionAutoResolved` | Policy: the declarative rule fired on a correlated fact (`OrderRepromised` within the window of the same work unit's order). |

## Hotspots the model answers

- **Re-firing sources** (`TaskCPTMissed` every sweep tick): touch, never a
  second case — the dedup key plus occurrence counting.
- **The fact that arrives after resolution**: counts, never reopens —
  `RESOLVED` is terminal by invariant.
- **Who caused it**: every event carries its actor (operator id, policy id
  or `system`).
- **Poison on the way OUT**: no wire shape → raw to the DLQ (ADR 0005).

## Pivotal events not modelled (deliberately)

- *Case reopened* — rejected for v1 (ADR 0001/0002); a reopening need
  materialises as a new fact → new case.
- *Case created manually* — rejected for v1; every case is a fact.
- *Associate paged* — external channel, would need an ACL (ADR 0001).
