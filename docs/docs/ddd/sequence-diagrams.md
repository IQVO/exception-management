---
id: sequence-diagrams
title: Sequence diagrams
sidebar_label: Sequence diagrams
sidebar_position: 19
description: The end-to-end flows as sequence diagrams — open and touch, operator lifecycle, the OrderRepromised auto-resolve, the SLA sweep, and the outbox relay.
---

# Sequence diagrams

## A fact opens a case (first occurrence)

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka (warehouse.fulfillment.events)
    participant C as fulfillment consumer
    participant UC as OpenOrTouchException
    participant AG as ExceptionCase
    participant PG as Postgres
    participant OB as outbox relay
    participant T as warehouse.exception-management.events

    K->>C: TaskCPTMissed (CloudEvent)
    C->>UC: decode, dispatch on full type
    UC->>PG: BEGIN claim event id
    UC->>PG: find open case by dedup key
    UC->>AG: Open(OpenInput) - policy vN of the source type
    AG-->>UC: ExceptionOpened
    UC->>PG: INSERT exception_cases + outbox (one tx)
    UC->>C: commit Kafka offset
    OB->>T: relay publishes, deletes row
```

A repeated fact (the sweeper re-fires `TaskCPTMissed` every tick) runs the
same path but returns **Touch**: occurrence + 1, `lastSeenAt`, one TOUCHED
audit entry, no event on the wire. A fact arriving after RESOLUTION also
touches — it never reopens.

## Operator lifecycle: triage, assign, resolve

```mermaid
sequenceDiagram
    autonumber
    participant O as Operator or console
    participant H as REST (chi + Idempotency-Key)
    participant UC as Triage or Assign or Resolve
    participant AG as ExceptionCase
    participant PG as Postgres
    participant T as warehouse.exception-management.events

    O->>H: POST /exceptions/{caseId}/triage-decisions
    H->>H: Idempotency-Key middleware (replay = stored answer)
    H->>UC: severity + queue, actor = operator
    UC->>AG: Triage(...)
    AG-->>UC: ExceptionTriaged
    UC->>PG: UPDATE ... WHERE version = expected - 1 + outbox row
    UC-->>O: 200, the case
    Note over H,T: resolve records code + root cause and is terminal - a later command is 409 case-already-resolved
```

## Auto-resolve: OrderRepromised closes the missed-CPT case

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka (warehouse.order-management.events)
    participant C as order-correlation consumer
    participant UC as AutoResolveOnCorrelation
    participant AG as ExceptionCase
    participant PG as Postgres

    K->>C: OrderRepromised
    C->>UC: decode (correlation only, never a case source)
    UC->>PG: BEGIN claim event id
    UC->>PG: scan open TaskCPTMissed cases
    UC->>UC: ParseWorkUnitRef(order_ref) reverse to order id
    UC->>UC: order ids match AND now less equal lastSeenAt + withinMinutes
    UC->>AG: AutoResolve(rule, correlated fact)
    AG-->>UC: ExceptionAutoResolved
    UC->>PG: UPDATE case + outbox row (one tx)
    Note over UC: no match is a no-op - the case stays open, never an error
```

## The SLA sweep

```mermaid
sequenceDiagram
    autonumber
    participant TI as ticker (SLA_SWEEP_INTERVAL, default 1m)
    participant UC as SweepSLABreaches
    participant PG as Postgres
    participant T as warehouse.exception-management.events

    TI->>UC: tick(now)
    UC->>PG: open cases, unflagged, slaDueAt less than now
    UC->>UC: Escalate each (BreachesAt is pure)
    UC->>PG: set escalated + outbox ExceptionEscalated (once per case)
    UC-->>T: ExceptionEscalated
    Note over UC: the sweeper reports and flags - it never blocks work
```

## Everything leaves through the outbox relay

```mermaid
sequenceDiagram
    autonumber
    participant R as relay (OUTBOX_RELAY_INTERVAL, default 1s)
    participant PG as Postgres
    participant K as Kafka
    participant D as warehouse.exception-management.events.dlq

    R->>PG: claim up to 100 due rows FOR UPDATE SKIP LOCKED
    loop each row in id order
        alt send ok
            R->>K: publish CloudEvent (key = case id)
            R->>PG: DELETE row (same tx - removal is the sent signal)
        else send error
            R->>PG: attempts + 1, next_attempt_at = 2^attempts s (cap 5 min)
            Note over R: rows behind the failure stay for the next pass
        else no wire shape (poison)
            R->>D: publish row raw with x-dlq-* headers, delete row
        end
    end
```

A row reaching `OUTBOX_MAX_ATTEMPTS` (default 20) is excluded from the
claim, logged once at ERROR and stays in the outbox for inspection;
re-arming is a runbook operation
([Runbook](/docs/operations/runbook#outbox-relay)).
