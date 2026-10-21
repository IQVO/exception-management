---
id: domain-message-flow
title: Domain message flow
sidebar_label: Domain message flow
sidebar_position: 15
description: A fact's journey from a producer's topic through the consumers and use cases to a resolved case and the events that announce it.
---

# Domain message flow

One page, end to end: how an anomaly fact becomes a resolved case. Every
box below is a real package in this repo.

```mermaid
flowchart TB
    subgraph PRODUCERS["Producer topics (Kafka, CloudEvents 1.0)"]
        T1["warehouse.fulfillment.events"]
        T2["warehouse.work-planning.events"]
        T3["warehouse.network-fulfillment.events"]
        T4["warehouse.order-management.events"]
        T5["warehouse.returns-processing.events"]
        T6["warehouse.inventory.events"]
    end
    subgraph INBOUND["internal/adapters/inbound/kafka"]
        C1["fulfillment consumer"]
        C2["work-planning consumer"]
        C3["network-fulfillment consumer"]
        C4["order-correlation consumer"]
        C5["returns consumer"]
        C6["inventory consumer"]
    end
    subgraph APP["internal/application/usecases"]
        OT["OpenOrTouchException"]
        AR["AutoResolveOnCorrelation"]
        SW["SweepSLABreaches"]
        CMD["Triage / Assign / Resolve"]
        DEC["DeclareExceptionPolicy"]
    end
    subgraph DOM["internal/domain"]
        CASE["ExceptionCase"]
        POL["ExceptionPolicy"]
    end
    subgraph OUT["Outbound (same transaction)"]
        PG[("Postgres<br/>exception_cases,<br/>exception_policies,<br/>processed_events,<br/>outbox, idempotency_keys")]
        OB["outbox rows"]
    end
    RELAY["outbox relay<br/>(OUTBOX_RELAY_INTERVAL, attempt cap)"]
    EVT["warehouse.exception-management.events"]
    DLQ["warehouse.exception-management.events.dlq"]

    T1 --> C1 --> OT
    T2 --> C2 --> OT
    T3 --> C3 --> OT
    T5 --> C5 --> OT
    T6 --> C6 --> OT
    T4 --> C4 --> AR
    C1 -- "TaskCPTMissed.order_ref" --> AR

    OT --> CASE
    OT -- "policy of the source type" --> POL
    AR --> CASE
    SW --> CASE
    CMD --> CASE
    DEC --> POL

    CASE -- "Save + events" --> PG
    POL -- "Save + events" --> PG
    CASE -- domain facts --> OB
    POL -- domain facts --> OB
    REST["REST commands + reads"] --> CMD
    MCG["MCP tools (read-only)"] --> RD["read use cases"]
    Ticker["SLA ticker"] --> SW

    OB --> RELAY --> EVT
    RELAY -- "poison rows" --> DLQ
```

The flow in words:

1. **A fact lands** on one of the six consumed topics; the consumer under
   its pinned group id decodes the CloudEvents envelope
   (`internal/adapters/kafka/cloudevents`) and dispatches on the FULL type;
   unknown types are ignored; a non-CloudEvent message is WARN + skip.
2. **Id claim first.** The CloudEvents `id` is claimed in the SAME Postgres
   transaction as the effect; a replayed id is a no-op that still commits
   the Kafka offset (`FetchMessage` + `CommitMessages` — offset only after
   a durable effect).
3. **Open or touch** by the dedup key (source type, source subject, site),
   under the policy version current for the source type. Open freezes the
   payload, computes the SLA clock and writes `ExceptionOpened` to the
   outbox; touch bumps occurrence and `lastSeenAt` only.
4. **Correlation.** An `OrderRepromised` reverses the open case's source
   `order_ref` to an order id, compares (order level), checks the inclusive
   window against `lastSeenAt`, and auto-resolves on match.
5. **Operators** triage, assign and resolve over REST behind the
   `Idempotency-Key` middleware; the ops-agent only reads (REST + MCP).
6. **Everything leaves through the outbox**: the relay drains due rows,
   encodes to CloudEvents, deletes on publish, backs off exponentially,
   caps attempts (`OUTBOX_MAX_ATTEMPTS`) and dead-letters deterministic
   poison (ADR 0005).
