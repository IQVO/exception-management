---
id: class-diagram
title: Class diagram
sidebar_label: Class diagram
sidebar_position: 17
description: The domain packages' types and relationships — exceptioncase, exceptionpolicy, shared — exactly as they exist in internal/domain.
---

# Class diagram

The domain model exactly as it exists in `internal/domain` (field and
method names taken from the code; pure Go, no I/O).

```mermaid
classDiagram
    class Case {
        +ID id
        +DedupKey key
        +Status status
        +Severity severity
        +string queue
        +string assignee
        +bool escalated
        +int64 occurrence
        +Time openedAt
        +Time lastSeenAt
        +int64 slaMinutes
        +string sourceEventID
        +[]byte sourcePayload
        +resolution resolution
        +int64 version
        +[]AuditEntry audit
        +[]Event events
        +Touch(at)
        +Triage(severity, queue, actor, at)
        +Assign(assignee, actor, at)
        +Resolve(code, cause, note, actor, at)
        +Escalate(at)
        +AutoResolve(rule, correlated, at)
        +BreachesAt(now) bool
        +SLADueAt() Time
        +DomainEvents() []Event
        +Snapshot() Snapshot
    }
    class OpenInput {
        +string CaseID
        +DedupKey Key
        +Severity Severity
        +string Queue
        +int64 SLAMinutes
        +string SourceEventID
        +[]byte Payload
        +Time At
    }
    class resolution {
        +string code
        +string rootCause
        +string note
        +Time at
        +Actor actor
        +autoResolution auto
    }
    class autoResolution {
        +string ruleID
        +string correlatedType
        +string correlatedEventID
    }
    class AuditEntry {
        +string action
        +Actor actor
        +Time at
    }
    class DedupKey {
        +string sourceType
        +string sourceSubject
        +string site
    }
    class Policy {
        +string sourceType
        +int64 version
        +Severity severity
        +string queue
        +int64 slaMinutes
        +Rule autoResolve
        +Time declaredAt
        +Redeclare(in)
        +SLADue(openedAt) Time
        +Snapshot() Snapshot
    }
    class Rule {
        +string onType
        +int64 withinMinutes
        +ID(sourceType) string
        +Matches(m Match) bool
    }
    class Match {
        +string CorrelatedType
        +string CorrelatedOrderID
        +string CaseSourceRef
        +Time LastOccurrenceAt
        +Time Now
    }
    class Actor {
        +string id
    }

    OpenInput ..> Case : Open() OpenOrTouch() build one
    Case *-- resolution : set only at Resolve
    resolution *-- autoResolution : when the rule fired
    Case *-- AuditEntry : one per accepted transition
    Case --> DedupKey : identity across occurrences
    Policy *-- Rule : optional, data not code
    Rule ..> Match : pure evaluation
    Case ..> Actor : every transition records one
```

Facts worth knowing that the diagram cannot show:

- `Open` and `OpenOrTouch` are **package functions** of
  `exceptioncase` (they validate the input and build the aggregate);
  everything after open is a method on `*Case`.
- `Status` is a string type: `OPEN | TRIAGED | ASSIGNED | RESOLVED`;
  `ESCALATED` is the bool `escalated` flag, set once by `Escalate` (the
  sweeper), never a status value.
- `resolution` and `autoResolution` are unexported value types; the
  published evidence surfaces through the getters (`ResolutionCode`,
  `RootCause`, `Note`, `ResolvedAt`, `AutoResolution`) and the
  `exceptioncase.Snapshot`.
- `Rule.ID(sourceType)` derives the deterministic rule id
  `<sourceType>-><onType>` recorded on auto-resolved cases.
- `Match` carries everything the pure matcher needs — the correlated
  fact's type and order id, the case's frozen source `order_ref`, the last
  occurrence instant and `now` — so the domain reads no clocks.
- The domain events (`ExceptionOpened` … in each aggregate's `events.go`)
  are plain structs with `OccurredAt()`; they know nothing of CloudEvents
  or Kafka — the outbound adapter maps them (ADR 0004).
- Repositories persist and restore through the immutable `Snapshot`
  (`exceptioncase/snapshot.go`; `Restore` validates, so a corrupt row can
  never become a live aggregate); the views DTO
  (`internal/application/views`) is what inbound adapters see.
- `shared.ID` (case ids, `case-<uuid>` from `system.CaseIDs` at open),
  `shared.DedupKey`, `shared.Actor` and `shared.ParseWorkUnitRef` (the
  `{order_id}-line-{line_no}` reversal) are the shared value objects.
