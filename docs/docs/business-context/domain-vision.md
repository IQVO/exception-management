---
id: domain-vision
title: Domain vision
sidebar_label: Domain vision
sidebar_position: 1
---

# Domain vision

Every warehouse produces anomalies at a steady rate: a pick task misses its CPT, a pack bench's
backlog breaches its threshold, a network order is about to miss its acknowledgement deadline, a
cycle count disagrees with the system, a package diverts, a return arrives nobody authorized.
Each *producing* context already publishes these as facts. What no producing context can do is own
the aftermath — its job ends at the fact.

Without an owner, the anomaly trail looks like this:

- Facts scroll past in logs and dashboards; nobody is accountable for the response.
- The same repeating anomaly (a task that re-fires its CPT miss every sweep tick) reads as many
  problems instead of one problem happening often.
- There is no clock: nobody can say whether a problem was solved within the minutes the operation
  promised, or how bad it got before someone looked.
- Resolution knowledge — what actually went wrong, what fixed it — evaporates with the shift.

**Exception Management ("Problem Solve") is that owner.** Its vision statement:

> Every anomaly fact the fleet publishes becomes exactly one owned case, visible in a queue,
> carrying an SLA clock, and closed with a recorded root cause — automatically when a correlated
> fact proves the problem is gone, by a human otherwise.

What this context deliberately is **not**:

- Not a workflow engine: v1 has exactly one lifecycle (open → triage → assign → resolve), no
  reopen, no custom states.
- Not a fixer: it never calls a sibling context to correct anything. It consumes facts and records
  decisions ([ADR 0001](/docs/adr/0001-exception-management-bounded-context)).
- Not a case-entry UI: there is no manual case creation, because a case that does not trace to a
  consumed fact has no evidence chain.

The modelling that follows from this vision is on the
[Bounded context canvas](/docs/ddd/bounded-context-canvas); the day-to-day shape of the work is
the [Problem Solve lifecycle](/docs/business-context/problem-solve).
