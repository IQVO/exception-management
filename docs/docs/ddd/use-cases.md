---
id: use-cases
title: Use cases
sidebar_label: Use cases
sidebar_position: 1
---

# Use cases

The application layer (`internal/application/usecases`) is thin: each use
case enforces one cross-aggregate rule, loads what it needs through the
ports, saves inside one unit of work and returns the events for the outbox.
This is the complete list — there is no use case that creates a case from
REST and none that reopens a resolved case.

## Consumer-driven use cases

### 1. `OpenOrTouchException` (`open_or_touch.go`)

The heart of the context. One consumed exception-class fact (a
`TaskCPTMissed`, a `BacklogThresholdBreached`, an at-risk acknowledgement,
an orphaned hold, a count discrepancy, an unlocated stock item, a diverted
package, a weight discrepancy, an unexpected return) becomes either:

- **Open** — first occurrence of the dedup key: the case is created under
  the current policy version for the source type (severity, queue,
  `sla_minutes`), with a frozen deep copy of the fact's `data` and the
  CloudEvents `id` that opened it. Raises `ExceptionOpened`.
- **Touch** — the key already has an open case: occurrence + 1,
  `lastSeenAt` advanced, one TOUCHED audit entry, no event.
- **Touch on a resolved case** — a fact arriving after RESOLUTION still
  counts (occurrence + 1) but NEVER reopens the case.

The CloudEvents `id` is claimed in the same transaction as the effect; a
replayed id is a no-op that still commits the offset. A source type with no
declared policy is WARN + commit-past (a config gap, not the message's
fault); a `TaskCPTMissed` arriving before its producer's topic exists is
the reader's internal retry, never unreadiness.

### 2. `AutoResolveOnCorrelation` (`auto_resolve.go`)

One consumed `OrderRepromised` closes the matching open case when the
policy of its source type carries an auto-resolve rule:

- reverses the case's source `order_ref` with `shared.ParseWorkUnitRef`
  (`{order_id}-line-{line_no}`) and compares the ORDER id — the correlation
  is order-level, the line number is ignored (`OrderRepromised` carries no
  line);
- the window edge is inclusive: `now <= lastSeenAt + withinMinutes`;
- a match records the rule id, the correlated fact's full type and id, and
  raises `ExceptionAutoResolved`;
- a mismatch is a silent no-op (the case stays open — fails soft, never
  errors the consumer). The rule id carries the derived deterministic form
  `<sourceType>-><onType>`.

`OrderRepromised` is never a case source; it opens nothing.

### 3. `SweepSLABreaches` (`sweep_sla.go`)

A bounded ticker (default once a minute, `SLA_SWEEP_INTERVAL`) passes `now`
to the pure `BreachesAt`: every open case past its `slaDueAt` that has not
been flagged gets the escalated flag and raises `ExceptionEscalated`
exactly once. The sweeper reports facts; it never blocks work.

## REST command use cases

### 4. `TriageException` (`commands.go`)

`POST /exceptions/{caseId}/triage-decisions` with `{severity, queue}`: sets or
changes severity and queue, records the operator as actor. Raises
`ExceptionTriaged`. Rejected with `409 case-already-resolved` on a terminal
case.

### 5. `AssignException`

`POST /exceptions/{caseId}/assignments` with `{assignee}`: gives the case an
owner; reassignment is allowed while unresolved. Raises
`ExceptionAssigned`.

### 6. `ResolveException`

`POST /exceptions/{caseId}/resolutions` with `{resolutionCode, rootCause,
note?}`: the terminal transition. Raises `ExceptionResolved`. Any later
command on the case is a `409 case-already-resolved` (`ErrCaseClosed`).

### 7. `DeclareExceptionPolicy` (`declare_policy.go`)

`PUT /exception-policies/{sourceType}`: appends the next version of the
handling policy (severity, queue, `slaMinutes`, optional
`autoResolve{onType, withinMinutes}`). The receiver is never mutated;
history is append-only. Raises `ExceptionPolicyDeclared`. A redeclare
racing another writer of the same next version is
`409 concurrent-modification`.

## Read use cases

### 8. `ListOpenExceptions` (`reads.go`)

`GET /exceptions` — the operator listing. Despite the historical name
(inherited from the guide's "open cases" view), the contract is: every case
matching the filters, whatever its status, unless a `status` filter names
one (resolve activity is part of the record). Filters: `status`, `queue`,
`site`, `sourceType`. REST answers one keyset page, `{items, nextCursor}`
(`limit` 1–500, default 100, ordered by opened-at then case id, newest first);
the use case's `Page` serves it and its `Execute` walks every page for the
`list_open_exceptions` MCP tool and the A2A skill, which return complete data.

### 9. `GetException`

`GET /exceptions/{caseId}` — the whole case: dedup identity, status,
severity, queue, occurrence count, escalated flag, assignee, the SLA clock,
the frozen `sourcePayload`, the resolution (and the auto-resolve evidence
when it closed itself) and the audit trail. Also behind `get_exception`.

### 10. `ListExceptionPolicies`

`GET /exception-policies` — the latest version per source type, ordered by
source type, one keyset page at a time (`{items, nextCursor}`). Its `Execute`
walks every page for `get_exception_policies` and the A2A skill.

### 11. `GetSLAReport`

The MCP `get_sla_report` view of the open caseload: `openCases`,
`breached` (open past `slaDueAt`), `escalated`, and the open counts
`bySeverity` and `byQueue`, stamped `generatedAt`.

## Read models

All reads project the `exception_cases` table directly (the case row IS
the snapshot — migration 0001 deliberately has no separate read-model
tables: the case freezes its own evidence, ADR 0003). `internal/application/views`
holds the wire DTOs; no aggregate type leaks past the application layer.
