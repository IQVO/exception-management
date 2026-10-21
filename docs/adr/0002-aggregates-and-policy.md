# ADR 0002: the ExceptionCase and ExceptionPolicy aggregates, their invariants and state machines

## Status

Accepted (2026-10-21).

## Context

ADR 0001 gives this context one job: own the lifecycle of the anomalies other
contexts detect. The producers stay the single writer of their own state; the
facts they publish are the only way a case begins. This ADR fixes the two
aggregates, their invariants and the fixed, small state machine before any
persistence or transport exists (`.claude/rules/domain-model.md` states them;
this ADR is their decision record).

## Decision

Domain code lives under `internal/domain/` and imports nothing outside
`internal/domain/`. Three packages: `shared` (id value objects, the dedup key,
`Actor`, the work-unit parser `{order_id}-line-{line_no}`), `exceptioncase`
(the richest aggregate and the mutation-gate package), `exceptionpolicy`.

### ExceptionCase (package `exceptioncase`)

One owned anomaly: the source fact that opened it (frozen), its dedup key,
its triage, its owner, its SLA clock, its occurrence count and its resolution.

**Dedup key and occurrence counting.** The dedup key is (source type, source
subject, site). Exactly ONE open case exists per dedup key: a repeated fact
never opens a second case, it TOUCHES the open one — occurrence count + 1,
`lastSeenAt` advanced, one TOUCHED audit entry, and no new domain event on the
wire. (The rule spans cases, so the use case enforces it and a partial unique
index backs it in the persistence step; the aggregate itself refuses to open a
second case for a key it holds open.)

**State machine (fixed and small on purpose).**

- `OPEN -> TRIAGED -> ASSIGNED -> RESOLVED`, one direction only.
- `ESCALATED` is a FLAG set by the bounded SLA sweeper, never a state and
  never a blocker: an escalated case can still be triaged, assigned and
  resolved.
- `TRIAGED` sets/changes severity and queue (operator command or policy
  default at open). `ASSIGNED` sets the owner (an operator id); reassignment
  is allowed while unresolved.
- `RESOLVED` requires a resolution code and a root cause, records an optional
  note, and is TERMINAL: any later command is rejected as a 409-class error
  (`ErrCaseClosed`), the aggregate raises nothing.
- Every transition records its actor (operator id, policy id or `system`) and
  time; the audit list grows by EXACTLY one entry per accepted transition.

**SLA clock.** `slaDueAt = openedAt + policy.slaMinutes` (computed once at
open, from the policy version then current). `BreachesAt(now)` is a PURE
function — no clock reads inside the domain; the sweeper passes `now`.
Breach sets the escalated flag once and raises `ExceptionEscalated` exactly
once per case.

**Frozen source payload.** The case holds a deep copy of the consumed
fact's `data`, taken once at open. It never changes after open — not on
touch, not on any transition — and nothing re-fetches it. A case is
self-contained evidence.

**Open and Touch.** `Open` (idempotent on the CloudEvents `id` at the use
case; the aggregate validates the input) raises `ExceptionOpened`. `Touch`
raises nothing on the wire (the occurrence count and audit trail are the
record); it is a no-op on a resolved case's history — a fact arriving after
resolution still counts as an occurrence but cannot reopen the case.

**Auto-resolve (consumer-driven, declarative).** `AutoResolve` closes an open
case when the policy's rule fires: it records the rule id and the correlated
fact's full type and id, and raises `ExceptionAutoResolved`. It is never
agent-initiated (warehouse-ops-agent stays zero-write).

**Work-unit correlation.** `shared.ParseWorkUnitRef("ord-7-line-3")` reverses
WES's frozen work-unit formula `{order_id}-line-{line_no}` (order-management
ADR 0018). The `OrderRepromised` correlation uses it: the repromised
`order_id` must match the order id inside the open case's source `order_ref`,
and the correlated fact must arrive within the rule's window. No match is a
NO-OP — the case stays open (fails soft, never errors the consumer).

### ExceptionPolicy (package `exceptionpolicy`)

A versioned, append-only value per source type:
`{SourceType, Version, Severity, Queue, SLAMinutes, AutoResolve}`.

- Invariants: source type is a full CloudEvents type and non-blank; severity
  valid; queue non-blank; `SLAMinutes >= 1`; `Version >= 1`. `Declare`
  creates version 1, `Redeclare` returns the next version (the receiver is
  untouched); history is append-only — no update in place, no delete.
- `AutoResolveRule` is DATA, not code: `{OnType (full CloudEvents type),
WithinMinutes >= 1}`, nullable. The rule is evaluated by a pure matcher
(`Matches(correlatedType, subject, case, now)`) inside the domain; nothing
about it is executable or pluggable code. Why data: a rule an operator can
declare through `PUT /exception-policies/{sourceType}` without a deploy, with
an auditable version trail, that the ops-agent can READ and recommend on but
never execute. The rule matches only the SAME subject within
`within_minutes` of the case's last occurrence, and is a no-op otherwise.
- A new policy version governs cases opened after its declaration; open cases
  keep the severity, queue and SLA they were opened with.

### Domain events

Plain structs in `events.go` with `OccurredAt()`; they carry no CloudEvents or
Kafka knowledge — the outbound adapter maps them to the wire (ADR 0004):
`ExceptionOpened`, `ExceptionTriaged`, `ExceptionAssigned`,
`ExceptionEscalated`, `ExceptionResolved`, `ExceptionAutoResolved` (case),
`ExceptionPolicyDeclared` (policy).

### Read models (projections, never aggregates)

Open cases by queue, the SLA report and root-cause frequency are projections
fed by the events; they hold no decision logic.

## Consequences

- Mutation testing covers every domain package (`make mutation`, the
  `mutation-fast` CI job; thresholds in `.gremlins.yaml` after measurement).
- The one-open-case-per-key rule is enforced twice: aggregate (refuses a
  second open of the key it holds) and use case + partial unique index (the
  rule spans cases).
- The fixed four-status machine with a terminal RESOLVED keeps the model
  explainable; a generic workflow engine was explicitly rejected (ADR 0001).
- No manual case creation exists anywhere in the model: every constructor
  path starts from a consumed fact.
