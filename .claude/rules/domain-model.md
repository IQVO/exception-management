# Domain model: ubiquitous language, aggregates, events, use cases

## Ubiquitous Language (use these exact names)

- **ExceptionCase (case)** — one owned anomaly with a lifecycle; exactly one open case per dedup key.
- **Source fact** — the producer's event that opened or touched the case; its payload is frozen on the case.
- **Dedup key** — (source type, source subject, site); a repeated fact touches the open case instead of opening another.
- **Occurrence** — one arrival of the source fact; the case counts them and keeps `lastSeenAt`.
- **Queue / severity** — triage outcome, from the policy or an operator.
- **SLA clock** — `openedAt + policy.slaMinutes`; breach sets the `ESCALATED` flag.
- **Resolution code / root cause** — mandatory on resolve; `RESOLVED` is terminal.
- **ExceptionPolicy** — per source type: default severity, queue, SLA minutes, auto-resolve rule; append-only versions.
- **Auto-resolve rule** — declarative: "close when `<other type>` arrives with the same subject within N minutes".

## Aggregates

- **ExceptionCase** (`internal/domain/exceptioncase`, the richest aggregate and the mutation-gate package): `OPEN -> TRIAGED -> ASSIGNED -> RESOLVED`; `ESCALATED` is a flag; one open case per dedup key; every transition records actor (operator id, policy id or `system`); holds a frozen copy of the source payload; `RESOLVED` requires a resolution code.
- **ExceptionPolicy** (`internal/domain/exceptionpolicy`): versioned, append-only; the auto-resolve rule is data, not code.

Read models (projections, never aggregates): open cases by queue, SLA report, root-cause frequency.

## Domain events

- `ExceptionOpened` — a source fact created a case (first occurrence).
- `ExceptionTriaged` / `ExceptionAssigned` — severity/queue set; owner took it.
- `ExceptionEscalated` — SLA breached (flag set, work continues).
- `ExceptionResolved` — resolution code + root cause recorded (terminal).
- `ExceptionAutoResolved` — a policy rule closed it on a correlated fact.
- `ExceptionPolicyDeclared` — new policy version.

## Key use cases (application layer, a later step of the service build)

- `OpenOrTouchException` — consumer-driven, idempotent on the CloudEvents `id`; opens or touches by dedup key.
- `TriageException`, `AssignException`, `ResolveException` — operator commands (REST).
- `AutoResolveOnCorrelation` — consumer-driven; applies the policy rule (e.g. `OrderRepromised` closes the `TaskCPTMissed` case for the same work unit).
- `SweepSLABreaches` — bounded ticker; sets the flag and raises `ExceptionEscalated`.
- `DeclareExceptionPolicy` — new policy version.
- `ListOpenExceptions`, `GetException`, `GetSLAReport` — reads (REST + read-only MCP).
