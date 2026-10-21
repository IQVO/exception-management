# exception-management

Exception Management ("Problem Solve"): WES-tier Supporting bounded context of the warehouse-systems fleet. It turns the anomaly facts other contexts already publish (missed CPTs, backlog breaches, at-risk acknowledgements, count discrepancies, diverted packages, unexpected returns) into owned cases with triage, assignment, an SLA clock, resolution and root cause. Hexagonal Go, Postgres, REST, Kafka (CloudEvents 1.0).

- Design and pinned contracts: `docs/adr/0001-exception-management-bounded-context.md` and the fleet plan `.hermes/plans/2026-10-20_231017-returns-processing-and-exception-management.md`.
- Scaffolded from `warehouse-harness-template` (v4). Guides: `.claude/rules/`, sensors: `HARNESS.md`.
- Commands: `make check-fast`, `make check-all`, `make integration`.

## What it does

- Opens exactly one case per dedup key (source type, subject, site) from a consumed fact; a repeated fact touches the open case instead of opening another.
- Operators triage, assign and resolve cases over REST (`/exceptions/{caseId}/triage|assign|resolve`); every transition records its actor.
- A versioned `ExceptionPolicy` per source type sets severity, queue, SLA and a declarative auto-resolve rule (for example `OrderRepromised` closes the `TaskCPTMissed` case of the same work unit).
- A bounded sweeper flags SLA breaches (`ExceptionEscalated`) without blocking work.
- Publishes case facts through a transactional outbox; exposes read-only MCP tools for warehouse-ops-agent (which stays zero-write).
- Keeps frozen local copies of the source payloads. It never calls a sibling over REST or MCP. Runs on real Postgres and real Kafka only. No auth.

## Status

Bootstrap commit: harness, guides and ADR 0001. Contracts, domain, application, adapters, packaging and docs follow the plan's waves.
