# exception-management

Exception Management ("Problem Solve"): WES-tier Supporting bounded context of the warehouse-systems fleet. It turns the anomaly facts other contexts already publish (missed CPTs, backlog breaches, at-risk acknowledgements, count discrepancies, diverted packages, unexpected returns) into owned cases with triage, assignment, an SLA clock, resolution and root cause. Hexagonal Go, Postgres, REST, Kafka (CloudEvents 1.0).

- Design and pinned contracts: `docs/adr/0001-exception-management-bounded-context.md` and the fleet plan `.hermes/plans/2026-10-20_231017-returns-processing-and-exception-management.md`.
- Scaffolded from `warehouse-harness-template` (v4). Guides: `.claude/rules/`, sensors: `HARNESS.md`.
- Commands: `make check-fast`, `make check-all`, `make integration`.

## Docs

The Docusaurus site in `docs/` (ddd-crew pack, ADRs, generated REST reference, event catalogue,
MCP tools, operations pages):

```bash
cd docs && npm ci && npm run build     # npm run gen-api-docs regenerates the REST pages
```

Published to GitHub Pages by `.github/workflows/docs.yml` (PRs build, `main` deploys); the
`docs-api-drift` CI job fails a PR whose generated reference is out of sync with
`apis/openapi.yaml`.

## What it does

- Opens exactly one case per dedup key (source type, subject, site) from a consumed fact; a repeated fact touches the open case instead of opening another.
- Operators triage, assign and resolve cases over REST (`/exceptions/{caseId}/triage|assign|resolve`); every transition records its actor.
- A versioned `ExceptionPolicy` per source type sets severity, queue, SLA and a declarative auto-resolve rule (for example `OrderRepromised` closes the `TaskCPTMissed` case of the same work unit).
- A bounded sweeper flags SLA breaches (`ExceptionEscalated`) without blocking work.
- Publishes case facts through a transactional outbox; exposes read-only MCP tools for warehouse-ops-agent (which stays zero-write).
- Answers delegated goals over A2A (`cmd/a2a`, read-only: `sla_status`, `find_exceptions`); see [A2A agent](#a2a-agent).
- Keeps frozen local copies of the source payloads. It never calls a sibling over REST or MCP. Runs on real Postgres and real Kafka only. No auth.

## A2A agent

`cmd/a2a` is the context's Agent2Agent (A2A) agent, the third driving adapter
(`internal/adapters/inbound/a2a`) over the same read use cases as REST and MCP
([ADR 0007](docs/adr/0007-a2a-inbound-adapter.md)). It is **read-only**, like the MCP surface, and has
two goal-shaped skills:

| Skill | Answers |
| --- | --- |
| `sla_status` | the open caseload against its SLA clocks (open, breached, escalated, by severity and queue), optionally with the declared policies (`includePolicies`) |
| `find_exceptions` | one case by `caseId`, or the cases filtered by `status`, `queue`, `siteId`, `sourceType` |

Request: a message part `{"skill":"sla_status","arguments":{}}`; the answer is a data part
`{skill, data | problem}` plus a one-line text part. Rejections are answered in-band as RFC 7807-style
problems with the REST adapter's status and slug. A2A carries delegation only; it never writes and never
publishes a fact. Unauthenticated, like every endpoint of the fleet.

```bash
A2A_AGENT_URL=http://localhost:8093/ DATABASE_URL=postgres://... go run ./cmd/a2a   # :8093
curl localhost:8093/.well-known/agent-card.json
```

In Kubernetes it is the optional `a2a` chart component (`a2a.enabled`, default off; Service
`<fullname>-a2a:8093`, Kong path `/api/a2a/exception-management`).

## Status

Bootstrap commit: harness, guides and ADR 0001. Contracts, domain, application, adapters, packaging and docs follow the plan's waves.
