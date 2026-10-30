# ADR 0007: A2A inbound adapter (read-only third driving adapter)

## Status

Accepted (2026-10-29). Mirrors the reference rollout in workforce-management
(ADR-0039) and wes-work-planning (ADR-0046), minus the write skill: this
context's MCP charter is read-only and the A2A surface honours it. Builds on
ADR 0001 (the context and its REST/MCP surfaces), ADR 0004 (CloudEvents on
every Kafka message) and the fleet rule that every endpoint is unauthenticated
(`.claude/rules/fleet/no-auth-and-mcp.md`).

## Context

The fleet is adopting the Agent2Agent (A2A) protocol so that a bounded context
can **delegate** a goal to another context's agent. exception-management
already exposes its read use cases over REST and, read-only, over MCP
(`list_open_exceptions`, `get_exception`, `get_exception_policies`,
`get_sla_report`; `docs/docs/mcp/tools.md`). The MCP charter is explicit:
triaging, assigning and resolving a case and declaring a policy stay on REST
(operators decide), the agent only reads.

Three transports now carry three different things and must not overlap:

| Transport | Carries |
| --- | --- |
| Kafka + CloudEvents | **facts** between contexts (ADR 0004) |
| MCP | an agent's **tools** |
| A2A | **delegation** of a goal to another context's agent |

## Decision

Add `internal/adapters/inbound/a2a`, a third driving adapter over the **same**
read use cases (`ListOpenExceptions`, `GetException`, `ListExceptionPolicies`,
`GetSLAReport`) as the REST and MCP adapters, built on the official
`a2aproject/a2a-go/v2` SDK (pinned v2.7.0) and served by a new `cmd/a2a` binary
(JSON-RPC 2.0 over HTTP, port **8093**, the Agent Card at
`/.well-known/agent-card.json`, the endpoint mounted at `/` and `/a2a`).

### The agent is READ-ONLY (the write gate)

A skill may change state only if the context's MCP surface already offers that
write. It does not, so **neither skill writes**. `Deps` holds the four read use
cases and nothing else; `cmd/a2a` builds no event sink, no unit of work and no
outbox repository, never starts the relay and never dials Kafka. Three tests
pin it: `TestDepsHoldNoWriteUseCase` (reflection over `Deps`),
`TestA2AAdapterReferencesNoWriteUseCase` (the adapter's sources may name only
the four read use cases) and `TestCardOffersNoWriteVerb`. A2A therefore never
publishes a fact; the "same use case + outbox" rule has nothing to apply to
here. If a write skill is ever wanted it needs a new ADR that widens the MCP
charter first, not an A2A-only widening.

### Two curated skills, not one per endpoint

| Skill | Kind | What it does |
| --- | --- | --- |
| `sla_status` | read | how the open caseload sits against its SLA clocks: `openCases`, `breached`, `escalated`, `bySeverity`, `byQueue`; with `includePolicies=true` also the policy declared per source type. Maps to `get_sla_report` (+ `get_exception_policies`). |
| `find_exceptions` | read | one case by `caseId`, or the cases matching `status`, `queue`, `siteId`, `sourceType` (no argument lists every case, open and resolved alike), newest opened first. Maps to `get_exception` / `list_open_exceptions`. |

The four MCP tools are folded into two goals ("how are we against SLA?" and
"show me the cases"): the policy list is only ever asked alongside the SLA
posture, and a case lookup by id is the narrow form of the filtered list.

### Message-only, answered in-band

Both skills are transactional request/response reads, so the agent is
message-only (no Task lifecycle). Rejections and malformed requests are
**answered** as RFC 7807-shaped `problem`s inside the message envelope (`type`
under the REST adapter's own `https://errors.exception-management...` base,
`title`, `detail`, `status`), never raised as JSON-RPC errors, with the **same
classification and slugs as the REST adapter**: `404 case-not-found`,
`400 invalid-case-id` / `invalid-status`, `500 internal-error` (the detail is
fixed text; DSNs and SQL never leave the process). A read raises no write
conflict (`409`), and REST reports `503` only on its readiness probe, so
neither is mapped. The A2A-specific request-shape problems are
`malformed-skill-request`, `unknown-skill` and `invalid-argument` (all `400`).

### Strict input

- Skill arguments are decoded with `DisallowUnknownFields` and trailing data is
  rejected, so a misspelled filter (`statu`) cannot silently list every case.
  Go's `encoding/json` matches names case-insensitively, so only genuinely
  unknown names are refused.
- `caseId` is validated exactly as REST validates `{caseId}` and cannot be
  combined with the list filters.
- A U+0000 in any string argument is refused with `400 malformed-request-body`,
  as the REST adapter's NUL guard does: PostgreSQL text cannot hold it and a
  statement carrying one aborts the transaction, so it would be a `500` on the
  real runtime. (The MCP list tool has no such guard today; see Consequences.)

### Unauthenticated, like REST and MCP

The card declares no security schemes: this surface is unauthenticated by the
same fleet-wide decision as REST and MCP (`TestNoAuthMiddlewareReintroduced`
covers the new package). Schemes are added to the card, not a redesign, if that
decision is ever reversed.

### Boundaries

- `internal/adapters/inbound/a2a` depends only on the application and domain
  layers and nothing depends on it (`TestA2AAdapterDependencyRule`); it never
  imports the SDK client (`TestA2AAdapterIsServerOnly`). **No outbound A2A
  client is added**: this context asks no sibling anything.
- Real Postgres only: `cmd/a2a` refuses to boot without `DATABASE_URL` and
  without `A2A_AGENT_URL` (an absolute http(s) URL; the card must advertise a
  dialable address). `TestA2AAnswersFromRealPostgresAndNeverWritesTheOutbox`
  (build tag `integration`) drives both skills over the real JSON-RPC wire on
  testcontainers Postgres and asserts the outbox is untouched.

## Consequences

- A fourth deployable (`/app/a2a`, built by the Dockerfile's `cmd/*` loop) and
  an `a2a.*` chart block (`<release>-a2a`, default **off**), exposed through
  Kong at `/api/a2a/exception-management` rewritten to `/`. The chart tests
  assert the Deployment carries no Kafka, publisher, relay, sweeper or
  consumer-group variable. Port 8093 is the fleet's A2A port (8090 mcp, 8091
  projector admin, 8092 reports).
- Delegating *to* this agent does not change who decides: triage, assignment
  and resolution stay operator work on REST. A delegating agent can see the
  caseload and the SLA posture; it cannot act on a case.
- The agent is stateless and read-only, so any number of replicas is safe.
- `list_open_exceptions` over MCP passes filter strings to Postgres unchecked
  (no NUL guard, no `status` validation), unlike REST and A2A. Not changed
  here; a follow-up should align MCP with REST.
