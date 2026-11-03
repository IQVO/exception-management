# ADR 0009: Agent authority levels and complete MCP surface

## Status

Accepted (2026-11-02). Applies the fleet decision "Agentic Standard v2"
(`docs/architecture/agentic-standard.md` and the fleet rule
`agents/fleet/agent-authority.md` in IQVO/warehouse-docs) to this context.
Extends [ADR 0007](/docs/adr/0007-a2a-inbound-adapter) (A2A) and the MCP
charter documented in `docs/docs/mcp/tools.md`; changes no event, no tool
name, argument or result, and nothing in
[ADR 0004](/docs/adr/0004-cloudevents-envelope-and-type-catalogue).

## Context

This context's agent surface is deliberately read-only: four MCP tools
(`list_open_exceptions`, `get_exception`, `get_exception_policies`,
`get_sla_report`) and two A2A skills (`sla_status`, `find_exceptions`), all
backed by the same read use cases REST uses. Triage, assign, resolve and
policy declaration stay on REST (the MCP charter;
`governance_test.go` fails the build on a write-verb tool name).

Two things were missing for a delegating agent — such as warehouse-ops-agent
— to use this surface responsibly:

1. **Nothing declared how much an agent may do with a tool.** The MCP
   `ReadOnlyHint` annotations and the A2A `read` tags say *whether* a call
   changes state, not *what level of authority* the call represents, so a
   caller cannot apply uniform gating across the fleet.
2. **The MCP surface was tools-only.** No resource let an agent load context
   data without a tool call, and no prompt standardised the operator
   procedure, so every client improvised its own triage workflow.

Separately, the fleet's MCP servers used the Streamable HTTP handler with
its default (stateful) options, which advertises only the 2025-11-25
revision and older, so a client speaking the 2026-07-28 per-request
handshake was refused with `-32022`.

## Decision

### Authority levels, declared where the tool is registered

Authority levels are the strings `L1`..`L4`. In this context:

| Surface | Name | Level | Why |
|---|---|---|---|
| MCP | `list_open_exceptions` | `L1` | reads facts |
| MCP | `get_exception` | `L1` | reads facts |
| MCP | `get_exception_policies` | `L1` | reads facts |
| MCP | `get_sla_report` | `L1` | reads facts (the SLA report read model) |
| A2A | `sla_status` | `L1` | reads; same level as the report it fronts |
| A2A | `find_exceptions` | `L1` | reads; same level as the tools it fronts |

- **MCP** declares the level in the tool's `Meta` under
  `com.warehouse/authority`, via the pinned shape
  `Meta: authorityMeta(authorityL1)` with the package consts; the SDK
  returns it verbatim in `tools/list` `_meta`, and
  `testdata/tool_registry.golden.json` pins it (a level can only change
  through a reviewed golden edit).
- **A2A** declares it as the skill tag `authority:L1` on the Agent Card.
- **The whole surface is L1.** Every tool and skill is a pure read of this
  context's own state; nothing computes a recommendation (L2) and nothing
  changes state (L3). **L4 is not granted** — no ADR in this context lets an
  agent act without a human, and the charter has no write operation at all.

The level is a label for callers and reviewers; **it is not access control**
and nothing here decides anything from it (the surface stays unauthenticated,
like REST, per the fleet decision).

### Stateless MCP

`mcp.NewStreamableHTTPHandler` now receives
`&mcp.StreamableHTTPOptions{Stateless: true}`. No `Mcp-Session-Id` is issued
and the server answers `server/discover` for the 2026-07-28 revision, while
a legacy `initialize` (for example `2025-06-18`) still answers 200, so
existing session clients keep working. Nothing in this adapter relied on
cross-call session state; a tool that ever needs it takes an explicit,
server-minted handle as an argument. `stateless_test.go` pins all three wire
facts with raw POSTs, and reverting the option to nil is proven to break the
first two (`-32022`).

### One resource and one prompt

Per the Agentic Standard an MCP server offers tools plus at least one
resource and one prompt (AS-3). This context adds exactly one of each,
staying read-only:

- **Resource** `warehouse://exception-management/sla-report` — the SLA
  report read model as a bounded JSON document (open/breached/escalated
  counts, by severity and queue), built from the same `GetSLAReport` use
  case the `get_sla_report` tool reads, so the two can never disagree. It is
  never a dump of a table.
- **Prompt** `triage_exception_caseload` — the operator SOP: read the SLA
  report, list the cases behind the breaches, read the cases and the
  declared policies, and recommend which cases a human triages, assigns or
  resolves first. It names the four read tools but never calls them, and it
  states that the write operations stay on REST.

### Sensors

Two layers enforce the declarations, both in CI:

- **Static** (this repo's copy of the harness template's
  `internal/architecture/mcp_authority_fitness_test.go`):
  `TestEveryMCPToolDeclaresAuthority`, `TestMCPWriteToolsAreAtLeastL3`,
  `TestA2ASkillsDeclareAuthority` and `TestMCPServerOffersResourceAndPrompt`
  scan the adapter sources with `go/ast`; a tool without a Meta, a skill
  without a tag, a write tool under L3, or a tools-only surface fails the
  build.
- **Runtime-wired** (`authority_test.go` in both adapter packages):
  `TestEveryAgentToolDeclaresAuthority` lists every tool over the real
  handler and every skill on the real card and asserts one valid level each.

## Consequences

- `tools/list` responses gain a `_meta` object per tool and the Agent Card
  skills gain a tag; both are additive — names, arguments, annotations and
  results are unchanged, and unknown arguments stay rejected.
- The warehouse-docs agent catalogue (built offline from this repo's pinned
  golden and the A2A card index) picks the new levels up on its next
  refresh; no other consumer reads the field yet.
- A future write tool or skill would arrive as L3 with `initiated_by`
  attribution per contracts G3/G4; that needs its own ADR, because today the
  charter has no write surface at all.
