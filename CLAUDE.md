# CLAUDE.md — exception-management

`exception-management` is a Supporting bounded context (`wes` tier,
CloudEvents subdomain `wes`, ADR 0001) of the `warehouse-systems` fleet (GitHub
org `IQVO`, Go module `github.com/claudioed/exception-management`): the
warehouse's "Problem Solve". It turns the anomaly facts other contexts already
publish (missed CPTs, backlog breaches, at-risk acknowledgements, count and
carton discrepancies, diverted or missed-load packages, unexpected returns,
yard seal, dwell and yard-check exceptions) into owned cases with triage,
assignment, an SLA clock, resolution and root cause. It owns the
`ExceptionCase` aggregate and the versioned `ExceptionPolicy` per source type.
Five binaries: `cmd/api` (REST `:8080`, the Kafka consumers, the outbox relay,
the SLA sweep), `cmd/mcp` (read-only MCP `:8090`), `cmd/a2a` (read-only A2A
`:8093`, ADR 0007), `cmd/exceptions-projector` (`:8091`, writer of the
analytical database) and `cmd/exceptions-reports` (`:8092`). Plus the `web/`
micro-frontend remote `exceptions_mfe`.

## Hard rules (each one is in an ADR or fitness-tested)

1. **Hexagonal dependency rule.** `internal/domain` depends on nothing internal
   but domain; `internal/application` only on domain and application; inbound
   and outbound adapters never depend on each other; only `cmd/` wires layers.
   `make arch-test` (`TestHexagonalArchitecture`) enforces it.
2. **CloudEvents 1.0 is MANDATORY** on every Kafka message produced or
   consumed (structured mode, no flat envelope, no envelope toggle). Build and
   decode ONLY through `internal/adapters/kafka/cloudevents/`; `type` is
   `com.warehouse.wes.exception-management.<entity>.<EventName>`. Topics
   `warehouse.exception-management.events` (integration) and
   `warehouse.exception-management.analytics` (own projector only). A breaking
   payload change is a new `.v2` type. Catalogue:
   `docs/adr/0004-cloudevents-envelope-and-type-catalogue.md`; payloads
   `apis/asyncapi.yaml` (`TestCloudEventsOnly`, `TestEventCatalogueMatchesContract`).
3. **Events leave through the transactional outbox only** (ADR 0005): the
   relay in `cmd/api` deletes a row once sent, retries with backoff up to an
   attempt cap (`OUTBOX_MAX_ATTEMPTS`, `TestOutboxRelaysPassAttemptCap`) and
   sends an unencodable row to `warehouse.exception-management.events.dlq`.
   Never write to Kafka from a handler or use case.
4. **Cases open only from consumed facts** (ADR 0001). `POST /exceptions`
   deliberately answers 405; there is no manual lane without a new ADR.
5. **Exactly one open case per dedup key** (source type, source subject,
   site): a repeated fact touches the open case. Status machine `OPEN ->
   TRIAGED -> ASSIGNED -> RESOLVED`; `ESCALATED` is a flag, not a state;
   `RESOLVED` needs a resolution code and is terminal; every transition
   records its actor (`X-Actor` header on REST, else `system`; a label, not
   authentication).
6. **Frozen local copies, no live lookup.** The case keeps the source payload;
   nothing calls a sibling over REST or MCP (ADR 0003). There is no HTTP
   client to another service. Auto-resolve is a declarative policy rule, never
   agent-initiated.
7. **Consumers**: one per source topic, every group id from its own env var
   (`*_EXCEPTIONS_CONSUMER_GROUP`, `CARTON_DISCREPANCY_CONSUMER_GROUP`,
   `ORDER_CORRELATION_CONSUMER_GROUP`; `TestKafkaConsumerGroupNeverHardcodedInline`);
   idempotent on the CloudEvents `id`; poison to `<topic>.dlq` with a
   cancellation guard (`TestDeadLetterCallsGuardCancellation`). A new source
   needs its routing ADR (as ADR 0006, ADR 0010, ADR 0011).
8. **REST guideline** (ADR 0008): every POST and PUT is behind the
   `Idempotency-Key` middleware; errors are RFC 7807; `.spectral.yaml` runs in
   the `api-lint` job.
9. **Real Postgres and Kafka only.** `internal/adapters/outbound/memory` is a
   test double no binary imports (`TestRuntimeBinaryHasNoInMemoryAdapters`).
10. **No auth layer on REST, MCP or A2A** (fleet rule): never add
    bearer/JWT/API-key middleware (`TestNoAuthMiddlewareReintroduced`).
11. **Agents only read** (ADR 0009): every MCP tool and A2A skill is L1; the
    A2A adapter references no write use case
    (`TestEveryMCPToolDeclaresAuthority`, `TestA2ASkillsDeclareAuthority`,
    `TestA2AAdapterReferencesNoWriteUseCase`). A write tool needs an ADR.
12. **An ADR comes first** for a new consumed source, a new published type,
    an agent write, or any auth. ADRs live in `docs/adr/`.

## Commands

```bash
make check-fast   # fmt-check + vet + arch-test + tests of changed packages: run before saying "done"
make check        # fmt-check vet build lint test (lefthook pre-push)
make check-all    # check + coverage (90% gate on domain + application) + arch-test + bdd
make integration  # real Postgres/Kafka via testcontainers (needs Docker)
make mutation     # gremlins on the domain packages (CI job mutation-fast, blocking)
make api-lint     # Spectral on apis/
make guide-lint   # these guides: skills load, references resolve, context budget
make harness-test # unit tests of the agent hooks and red-issue loop
```

Run locally: `DATABASE_URL=... KAFKA_BROKERS=... <the consumer group vars> go
run ./cmd/api`; `cmd/mcp` and `cmd/a2a` need `DATABASE_URL`; the projector and
reports need `ANALYTICS_DATABASE_URL`. Every variable:
`docs/docs/operations/configuration.md`. The `web/` remote has its own npm
build (see `.claude/rules/frontend.md`).

## Where things are written down (read before editing the matching area)

- `.claude/rules/domain-model.md`: language, aggregates, events, use cases.
  `.claude/rules/rest-api.md`: routes and error shape.
  `.claude/rules/integration-events.md`: published and consumed events.
  `.claude/rules/frontend.md`: the `web/` remote.
- `.claude/rules/fleet/*.md`: fleet-wide rules copied from the harness
  template; never edit them here.
- `docs/adr/`: decisions (index `docs/adr/index.md`). Contracts:
  `apis/openapi.yaml`, `apis/asyncapi.yaml`. Operating docs:
  `docs/docs/operations/` (configuration, runbook, observability,
  troubleshooting), `docs/docs/development/testing.md`,
  `docs/docs/overview/quickstart.md`. Helm chart: `charts/exception-management`.
- `HARNESS.md`: what each sensor runs. `INCIDENTS.md`: fleet incident log.
  `.claude/memory/MEMORY.md`: repo heuristics.

harness-template: v4
(hooks, guide-lint).

<!-- harness:scoped-rules:start (generated by tools/migrate_v3.py in warehouse-harness-template; do not hand-edit) -->
## Scoped rules and harness

Claude Code loads each rule below automatically when you touch the matching paths. OpenCode and Codex do NOT: read the rule BEFORE editing matching files.

| When touching | Read |
|---|---|
| `internal/domain/**`, `internal/application/**`, `features/**` | `.claude/rules/domain-model.md` |
| `web/**`, `charts/exception-management/templates/frontend-*.yaml` | `.claude/rules/frontend.md` |
| `internal/adapters/**/kafka/**`, `internal/adapters/outbound/events/**`, `apis/asyncapi*` | `.claude/rules/integration-events.md` |
| `internal/adapters/inbound/http/**`, `apis/openapi*.yaml`, `apis/openapi/**` | `.claude/rules/rest-api.md` |

Hooks (`scripts/harness/hook.py`, wired for Claude Code, Codex and OpenCode) block pushes to develop/main, `--no-verify`, bare `rm -rf`, and edits to generated files, and feed gofmt/vet findings back after each edit. Before saying "done" run `make check-fast`; the full gate is `make check-all`. `HARNESS_OFF=1` disables the hooks when debugging the harness itself.
<!-- harness:scoped-rules:end -->
