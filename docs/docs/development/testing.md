---
id: testing
title: Testing
sidebar_label: Testing
sidebar_position: 1
---

# Testing

The gates, in the order you normally run them. `lefthook` runs `make check` on commit and on push,
so most of this is enforced before a branch can leave the machine.

## Unit and fast gates

```bash
make check        # fmt-check + vet + build + golangci-lint + go test ./... -race
make check-fast   # fmt-check + arch-test only (the tight loop while coding)
make check-all    # check + coverage + arch-test + bdd — run before pushing
```

## Coverage

`make coverage` gates at **90 %** over the scope packages and writes the number into the CI step
summary. The domain packages are additionally held by mutation testing: `make mutation` runs
gremlins on the fast blocking subset (`exceptioncase`, `exceptionpolicy`, `shared`);
`make mutation-full` is the exhaustive scheduled run over all of `internal/domain`.

## Integration (testcontainers)

```bash
make integration   # go test -tags=integration ./... -race -count=1
```

Kafka and Postgres integration tests use **testcontainers only** — they boot their own
`apache/kafka` and `postgres` containers, create topics explicitly and wait for the partition
leader; nothing reads `localhost:9092`, nothing skips on a missing env var, and every Kafka read
is bounded by a context deadline.

## Repository contract suites

`internal/testing/repocontract` holds one behavioural contract per outbound port. The in-memory
doubles (`internal/adapters/outbound/memory`, in the unit run) and the Postgres adapters (under
`-tags=integration`, on a testcontainers Postgres) call the **same** cases, so what the BDD suite
shows over memory is what production does over Postgres. Four suites exist: `CaseRepo`,
`PolicyRepo`, `IdempotencyStore` and `ProcessedEvents`.

The `PolicyRepo` suite covers an unknown source type, the version-1 round trip (with and without
the auto-resolve rule), a redeclaration becoming the latest version, a same-version save losing
the declare race, and the list of latest versions. Its not-found case reads the sentinel source
type `com.warehouse.wes.fulfillment-execution.task.NeverSeen`. That string only has to look like a
CloudEvents type and never match a declared policy; it is a lookup-miss fixture, **not an event
the service consumes or publishes**. The consumed and published types are in
[Domain events](/docs/ddd/domain-events).

## Acceptance (BDD) and architecture fitness

- `make bdd` — godog/Gherkin acceptance over `features/`, the operator-visible lifecycle
  (open from a fact, touch, triage, assign, resolve, auto-resolve, escalation).
- `make arch-test` — the hexagonal fitness suite in `internal/architecture`: the domain imports
  nothing internal but the domain, adapters never import each other, only `cmd/` wires, no
  shipped binary imports the in-memory outbound adapter, and **no auth middleware**
  (`TestNoAuthMiddlewareReintroduced`).

Two governance tests pin the MCP surface: `TestToolSurface` fails on any write-verb tool name, and
the golden `tool_registry.golden.json` pins the exact registry
([MCP tools](/docs/mcp/tools)).

## Contracts and supply chain

```bash
make api-lint   # spectral on apis/openapi.yaml + apis/asyncapi.yaml (fail-severity=warn)
make vuln       # govulncheck — run after any go get / go mod tidy
```

## The docs site

```bash
cd docs
npm ci
npm run gen-api-docs   # regenerate docs/docs/api-reference/rest from apis/openapi.yaml
npm run build          # what the Docs workflow runs; onBrokenLinks: throw
```

The `docs-api-drift` CI job regenerates the REST reference on every PR and fails if it differs
from what is committed — never hand-edit the generated pages.
