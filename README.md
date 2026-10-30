# exception-management

[![CI](https://github.com/IQVO/exception-management/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/IQVO/exception-management/actions/workflows/ci.yml?query=branch%3Adevelop) [![coverage gate](https://img.shields.io/badge/coverage%20gate-90%25-brightgreen)](https://github.com/IQVO/exception-management/blob/develop/Makefile) [![Go](https://img.shields.io/badge/go-1.27.3-00ADD8?logo=go&logoColor=white)](https://github.com/IQVO/exception-management/blob/develop/go.mod) [![docs](https://img.shields.io/badge/docs-iqvo.github.io-blue)](https://iqvo.github.io/exception-management/) [![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/IQVO/exception-management/blob/develop/LICENSE)

The **Exception Management** bounded context of the `warehouse-systems`
fleet — WES-tier **Supporting Subdomain**, the warehouse's "Problem Solve".
It turns the anomaly facts other contexts already publish (missed CPTs,
backlog breaches, at-risk acknowledgements, count discrepancies, diverted
packages, unexpected returns) into owned cases with triage, assignment, an
SLA clock, resolution and root cause.

It owns the `ExceptionCase` aggregate and the versioned `ExceptionPolicy`
per source type (severity, queue, SLA, declarative auto-resolve rules). It
does **not** own the source facts — it keeps frozen local copies of the
consumed payloads and never calls a sibling over REST or MCP; cases open
only from consumed facts (ADR 0001), which is why `POST /exceptions`
deliberately does not exist. Downstream consumers of
`warehouse.exception-management.events`: the operator consoles and the
fleet's analytics; `warehouse-ops-agent` reads it through the MCP tools.
Hexagonal Go, Postgres, REST, Kafka (CloudEvents 1.0).

## Disclaimer

This project is a domain-driven design study. It is not associated with,
endorsed by or affiliated with any company, and it uses no company's code,
data, logos or trademarks. All names, figures and examples are illustrative
or synthetic.

---

## Documentation

The Docusaurus site in `docs/` (ddd-crew pack, ADRs, generated REST
reference, event catalogue, MCP tools, operations pages) publishes to GitHub
Pages at **<https://iqvo.github.io/exception-management/>** by
`.github/workflows/docs.yml` (PRs build, `main` deploys). Build locally:

```bash
cd docs && npm ci && npm run build     # npm run gen-api-docs regenerates the REST pages
```

Page map (source under `docs/docs/`):

| Area | Pages |
|---|---|
| Overview | [Introduction](docs/docs/overview/introduction.md), [Architecture](docs/docs/overview/architecture.md), [Quickstart](docs/docs/overview/quickstart.md) |
| Business context | [Domain vision](docs/docs/business-context/domain-vision.md), [Problem solve](docs/docs/business-context/problem-solve.md) |
| Operations | [Configuration](docs/docs/operations/configuration.md), [Runbook](docs/docs/operations/runbook.md), [Observability](docs/docs/operations/observability.md), [Troubleshooting](docs/docs/operations/troubleshooting.md) |
| Development | [Testing](docs/docs/development/testing.md) |
| Ecosystem | [Integration contracts](docs/docs/ecosystem/integration.md) |
| MCP | [MCP tools](docs/docs/mcp/tools.md) |
| DDD | [Use cases](docs/docs/ddd/use-cases.md), [Subdomain classification](docs/docs/ddd/subdomain-classification.md), [Ubiquitous language](docs/docs/ddd/ubiquitous-language.md), [Domain events](docs/docs/ddd/domain-events.md), [Core domain chart](docs/docs/ddd/core-domain-chart.md), [Bounded context canvas](docs/docs/ddd/bounded-context-canvas.md), [Context map](docs/docs/ddd/context-map.md), [Aggregate design canvas](docs/docs/ddd/aggregate-design-canvas.md), [Domain message flow](docs/docs/ddd/domain-message-flow.md), [EventStorming](docs/docs/ddd/eventstorming.md), [Class diagram](docs/docs/ddd/class-diagram.md), [Sequence diagrams](docs/docs/ddd/sequence-diagrams.md), [Entity-relationship](docs/docs/ddd/entity-relationship.md), [DDD artifacts index](docs/docs/ddd/ddd-artifacts.md) |
| Reference | [API overview](docs/docs/api-reference/overview.md), [Event catalogue](docs/docs/api-reference/events.md), generated REST pages under `docs/docs/api-reference/rest/`, [A2A inbound adapter (ADR 0007)](docs/adr/0007-a2a-inbound-adapter.md) |
| Decisions | [ADR index](docs/adr/index.md) |

The `docs-api-drift` CI job fails a PR whose generated reference is out of
sync with `apis/openapi.yaml`. ADRs live in `docs/adr/` (bodies immutable). Design
and pinned contracts:
[ADR 0001](docs/adr/0001-exception-management-bounded-context.md). Scaffolded
from `warehouse-harness-template` (v4); guides in `.claude/rules/`, sensors in
`HARNESS.md`.

---

## What it does

- Opens exactly one case per dedup key (source type, subject, site) from a
  consumed fact; a repeated fact touches the open case instead of opening
  another.
- Operators triage, assign and resolve cases over REST
  (`/exceptions/{caseId}/triage|assign|resolve`); every transition records
  its actor.
- A versioned `ExceptionPolicy` per source type sets severity, queue, SLA
  and a declarative auto-resolve rule (for example `OrderRepromised` closes
  the `TaskCPTMissed` case of the same work unit).
- A bounded sweeper flags SLA breaches (`ExceptionEscalated`) without
  blocking work.
- Publishes case facts through a transactional outbox; exposes read-only MCP
  tools for warehouse-ops-agent (which stays zero-write).
- Answers delegated goals over A2A (`cmd/a2a`, read-only: `sla_status`,
  `find_exceptions`); see
  [Running the A2A agent in Kubernetes](#running-the-a2a-agent-in-kubernetes).
- Keeps frozen local copies of the source payloads. It never calls a sibling
  over REST or MCP. Runs on real Postgres and real Kafka only. No auth.

## Status

Domain, application, adapters, packaging (chart, Dockerfile, CI), the docs
site and the console web remote are built on `develop`, following the fleet
plan's waves (`.hermes/plans/2026-10-20_231017-returns-processing-and-exception-management.md`).

---

## Architecture

Hexagonal / ports & adapters; dependencies point inward only:

```
cmd/api/                       composition root: REST service, outbox relay, the seven fact consumers, the SLA sweeper
cmd/mcp/                       read-only MCP server (Streamable HTTP)
cmd/a2a/                       read-only A2A agent (JSON-RPC over HTTP, Agent Card at /.well-known/agent-card.json)
cmd/exceptions-projector/      analytics projector (the only analytical-DB writer)
cmd/exceptions-reports/        analytics reader (serves the /reports routes)
internal/
  domain/                      pure business logic
    exceptioncase/             ExceptionCase aggregate (dedup key, transitions, actors)
    exceptionpolicy/           versioned ExceptionPolicy per source type (auto-resolve rules)
    shared/                    severity, queue, work unit, actor values
  application/
    ports/                     OUT interfaces only (repos, EventPublisher, UnitOfWork, Clock)
    usecases/                  one struct per use case (open/touch, triage, assign, resolve, sweep)
    views/                     read models for REST/MCP/A2A
    repository/                repository contracts
    outbox/                    transactional outbox
  adapters/
    inbound/http/              chi handlers, DTOs, RFC 7807 error mapping, Idempotency-Key middleware
    inbound/mcp/               the four read-only MCP tools
    inbound/a2a/               A2A skills: sla_status, find_exceptions (both reads)
    inbound/kafka/             the seven fact consumers + the projector's analytics consumer
    kafka/cloudevents/         the CloudEvents 1.0 envelope
    outbound/postgres/         pgxpool repos, golang-migrate runner, UnitOfWork, outbox + relay
    outbound/kafka/            integration + analytics topic publishers
    outbound/outbox/           outbox support types
    outbound/analyticsstore/   analytical DB projection and report queries
    outbound/memory/           in-memory test doubles (never linked into a binary)
    outbound/system/           the wall clock the binaries use
  analytics/report/            the reports data product model
migrations/                    golang-migrate SQL (OLTP); analytics/migrations/ (analytical DB)
```

The rule is enforced as an executable test, not a convention:
`internal/architecture/architecture_test.go` (arch-go) fails the build on any
layering violation, and its sibling fleet fitness tests keep the invariants
that matter here: `runtime_no_memory_fitness_test.go`,
`kafka_writer_topic_fitness_test.go`, `kafka_writer_acks_fitness_test.go`,
`kafka_dlq_shutdown_fitness_test.go`, `outbox_relay_cap_fitness_test.go`,
`events_fitness_test.go`, `catalogue_fitness_test.go`,
`mcp_optional_inputs_test.go`, `a2a_fitness_test.go` and
`no_go_files_at_root_fitness_test.go`.

---

## Domain events

Seven published event types — `ExceptionOpened`, `ExceptionTriaged`,
`ExceptionAssigned`, `ExceptionEscalated`, `ExceptionResolved`,
`ExceptionAutoResolved` (entity `exceptioncase`) and `ExceptionPolicyDeclared`
(entity `exceptionpolicy`) — go through the transactional outbox to BOTH
`warehouse.exception-management.events` (the integration topic) and
`warehouse.exception-management.analytics` (this service's own analytics
stream, ADR 0006), specified in `apis/asyncapi.yaml`.

Every Kafka message on both topics is a **CloudEvents 1.0** event in
structured content mode (ADR 0004), header
`content-type: application/cloudevents+json; charset=UTF-8`, `subject` = the
case id, `dataschema` =
`urn:warehouse:exception-management:<events|analytics>:<EventName>:v1`.
CloudEvents `type` convention, identical to the other warehouse-systems
services — this context's subdomain segment is `wes`:

```
com.warehouse.wes.exception-management.<entity>.<EventName>

com.warehouse.wes.exception-management.exceptioncase.ExceptionOpened
com.warehouse.wes.exception-management.exceptionpolicy.ExceptionPolicyDeclared
```

Consumed anomaly facts (one pinned consumer group each, frozen local copies
per ADR 0003):

| Topic | Types consumed | Consumer group env |
|---|---|---|
| `warehouse.fulfillment.events` | `TaskCPTMissed`, `PackageDiverted`, `WeightDiscrepancyDetected` | `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` |
| `warehouse.work-planning.events` | `BacklogThresholdBreached` | `WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP` |
| `warehouse.network-fulfillment.events` | `AcknowledgementDeadlineAtRisk`, `OrphanedHoldCancelled` | `NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` |
| `warehouse.order-management.events` | `OrderRepromised` (policy correlation) | `ORDER_CORRELATION_CONSUMER_GROUP` |
| `warehouse.inventory.events` | `DiscrepancyDetected`, `ItemUnlocated` | `INVENTORY_EXCEPTIONS_CONSUMER_GROUP` |
| `warehouse.returns-processing.events` | `UnexpectedReturnReceived` | `RETURNS_EXCEPTIONS_CONSUMER_GROUP` |
| `warehouse.inbound-receiving.events` | `CartonDiscrepancyRaised` (ADR 0006) | `CARTON_DISCREPANCY_CONSUMER_GROUP` |

The projector consumes `warehouse.exception-management.analytics` under
`ANALYTICS_CONSUMER_GROUP` and dead-letters poison rows to
`warehouse.exception-management.analytics.dlq`; other contexts keep reading
`warehouse.exception-management.events`.

---

## Running it

Postgres and Kafka are the only runtime: there is no in-memory mode.
`cmd/api` exits at boot with an error naming every missing variable.

```sh
docker compose up -d --wait   # postgres:16-alpine on localhost:5433 (exceptions/exceptions/exceptions)
                              # NOTE: the compose file starts NO Kafka — point
                              # KAFKA_BROKERS at a fleet broker (the cluster maps :9092)
export DATABASE_URL="postgres://exceptions:exceptions@localhost:5433/exceptions?sslmode=disable"
export KAFKA_BROKERS="localhost:9092"
export FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP=ex-local-fulfillment
export WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP=ex-local-work-planning
export NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP=ex-local-network-fulfillment
export ORDER_CORRELATION_CONSUMER_GROUP=ex-local-order-correlation
export INVENTORY_EXCEPTIONS_CONSUMER_GROUP=ex-local-inventory
export RETURNS_EXCEPTIONS_CONSUMER_GROUP=ex-local-returns
export CARTON_DISCREPANCY_CONSUMER_GROUP=ex-local-carton-discrepancy
go run ./cmd/api              # migrations run automatically at startup; listens on :8080
```

### Configuration

`cmd/api`:

| Env var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | **required** | Postgres DSN; unset ⇒ boot error naming it |
| `KAFKA_BROKERS` | **required** | comma-separated broker list |
| `FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP` … `CARTON_DISCREPANCY_CONSUMER_GROUP` | **required** (all seven) | the pinned group ids of the table above |
| `HTTP_ADDR` | `:8080` | REST listen address |
| `OUTBOX_RELAY_INTERVAL` | `1s` | Go duration (or plain seconds) between relay passes |
| `OUTBOX_MAX_ATTEMPTS` | `20` | attempts before the relay excludes a row (`0` = unlimited) |
| `SLA_SWEEP_INTERVAL` | `1m` | how often the SLA sweeper ticks (escalations) |
| `SHUTDOWN_DRAIN_DELAY` | `5s` | wait after flipping `/readyz` before closing the listener (`0` = none) |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | direct (non-PgBouncer) DSN for the boot migrations |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` (`warning` accepted), case-insensitive |

`cmd/mcp`: `MCP_ADDR` (`:8090`), `DATABASE_URL` (**required**),
`MIGRATIONS_DATABASE_URL`, `LOG_LEVEL`.
`cmd/a2a`: `A2A_ADDR` (`:8093`), `DATABASE_URL` (**required**),
`A2A_AGENT_URL` (**required**, the absolute URL advertised on the Agent
Card), `MIGRATIONS_DATABASE_URL`, `LOG_LEVEL`.
`cmd/exceptions-projector`: `ANALYTICS_DATABASE_URL` (**required**),
`KAFKA_BROKERS` (**required**), `ANALYTICS_CONSUMER_GROUP` (**required**),
`ADMIN_ADDR` (`:8091`), `ANALYTICS_MIGRATIONS_PATH`
(`analytics/migrations`), `LOG_LEVEL`.
`cmd/exceptions-reports`: `ANALYTICS_DATABASE_URL` (**required**),
`HTTP_ADDR` (`:8092`), `LOG_LEVEL`.

### Analytics data product (exception analytics)

An additive analytical read side (ADR 0006) built from this service's own
published facts, delivered on the separate topic
`warehouse.exception-management.analytics`, projected into a **separate**
analytical database, and served read-only. Three processes with one writer:

```sh
# 1. OLTP service — fans every published fact out to BOTH topics (see above)
go run ./cmd/api

# 2. Projector — the ONLY writer of the analytical DB (runs analytics/migrations itself)
export ANALYTICS_DATABASE_URL="postgres://exceptions:***@localhost:5433/exceptions_analytics?sslmode=disable"
KAFKA_BROKERS=localhost:9092 ANALYTICS_CONSUMER_GROUP=exceptions-analytics go run ./cmd/exceptions-projector   # :8091

# 3. Reports — read-only reader over a read-only pool
ANALYTICS_DATABASE_URL="$ANALYTICS_DATABASE_URL" go run ./cmd/exceptions-reports   # :8092
curl http://localhost:8092/reports/sla-breach-rate http://localhost:8092/reports/mean-time-to-resolve
curl http://localhost:8092/reports/root-cause-frequency http://localhost:8092/reports/freshness
```

---

## Observability

What exists today, and what deliberately does not (see the
[observability page](docs/docs/operations/observability.md)):

- **Logs** — structured JSON (`slog`) on stdout, one line per event, level
  from `LOG_LEVEL`. The lines that matter: boot configuration problems
  (reported all at once, each naming its variable), readiness dependency
  failures, consumer skips (an invalid CloudEvent, or a fact for an
  undeclared source type — that WARN is the signal of a missing policy, not
  an incident), relay DLQ writes and sweep escalations.
- **Metrics** — `GET /metrics` (Prometheus text exposition) currently
  carries the process presence gauge `exception_management_up`. The full
  registry (case counters by status/queue, escalation counts, outbox depth,
  consumer lag) is a fleet-wide instrumentation step, **roadmap, not
  built**: no prometheus client is in the module yet.
- **Traces — no tracing SDK is wired.**
- **Health endpoints** — `GET /healthz` answers `200 {"status":"ok"}`
  (liveness only); `GET /readyz` answers `200 {"status":"ready"}` or `503`
  naming the failing dependency, backed by the drain gate + a Postgres ping
  + a Kafka broker metadata check (cached 3 s, one round bounded at 2 s).

---

## API

Errors are **RFC 7807** (`application/problem+json`) throughout. Every route
below is declared at the same path in `apis/openapi.yaml`.

| Method | Path | Purpose | Success |
|---|---|---|---|
| `GET` | `/exceptions?status=&queue=&site=&sourceType=` | List cases, newest first (empty is `200 []`) | `200` |
| `POST` | `/exceptions` | **deliberately absent**: permanent `405` with `Allow: GET` — every case opens from a consumed fact (ADR 0001) | `405` |
| `GET` | `/exceptions/{caseId}` | Get one case | `200` |
| `POST` | `/exceptions/{caseId}/triage` | TriageException (records its actor) | `201` + `Location` |
| `POST` | `/exceptions/{caseId}/assign` | AssignException | `201` + `Location` |
| `POST` | `/exceptions/{caseId}/resolve` | ResolveException (root cause) | `201` + `Location` |
| `GET` | `/exception-policies` | List the declared policies | `200` |
| `PUT` | `/exception-policies/{sourceType}` | DeclareExceptionPolicy (versioned) | `200` |
| `GET` | `/healthz` / `/readyz` / `/metrics` | Liveness / readiness / the presence gauge | `200` |

### Idempotency

The four operator write routes — `triage`, `assign`, `resolve` and
`PUT /exception-policies/{sourceType}` — require a caller-supplied
`Idempotency-Key` header (Postgres-backed idempotency pool, migration
`0002_idempotency_keys`). A retry with the same key and the same body
replays the original response verbatim; the same key with a different body
is rejected `422`; a missing header is `400`.

`apis/openapi.yaml` (OpenAPI 3.0.3) documents every route with full schemas
and a shared `Problem` component; `apis/asyncapi.yaml` (AsyncAPI 2.6)
specifies both topics and all messages. The OpenAPI document is linted in CI
(`api-lint`, Spectral).

---

## Helm chart

```sh
helm install exception-management ./charts/exception-management \
  --set database.url="postgres://exceptions:***@postgres:5432/exceptions?sslmode=disable" \
  --set kafka.brokers="kafka:9092"
```

The chart refuses to render without `database.url` (or
`database.existingSecret`), `kafka.brokers` and the seven pinned consumer
groups (`config.*ConsumerGroup`). Optional components, all off by default:
`mcp`, `analytics` (projector + reports), `frontend` and `a2a`. Chart
invariants are asserted by `charts/exception-management/tests/`
(`render_test.sh`, `test_service_selectors.py`, `test_kong_config.py`) and
the `chart-tests` CI job; `helm-lint` runs `ct lint --charts
charts/exception-management` in CI.

### Running the MCP server in Kubernetes

`cmd/mcp` ships in the same image as `/app/mcp` and is a separate, opt-in
deployable: `mcp.enabled=true` renders a `<release>-mcp` Deployment and
ClusterIP Service on port `8090`. It reads the same `DATABASE_URL` secret,
runs the OLTP migrations on start (idempotent), and serves MCP over
Streamable HTTP at `/` and `/mcp` with `GET /healthz` behind the probes. Its
surface is **read-only**: four tools over the same read use cases as REST —
`get_exception`, `list_open_exceptions`, `get_sla_report`,
`get_exception_policies` — the curated surface `warehouse-ops-agent` reads
(which stays zero-write). There is no authentication layer in front of it;
access control is the in-cluster ClusterIP boundary.

### Running the A2A agent in Kubernetes

`cmd/a2a` ([ADR 0007](docs/adr/0007-a2a-inbound-adapter.md)) is the context's
Agent2Agent agent, the third driving adapter
(`internal/adapters/inbound/a2a`) over the same read use cases as REST and
MCP. It is **read-only**, like the MCP surface, with two goal-shaped skills,
called as a `SendMessage` data part `{"skill": "<id>", "arguments": {...}}`:

| Skill | Answers |
|---|---|
| `sla_status` | the open caseload against its SLA clocks (open, breached, escalated, by severity and queue), optionally with the declared policies (`includePolicies`) |
| `find_exceptions` | one case by `caseId`, or the cases filtered by `status`, `queue`, `siteId`, `sourceType` |

The answer is a data part `{skill, data | problem}` plus a one-line text
part. Rejections are answered in-band as RFC 7807-style problems with the
REST adapter's status and slug. A2A carries delegation only; it never writes
and never publishes a fact. Unauthenticated, like every endpoint of the
fleet. `a2a.enabled` (default off) renders Service `<fullname>-a2a:8093`,
exposed through Kong at `/api/a2a/exception-management`.

```bash
A2A_AGENT_URL=http://localhost:8093/ DATABASE_URL=postgres://... go run ./cmd/a2a   # :8093
curl localhost:8093/.well-known/agent-card.json
```

---

## Operator micro-frontend (`web/`)

`web/` is the `exceptions_mfe` Module Federation remote for the
`warehouse-console` shell: the case queue, case detail with the triage →
assign → resolve flow, and the declared policies — all over the existing
REST. Standalone development:

```sh
cd web && npm ci && npm run dev   # http://localhost:4011 (talks to :8080)
```

Deployed to the kind cluster it is built into a static bundle and served by
its own `nginx-unprivileged` pod under `/mfes/exception-management/`
(`web/Dockerfile`, chart `frontend.enabled`). The remote reads its API
origin at runtime from `window.__WAREHOUSE_CONFIG__.apiOrigin`
(`${apiOrigin}/api/exception-management`, published by the console shell) —
`web/src/config.ts`.

---

## Local development / quality gate

Every CI sensor is also a `make` target (`make help` lists them all):

```sh
make check-fast   # fmt-check, vet, arch-test
make check        # fmt-check, vet, build, lint, test
make check-all    # check + coverage (90% gate) + arch-test + bdd
make integration  # testcontainers Postgres + Kafka (needs only Docker)
make mutation     # gremlins over ./internal/domain
make vuln         # govulncheck ./...
make api-lint     # spectral over apis/openapi.yaml
make guide-lint   # agent guides: references resolve, context budget
```

Git hooks are managed by [lefthook](https://github.com/evilmartians/lefthook)
(`lefthook.yml`): pre-commit runs `fmt-check` + `vet` + `lint`; pre-push runs
`make check`. Activate once per clone: `brew install lefthook && lefthook
install`.

## Tests

```sh
go test ./... -race                                  # unit
go test -tags=integration ./... -race -count=1       # testcontainers Postgres/Kafka
go test ./features/ -run TestFeatures -v             # godog/Gherkin BDD over the real HTTP API
go test ./internal/architecture/... -v               # arch-go + fleet fitness tests
make coverage                                        # domain+application coverage, CI gate >= 90%
```

CI (`.github/workflows/ci.yml`) runs `lint`, `guide-lint`, `complexity`,
`test` (with the 90% coverage gate), `bdd`, `integration`, `mutation-fast`,
`drift` and `mutation` (scheduled), `api-lint`, `vuln`, `docs-api-drift`,
`helm-lint`, `chart-tests`, `arch-test`, `web` on every push/PR to
`develop`/`main`; `trivy-scan` only on PRs into `main`; `docker-publish` and
`release` on push to `main`.

## Architecture Decision Records

The ADR index lives at [docs/adr/index.md](docs/adr/index.md) — 7 decisions,
from the bounded context itself to the aggregates, the consumed contracts,
the CloudEvents catalogue, the outbox attempt cap and DLQ, the
carton-discrepancy consumer and the A2A inbound adapter.

## License

MIT — see [LICENSE](LICENSE).
