---
name: how-to-test
description: Write or review tests and diagnose a failing coverage, mutation, bdd or integration CI job in exception-management: the four test layers, the 90% gate, gremlins threshold semantics, the testcontainers rule. Use when adding tests, killing a surviving mutant, or fixing a red check.
---

# How to test

Use when writing or reviewing tests in this repo, or diagnosing a failing
`test` (coverage), `mutation-fast`, `bdd` or `integration` CI job. The quality
bar is layered: passing `go test` is necessary but is the WEAKEST of four
signals; mutation testing exists because green tests can assert nothing.

## The four layers, in order of what they actually prove

1. **Unit tests** (`go test ./...`, `make test`): prove the code runs and
   returns something. Table-driven; the domain packages
   (`internal/domain/exceptioncase`, `internal/domain/exceptionpolicy`,
   `internal/domain/shared`) are pure and need no fakes. Use cases (a later
   step) are tested against in-memory implementations of their ports, never a
   real network or DB call.
2. **Coverage** (`make coverage`, 90% gate over
   `./internal/domain/...,./internal/application/...`, set by `COVERAGE_PKGS`
   and `COVERAGE_THRESHOLD` in the `Makefile`): proves lines executed, nothing
   about whether the test asserted the right thing.
3. **Mutation testing** (`make mutation`, gremlins over every package in
   `MUTATION_FAST_PKGS`: `exceptioncase`, `exceptionpolicy`, `shared`):
   proves the tests actually ASSERT. A mutant is a deliberately broken copy
   (`<` becomes `<=`, `+` becomes `-`); if the suite still passes, it LIVED and
   no test would catch that bug. `make mutation-full` is the exhaustive
   scheduled run. A new package under `internal/domain/` must be added to
   `MUTATION_FAST_PKGS` and to the `mutation-fast` loop in
   `.github/workflows/ci.yml`.
4. **BDD / behaviour** (`make bdd`, godog `TestFeatures`): proves a use case
   end to end over the in-memory adapters (`.feature` files under
   `features/`, steps in `features/features_*_test.go`, package
   `features_test`). Never put a Go file at the repo root:
   `TestNoGoFilesAtRepoRoot` fails.

## Mutation testing: thresholds fail on `<=`

`.gremlins.yaml` sets `efficacy` and `mutant-coverage`; gremlins fails when the
MEASURED value is `<=` the threshold, so a threshold must sit strictly BELOW
what the repo meets. Measure first (`make mutation`), then set it. The comment
in `.gremlins.yaml` records the last measurement. If you deliberately lower a
package's score, lower the threshold in the SAME PR with a dated comment;
never silently.

## Pitfalls that have each cost a real CI failure

### 1. Write decisions as `if` chains, not `switch`/`case` — and split long tests

gremlins reports every `case` condition NOT COVERED, which drags the score
down without a real gap. `exceptioncase.Status.Valid`, `shared.ValidText` and
the payload freeze checks are `if` chains with early returns for that reason.
Keep cyclomatic and cognitive complexity under 15 and 20 in production AND
test files: a table test that grew into one 25-branch function fails `cyclop`
— split it into one function per invariant (or extract the per-row assertions
into a helper), as the case/transition/snapshot tests here do.

### 2. Zero or identical fixtures hide arithmetic mutants

`SLADueAt = openedAt + slaMinutes` survives a `+` to `-` mutant if a test only
uses a 30-minute policy and never asserts the exact instant. Use fixtures
where every operand is distinct and non-zero and assert the exact value, not
just "no error" (`TestSLADueIsOpenedAtPlusThePolicySLAMinutes` checks 30 and
1440).

### 3. Boundary guards need the boundary value itself

Every `< 1` / `>= 'A'` guard needs a test that hits the exact boundary:
`SLAMinutes == 1` in `Open`, `Redeclare` and `Restore`
(`TestASingleMinuteSLAIsTheSmallestValidBudget`), `withinMinutes == 1` at the
window edge of `Rule.Matches`, an Event segment starting exactly at `'A'` and
at `'Z'` in the full-type check
(`TestFullTypeBoundariesAreAccepted`), exactly two dots after the prefix, and
a `'0'` INSIDE a multi-digit line number of `shared.ParseWorkUnitRef`
(`ord-7-line-10`): the no-leading-zero guard made `line-0`/`line-01`
equivalent on both sides of the mutant until `line-10` pinned the digit loop.

### 4. Equivalent mutants: know when NOT to chase them

A mutant that only diverges on a structurally unreachable or unspecified case
is not worth a contrived fixture; pinning an arbitrary choice as an invariant
is worse than an accepted survivor. Record the reasoning in the PR and move
on. Today every domain package measures 0 lived mutants, so any LIVED entry
is a regression.

## Diagnosing a `mutation-fast` CI failure

```bash
gremlins unleash ./internal/domain/exceptioncase   # on your branch, per package
```

Compare the LIVED set against `origin/develop`; only entries NEW on your
branch are your regression.

## Kafka/Postgres integration tests: testcontainers, never a skip-gate

A `-tags=integration` test touching Kafka or Postgres MUST start its own
container through `testcontainers-go`. Never gate on `os.Getenv("KAFKA_BROKERS")`
or `os.Getenv("DATABASE_URL")` with `t.Skip(...)`, and never hardcode
`localhost:9092`. CI's `integration` job provisions NO service containers
(`make integration` needs only Docker), so a skip-gated test silently skips
there. Fitness tests enforce it (`TestKafkaIntegrationTestsUseTestcontainers`,
`TestPostgresIntegrationTestsUseTestcontainers` in `internal/architecture/`).
Share one container per package through a helper, use a unique topic per test,
create topics explicitly and wait for the partition leader before the first
read or write. There are no integration tests yet (the adapters arrive in a
later step); the CloudEvents helper has unit tests in
`internal/adapters/kafka/cloudevents/cloudevents_test.go`, and the
build-tagged code must still compile: `go build -tags=integration ./...` and
`go vet -tags=integration ./...`.

## Verify before opening the PR

```bash
make check-all   # check + coverage + arch-test + bdd (the full local gate)
make mutation    # CI runs it as mutation-fast; check-all does not
make vuln        # CI runs it too
```
