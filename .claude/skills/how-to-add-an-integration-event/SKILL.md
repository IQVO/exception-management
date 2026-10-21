---
name: how-to-add-an-integration-event
description: Publish or consume a cross-service Kafka event in exception-management: CloudEvents 1.0 type naming, AsyncAPI, transactional outbox, consumer-group rules. Use when touching Kafka or outbox adapters, a consumer, or apis/asyncapi.yaml.
---

# How to add an integration event (publish and consume)

Use when asked to publish a new cross-context integration event, or to consume
one from a sibling bounded context. This fleet's Kafka is ONE broker
platform-wide; every design decision below exists because that shared-broker
reality caused a real incident once.

Layer status: `apis/asyncapi.yaml`, `docs/adr/0004-cloudevents-envelope-and-type-catalogue.md`,
the CloudEvents helper `internal/adapters/kafka/cloudevents/cloudevents.go`
and the scaffold writer `internal/adapters/outbound/kafka/writer.go` are
committed. The outbox, the relay and the consumers are built in a later step
and are described in words below.

## Publishing a new integration event

### 1. Is it actually cross-service?

Not every domain event belongs on the wire. This service publishes the case
lifecycle events (`exceptioncase.ExceptionOpened` through
`exceptioncase.ExceptionAutoResolved`, subject/key = caseId) and
`exceptionpolicy.ExceptionPolicyDeclared` (subject/key = sourceType) on
`warehouse.exception-management.events`. Before adding another, confirm a
sibling context genuinely needs it; the context map and the deliberate
exclusions (no manual case creation, ops-agent read-only) are in
`docs/adr/0001-exception-management-bounded-context.md`.

### 2. Envelope: CloudEvents 1.0, structured mode, MANDATORY

Every message is a CloudEvents 1.0 JSON document (Kafka value =
`application/cloudevents+json`) with the Kafka header
`content-type: application/cloudevents+json; charset=UTF-8`. No other envelope
exists in this fleet: no flat `event_id`/`event_type`/`occurred_at` shape, no
dual-write, no `EVENT_ENVELOPE_MODE` toggle (see
`.claude/rules/integration-events.md`; fitness tests in
`internal/architecture/` enforce it).

```json
{
  "specversion": "1.0",
  "id": "<uuid v4, minted once, persisted with the outbox row>",
  "source": "/warehouse/exception-management",
  "type": "com.warehouse.wes.exception-management.exceptioncase.ExceptionOpened",
  "subject": "case-9f21",
  "time": "<domain occurred-at, RFC3339 UTC>",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:exception-management:events:ExceptionOpened:v1",
  "data": { "case_id": "case-9f21", "source_type": "...", "queue": "cps-front" }
}
```

`type` is `com.warehouse.wes.exception-management.<entity>.<EventName>`;
`<entity>` is the raising aggregate, lowercase with no separators
(`exceptioncase` or `exceptionpolicy`). The helper builds it:
`cloudevents.Type(entity, eventName)`. The subdomain table is on the
CloudEvents event standard page of the warehouse-docs repository; do not guess
a prefix for a sibling's type. The full produced catalogue — type, subject,
dataschema, one example payload each — is
`docs/adr/0004-cloudevents-envelope-and-type-catalogue.md`.

### 3. Implementation

The event must already exist as a domain event raised by the aggregate (see
`internal/domain/exceptioncase/events.go`); publishing maps an EXISTING domain
event onto the wire, it does not invent a payload shape in the adapter. In the
outbound adapter (the outbox encoder, a later step):

- Encode ONLY through `internal/adapters/kafka/cloudevents`:
  ```go
  value, err := cloudevents.New(cloudevents.Spec{
      ID:        eventID,                  // minted once; the outbox row stores it
      Entity:    "exceptioncase",
      EventName: "ExceptionOpened",
      Subject:   evt.CaseID(),
      Time:      evt.OccurredAt(),
      Stream:    cloudevents.StreamEvents,
      Version:   1,
      Data:      payload,                  // snake_case wire payload
  })
  msg := kafkago.Message{
      Topic:   topic,                      // per message; never on the Writer
      Key:     []byte(evt.CaseID()),
      Value:   value,
      Headers: append(traceHeaders, cloudevents.ContentTypeHeader()),
  }
  ```
- Write the encoded event into the outbox in the SAME database transaction as
  the aggregate change; a relay (built on the `kafka.NewWriter` scaffold)
  drains it at-least-once. Never call the Writer from a use case. The relay
  sets the attempt cap (see `.claude/rules/fleet/idempotency-and-outbox.md`).
- Key = the subject (caseId for case events, sourceType for
  `ExceptionPolicyDeclared`), with the `kafkago.Hash{}` balancer, so all
  events of one aggregate stay ordered. Use this service's own topic
  (`warehouse.exception-management.events`), never a sibling's.

### 4. Contract + docs

- Add the message to `apis/asyncapi.yaml` (the channel, the message with its
  exact `type` const and `dataschema`, the payload schema).
- Add the type's line to the catalogue in
  `docs/adr/0004-cloudevents-envelope-and-type-catalogue.md`;
  `TestEventCatalogueMatchesContract` (`internal/architecture/catalogue_fitness_test.go`)
  fails when the two disagree.
- Update the published table in `.claude/rules/integration-events.md`.
- Lint the contract the way CI does:
  ```bash
  spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml --fail-severity=warn
  ```
- A breaking payload change is a new `.v2` type and dataschema, never a
  mutation of v1.

### 5. Test

Add a golden exact-JSON unit test for the new `type` asserting every
CloudEvents attribute and the `content-type` header, against a fake writer
(never a real broker in a unit test; the helper's own tests are in
`internal/adapters/kafka/cloudevents/cloudevents_test.go`). Tests that assert
real delivery use testcontainers only
(`TestKafkaIntegrationTestsUseTestcontainers`): a skip-gated `KAFKA_BROKERS`
test or a hardcoded `localhost:9092` fails CI. Create topics explicitly and
wait for the leader.

## Consuming an integration event from a sibling context

The contracts this service consumes (`TaskCPTMissed`, `OrderRepromised`,
`PackageManifested`, the inventory-storage facts and the rest) are listed in
`docs/adr/0003-local-copies-and-consumed-contracts.md` (verified against each
producer's develop branch). Adding a consumed type means updating that ADR,
the receive side of `apis/asyncapi.yaml`, ADR 0004's consumed table and
`.claude/rules/integration-events.md`.

### 1. Never import the sibling's Go packages

This service knows a sibling's topic name, its exact CloudEvents `type`
strings and the payload shape ONLY, never its Go types. Hand-mirror the
payload struct locally and restate it in `apis/asyncapi.yaml`. There is also
no live REST or MCP call to a sibling (ADR 0001, ADR 0003): consume the event
into a durable local copy and read the copy.

### 2. Decode CloudEvents only, dispatch on the full `type`

```go
evt, err := cloudevents.Decode(msg.Value)
if err != nil { // errors.Is(err, cloudevents.ErrNotCloudEvent): deterministic poison
    logger.Warn("skipping non-CloudEvents message", "topic", msg.Topic,
        "partition", msg.Partition, "offset", msg.Offset, "err", err)
    return commit(msg) // never crash, never block the partition
}
if evt.Type() != "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" {
    return commit(msg) // unknown types are ignored, not errors
}
var p taskCPTMissedData // local mirror of the payload
if err := evt.DataAs(&p); err != nil { /* poison: skip as above */ }
// dedupe on evt.ID() in the SAME DB transaction as the effect
```

Dispatch is an `if` chain on the byte-identical full type (the mutation gate
counts every `case`). Never parse a legacy flat shape as a fallback, never
match a short name or suffix. Test that a legacy flat-envelope message is
rejected. Use `FetchMessage` and commit with `CommitMessages` only after the
effect succeeded. Rules: `.claude/rules/fleet/kafka-testing-and-consumers.md`.

### 3. Choose the right consumer-group pattern

Two different correct patterns exist; picking the wrong one is the most common
integration-event mistake in this fleet (real incident: wes-work-planning#67).

**Pattern A, stable group from env.** Use when the consumer resumes from its
committed offset (a durable local copy, the exception-opening pipeline, an
analytics projector). The group id comes from an env var
(`<CONSUMER>_CONSUMER_GROUP`, named when the consumers land) and is NEVER a
string literal (`TestKafkaConsumerGroupNeverHardcodedInline`), because a
local process joining the same group as the live in-cluster Deployment would
starve one of them. A missing group env is a boot error.

**Pattern B, per-process-unique group.** Use only when a consumer rebuilds an
in-memory read model from a topic's FULL history on every start. The group id
must then be unique per process instance (hostname, PID, timestamp). This
service has no such consumer; adding one needs an ADR.

### 4. Local copies, idempotency, correlation

A consumer that feeds a local copy applies the change and writes the
CloudEvents `id` claim in ONE transaction, so redelivery is a no-op. The
dedup decision (one open case per `shared.DedupKey`) and the auto-resolve
correlation (`exceptionpolicy.Rule.Matches` reversing the work-unit formula
`{order_id}-line-{line_no}` via `shared.ParseWorkUnitRef`, which fails soft)
are domain rules, not consumer code. `TaskCPTMissed` is re-fired every sweep
tick by design: the touch path (occurrence + 1, lastSeenAt) is the
idempotency, not an error.

## Verify before opening the PR

```bash
make check-all    # includes arch-test; catches a sibling-package import
make integration  # needs Docker; boots Postgres/Kafka with testcontainers
```
