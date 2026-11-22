package kafka_test

// Event consumer contract tests (E1-E4) for the analytics SELF-edge:
// this repo's own projector (cmd/exceptions-projector) consumes this
// repo's own internal topic warehouse.exception-management.analytics,
// folding the seven published analytics types into the read model. This
// repo is both the producer and the consumer, so the contract is the
// SELF-pin of its own apis/asyncapi.yaml
// (testdata/exception-management.asyncapi.yaml, registered in
// contracts/pinned.json): the pin keeps the projector honest against the
// same spec its publisher documents for sibling consumers. Golden
// fixtures are the spec's OWN examples — the projector decodes exactly
// what the analytics publisher documents it emits.
//
// NOTE spec shape: the shared envelope schema types `type` as plain
// string; each message documents its concrete CloudEvents type in its
// own example payload — the harness reads it from there.

import (
	"context"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/exception-management/internal/analytics/report"
)

// ownAnalyticsTopic mirrors the composition root's analytics topic
// (warehouse.exception-management.analytics).
const ownAnalyticsTopic = "warehouse.exception-management.analytics"

// contractProjection records the events it is handed (analytics_consumer_
// test.go's fakeProjection lives in the in-package test package; the
// contract tests run in kafka_test, so this is the same double, local).
type contractProjection struct {
	applied []report.Event
}

func (f *contractProjection) Apply(_ context.Context, e report.Event) (bool, error) {
	f.applied = append(f.applied, e)
	return true, nil
}

// ownAnalyticsRouted maps each routed CloudEvents type to the report.Kind
// the projector must fold it into — mirroring analytics_consumer.go's
// analyticsKinds (cloudevents.Type builds com.warehouse.wes.
// exception-management.<entity>.<EventName>).
var ownAnalyticsRouted = map[string]report.Kind{
	cloudevents.Type("exceptioncase", string(report.KindExceptionOpened)):           report.KindExceptionOpened,
	cloudevents.Type("exceptioncase", string(report.KindExceptionTriaged)):          report.KindExceptionTriaged,
	cloudevents.Type("exceptioncase", string(report.KindExceptionAssigned)):         report.KindExceptionAssigned,
	cloudevents.Type("exceptioncase", string(report.KindExceptionEscalated)):        report.KindExceptionEscalated,
	cloudevents.Type("exceptioncase", string(report.KindExceptionResolved)):         report.KindExceptionResolved,
	cloudevents.Type("exceptioncase", string(report.KindExceptionAutoResolved)):     report.KindExceptionAutoResolved,
	cloudevents.Type("exceptionpolicy", string(report.KindExceptionPolicyDeclared)): report.KindExceptionPolicyDeclared,
}

// ownAnalyticsConsumer builds the REAL analytics consumer around a
// recording projection (the reader is never dialed inside HandleMessage).
func ownAnalyticsConsumer(proj *contractProjection) *kafka.Consumer {
	return kafka.NewAnalyticsConsumer([]string{"localhost:9092"}, ownAnalyticsTopic,
		"contract-test", proj, contractLogger())
}

// E1: the topic the projector's reader subscribes to is documented by
// the repo's own pinned spec, and its message list documents every type
// the projector routes on.
func TestPinnedAsyncAPICoversSubscriptions_OwnAnalytics(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOwnAnalyticsSpec)
	documented := doc.documentedTypes(t, ownAnalyticsTopic)
	for typ := range ownAnalyticsRouted {
		if _, ok := documented[typ]; !ok {
			t.Errorf("pinned own spec does not document %s on %s (documented: %v)", typ, ownAnalyticsTopic, documented)
		}
	}
}

// E2: every routed type's own example decodes through the REAL
// HandleMessage → project → Projection.Apply path and produces exactly
// one projection call carrying the wire's own facts — the kind, the
// CloudEvents id, and the case id (the governed source type for the
// policy declaration); every redelivery applies exactly once more (the
// model is a rebuildable projection, at-least-once by design).
func TestGoldenEventsDecode_OwnAnalytics(t *testing.T) {
	for typ, kind := range ownAnalyticsRouted {
		for _, ex := range goldenExamples(t, pinnedOwnAnalyticsSpec, ownAnalyticsTopic, typ) {
			t.Run(ex.Name, func(t *testing.T) {
				assertProjectedGolden(t, ex, kind)
			})
		}
	}
}

// assertProjectedGolden runs one analytics golden example through the
// real decode+dispatch and asserts the projection carries the wire's own
// facts (and only one apply per delivery).
func assertProjectedGolden(t *testing.T, ex pinnedExample, kind report.Kind) {
	t.Helper()
	proj := &contractProjection{}
	c := ownAnalyticsConsumer(proj)
	defer c.Close() // stops the reader's background group-retry loop
	raw := goldenJSON(t, ex.Payload)
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("handle golden example: %v", err)
	}
	if len(proj.applied) != 1 {
		t.Fatalf("one delivery must apply exactly one event; applied %d", len(proj.applied))
	}
	applied := proj.applied[0]
	if applied.Kind != kind {
		t.Errorf("projected kind %q, want %q", applied.Kind, kind)
	}
	if applied.EventID != idOf(t, ex.Payload) {
		t.Errorf("projected event id %q, wire id is %q", applied.EventID, idOf(t, ex.Payload))
	}
	assertProjectedIdentity(t, applied, dataOf(t, ex.Payload), kind)
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if len(proj.applied) != 2 {
		t.Errorf("a redelivery must re-apply exactly once (rebuildable projection); applied %d", len(proj.applied))
	}
}

// assertProjectedIdentity checks the identity fields the projector reads
// per kind: the case id for case facts, the governed source type for the
// policy declaration.
func assertProjectedIdentity(t *testing.T, applied report.Event, data map[string]any, kind report.Kind) {
	t.Helper()
	if kind.IsCase() {
		if applied.CaseID != dataString(data, "case_id") {
			t.Errorf("projected case id %q, wire case_id is %q", applied.CaseID, dataString(data, "case_id"))
		}
		return
	}
	if applied.SourceType != dataString(data, "source_type") {
		t.Errorf("projected source type %q, wire source_type is %q", applied.SourceType, dataString(data, "source_type"))
	}
}

// E3: every routed type's golden fixture carries the fleet-mandatory
// CloudEvents 1.0 envelope — dataschema
// urn:warehouse:exception-management:analytics:<EventName>:vN — and the
// pinned own spec's envelope schemas require the full attribute set.
func TestCloudEventsEnvelopeInvariants_OwnAnalytics(t *testing.T) {
	for typ := range ownAnalyticsRouted {
		for _, ex := range goldenExamples(t, pinnedOwnAnalyticsSpec, ownAnalyticsTopic, typ) {
			assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "exception-management", eventNameOf(typ))
		}
		assertEnvelopeRequired(t, pinnedOwnAnalyticsSpec, ownAnalyticsTopic, typ)
	}
	doc := loadPinnedAsyncAPI(t, pinnedOwnAnalyticsSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned own spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
}

// E4: the projector is the sole consumer of a topic nobody else writes —
// still a Conformist: unknown additive data keys must not change the
// projection; an unknown event type is acknowledged and ignored; a
// non-CloudEvent is WARNed and committed past; a known type whose
// payload contradicts its envelope (the subject names another case) is
// the deterministic dead-letter the projector documents.
func TestUnknownAttributesTolerated_OwnAnalytics(t *testing.T) {
	golden := clonePayload(goldenExamples(t, pinnedOwnAnalyticsSpec, ownAnalyticsTopic,
		cloudevents.Type("exceptioncase", string(report.KindExceptionOpened)))[0].Payload)

	tolerant := clonePayload(golden)
	data := dataOf(t, tolerant)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	proj := &contractProjection{}
	c := ownAnalyticsConsumer(proj)
	defer c.Close() // stops the reader's background group-retry loop
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if len(proj.applied) != 1 {
		t.Errorf("unknown data attributes changed the projection: %d applied", len(proj.applied))
	}

	otherType := clonePayload(golden)
	otherType["type"] = cloudevents.Type("exceptioncase", "SomethingElse")
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be ignored, not an error: %v", err)
	}
	if len(proj.applied) != 1 {
		t.Errorf("unknown event type was projected (%d applied)", len(proj.applied))
	}

	if err := c.HandleMessage(context.Background(), goldenJSON(t, map[string]any{"case_id": "case-x"})); err != nil {
		t.Fatalf("a non-CloudEvent must be WARNed and committed past, not an error: %v", err)
	}

	tampered := clonePayload(golden)
	tampered["subject"] = "case-somebody-else"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tampered)); err == nil {
		t.Fatal("a known type whose payload contradicts its envelope must surface the dead-letter refusal")
	}
}
