package kafka_test

// Event consumer contract tests (E1-E4) for the returns-processing edge:
// the returns exceptions consumer reads returns-processing's shared
// topic warehouse.returns-processing.events (kafka.ReturnsTopic) and
// acts only on returnreceipt.UnexpectedReturnReceived. The contract is
// the PINNED copy of returns-processing's apis/asyncapi.yaml
// (testdata/returns-processing.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's own
// examples.
//
// NOTE spec shape: like inbound-receiving, returns-processing pins the
// CloudEvents `type` of each message only in its EXAMPLES (the shared
// envelope schema types it as plain string) — the harness reads the
// documented type from there.

import (
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// E1: the topic the reader subscribes to is documented by the producer's
// pinned spec, and its message list documents the exact CloudEvents type
// this repo's decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_ReturnsEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedReturnsSpec)
	documented := doc.documentedTypes(t, kafka.ReturnsTopic)
	if _, ok := documented[kafka.TypeUnexpectedReturnReceived]; !ok {
		t.Errorf("pinned returns-processing spec does not document %s on %s (documented: %v)",
			kafka.TypeUnexpectedReturnReceived, kafka.ReturnsTopic, documented)
	}
}

// E2: the producer's own UnexpectedReturnReceived example decodes
// through the REAL HandleMessage decode+dispatch and opens exactly one
// case under (type, receipt_id, site_id); a redelivered CloudEvents id
// creates nothing more.
func TestGoldenEventsDecode_UnexpectedReturnReceived(t *testing.T) {
	assertGoldenFact(t, func(t *testing.T) (*consumeWorld, *kafka.Consumer) {
		w := newConsumeWorld(t, kafka.TypeUnexpectedReturnReceived)
		return w, contractConsumer("returns contract", kafka.ReturnsTopic,
			kafka.ReturnsExceptionsHandlers(w.consume, contractLogger()))
	}, pinnedReturnsSpec, kafka.ReturnsTopic,
		kafka.TypeUnexpectedReturnReceived, keyBy("receipt_id", "site_id"))
}

// E3: the golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — dataschema
// urn:warehouse:returns-processing:events:UnexpectedReturnReceived:vN —
// and the pinned spec's envelope schema requires the full attribute set.
func TestCloudEventsEnvelopeInvariants_UnexpectedReturnReceived(t *testing.T) {
	for _, ex := range goldenExamples(t, pinnedReturnsSpec, kafka.ReturnsTopic, kafka.TypeUnexpectedReturnReceived) {
		assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "returns-processing", "UnexpectedReturnReceived")
	}
	doc := loadPinnedAsyncAPI(t, pinnedReturnsSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	assertEnvelopeRequired(t, pinnedReturnsSpec, kafka.ReturnsTopic, kafka.TypeUnexpectedReturnReceived)
}

// E4: unknown additive data keys must not change the effect; an unknown
// event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; an UnexpectedReturnReceived that lost its receipt_id
// is the dead-lettered poison refusal.
func TestUnknownAttributesTolerated_Returns(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeUnexpectedReturnReceived)
	c := contractConsumer("returns contract", kafka.ReturnsTopic,
		kafka.ReturnsExceptionsHandlers(w.consume, contractLogger()))
	assertUnknownTolerance(t, c, w, pinnedReturnsSpec, kafka.ReturnsTopic,
		kafka.TypeUnexpectedReturnReceived, keyBy("receipt_id", "site_id"),
		func(data map[string]any) { data["receipt_id"] = "" })
}
