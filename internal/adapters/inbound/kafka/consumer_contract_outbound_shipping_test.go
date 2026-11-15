package kafka_test

// Event consumer contract tests (E1-E4) for the outbound-shipping edge:
// the outbound shipping exceptions consumer reads outbound-shipping's
// shared topic warehouse.outbound-shipping.events
// (kafka.OutboundShippingTopic) and acts only on
// shippingpackage.PackageMissedLoad. The contract is the PINNED copy of
// outbound-shipping's apis/asyncapi.yaml
// (testdata/outbound-shipping.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's own
// examples.

import (
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// E1: the topic the reader subscribes to is documented by the producer's
// pinned spec, and its message list documents the exact CloudEvents type
// this repo's decoder routes on (the channel carries thirteen load and
// package types; the rest stay ignored).
func TestPinnedAsyncAPICoversSubscriptions_OutboundShippingEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOutboundShippingSpec)
	documented := doc.documentedTypes(t, kafka.OutboundShippingTopic)
	if _, ok := documented[kafka.TypePackageMissedLoad]; !ok {
		t.Errorf("pinned outbound-shipping spec does not document %s on %s (documented: %v)",
			kafka.TypePackageMissedLoad, kafka.OutboundShippingTopic, documented)
	}
}

// E2: the producer's own PackageMissedLoad example decodes through the
// REAL HandleMessage decode+dispatch and opens exactly one case under
// (type, package_id, site_id); a redelivered CloudEvents id creates
// nothing more.
func TestGoldenEventsDecode_PackageMissedLoad(t *testing.T) {
	assertGoldenFact(t, func(t *testing.T) (*consumeWorld, *kafka.Consumer) {
		w := newConsumeWorld(t, kafka.TypePackageMissedLoad)
		return w, contractConsumer("outbound shipping contract", kafka.OutboundShippingTopic,
			kafka.OutboundShippingHandlers(w.consume, contractLogger()))
	}, pinnedOutboundShippingSpec, kafka.OutboundShippingTopic,
		kafka.TypePackageMissedLoad, keyBy("package_id", "site_id"))
}

// E3: the golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — dataschema
// urn:warehouse:outbound-shipping:events:PackageMissedLoad:vN — and the
// pinned spec's envelope schema requires the full attribute set.
//
// FINDING (documented in the PR body, not enforced here): the pinned
// spec's document-level defaultContentType is application/json, off the
// fleet's application/cloudevents+json convention — the per-MESSAGE
// contentType carries the convention, asserted below.
func TestCloudEventsEnvelopeInvariants_PackageMissedLoad(t *testing.T) {
	for _, ex := range goldenExamples(t, pinnedOutboundShippingSpec, kafka.OutboundShippingTopic, kafka.TypePackageMissedLoad) {
		assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "outbound-shipping", "PackageMissedLoad")
	}
	doc := loadPinnedAsyncAPI(t, pinnedOutboundShippingSpec)
	msg, ok := doc.messageForType(t, kafka.OutboundShippingTopic, kafka.TypePackageMissedLoad)
	if !ok {
		t.Fatalf("pinned spec does not document %s on %s", kafka.TypePackageMissedLoad, kafka.OutboundShippingTopic)
	}
	if msg.ContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec message contentType %q, want application/cloudevents+json", msg.ContentType)
	}
	assertEnvelopeRequired(t, pinnedOutboundShippingSpec, kafka.OutboundShippingTopic, kafka.TypePackageMissedLoad)
}

// E4: unknown additive data keys must not change the effect; an unknown
// event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; a PackageMissedLoad that lost its package_id is the
// dead-lettered poison refusal.
func TestUnknownAttributesTolerated_OutboundShipping(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypePackageMissedLoad)
	c := contractConsumer("outbound shipping contract", kafka.OutboundShippingTopic,
		kafka.OutboundShippingHandlers(w.consume, contractLogger()))
	assertUnknownTolerance(t, c, w, pinnedOutboundShippingSpec, kafka.OutboundShippingTopic,
		kafka.TypePackageMissedLoad, keyBy("package_id", "site_id"),
		func(data map[string]any) { data["package_id"] = "" })
}
