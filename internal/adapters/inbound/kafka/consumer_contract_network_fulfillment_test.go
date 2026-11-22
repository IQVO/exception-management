package kafka_test

// Event consumer contract tests (E1-E4) for the network-fulfillment
// edge: the network fulfillment consumer reads network-fulfillment's
// shared topic warehouse.network-fulfillment.events
// (kafka.NetworkFulfillmentTopic) and acts on
// networkorder.AcknowledgementDeadlineAtRisk and
// networkorder.OrphanedHoldCancelled — the two consumed types whose v1
// payloads carry a site (camelCase networkRef / siteId). The contract is
// the PINNED copy of network-fulfillment's apis/asyncapi.yaml
// (testdata/network-fulfillment.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's own
// examples.

import (
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// networkKeys maps each routed type to the payload fields its fact()
// keys the case on (networkRef is the natural id; siteId the site part).
var networkKeys = map[string]factKey{
	kafka.TypeAcknowledgementDeadlineAtRisk: keyBy("networkRef", "siteId"),
	kafka.TypeOrphanedHoldCancelled:         keyBy("networkRef", "siteId"),
}

// E1: the topic the reader subscribes to is documented by the producer's
// pinned spec, and its message list documents every type this repo's
// decoder routes on (the channel carries seven; the order-copy types
// stay ignored — E4 exercises that skip).
func TestPinnedAsyncAPICoversSubscriptions_NetworkFulfillmentEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedNetworkFulfillmentSpec)
	documented := doc.documentedTypes(t, kafka.NetworkFulfillmentTopic)
	for typ := range networkKeys {
		if _, ok := documented[typ]; !ok {
			t.Errorf("pinned network-fulfillment spec does not document %s on %s (documented: %v)",
				typ, kafka.NetworkFulfillmentTopic, documented)
		}
	}
}

// E2: every routed type's own example decodes through the REAL
// HandleMessage decode+dispatch and opens exactly one case under
// (type, networkRef, siteId); a redelivered CloudEvents id creates
// nothing more.
func TestGoldenEventsDecode_NetworkFulfillment(t *testing.T) {
	for typ, key := range networkKeys {
		assertGoldenFact(t, func(t *testing.T) (*consumeWorld, *kafka.Consumer) {
			w := newConsumeWorld(t, typ)
			return w, contractConsumer("network contract", kafka.NetworkFulfillmentTopic,
				kafka.NetworkFulfillmentHandlers(w.consume, contractLogger()))
		}, pinnedNetworkFulfillmentSpec, kafka.NetworkFulfillmentTopic, typ, key)
	}
}

// E3: every routed type's golden fixture carries the fleet-mandatory
// CloudEvents 1.0 envelope, and the pinned spec's envelope schemas
// require the full attribute set.
func TestCloudEventsEnvelopeInvariants_NetworkFulfillment(t *testing.T) {
	for typ := range networkKeys {
		for _, ex := range goldenExamples(t, pinnedNetworkFulfillmentSpec, kafka.NetworkFulfillmentTopic, typ) {
			assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "network-fulfillment", eventNameOf(typ))
		}
		assertEnvelopeRequired(t, pinnedNetworkFulfillmentSpec, kafka.NetworkFulfillmentTopic, typ)
	}
	doc := loadPinnedAsyncAPI(t, pinnedNetworkFulfillmentSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
}

// E4: unknown additive data keys must not change the effect; an unknown
// event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; an AcknowledgementDeadlineAtRisk that lost its
// networkRef is the dead-lettered poison refusal.
func TestUnknownAttributesTolerated_NetworkFulfillment(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeAcknowledgementDeadlineAtRisk)
	c := contractConsumer("network contract", kafka.NetworkFulfillmentTopic,
		kafka.NetworkFulfillmentHandlers(w.consume, contractLogger()))
	assertUnknownTolerance(t, c, w, pinnedNetworkFulfillmentSpec, kafka.NetworkFulfillmentTopic,
		kafka.TypeAcknowledgementDeadlineAtRisk, keyBy("networkRef", "siteId"),
		func(data map[string]any) { data["networkRef"] = "" })
}
