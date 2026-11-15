package kafka_test

// Event consumer contract tests (E1-E4) for the fulfillment-execution
// edge: the fulfillment exceptions consumer reads fulfillment-execution's
// shared topic warehouse.fulfillment.events (kafka.FulfillmentTopic) and
// acts on TaskCPTMissed, PackageDiverted and
// WeightDiscrepancyDetected. The contract is the PINNED copy of
// fulfillment-execution's apis/asyncapi.yaml
// (testdata/fulfillment-execution.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's own
// examples.

import (
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// fulfillmentKeys maps each routed type to the payload fields its fact()
// keys the case on (none of the three v1 payloads carries a site).
var fulfillmentKeys = map[string]factKey{
	kafka.TypeTaskCPTMissed:             keyBy("order_ref", ""),
	kafka.TypePackageDiverted:           keyBy("package_id", ""),
	kafka.TypeWeightDiscrepancyDetected: keyBy("package_id", ""),
}

// E1: the topic the reader subscribes to is documented by the producer's
// pinned spec, and its message list documents every type this repo's
// decoder routes on (the topic carries thirteen; the other ten stay
// ignored — E4 exercises that skip).
func TestPinnedAsyncAPICoversSubscriptions_FulfillmentEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	documented := doc.documentedTypes(t, kafka.FulfillmentTopic)
	for typ := range fulfillmentKeys {
		if _, ok := documented[typ]; !ok {
			t.Errorf("pinned fulfillment-execution spec does not document %s on %s (documented: %v)",
				typ, kafka.FulfillmentTopic, documented)
		}
	}
}

// E2: every routed type's own example decodes through the REAL
// HandleMessage decode+dispatch and opens exactly one case under its
// payload natural id; a redelivered CloudEvents id creates nothing more.
// Each type runs in a FRESH world: the producer's three examples share
// one CloudEvents id, and the id claim is global per world.
func TestGoldenEventsDecode_FulfillmentExceptions(t *testing.T) {
	for typ, key := range fulfillmentKeys {
		assertGoldenFact(t, func(t *testing.T) (*consumeWorld, *kafka.Consumer) {
			w := newConsumeWorld(t, typ)
			return w, contractConsumer("fulfillment contract", kafka.FulfillmentTopic,
				kafka.FulfillmentHandlers(w.consume, contractLogger()))
		}, pinnedFulfillmentSpec, kafka.FulfillmentTopic, typ, key)
	}
}

// E3: every routed type's golden fixture carries the fleet-mandatory
// CloudEvents 1.0 envelope, and the pinned spec's envelope schemas
// require the full attribute set.
func TestCloudEventsEnvelopeInvariants_FulfillmentEvents(t *testing.T) {
	for typ := range fulfillmentKeys {
		for _, ex := range goldenExamples(t, pinnedFulfillmentSpec, kafka.FulfillmentTopic, typ) {
			assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "fulfillment-execution", eventNameOf(typ))
		}
		assertEnvelopeRequired(t, pinnedFulfillmentSpec, kafka.FulfillmentTopic, typ)
	}
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
}

// E4: unknown additive data keys must not change the effect; an unknown
// event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; a TaskCPTMissed that lost its order_ref is the
// dead-lettered poison refusal.
func TestUnknownAttributesTolerated_Fulfillment(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeTaskCPTMissed)
	c := contractConsumer("fulfillment contract", kafka.FulfillmentTopic,
		kafka.FulfillmentHandlers(w.consume, contractLogger()))
	assertUnknownTolerance(t, c, w, pinnedFulfillmentSpec, kafka.FulfillmentTopic,
		kafka.TypeTaskCPTMissed, keyBy("order_ref", ""),
		func(data map[string]any) { data["order_ref"] = "" })
}
