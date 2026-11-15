package kafka_test

// Event consumer contract tests (E1-E4) for the order-management edge:
// the order correlation consumer reads order-management's shared topic
// warehouse.order-management.events (kafka.OrderManagementTopic) and
// acts only on OrderRepromised — the correlated fact of the auto-resolve
// rules (ADR 0002). It opens NO case: v1 exception-management keeps no
// local order copy. The contract is the PINNED copy of order-management's
// apis/asyncapi.yaml (testdata/order-management.asyncapi.yaml,
// registered in contracts/pinned.json). Golden fixtures are the producer
// spec's own examples.

import (
	"context"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// E1: the topic the reader subscribes to is documented by the producer's
// pinned spec, and its message list documents the exact CloudEvents type
// this repo's decoder routes on (the channel also carries the order-copy
// facts OrderAllocated / OrderPartiallyAllocated — deliberately ignored).
func TestPinnedAsyncAPICoversSubscriptions_OrderManagementEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOrderManagementSpec)
	documented := doc.documentedTypes(t, kafka.OrderManagementTopic)
	if _, ok := documented[kafka.TypeOrderRepromised]; !ok {
		t.Errorf("pinned order-management spec does not document %s on %s (documented: %v)",
			kafka.TypeOrderRepromised, kafka.OrderManagementTopic, documented)
	}
}

// E2: the producer's own OrderRepromised example decodes through the
// REAL HandleMessage decode+dispatch and correlates: with no case open
// the fact is a clean no-op (no case, no error, no payload frozen), and
// a redelivered CloudEvents id changes nothing.
func TestGoldenEventsDecode_OrderRepromised(t *testing.T) {
	w := newConsumeWorld(t)
	c := contractConsumer("order correlation contract", kafka.OrderManagementTopic,
		kafka.OrderCorrelationHandlers(w.correlate, contractLogger()))
	for _, ex := range goldenExamples(t, pinnedOrderManagementSpec, kafka.OrderManagementTopic, kafka.TypeOrderRepromised) {
		t.Run(ex.Name, func(t *testing.T) {
			raw := goldenJSON(t, ex.Payload)
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			if got := openCaseCount(t, w); got != 0 {
				t.Errorf("OrderRepromised opens no case; %d open", got)
			}
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("redelivery: %v", err)
			}
			if got := openCaseCount(t, w); got != 0 {
				t.Errorf("redelivery opened a case: %d", got)
			}
		})
	}
}

// E3: the golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — dataschema
// urn:warehouse:order-management:events:OrderRepromised:vN — and the
// pinned spec's envelope schema requires the full attribute set.
func TestCloudEventsEnvelopeInvariants_OrderRepromised(t *testing.T) {
	for _, ex := range goldenExamples(t, pinnedOrderManagementSpec, kafka.OrderManagementTopic, kafka.TypeOrderRepromised) {
		assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "order-management", "OrderRepromised")
	}
	doc := loadPinnedAsyncAPI(t, pinnedOrderManagementSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	assertEnvelopeRequired(t, pinnedOrderManagementSpec, kafka.OrderManagementTopic, kafka.TypeOrderRepromised)
}

// E4: unknown additive data keys must not change the correlation; an
// unknown event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; an OrderRepromised that lost its order_id is the
// dead-lettered poison refusal.
func TestUnknownAttributesTolerated_OrderManagement(t *testing.T) {
	w := newConsumeWorld(t)
	c := contractConsumer("order correlation contract", kafka.OrderManagementTopic,
		kafka.OrderCorrelationHandlers(w.correlate, contractLogger()))
	golden := clonePayload(goldenExamples(t, pinnedOrderManagementSpec, kafka.OrderManagementTopic, kafka.TypeOrderRepromised)[0].Payload)

	tolerant := clonePayload(golden)
	data := dataOf(t, tolerant)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}

	otherType := clonePayload(golden)
	otherType["type"] = kafka.TypeOrderRepromised + "SomethingElse"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if got := openCaseCount(t, w); got != 0 {
		t.Errorf("unknown event type was acted on (%d open cases)", got)
	}

	if err := c.HandleMessage(context.Background(), goldenJSON(t, map[string]any{"order_id": "ord-1"})); err != nil {
		t.Fatalf("a non-CloudEvent must be WARNed and committed past, not an error: %v", err)
	}

	poison := clonePayload(golden)
	dataOf(t, poison)["order_id"] = ""
	pc := contractConsumer("order correlation contract", kafka.OrderManagementTopic, c.Handlers)
	pc.DLQ = &recordingDLQ{}
	if err := pc.HandleMessage(context.Background(), goldenJSON(t, poison)); err == nil {
		t.Fatal("an OrderRepromised without an order id must surface the dead-letter refusal")
	}
}
