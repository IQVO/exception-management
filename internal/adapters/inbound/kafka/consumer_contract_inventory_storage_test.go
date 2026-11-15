package kafka_test

// Event consumer contract tests (E1-E4) for the inventory-storage
// exceptions edge: the inventory exceptions consumer reads
// inventory-storage's shared topic warehouse.inventory.events
// (kafka.InventoryTopic) and acts on bin.DiscrepancyDetected and
// stock.ItemUnlocated (the held-stock consumer reads the SAME topic a
// second time under its own group, ADR 0012 — its contract tests live in
// consumer_contract_inventory_held_stock_test.go). The contract is the
// PINNED copy of inventory-storage's apis/asyncapi.yaml
// (testdata/inventory-storage.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's own
// examples.

import (
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// inventoryKeys maps each routed exceptions type to the payload fields
// its fact() keys the case on (neither v1 payload carries a site).
var inventoryKeys = map[string]factKey{
	kafka.TypeDiscrepancyDetected: keyBy("bin_id", ""),
	kafka.TypeItemUnlocated:       keyBy("stock_unit_id", ""),
}

// E1: the topic the reader subscribes to is documented by the producer's
// pinned spec, and its message list documents every type this repo's
// decoder routes on (the channel carries nineteen; the rest stay
// ignored — E4 exercises that skip).
func TestPinnedAsyncAPICoversSubscriptions_InventoryEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInventoryStorageSpec)
	documented := doc.documentedTypes(t, kafka.InventoryTopic)
	for typ := range inventoryKeys {
		if _, ok := documented[typ]; !ok {
			t.Errorf("pinned inventory-storage spec does not document %s on %s (documented: %v)",
				typ, kafka.InventoryTopic, documented)
		}
	}
}

// E2: every routed type's own example decodes through the REAL
// HandleMessage decode+dispatch and opens exactly one case under its
// payload natural id; a redelivered CloudEvents id creates nothing more.
func TestGoldenEventsDecode_InventoryExceptions(t *testing.T) {
	for typ, key := range inventoryKeys {
		assertGoldenFact(t, func(t *testing.T) (*consumeWorld, *kafka.Consumer) {
			w := newConsumeWorld(t, typ)
			return w, contractConsumer("inventory contract", kafka.InventoryTopic,
				kafka.InventoryExceptionsHandlers(w.consume, contractLogger()))
		}, pinnedInventoryStorageSpec, kafka.InventoryTopic, typ, key)
	}
}

// E3: every routed type's golden fixture carries the fleet-mandatory
// CloudEvents 1.0 envelope, and the pinned spec's envelope schemas
// require the full attribute set.
func TestCloudEventsEnvelopeInvariants_InventoryEvents(t *testing.T) {
	for typ := range inventoryKeys {
		for _, ex := range goldenExamples(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, typ) {
			assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "inventory-storage", eventNameOf(typ))
		}
		assertEnvelopeRequired(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, typ)
	}
	doc := loadPinnedAsyncAPI(t, pinnedInventoryStorageSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
}

// E4: unknown additive data keys must not change the effect; an unknown
// event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; a DiscrepancyDetected that lost its bin_id is the
// dead-lettered poison refusal.
func TestUnknownAttributesTolerated_Inventory(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeDiscrepancyDetected)
	c := contractConsumer("inventory contract", kafka.InventoryTopic,
		kafka.InventoryExceptionsHandlers(w.consume, contractLogger()))
	assertUnknownTolerance(t, c, w, pinnedInventoryStorageSpec, kafka.InventoryTopic,
		kafka.TypeDiscrepancyDetected, keyBy("bin_id", ""),
		func(data map[string]any) { data["bin_id"] = "" })
}
