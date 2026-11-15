package kafka_test

// Event consumer contract tests (E1-E4) for the inventory-storage
// HELD-STOCK edge (ADR 0012): the held-stock consumer reads the SAME
// warehouse.inventory.events topic as the inventory exceptions consumer
// under its OWN consumer group, acting on stock.StockHeld (open-or-touch
// one case per held unit), stock.StockDispositioned and
// stock.StockReleased (close it — except a REWORK disposition, which
// keeps the unit held and the case open). The contract is the same
// PINNED copy of inventory-storage's apis/asyncapi.yaml as the
// exceptions edge (contracts/pinned.json registers one pin per producer
// file, and both consumers read that one file). Golden fixtures are the
// producer spec's own examples — the three held-stock examples share one
// held unit, so the open -> rework-touch -> release lifecycle runs on
// the producer's own documents.

import (
	"context"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// heldTypes are the three routed types of the held-stock handler map.
var heldTypes = []string{usecases.TypeStockHeld, usecases.TypeStockDispositioned, usecases.TypeStockReleased}

// E1: the pinned spec's inventory channel documents every held-stock
// type the handler map routes on.
func TestPinnedAsyncAPICoversSubscriptions_HeldStock(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInventoryStorageSpec)
	documented := doc.documentedTypes(t, kafka.InventoryTopic)
	for _, typ := range heldTypes {
		if _, ok := documented[typ]; !ok {
			t.Errorf("pinned inventory-storage spec does not document %s on %s (documented: %v)",
				typ, kafka.InventoryTopic, documented)
		}
	}
}

// E2: the producer's own StockHeld example opens exactly one case under
// (StockHeld, held_unit_id, "") and freezes the wire payload; the
// StockDispositioned example (REWORK) keeps that case OPEN; the
// StockReleased example closes it; each redelivered CloudEvents id
// creates nothing more. The closing steps run on ADAPTED clones with
// FRESH CloudEvents ids: the producer's three held-stock examples share
// one id, and the verbatim StockHeld run above already claimed it (the
// id is the ProcessedEvents claim key).
func TestGoldenEventsDecode_HeldStockLifecycle(t *testing.T) {
	w := newConsumeWorld(t, usecases.TypeStockHeld)
	c := contractConsumer("held stock contract", kafka.InventoryTopic,
		kafka.HeldStockHandlers(w.consume, w.settle, contractLogger()))

	for _, ex := range goldenExamples(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, usecases.TypeStockHeld) {
		if err := c.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
			t.Fatalf("handle StockHeld golden example: %v", err)
		}
		unit := dataString(dataOf(t, ex.Payload), "held_unit_id")
		held := heldCase(t, w, unit)
		if held.SourceEventID() != idOf(t, ex.Payload) {
			t.Errorf("frozen source event id %q, wire id is %q", held.SourceEventID(), idOf(t, ex.Payload))
		}
		if err := c.HandleMessage(context.Background(), goldenJSON(t, ex.Payload)); err != nil {
			t.Fatalf("StockHeld redelivery: %v", err)
		}
		if got := openCaseCount(t, w); got != 1 {
			t.Errorf("replayed StockHeld id changed the open-case set: %d", got)
		}
	}

	// REWORK keeps the unit held: the case survives the disposition.
	rework := clonePayload(goldenExamples(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, usecases.TypeStockDispositioned)[0].Payload)
	rework["id"] = "contract-held-rework-fresh-id"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, rework)); err != nil {
		t.Fatalf("handle StockDispositioned golden example: %v", err)
	}
	if got := openCaseCount(t, w); got != 1 {
		t.Fatalf("a REWORK disposition must keep the held-stock case open: %d open", got)
	}

	// The release ends the hold: the case auto-resolves away.
	release := clonePayload(goldenExamples(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, usecases.TypeStockReleased)[0].Payload)
	release["id"] = "contract-held-release-fresh-id"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, release)); err != nil {
		t.Fatalf("handle StockReleased golden example: %v", err)
	}
	if got := openCaseCount(t, w); got != 0 {
		t.Fatalf("a release must auto-resolve the held-stock case: %d still open", got)
	}
}

// idOf returns a golden payload's CloudEvents id.
func idOf(t *testing.T, payload map[string]any) string {
	t.Helper()
	id, _ := payload["id"].(string)
	if id == "" {
		t.Fatalf("golden payload carries no CloudEvents id")
	}
	return id
}

// E3: every held-stock type's golden fixture carries the fleet-mandatory
// CloudEvents 1.0 envelope, and the pinned spec's envelope schemas
// require the full attribute set.
func TestCloudEventsEnvelopeInvariants_HeldStock(t *testing.T) {
	for _, typ := range heldTypes {
		for _, ex := range goldenExamples(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, typ) {
			assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "inventory-storage", eventNameOf(typ))
		}
		assertEnvelopeRequired(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, typ)
	}
}

// E4: unknown additive data keys must not change the held-stock effect;
// an unknown event type is silently skipped; a non-CloudEvent is WARNed
// and committed past; a StockReleased that lost its held_unit_id is the
// dead-lettered poison refusal; a disposition OUTSIDE the pinned
// contract is the documented WARN-and-commit-past skip, never a retry.
func TestUnknownAttributesTolerated_HeldStock(t *testing.T) {
	w := newConsumeWorld(t, usecases.TypeStockHeld)
	c := contractConsumer("held stock contract", kafka.InventoryTopic,
		kafka.HeldStockHandlers(w.consume, w.settle, contractLogger()))

	golden := clonePayload(goldenExamples(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, usecases.TypeStockHeld)[0].Payload)
	tolerant := clonePayload(golden)
	data := dataOf(t, tolerant)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle StockHeld with unknown data attributes: %v", err)
	}
	if got := openCaseCount(t, w); got != 1 {
		t.Errorf("unknown data attributes changed the outcome: %d open cases", got)
	}

	otherType := clonePayload(golden)
	otherType["type"] = usecases.TypeStockHeld + "SomethingElse"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if got := openCaseCount(t, w); got != 1 {
		t.Errorf("unknown event type was acted on (%d open cases)", got)
	}

	if err := c.HandleMessage(context.Background(), goldenJSON(t, map[string]any{"sku": "SKU-9"})); err != nil {
		t.Fatalf("a non-CloudEvent must be WARNed and committed past, not an error: %v", err)
	}

	poison := clonePayload(goldenExamples(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, usecases.TypeStockReleased)[0].Payload)
	dataOf(t, poison)["held_unit_id"] = ""
	poisonDLQ := &recordingDLQ{}
	pc := contractConsumer("held stock contract", kafka.InventoryTopic, c.Handlers)
	pc.DLQ = poisonDLQ
	if err := pc.HandleMessage(context.Background(), goldenJSON(t, poison)); err == nil {
		t.Fatal("a poison closing payload must surface the dead-letter refusal")
	}

	unknownDisposition := clonePayload(goldenExamples(t, pinnedInventoryStorageSpec, kafka.InventoryTopic, usecases.TypeStockDispositioned)[0].Payload)
	dataOf(t, unknownDisposition)["disposition"] = "OUTSIDE_THE_PINNED_CONTRACT"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, unknownDisposition)); err != nil {
		t.Fatalf("a disposition outside the pinned contract is a WARN-and-commit-past skip, not an error: %v", err)
	}
}
