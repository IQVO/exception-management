package kafka_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// TestInventoryExceptionsHandlers_KeyOnBinAndStockUnit: a discrepancy keys
// on the bin, an unlocated item on the stock unit; no site in either v1
// payload.
func TestInventoryExceptionsHandlers_KeyOnBinAndStockUnit(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeDiscrepancyDetected, kafka.TypeItemUnlocated)
	handlers := kafka.InventoryExceptionsHandlers(w.consume, slog.Default())
	ctx := context.Background()

	discrepancy := producerEvent(t, "e-disc", "/warehouse/inventory-storage",
		kafka.TypeDiscrepancyDetected, "A-12-3", `{"bin_id":"A-12-3","counted":47,"system":50}`)
	if err := handlers[kafka.TypeDiscrepancyDetected](ctx, discrepancy); err != nil {
		t.Fatalf("DiscrepancyDetected: %v", err)
	}
	openCase(t, w, kafka.TypeDiscrepancyDetected, "A-12-3", "")

	unlocated := producerEvent(t, "e-unl", "/warehouse/inventory-storage",
		kafka.TypeItemUnlocated, "su-7781",
		`{"stock_unit_id":"su-7781","sku":"SKU-1001","bin_id":"A-12-3","quantity":3}`)
	if err := handlers[kafka.TypeItemUnlocated](ctx, unlocated); err != nil {
		t.Fatalf("ItemUnlocated: %v", err)
	}
	openCase(t, w, kafka.TypeItemUnlocated, "su-7781", "")
}
