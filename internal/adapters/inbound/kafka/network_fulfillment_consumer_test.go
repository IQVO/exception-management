package kafka_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// TestNetworkFulfillmentHandlers_KeyOnNetworkRefAndSite: both consumed
// types dedupe on (type, networkRef, siteId) — the site part is NOT empty
// here, so the same networkRef at two sites is two cases.
func TestNetworkFulfillmentHandlers_KeyOnNetworkRefAndSite(t *testing.T) {
	w := newConsumeWorld(t,
		kafka.TypeAcknowledgementDeadlineAtRisk, kafka.TypeOrphanedHoldCancelled)
	handlers := kafka.NetworkFulfillmentHandlers(w.consume, slog.Default())
	ctx := context.Background()

	risk := producerEvent(t, "e-risk", "/warehouse/network-fulfillment",
		kafka.TypeAcknowledgementDeadlineAtRisk, "po-1",
		`{"networkRef":"po-1","siteId":"site-1","acknowledgeBy":"2026-10-21T10:00:00Z","at":"2026-10-21T09:40:00Z"}`)
	if err := handlers[kafka.TypeAcknowledgementDeadlineAtRisk](ctx, risk); err != nil {
		t.Fatalf("AcknowledgementDeadlineAtRisk: %v", err)
	}
	openCase(t, w, kafka.TypeAcknowledgementDeadlineAtRisk, "po-1", "site-1")

	orphaned := producerEvent(t, "e-orph", "/warehouse/network-fulfillment",
		kafka.TypeOrphanedHoldCancelled, "po-2",
		`{"networkRef":"po-2","siteId":"site-2","localOrderId":"ord-9","acknowledgeBy":"2026-10-21T10:00:00Z","answer":"CANCELLED","at":"2026-10-21T09:59:00Z"}`)
	if err := handlers[kafka.TypeOrphanedHoldCancelled](ctx, orphaned); err != nil {
		t.Fatalf("OrphanedHoldCancelled: %v", err)
	}
	openCase(t, w, kafka.TypeOrphanedHoldCancelled, "po-2", "site-2")
}
