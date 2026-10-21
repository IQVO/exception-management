package kafka_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// TestReturnsHandlers_KeyOnReceiptAndSite: the unexpected return keys on
// (type, receipt_id, site_id) — the receipt's site is part of the key, so
// the same receipt at another site is another case.
func TestReturnsHandlers_KeyOnReceiptAndSite(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeUnexpectedReturnReceived)
	handlers := kafka.ReturnsExceptionsHandlers(w.consume, slog.Default())
	e := producerEvent(t, "e-unex", "/warehouse/returns-processing",
		kafka.TypeUnexpectedReturnReceived, "rcpt-1",
		`{"receipt_id":"rcpt-1","line_no":2,"sku":"SKU-9","quantity":1,"site_id":"SIM1","received_at":"2026-10-21T09:05:00Z"}`)
	if err := handlers[kafka.TypeUnexpectedReturnReceived](context.Background(), e); err != nil {
		t.Fatalf("UnexpectedReturnReceived: %v", err)
	}
	openCase(t, w, kafka.TypeUnexpectedReturnReceived, "rcpt-1", "SIM1")
}
