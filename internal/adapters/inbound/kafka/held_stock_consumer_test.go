package kafka_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

const inventorySource = "/warehouse/inventory-storage"

func heldStockHandlers(w *consumeWorld) map[string]kafka.HandlerFunc {
	return kafka.HeldStockHandlers(w.consume, w.settle, slog.Default())
}

// heldCase fetches the held unit's case (the key is the StockHeld type +
// held unit id, no site) without pinning the occurrence, which grows with
// every re-emitted hold or rework arrival.
func heldCase(t *testing.T, w *consumeWorld, unit string) *exceptioncase.Case {
	t.Helper()
	key, err := shared.NewDedupKey(usecases.TypeStockHeld, unit, "")
	if err != nil {
		t.Fatal(err)
	}
	c, err := w.cases.FindOpenByDedupKey(context.Background(), key)
	if err != nil {
		t.Fatalf("no case for held unit %s: %v", unit, err)
	}
	return c
}

func heldPayload(unit string, extra string) string {
	return `{"held_unit_id":"` + unit + `","sku":"SKU-9","quantity":4,"reason":"DAMAGED",` +
		`"receipt_id":"rcpt-77","asn_number":"ASN-88","line_no":2` + extra + `}`
}

// TestHeldStockHandlers_StockHeldOpensOneCasePerHeldUnit: the case keys on
// the held unit id (never the event id), freezes the producer's payload
// (sku, quantity, reason, receipt, asn, line) and inherits the seeded
// policy; a re-emitted StockHeld under a NEW id touches, it never opens a
// second case.
func TestHeldStockHandlers_StockHeldOpensOneCasePerHeldUnit(t *testing.T) {
	w := newConsumeWorld(t, usecases.TypeStockHeld)
	handlers := heldStockHandlers(w)
	ctx := context.Background()

	held := producerEvent(t, "hold-1", inventorySource, usecases.TypeStockHeld, "hu-1", heldPayload("hu-1", `,"held_at":"2026-11-12T09:00:00Z"`))
	if err := handlers[usecases.TypeStockHeld](ctx, held); err != nil {
		t.Fatalf("StockHeld: %v", err)
	}
	c := openCase(t, w, usecases.TypeStockHeld, "hu-1", "")
	if c.SourceEventID() != "hold-1" {
		t.Fatalf("source event = %s, want hold-1", c.SourceEventID())
	}
	payload := string(c.SourcePayload())
	for _, fragment := range []string{
		`"held_unit_id":"hu-1"`, `"sku":"SKU-9"`, `"quantity":4`, `"reason":"DAMAGED"`,
		`"receipt_id":"rcpt-77"`, `"asn_number":"ASN-88"`, `"line_no":2`,
	} {
		if !strings.Contains(payload, fragment) {
			t.Fatalf("frozen payload = %s, want it to carry %s", payload, fragment)
		}
	}

	// A re-emitted StockHeld (new event id, same unit) touches: occurrence 2.
	reemitted := producerEvent(t, "hold-2", inventorySource, usecases.TypeStockHeld, "hu-1", heldPayload("hu-1", `,"held_at":"2026-11-12T09:05:00Z"`))
	if err := handlers[usecases.TypeStockHeld](ctx, reemitted); err != nil {
		t.Fatalf("re-emitted StockHeld: %v", err)
	}
	if c2 := heldCase(t, w, "hu-1"); c2.Occurrence() != 2 {
		t.Fatalf("occurrence = %d, want 2 (touch, not a second case)", c2.Occurrence())
	}

	// A blank held unit id is poison: dead-lettered, never a case.
	blank := producerEvent(t, "hold-3", inventorySource, usecases.TypeStockHeld, "hu-x", `{}`)
	if err := handlers[usecases.TypeStockHeld](ctx, blank); err == nil {
		t.Fatal("a payload with no held_unit_id must be poison")
	}
}

// TestHeldStockHandlers_ClosingFactsCloseTheCase: a disposition that ends
// the hold and a release both auto-resolve the held unit's case with the
// outcome in the note.
func TestHeldStockHandlers_ClosingFactsCloseTheCase(t *testing.T) {
	w := newConsumeWorld(t, usecases.TypeStockHeld)
	handlers := heldStockHandlers(w)
	ctx := context.Background()

	hold := func(unit, eventID string) {
		t.Helper()
		e := producerEvent(t, eventID, inventorySource, usecases.TypeStockHeld, unit,
			heldPayload(unit, `,"held_at":"2026-11-12T09:00:00Z"`))
		if err := handlers[usecases.TypeStockHeld](ctx, e); err != nil {
			t.Fatalf("hold %s: %v", unit, err)
		}
	}
	assertResolved := func(unit, note string) {
		t.Helper()
		c := openCase(t, w, usecases.TypeStockHeld, unit, "")
		if c.Status() != exceptioncase.StatusResolved {
			t.Fatalf("%s status = %s, want RESOLVED", unit, c.Status())
		}
		if c.Note() != note {
			t.Fatalf("%s note = %q, want %q", unit, c.Note(), note)
		}
	}

	hold("hu-rel", "h-rel")
	rel := producerEvent(t, "rel-1", inventorySource, usecases.TypeStockReleased, "hu-rel",
		`{"held_unit_id":"hu-rel","sku":"SKU-9","quantity":4,"receipt_id":"rcpt-77","asn_number":"ASN-88","line_no":2,"released_at":"2026-11-12T10:00:00Z"}`)
	if err := handlers[usecases.TypeStockReleased](ctx, rel); err != nil {
		t.Fatalf("StockReleased: %v", err)
	}
	assertResolved("hu-rel", "stock released")

	hold("hu-rtv", "h-rtv")
	disp := producerEvent(t, "disp-1", inventorySource, usecases.TypeStockDispositioned, "hu-rtv",
		`{"held_unit_id":"hu-rtv","sku":"SKU-9","quantity":4,"disposition":"RETURN_TO_VENDOR","reason":"DAMAGED","receipt_id":"rcpt-77","asn_number":"ASN-88","line_no":2,"decided_at":"2026-11-12T10:30:00Z"}`)
	if err := handlers[usecases.TypeStockDispositioned](ctx, disp); err != nil {
		t.Fatalf("StockDispositioned: %v", err)
	}
	assertResolved("hu-rtv", "disposition RETURN_TO_VENDOR")
}

// TestHeldStockHandlers_ReworkKeepsTheCaseOpen: the producer's REWORK keeps
// the unit HELD, so the case stays open and the arrival is a touch.
func TestHeldStockHandlers_ReworkKeepsTheCaseOpen(t *testing.T) {
	w := newConsumeWorld(t, usecases.TypeStockHeld)
	handlers := heldStockHandlers(w)
	ctx := context.Background()

	held := producerEvent(t, "h-1", inventorySource, usecases.TypeStockHeld, "hu-9", heldPayload("hu-9", `,"held_at":"2026-11-12T09:00:00Z"`))
	if err := handlers[usecases.TypeStockHeld](ctx, held); err != nil {
		t.Fatal(err)
	}
	rework := producerEvent(t, "disp-2", inventorySource, usecases.TypeStockDispositioned, "hu-9",
		`{"held_unit_id":"hu-9","sku":"SKU-9","quantity":4,"disposition":"REWORK","reason":"DAMAGED","receipt_id":"rcpt-77","asn_number":"ASN-88","line_no":2,"decided_at":"2026-11-12T11:00:00Z"}`)
	if err := handlers[usecases.TypeStockDispositioned](ctx, rework); err != nil {
		t.Fatalf("rework: %v", err)
	}
	c := heldCase(t, w, "hu-9")
	if c.Status() != exceptioncase.StatusOpen {
		t.Fatalf("status = %s, want OPEN (the unit is still held)", c.Status())
	}
	if c.Occurrence() != 2 {
		t.Fatalf("occurrence = %d, want 2 (the rework is recorded)", c.Occurrence())
	}
}

// TestHeldStockHandlers_UnknownHeldUnitCommitsPast: a closing fact about a
// hold this service never tracked is WARNed and committed past (nil), and
// is never dead-lettered.
func TestHeldStockHandlers_UnknownHeldUnitCommitsPast(t *testing.T) {
	w := newConsumeWorld(t, usecases.TypeStockHeld)
	handlers := heldStockHandlers(w)
	disp := producerEvent(t, "disp-9", inventorySource, usecases.TypeStockDispositioned, "hu-ghost",
		`{"held_unit_id":"hu-ghost","sku":"SKU-9","quantity":4,"disposition":"SCRAP","reason":"DAMAGED","receipt_id":"r","asn_number":"a","line_no":1,"decided_at":"2026-11-12T12:00:00Z"}`)
	c := &kafka.Consumer{Name: "test consumer", Topic: "t", Handlers: handlers, Logger: slog.Default(), DLQ: &recordingDLQ{}}
	if err := c.HandleMessage(context.Background(), encode(t, disp)); err != nil {
		t.Fatalf("unknown held unit = %v, want nil (WARN + commit past)", err)
	}
	if n := len(c.DLQ.(*recordingDLQ).writes); n != 0 {
		t.Fatalf("dead-lettered a well-formed closing fact (%d writes)", n)
	}
}

// TestHeldStockHandlers_UnknownDispositionCommitsPast: a disposition
// outside the pinned contract is a WARN-and-commit-past skip, never a
// retry loop and never the DLQ.
func TestHeldStockHandlers_UnknownDispositionCommitsPast(t *testing.T) {
	w := newConsumeWorld(t, usecases.TypeStockHeld)
	handlers := heldStockHandlers(w)
	held := producerEvent(t, "h-2", inventorySource, usecases.TypeStockHeld, "hu-10", heldPayload("hu-10", `,"held_at":"2026-11-12T09:00:00Z"`))
	if err := handlers[usecases.TypeStockHeld](context.Background(), held); err != nil {
		t.Fatal(err)
	}
	future := producerEvent(t, "disp-f", inventorySource, usecases.TypeStockDispositioned, "hu-10",
		`{"held_unit_id":"hu-10","sku":"SKU-9","quantity":4,"disposition":"DONATE","reason":"DAMAGED","receipt_id":"r","asn_number":"a","line_no":1,"decided_at":"2026-11-12T12:00:00Z"}`)
	c := &kafka.Consumer{Name: "test consumer", Topic: "t", Handlers: handlers, Logger: slog.Default(), DLQ: &recordingDLQ{}}
	if err := c.HandleMessage(context.Background(), encode(t, future)); err != nil {
		t.Fatalf("unknown disposition = %v, want nil (WARN + commit past)", err)
	}
	if n := len(c.DLQ.(*recordingDLQ).writes); n != 0 {
		t.Fatalf("dead-lettered a well-formed closing fact (%d writes)", n)
	}
	still := openCase(t, w, usecases.TypeStockHeld, "hu-10", "")
	if still.Status() != exceptioncase.StatusOpen {
		t.Fatal("the case must stay open")
	}
}
