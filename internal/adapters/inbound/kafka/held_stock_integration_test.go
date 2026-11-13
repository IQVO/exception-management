//go:build integration

package kafka_test

import (
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/testing/kafkatest"
)

// TestHeldStock_RealKafkaAndPostgres (ADR 0012): the held-stock consumer on
// a real broker and database — a StockHeld opens ONE case per held unit id,
// a re-emitted hold touches it and a replayed id changes nothing, a closing
// disposition auto-resolves with the outcome in the note, the paired
// StockReleased and the case's terminal state make each other no-ops, a
// REWORK keeps the case open (the unit is still held), and a closing fact
// for an unknown held unit is committed past without a dead letter.
func TestHeldStock_RealKafkaAndPostgres(t *testing.T) {
	w := newPGWorld(t)
	w.declarePolicy(t, usecases.TypeStockHeld, "", 0)
	topic := kafkatest.Topic(t, kafka.InventoryTopic)

	const inv = "/warehouse/inventory-storage"
	held := func(id, unit string) []byte {
		return produced(t, id, inv, usecases.TypeStockHeld, unit,
			`{"held_unit_id":"`+unit+`","sku":"SKU-9","quantity":4,"reason":"DAMAGED",`+
				`"receipt_id":"rcpt-77","asn_number":"ASN-88","line_no":2,"held_at":"2026-11-12T09:00:00Z"}`)
	}
	dispositioned := func(id, unit, disposition string) []byte {
		return produced(t, id, inv, usecases.TypeStockDispositioned, unit,
			`{"held_unit_id":"`+unit+`","sku":"SKU-9","quantity":4,"disposition":"`+disposition+`",`+
				`"reason":"DAMAGED","receipt_id":"rcpt-77","asn_number":"ASN-88","line_no":2,"decided_at":"2026-11-12T10:00:00Z"}`)
	}
	// The whole held-stock slice of the topic, in order:
	//  1. hold-1: hu-1 held — opens its case
	//  2. hold-2: hu-1 re-emitted (another id) — TOUCHES it
	//  3. hold-1 again: the replayed id changes nothing
	//  4. disp-1: hu-1 RETURN_TO_VENDOR — auto-resolves with the note
	//  5. rel-1: hu-1 released — the pair's second fact, no-op (terminal)
	//  6. hold-3: hu-2 held — opens its case
	//  7. disp-2: hu-2 REWORK — keeps the case open, records the arrival
	//  8. disp-9: a closing fact for a unit never held — WARN, commit past
	//  9. hold-4: hu-3 held — the sentinel
	messages := keyed("k",
		held("hold-1", "hu-1"),
		held("hold-2", "hu-1"),
		held("hold-1", "hu-1"),
		dispositioned("disp-1", "hu-1", "RETURN_TO_VENDOR"),
		produced(t, "rel-1", inv, usecases.TypeStockReleased, "hu-1",
			`{"held_unit_id":"hu-1","sku":"SKU-9","quantity":4,"receipt_id":"rcpt-77","asn_number":"ASN-88","line_no":2,"released_at":"2026-11-12T10:05:00Z"}`),
		held("hold-3", "hu-2"),
		dispositioned("disp-2", "hu-2", "REWORK"),
		dispositioned("disp-9", "hu-ghost", "SCRAP"),
		held("hold-4", "hu-3"),
	)
	kafkatest.Produce(t, topic, messages...)

	group := theGroup("held-stock")
	runConsumer(t, kafka.NewHeldStockConsumerForTopic(kafkatest.Brokers(t), topic, group, w.consume, w.settle, itLogger(t)))

	// The sentinel: hu-3's case exists — the consumer reached the end.
	eventually(t, "hu-3's case to open", func() bool {
		_, err := w.lookupCase(t, usecases.TypeStockHeld, "hu-3", "")
		return err == nil
	})
	assertCommittedPast(t, group, topic, int64(len(messages)))

	// hu-1: closed by the disposition, with the outcome in the note; the
	// touch applied, the replay and the paired release changed nothing.
	c1 := w.openCase(t, usecases.TypeStockHeld, "hu-1", "")
	if c1.Status() != exceptioncase.StatusResolved {
		t.Fatalf("hu-1 status = %s, want RESOLVED", c1.Status())
	}
	if c1.Note() != "disposition RETURN_TO_VENDOR" {
		t.Fatalf("hu-1 note = %q, want the disposition outcome", c1.Note())
	}
	if c1.Occurrence() != 2 {
		t.Fatalf("hu-1 occurrence = %d, want 2 (open + touch; the replay and the pair add nothing)", c1.Occurrence())
	}
	s1 := c1.Snapshot()
	wantRule := usecases.TypeStockHeld + "->" + usecases.TypeStockDispositioned
	if !s1.AutoResolved || s1.AutoRuleID != wantRule || s1.AutoEventID != "disp-1" {
		t.Fatalf("hu-1 auto-resolution = %+v, want rule %s / disp-1", s1, wantRule)
	}

	// hu-2: REWORK keeps the unit held, so the case stays open with the
	// arrival recorded.
	c2 := w.openCase(t, usecases.TypeStockHeld, "hu-2", "")
	if c2.Status() != exceptioncase.StatusOpen {
		t.Fatalf("hu-2 status = %s, want OPEN (the unit is still held)", c2.Status())
	}
	if c2.Occurrence() != 2 {
		t.Fatalf("hu-2 occurrence = %d, want 2 (the rework is recorded)", c2.Occurrence())
	}

	// Three cases, nothing else; the ghost never opened one.
	if n := w.countTable("exception_cases"); n != 3 {
		t.Fatalf("cases = %d, want 3", n)
	}
	// The declaration + three openings + exactly one auto-resolution: the
	// touches, the replay, the paired release and the ghost raise nothing.
	if n := w.countTable("outbox"); n != 5 {
		t.Fatalf("outbox rows = %d, want 5 (declaration + 3 openings + 1 auto-resolve)", n)
	}
	// Eight unique event ids claimed (the replay claims nothing new).
	if n := w.countTable("processed_events"); n != 8 {
		t.Fatalf("processed_events rows = %d, want 8", n)
	}
}
