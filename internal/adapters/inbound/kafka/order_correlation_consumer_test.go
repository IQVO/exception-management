package kafka_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
)

// TestOrderCorrelationHandlers_RepromiseAutoResolvesTheCase: a TaskCPTMissed
// case opened for work unit ord-8a1f-line-1 is auto-resolved by
// OrderRepromised of order ord-8a1f — order-level correlation, the line
// ignored — with the derived rule id and exactly one ExceptionAutoResolved.
func TestOrderCorrelationHandlers_RepromiseAutoResolvesTheCase(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeTaskCPTMissed, kafka.TypeOrderRepromised)
	// Open the case through the fulfillment handler: the fact's subject is
	// the work unit the correlation reverses.
	fulfillment := kafka.FulfillmentHandlers(w.consume, slog.Default())
	cpt := producerEvent(t, "e-cpt", "/warehouse/fulfillment-execution",
		kafka.TypeTaskCPTMissed, "wu-8a1f",
		`{"task_id":"task-8a1f","order_ref":"ord-8a1f-line-1","task_type":"PICK","cpt":"2026-09-30T14:55:00Z"}`)
	if err := fulfillment[kafka.TypeTaskCPTMissed](context.Background(), cpt); err != nil {
		t.Fatalf("TaskCPTMissed: %v", err)
	}

	handlers := kafka.OrderCorrelationHandlers(w.correlate, slog.Default())
	repromised := producerEvent(t, "e-rep", "/warehouse/order-management",
		kafka.TypeOrderRepromised, "ord-8a1f",
		`{"order_id":"ord-8a1f","cpt_id_old":"sp1-1200","cpt_id_new":"sp1-1800","reason":"TaskCPTMissed"}`)
	if err := handlers[kafka.TypeOrderRepromised](context.Background(), repromised); err != nil {
		t.Fatalf("OrderRepromised: %v", err)
	}

	c := openCase(t, w, kafka.TypeTaskCPTMissed, "ord-8a1f-line-1", "")
	if c.Status() != exceptioncase.StatusResolved || c.AutoResolution() == nil {
		t.Fatalf("case = %s auto=%v, want auto-resolved", c.Status(), c.AutoResolution())
	}
	// The evidence's shape is asserted through the raised fact (the domain
	// keeps the auto-resolution struct in-package).
	names := w.events.CaseEventNames()
	if len(names) != 2 || names[1] != "ExceptionAutoResolved" {
		t.Fatalf("case events = %v, want opened then exactly one auto-resolve", names)
	}
	auto, ok := w.events.Case[1].(exceptioncase.ExceptionAutoResolved)
	if !ok {
		t.Fatalf("second event = %T, want ExceptionAutoResolved", w.events.Case[1])
	}
	wantRule := kafka.TypeTaskCPTMissed + "->" + kafka.TypeOrderRepromised
	if auto.RuleID != wantRule || auto.CorrelatedEventID != "e-rep" || auto.CorrelatedType != kafka.TypeOrderRepromised {
		t.Fatalf("auto-resolve evidence = %+v, want the derived rule %s and the repromise fact", auto, wantRule)
	}
}

// TestOrderCorrelationHandlers_RepromiseOfAnotherOrderChangesNothing: the
// fail-soft no-match — the case stays open, no error.
func TestOrderCorrelationHandlers_RepromiseOfAnotherOrderChangesNothing(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeTaskCPTMissed, kafka.TypeOrderRepromised)
	fulfillment := kafka.FulfillmentHandlers(w.consume, slog.Default())
	cpt := producerEvent(t, "e-cpt", "/warehouse/fulfillment-execution",
		kafka.TypeTaskCPTMissed, "wu-8a1f",
		`{"task_id":"task-8a1f","order_ref":"ord-8a1f-line-1","task_type":"PICK","cpt":"2026-09-30T14:55:00Z"}`)
	if err := fulfillment[kafka.TypeTaskCPTMissed](context.Background(), cpt); err != nil {
		t.Fatal(err)
	}

	handlers := kafka.OrderCorrelationHandlers(w.correlate, slog.Default())
	other := producerEvent(t, "e-rep-x", "/warehouse/order-management",
		kafka.TypeOrderRepromised, "ord-OTHER", `{"order_id":"ord-OTHER","reason":"TaskCPTMissed"}`)
	if err := handlers[kafka.TypeOrderRepromised](context.Background(), other); err != nil {
		t.Fatalf("no-match repromise = %v, want the fail-soft nil", err)
	}
	c := openCase(t, w, kafka.TypeTaskCPTMissed, "ord-8a1f-line-1", "")
	if c.Status() != exceptioncase.StatusOpen {
		t.Fatalf("case status = %s, want still OPEN", c.Status())
	}
}
