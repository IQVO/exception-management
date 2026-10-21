package kafka_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// consumeWorld wires ConsumeFact (and ConsumeCorrelation) over the memory
// doubles and declares a policy per source type a test consumes.
type consumeWorld struct {
	cases     *memory.Cases
	policies  *memory.Policies
	events    *memory.Events
	consume   *usecases.ConsumeFact
	correlate *usecases.ConsumeCorrelation
	now       time.Time
}

func newConsumeWorld(t *testing.T, sourceTypes ...string) *consumeWorld {
	t.Helper()
	w := &consumeWorld{
		cases: memory.NewCases(), policies: memory.NewPolicies(), events: &memory.Events{},
		now: time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC),
	}
	open := &usecases.OpenOrTouchException{
		Cases: w.cases, Policies: w.policies, Events: w.events,
		IDs: memory.NewIDGen(), Clock: memory.Clock{T: w.now},
	}
	auto := &usecases.AutoResolveOnCorrelation{
		Cases: w.cases, Policies: w.policies, Events: w.events, Clock: memory.Clock{T: w.now},
	}
	w.consume = &usecases.ConsumeFact{Processed: memory.NewProcessedEvents(), OpenOrTouch: open}
	w.correlate = &usecases.ConsumeCorrelation{Processed: memory.NewProcessedEvents(), AutoResolve: auto}
	declare := &usecases.DeclareExceptionPolicy{
		Policies: w.policies, Events: w.events, Clock: memory.Clock{T: w.now},
	}
	for _, typ := range sourceTypes {
		in := usecases.PolicyDeclaration{
			SourceType: typ, Severity: shared.SeverityHigh, Queue: "ps-main", SLAMinutes: 30, Actor: "op-1",
		}
		// The one rule the end-to-end correlation needs: TaskCPTMissed
		// auto-resolves on OrderRepromised.
		if typ == kafka.TypeTaskCPTMissed && len(sourceTypes) > 1 {
			in.AutoResolveOn, in.WithinMinutes = "com.warehouse.wes.order-management.order.OrderRepromised", 10
		}
		if _, err := declare.Execute(context.Background(), in); err != nil {
			t.Fatalf("declare policy for %s: %v", typ, err)
		}
	}
	return w
}

// TestFulfillmentHandlers_MapsTheFactsToOpenOrTouch: each of the three
// types opens a case keyed on the payload's natural id (order_ref for
// TaskCPTMissed — the work unit the OrderRepromised correlation reverses),
// an empty site part, and the frozen producer payload.
func TestFulfillmentHandlers_MapsTheFactsToOpenOrTouch(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeTaskCPTMissed, kafka.TypePackageDiverted, kafka.TypeWeightDiscrepancyDetected)
	handlers := kafka.FulfillmentHandlers(w.consume, slog.Default())
	ctx := context.Background()

	cpt := producerEvent(t, "e-cpt", "/warehouse/fulfillment-execution",
		kafka.TypeTaskCPTMissed, "wu-8a1f",
		`{"task_id":"task-8a1f","order_ref":"wu-8a1f","task_type":"PICK","cpt":"2026-09-30T14:55:00Z"}`)
	if err := handlers[kafka.TypeTaskCPTMissed](ctx, cpt); err != nil {
		t.Fatalf("TaskCPTMissed: %v", err)
	}
	c := openCase(t, w, kafka.TypeTaskCPTMissed, "wu-8a1f", "")
	if c.SourceEventID() != "e-cpt" || string(c.SourcePayload()) == "" {
		t.Fatalf("frozen source = %s %q, want the event id and payload", c.SourceEventID(), c.SourcePayload())
	}

	if err := handlers[kafka.TypePackageDiverted](ctx, producerEvent(t, "e-div",
		"/warehouse/fulfillment-execution", kafka.TypePackageDiverted, "pkg-1",
		`{"package_id":"pkg-1"}`)); err != nil {
		t.Fatalf("PackageDiverted: %v", err)
	}
	openCase(t, w, kafka.TypePackageDiverted, "pkg-1", "")

	if err := handlers[kafka.TypeWeightDiscrepancyDetected](ctx, producerEvent(t, "e-wgt",
		"/warehouse/fulfillment-execution", kafka.TypeWeightDiscrepancyDetected, "pkg-2",
		`{"package_id":"pkg-2","expected_g":1000,"actual_g":1200}`)); err != nil {
		t.Fatalf("WeightDiscrepancyDetected: %v", err)
	}
	openCase(t, w, kafka.TypeWeightDiscrepancyDetected, "pkg-2", "")

	// A blank natural id is deterministic: the handler maps it to a skip,
	// never to a case.
	blank := producerEvent(t, "e-blank", "/warehouse/fulfillment-execution",
		kafka.TypeTaskCPTMissed, "wu-x", `{"task_id":"t-1","task_type":"PICK"}`)
	if err := handlers[kafka.TypeTaskCPTMissed](ctx, blank); err == nil {
		t.Fatal("a payload with no order_ref must refuse deterministically")
	}
}

// openCase fetches the case the fact must have opened under key
// (sourceType, subject, site).
func openCase(t *testing.T, w *consumeWorld, sourceType, subject, site string) *exceptioncase.Case {
	t.Helper()
	key, err := shared.NewDedupKey(sourceType, subject, site)
	if err != nil {
		t.Fatal(err)
	}
	c, err := w.cases.FindOpenByDedupKey(context.Background(), key)
	if err != nil {
		t.Fatalf("no case for (%s, %s, %q): %v", sourceType, subject, site, err)
	}
	if c.Queue() != "ps-main" || c.SLAMinutes() != 30 || c.Occurrence() != 1 {
		t.Fatalf("case opened outside the declared policy: queue=%s sla=%d occ=%d", c.Queue(), c.SLAMinutes(), c.Occurrence())
	}
	return c
}
