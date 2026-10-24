package kafka_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

const cartonSSCC = "340123450000000013"

func cartonPayload(kind, sscc string) string {
	s := ""
	if sscc != "" {
		s = `"sscc":"` + sscc + `",`
	}
	return `{"receipt_id":"rcpt-7",` + s + `"kind":"` + kind + `","expected":"12","actual":"10","at":"2026-10-24T09:00:00Z"}`
}

func cartonWorld(t *testing.T) (*consumeWorld, map[string]kafka.HandlerFunc) {
	t.Helper()
	w2 := newConsumeWorld(t, usecases.RouteProblemSolve, usecases.RouteReceivingLead)
	open := &usecases.OpenCartonDiscrepancy{Consume: w2.consume}
	return w2, kafka.CartonDiscrepancyHandlers(open, slog.Default())
}

func caseOf(t *testing.T, w *consumeWorld, subject string) (queue string, occ int64) {
	t.Helper()
	key, err := shared.NewDedupKey(usecases.TypeCartonDiscrepancyRaised, subject, "")
	if err != nil {
		t.Fatal(err)
	}
	c, err := w.cases.FindOpenByDedupKey(context.Background(), key)
	if err != nil {
		t.Fatalf("no case for %s: %v", subject, err)
	}
	return c.Queue(), c.Occurrence()
}

// TestCartonDiscrepancyHandler_OneCasePerReceiptSsccKind: the natural key is
// (receipt, sscc|"-", kind); the same key re-raised under another event id
// touches, a replayed id changes nothing, a different kind or SSCC opens
// another case, and a carton-less discrepancy keys on "-".
func TestCartonDiscrepancyHandler_OneCasePerReceiptSsccKind(t *testing.T) {
	w, handlers := cartonWorld(t)
	h := handlers[kafka.TypeCartonDiscrepancyRaised]
	ctx := context.Background()
	send := func(id, kind, sscc string) {
		t.Helper()
		e := producerEvent(t, id, "/warehouse/inbound-receiving", kafka.TypeCartonDiscrepancyRaised, "rcpt-7", cartonPayload(kind, sscc))
		if err := h(ctx, e); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	send("e-1", "CaseQtyMismatch", cartonSSCC)
	send("e-2", "CaseQtyMismatch", cartonSSCC) // touch
	send("e-1", "CaseQtyMismatch", cartonSSCC) // replay
	send("e-3", "MissingCarton", "")           // no sscc -> "-"
	send("e-4", "MissingCarton", cartonSSCC)   // another key

	if _, occ := caseOf(t, w, "rcpt-7/"+cartonSSCC+"/CaseQtyMismatch"); occ != 2 {
		t.Fatalf("occurrence = %d, want 2 (touch once, replay ignored)", occ)
	}
	caseOf(t, w, "rcpt-7/-/MissingCarton")
	caseOf(t, w, "rcpt-7/"+cartonSSCC+"/MissingCarton")
}

// TestRouteForCartonKind: contract R12 routing, unknown kinds default to
// problem-solve.
func TestRouteForCartonKind(t *testing.T) {
	for _, k := range []string{"MissingCarton", "DuplicateSscc", "UnexpectedCarton", "CaseQtyMismatch", "MixedCartonContentMismatch",
		"MixedCaseDeclaredAsCasePack", "UnknownGtin", "SomethingNew", ""} {
		if got := usecases.RouteForCartonKind(k); got != usecases.RouteProblemSolve {
			t.Errorf("%q -> %s, want problem-solve", k, got)
		}
	}
	for _, k := range []string{"UnlabelledCarton", "CartonOverLimit"} {
		if got := usecases.RouteForCartonKind(k); got != usecases.RouteReceivingLead {
			t.Errorf("%q -> %s, want receiving-lead", k, got)
		}
	}
}

// TestCartonDiscrepancyHandler_PoisonAndUndeclared: a payload without a
// receipt or kind is a dead-letter poison; an undeclared route is the
// commit-past skip.
func TestCartonDiscrepancyHandler_PoisonAndUndeclared(t *testing.T) {
	_, handlers := cartonWorld(t)
	h := handlers[kafka.TypeCartonDiscrepancyRaised]
	bad := producerEvent(t, "e-bad", "/warehouse/inbound-receiving", kafka.TypeCartonDiscrepancyRaised, "x", `{"kind":"MissingCarton","at":"2026-10-24T09:00:00Z"}`)
	if err := h(context.Background(), bad); err == nil {
		t.Fatal("a payload without receipt_id must be refused")
	}
	bare := newConsumeWorld(t) // no policies declared
	hb := kafka.CartonDiscrepancyHandlers(&usecases.OpenCartonDiscrepancy{Consume: bare.consume}, slog.Default())[kafka.TypeCartonDiscrepancyRaised]
	e := producerEvent(t, "e-2", "/warehouse/inbound-receiving", kafka.TypeCartonDiscrepancyRaised, "rcpt-7", cartonPayload("MissingCarton", ""))
	if err := hb(context.Background(), e); err == nil {
		t.Fatal("an undeclared route must be a skip error, not success")
	}
}
