package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// heldStockFixture: an open case for one held unit (opened the way the
// consumer opens it — a StockHeld fact through ConsumeFact under the
// seeded held-stock policy), plus the closing use case under test.
type heldStockFixture struct {
	consumedFixture
	settle *usecases.AutoResolveHeldStock
}

func newHeldStockFixture(t *testing.T) *heldStockFixture {
	t.Helper()
	base := newConsumedFixture(t)
	f := &heldStockFixture{consumedFixture: *base}
	f.settle = &usecases.AutoResolveHeldStock{
		Processed: f.processed, Cases: f.cases, Events: f.ev,
		Clock: memory.Clock{T: f.now.Add(2 * time.Hour)},
	}
	return f
}

// hold opens the case of one held unit the way the consumer does: a
// StockHeld fact under the seeded policy, and returns the case.
func (f *heldStockFixture) hold(t *testing.T, unit, eventID string) *exceptioncase.Case {
	t.Helper()
	uc := &usecases.DeclareExceptionPolicy{Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := uc.Execute(context.Background(), usecases.PolicyDeclaration{
		SourceType: usecases.TypeStockHeld, Severity: shared.SeverityMedium,
		Queue: "problem-solve", SLAMinutes: 1440, Actor: "op-1",
	}); err != nil {
		t.Fatalf("declare held-stock policy: %v", err)
	}
	payload := []byte(`{"held_unit_id":"` + unit + `","sku":"SKU-9","quantity":4,"reason":"DAMAGED",` +
		`"receipt_id":"rcpt-77","asn_number":"ASN-88","line_no":2,"held_at":"2026-11-12T09:00:00Z"}`)
	if out, err := f.consume.Execute(context.Background(), usecases.Fact{
		SourceType: usecases.TypeStockHeld, SourceSubject: unit,
		SourceEventID: eventID, Payload: payload,
	}); err != nil || out != usecases.OutcomeApplied {
		t.Fatalf("hold: %v, %v", out, err)
	}
	c, err := f.cases.FindOpenByDedupKey(context.Background(), mustKey(t, unit))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestHeldStock_DispositionClosesTheCase(t *testing.T) {
	f := newHeldStockFixture(t)
	ctx := context.Background()

	for _, disp := range []string{"RELEASE", "RETURN_TO_VENDOR", "SCRAP"} {
		t.Run(disp, func(t *testing.T) {
			// one unit per subtest: hu-<disp>
			unit := "hu-" + disp
			f.hold(t, unit, "hold-"+disp)
			out, err := f.settle.Execute(ctx, usecases.HeldStockClosure{
				EventID: "disp-" + disp, HeldUnitID: unit,
				ClosingType: usecases.TypeStockDispositioned, Disposition: disp,
			})
			if err != nil || out != usecases.OutcomeApplied {
				t.Fatalf("settle = %v, %v; want applied", out, err)
			}
			c, err := f.cases.FindOpenByDedupKey(ctx, mustKey(t, unit))
			if err != nil {
				t.Fatal(err)
			}
			if c.Status() != exceptioncase.StatusResolved {
				t.Fatalf("status = %s, want RESOLVED", c.Status())
			}
			if c.Note() != "disposition "+disp {
				t.Fatalf("note = %q, want the disposition outcome", c.Note())
			}
			wantRule := usecases.TypeStockHeld + "->" + usecases.TypeStockDispositioned
			if c.AutoResolution() == nil || c.RootCause() != usecases.TypeStockDispositioned ||
				f.autoRuleOf(c) != wantRule {
				t.Fatalf("auto-resolution = %+v, want rule %s", c.AutoResolution(), wantRule)
			}
		})
	}
}

// REWORK keeps the unit HELD (inventory-storage ADR 0052), so the case
// STAYS OPEN and the arrival is recorded as a touch.
func TestHeldStock_ReworkKeepsTheCaseOpen(t *testing.T) {
	f := newHeldStockFixture(t)
	ctx := context.Background()
	f.hold(t, "hu-2", "hold-2")

	out, err := f.settle.Execute(ctx, usecases.HeldStockClosure{
		EventID: "disp-1", HeldUnitID: "hu-2",
		ClosingType: usecases.TypeStockDispositioned, Disposition: "REWORK",
	})
	if err != nil || out != usecases.OutcomeApplied {
		t.Fatalf("rework = %v, %v; want applied (touch)", out, err)
	}
	c, err := f.cases.FindOpenByDedupKey(ctx, mustKey(t, "hu-2"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Status() != exceptioncase.StatusOpen {
		t.Fatalf("status = %s, want OPEN (the unit is still held)", c.Status())
	}
	if c.Occurrence() != 2 {
		t.Fatalf("occurrence = %d, want 2 (the rework arrival is recorded)", c.Occurrence())
	}
	for _, name := range f.ev.CaseEventNames() {
		if name == "ExceptionAutoResolved" {
			t.Fatal("a rework must not auto-resolve")
		}
	}
}

// StockReleased closes the case with a released note; the paired
// StockDispositioned that follows is an idempotent no-op (terminal case).
func TestHeldStock_ReleaseClosesAndThePairIsIdempotent(t *testing.T) {
	f := newHeldStockFixture(t)
	ctx := context.Background()
	f.hold(t, "hu-3", "hold-3")

	out, err := f.settle.Execute(ctx, usecases.HeldStockClosure{
		EventID: "rel-1", HeldUnitID: "hu-3", ClosingType: usecases.TypeStockReleased,
	})
	if err != nil || out != usecases.OutcomeApplied {
		t.Fatalf("release = %v, %v; want applied", out, err)
	}
	c, _ := f.cases.FindOpenByDedupKey(ctx, mustKey(t, "hu-3"))
	if c.Status() != exceptioncase.StatusResolved || c.Note() != "stock released" {
		t.Fatalf("case = %s note=%q, want RESOLVED with a released note", c.Status(), c.Note())
	}
	wantRule := usecases.TypeStockHeld + "->" + usecases.TypeStockReleased
	if f.autoRuleOf(c) != wantRule {
		t.Fatalf("rule = %q, want %s", f.autoRuleOf(c), wantRule)
	}

	// The paired disposition (RELEASE) arrives after: nothing changes.
	out, err = f.settle.Execute(ctx, usecases.HeldStockClosure{
		EventID: "disp-2", HeldUnitID: "hu-3",
		ClosingType: usecases.TypeStockDispositioned, Disposition: "RELEASE",
	})
	if err != nil || out != usecases.OutcomeDuplicate {
		t.Fatalf("paired disposition = %v, %v; want duplicate (already resolved)", out, err)
	}
	resolved, _ := f.cases.FindOpenByDedupKey(ctx, mustKey(t, "hu-3"))
	if resolved.Version() != c.Version() {
		t.Fatal("the already-resolved case must not change")
	}
}

// A replay of the SAME closing event id is a duplicate claim: no second
// resolution, no state change.
func TestHeldStock_ReplayOfTheClosingEventIsDuplicate(t *testing.T) {
	f := newHeldStockFixture(t)
	ctx := context.Background()
	f.hold(t, "hu-4", "hold-4")
	in := usecases.HeldStockClosure{
		EventID: "disp-3", HeldUnitID: "hu-4",
		ClosingType: usecases.TypeStockDispositioned, Disposition: "SCRAP",
	}
	if out, err := f.settle.Execute(ctx, in); err != nil || out != usecases.OutcomeApplied {
		t.Fatalf("first = %v, %v", out, err)
	}
	if out, err := f.settle.Execute(ctx, in); err != nil || out != usecases.OutcomeDuplicate {
		t.Fatalf("replay = %v, %v; want duplicate", out, err)
	}
}

// An unknown held unit (a hold from before this feature, or never seen)
// is the fail-soft no-match: the event id is still claimed, nothing opens,
// nothing errors — the consumer logs WARN and commits past.
func TestHeldStock_UnknownHeldUnitIsNoMatchStillClaims(t *testing.T) {
	f := newHeldStockFixture(t)
	ctx := context.Background()
	in := usecases.HeldStockClosure{
		EventID: "disp-9", HeldUnitID: "hu-never",
		ClosingType: usecases.TypeStockDispositioned, Disposition: "SCRAP",
	}
	out, err := f.settle.Execute(ctx, in)
	if err != nil || out != usecases.OutcomeNoMatch {
		t.Fatalf("unknown unit = %v, %v; want no-match", out, err)
	}
	if out, err := f.settle.Execute(ctx, in); err != nil || out != usecases.OutcomeDuplicate {
		t.Fatalf("replay of the no-match = %v, %v; want duplicate (claimed)", out, err)
	}
}

// A disposition the pinned contract does not define is a deterministic
// typed error: the consumer WARNs and commits past, never retries and
// never dead-letters a well-formed fact.
func TestHeldStock_UnknownDispositionIsTyped(t *testing.T) {
	f := newHeldStockFixture(t)
	f.hold(t, "hu-5", "hold-5")
	_, err := f.settle.Execute(context.Background(), usecases.HeldStockClosure{
		EventID: "disp-x", HeldUnitID: "hu-5",
		ClosingType: usecases.TypeStockDispositioned, Disposition: "DONATE",
	})
	if !errors.Is(err, usecases.ErrUnknownDisposition) {
		t.Fatalf("err = %v, want ErrUnknownDisposition", err)
	}
	// The case is untouched and the id NOT claimed (a deterministic refusal
	// leaves no trace: the claim runs only once the fact is actionable).
	c, _ := f.cases.FindOpenByDedupKey(context.Background(), mustKey(t, "hu-5"))
	if c.Status() != exceptioncase.StatusOpen {
		t.Fatal("an unknown disposition must not close the case")
	}
}

func TestHeldStock_InputValidation(t *testing.T) {
	f := newHeldStockFixture(t)
	ctx := context.Background()
	f.hold(t, "hu-6", "hold-6")
	cases := []struct {
		name string
		in   usecases.HeldStockClosure
		want error
	}{
		{"no event id", usecases.HeldStockClosure{HeldUnitID: "hu-6", ClosingType: usecases.TypeStockDispositioned, Disposition: "SCRAP"}, usecases.ErrNoEventID},
		{"no held unit", usecases.HeldStockClosure{EventID: "e", ClosingType: usecases.TypeStockDispositioned, Disposition: "SCRAP"}, shared.ErrBlankSourceSubject},
		{"no closing type", usecases.HeldStockClosure{EventID: "e", HeldUnitID: "hu-6", Disposition: "SCRAP"}, shared.ErrBlankSourceType},
	}
	for _, tc := range cases {
		if _, err := f.settle.Execute(ctx, tc.in); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func mustKey(t *testing.T, unit string) shared.DedupKey {
	t.Helper()
	k, err := shared.NewDedupKey(usecases.TypeStockHeld, unit, "")
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func (f *heldStockFixture) autoRuleOf(c *exceptioncase.Case) string {
	s := c.Snapshot()
	if !s.AutoResolved {
		return ""
	}
	return s.AutoRuleID
}
