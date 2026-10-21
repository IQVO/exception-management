package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

const (
	srcType    = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	repromised = "com.warehouse.wes.order-management.order.OrderRepromised"
)

type fixture struct {
	cases *memory.Cases
	pol   *memory.Policies
	ev    *memory.Events
	ids   *memory.IDGen
	now   time.Time
	open  *usecases.OpenOrTouchException
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	f := fixture{
		cases: memory.NewCases(), pol: memory.NewPolicies(), ev: &memory.Events{},
		ids: memory.NewIDGen(), now: time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC),
	}
	f.open = &usecases.OpenOrTouchException{Cases: f.cases, Policies: f.pol, Events: f.ev, IDs: f.ids, Clock: memory.Clock{T: f.now}}
	return f
}

func (f fixture) declarePolicy(t *testing.T, withRule bool) {
	t.Helper()
	in := usecases.PolicyDeclaration{SourceType: srcType, Severity: shared.SeverityHigh, Queue: "ps-main", SLAMinutes: 30, Actor: "op-1"}
	if withRule {
		in.AutoResolveOn, in.WithinMinutes = repromised, 10
	}
	uc := &usecases.DeclareExceptionPolicy{Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := uc.Execute(context.Background(), in); err != nil {
		t.Fatalf("declare policy: %v", err)
	}
}

func (f fixture) fact(id string) usecases.Fact {
	return usecases.Fact{SourceType: srcType, SourceSubject: "ord-7-line-1", SiteID: "SIM1", SourceEventID: id, Payload: []byte(`{"a":1}`)}
}

func TestOpenOrTouch_OpensUnderPolicy(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	c, err := f.open.Execute(context.Background(), f.fact("ev-1"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if c.Status() != exceptioncase.StatusOpen || c.Queue() != "ps-main" || c.SLAMinutes() != 30 {
		t.Fatalf("case not opened under the policy: status=%s queue=%s sla=%d", c.Status(), c.Queue(), c.SLAMinutes())
	}
	names := f.ev.CaseEventNames()
	if c.Occurrence() != 1 || len(names) != 1 || names[0] != "ExceptionOpened" {
		t.Fatalf("occurrence=%d events=%v", c.Occurrence(), names)
	}
}

func TestOpenOrTouch_TouchesSameKey(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	c, err := f.open.Execute(context.Background(), f.fact("ev-2"))
	if err != nil {
		t.Fatalf("touch: %v", err)
	}
	if c.Occurrence() != 2 {
		t.Fatalf("occurrence = %d, want 2 (no second case)", c.Occurrence())
	}
	if names := f.ev.CaseEventNames(); len(names) != 1 {
		t.Fatalf("a touch raised events: %v", names)
	}
}

func TestOpenOrTouch_ReplayOfOpeningEventIsNoOp(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	c, err := f.open.Execute(context.Background(), f.fact("ev-1"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if c.Occurrence() != 1 {
		t.Fatalf("replay changed the case: occurrence = %d", c.Occurrence())
	}
}

func TestOpenOrTouch_UndeclaredPolicyFailsTyped(t *testing.T) {
	f := newFixture(t)
	_, err := f.open.Execute(context.Background(), f.fact("ev-1"))
	if !errors.Is(err, usecases.ErrPolicyUndeclared) {
		t.Fatalf("err = %v, want ErrPolicyUndeclared", err)
	}
}

func (f fixture) triagedCase(t *testing.T) *exceptioncase.Case {
	t.Helper()
	f.declarePolicy(t, false)
	c, err := f.open.Execute(context.Background(), f.fact("ev-1"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCommands_LifecycleAndClosedCase(t *testing.T) {
	f := newFixture(t)
	c := f.triagedCase(t)
	ctx := context.Background()
	tri := &usecases.TriageException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := tri.Execute(ctx, c.ID(), shared.SeverityCritical, "ps-urgent", "op-9"); err != nil {
		t.Fatalf("triage: %v", err)
	}
	asn := &usecases.AssignException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := asn.Execute(ctx, c.ID(), "op-7", "op-9"); err != nil {
		t.Fatalf("assign: %v", err)
	}
	res := &usecases.ResolveException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := res.Execute(ctx, c.ID(), "RELOCATED", "bin-mismatch", "", "op-7"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if _, err := res.Execute(ctx, c.ID(), "RELOCATED", "bin-mismatch", "", "op-7"); !errors.Is(err, exceptioncase.ErrCaseClosed) {
		t.Fatalf("second resolve err = %v, want ErrCaseClosed", err)
	}
}

func TestAutoResolve_OrderLevelInclusiveWindow(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, true)
	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	uc := &usecases.AutoResolveOnCorrelation{Cases: f.cases, Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now.Add(10 * time.Minute)}}
	n, err := uc.Execute(context.Background(), usecases.CorrelatedFact{CorrelatedType: repromised, CorrelatedOrderID: "ord-7", CorrelatedEventID: "ev-9"})
	if err != nil || n != 1 {
		t.Fatalf("auto-resolve n=%d err=%v, want 1/nil (inclusive window, order-level)", n, err)
	}
	open, _ := f.cases.ListOpen(context.Background(), "", "", "")
	if len(open) != 0 {
		t.Fatalf("case still open after auto-resolve")
	}
}

func TestAutoResolve_OutsideWindowIsFailSoft(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, true)
	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	uc := &usecases.AutoResolveOnCorrelation{Cases: f.cases, Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now.Add(11 * time.Minute)}}
	n, err := uc.Execute(context.Background(), usecases.CorrelatedFact{CorrelatedType: repromised, CorrelatedOrderID: "ord-7", CorrelatedEventID: "ev-9"})
	if err != nil || n != 0 {
		t.Fatalf("outside window n=%d err=%v, want 0/nil", n, err)
	}
	if open, _ := f.cases.ListOpen(context.Background(), "", "", ""); len(open) != 1 {
		t.Fatal("case must stay open outside the window")
	}
}

func TestSweep_FlagsBreachExactlyOnce(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	opened, err := f.open.Execute(context.Background(), f.fact("ev-1"))
	if err != nil {
		t.Fatal(err)
	}
	late := memory.Clock{T: f.now.Add(31 * time.Minute)}
	uc := &usecases.SweepSLABreaches{Cases: f.cases, Events: f.ev, Clock: late}
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	c, err := f.cases.FindByID(context.Background(), opened.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !c.Escalated() || c.Status() != exceptioncase.StatusOpen {
		t.Fatalf("escalated=%v status=%s, want true/OPEN (flag, never a state)", c.Escalated(), c.Status())
	}
	names := f.ev.CaseEventNames()
	count := 0
	for _, n := range names {
		if n == "ExceptionEscalated" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("ExceptionEscalated raised %d times, want exactly 1", count)
	}
}

func TestDeclarePolicy_RedeclareBumpsVersion(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	uc := &usecases.DeclareExceptionPolicy{Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now}}
	p, err := uc.Execute(context.Background(), usecases.PolicyDeclaration{SourceType: srcType, Severity: shared.SeverityLow, Queue: "ps-2", SLAMinutes: 60, Actor: "op-1"})
	if err != nil {
		t.Fatal(err)
	}
	if p.Version() != 2 || p.SLAMinutes() != 60 {
		t.Fatalf("redeclare version=%d sla=%d, want 2/60", p.Version(), p.SLAMinutes())
	}
}

func TestReads_ViewsAndSLAReport(t *testing.T) {
	f := newFixture(t)
	c := f.triagedCase(t)
	ctx := context.Background()
	get := &usecases.GetException{Cases: f.cases}
	v, err := get.Execute(ctx, c.ID())
	if err != nil {
		t.Fatal(err)
	}
	if v.ID != c.ID() || v.SourceType != srcType || len(v.Audit) < 1 || v.SLADueAt.IsZero() {
		t.Fatalf("view incomplete: %+v", v)
	}
	if _, err := get.Execute(ctx, "nope"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("missing case err = %v", err)
	}
	rep, err := (&usecases.GetSLAReport{Cases: f.cases, Clock: memory.Clock{T: f.now.Add(29 * time.Minute)}}).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OpenCases != 1 || rep.Breached != 0 || rep.ByQueue["ps-main"] != 1 {
		t.Fatalf("report = %+v", rep)
	}
}
