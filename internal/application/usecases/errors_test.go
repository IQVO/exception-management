package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

func TestOpenOrTouch_RejectsInvalidKeyAndPayload(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	ctx := context.Background()
	cases := []struct {
		name string
		fact usecases.Fact
	}{
		{"blank subject", usecases.Fact{SourceType: srcType, SourceSubject: "  ", SiteID: "SIM1", SourceEventID: "e", Payload: []byte(`{}`)}},
		{"blank source event id", usecases.Fact{SourceType: srcType, SourceSubject: "ord-1-line-1", SiteID: "SIM1", Payload: []byte(`{}`)}},
		{"empty payload", usecases.Fact{SourceType: srcType, SourceSubject: "ord-1-line-1", SiteID: "SIM1", SourceEventID: "e"}},
	}
	for _, tc := range cases {
		if _, err := f.open.Execute(ctx, tc.fact); err == nil {
			t.Fatalf("%s: expected a typed domain error, got nil", tc.name)
		}
	}
}

func TestCommands_RejectBlankActorAndBadState(t *testing.T) {
	f := newFixture(t)
	c := f.triagedCase(t)
	ctx := context.Background()
	tri := &usecases.TriageException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := tri.Execute(ctx, c.ID(), shared.SeverityHigh, "ps", "  "); err == nil {
		t.Fatal("blank actor accepted")
	}
	if _, err := tri.Execute(ctx, "missing", shared.SeverityHigh, "ps", "op-1"); err == nil {
		t.Fatal("missing case accepted")
	}
	asn := &usecases.AssignException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := asn.Execute(ctx, c.ID(), " ", "op-1"); err == nil {
		t.Fatal("blank assignee accepted")
	}
	res := &usecases.ResolveException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := res.Execute(ctx, c.ID(), "", "rc", "", "op-1"); err == nil {
		t.Fatal("blank resolution code accepted")
	}
	if _, err := res.Execute(ctx, c.ID(), "CODE", "", "", "op-1"); err == nil {
		t.Fatal("blank root cause accepted")
	}
}

func TestResolve_OnClosedCaseIsErrCaseClosed(t *testing.T) {
	f := newFixture(t)
	c := f.triagedCase(t)
	ctx := context.Background()
	res := &usecases.ResolveException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := res.Execute(ctx, c.ID(), "DONE", "rc", "", "op-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := res.Execute(ctx, c.ID(), "DONE", "rc", "", "op-1"); !errors.Is(err, exceptioncase.ErrCaseClosed) {
		t.Fatalf("err = %v, want ErrCaseClosed", err)
	}
	tri := &usecases.TriageException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := tri.Execute(ctx, c.ID(), shared.SeverityLow, "ps", "op-1"); !errors.Is(err, exceptioncase.ErrCaseClosed) {
		t.Fatalf("triage on closed err = %v", err)
	}
}

func TestDeclarePolicy_RejectsInvalidInput(t *testing.T) {
	f := newFixture(t)
	uc := &usecases.DeclareExceptionPolicy{Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now}}
	ctx := context.Background()
	bad := []usecases.PolicyDeclaration{
		{SourceType: "not-a-full-type", Severity: shared.SeverityLow, Queue: "q", SLAMinutes: 5, Actor: "op"},
		{SourceType: srcType, Severity: shared.SeverityLow, Queue: "q", SLAMinutes: 0, Actor: "op"},
		{SourceType: srcType, Severity: shared.SeverityLow, Queue: " ", SLAMinutes: 5, Actor: "op"},
		{SourceType: srcType, Severity: shared.SeverityLow, Queue: "q", SLAMinutes: 5, AutoResolveOn: repromised, WithinMinutes: 0, Actor: "op"},
		{SourceType: srcType, Severity: shared.SeverityLow, Queue: "q", SLAMinutes: 5, Actor: " "},
	}
	for i, in := range bad {
		if _, err := uc.Execute(ctx, in); err == nil {
			t.Fatalf("case %d: invalid declaration accepted", i)
		}
	}
}

func TestAutoResolve_WrongTypeOrNoRuleIsNoOp(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, true)
	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	uc := &usecases.AutoResolveOnCorrelation{Cases: f.cases, Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now}}
	n, err := uc.Execute(context.Background(), usecases.CorrelatedFact{CorrelatedType: "com.warehouse.wes.something.Else", CorrelatedOrderID: "ord-7", CorrelatedEventID: "e"})
	if err != nil || n != 0 {
		t.Fatalf("wrong type n=%d err=%v", n, err)
	}
}

func TestSweep_BeforeDeadlineDoesNothing(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	uc := &usecases.SweepSLABreaches{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now.Add(29 * time.Minute)}}
	flagged, err := uc.Execute(context.Background())
	if err != nil || len(flagged) != 0 {
		t.Fatalf("early sweep flagged=%v err=%v", flagged, err)
	}
}

func TestCaseView_MapsAutoResolution(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, true)
	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	uc := &usecases.AutoResolveOnCorrelation{Cases: f.cases, Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now}}
	if _, err := uc.Execute(context.Background(), usecases.CorrelatedFact{CorrelatedType: repromised, CorrelatedOrderID: "ord-7", CorrelatedEventID: "ev-9"}); err != nil {
		t.Fatal(err)
	}
	v, err := (&usecases.GetException{Cases: f.cases}).Execute(context.Background(), "case-0001")
	if err != nil {
		t.Fatal(err)
	}
	if !v.AutoResolved || v.RuleID == "" || v.CorrelatedType != repromised || v.CorrelatedEventID != "ev-9" {
		t.Fatalf("auto-resolution not mapped: %+v", v)
	}
	if v.Status != "RESOLVED" || v.ResolvedAt.IsZero() {
		t.Fatalf("resolved fields not mapped: %+v", v)
	}
}

var _ = exceptionpolicy.Declare
var _ = shared.SeverityLow
