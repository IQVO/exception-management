package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// consumedFixture wires the consumer-driven use cases over the memory
// doubles (no UnitOfWork: unwrapped, the nil contract of ports.UnitOfWork).
type consumedFixture struct {
	fixture
	processed *memory.ProcessedEvents
	consume   *usecases.ConsumeFact
	correlate *usecases.ConsumeCorrelation
}

func newConsumedFixture(t *testing.T) *consumedFixture {
	t.Helper()
	base := newFixture(t)
	f := &consumedFixture{
		fixture:   base,
		processed: memory.NewProcessedEvents(),
	}
	f.consume = &usecases.ConsumeFact{Processed: f.processed, OpenOrTouch: f.open}
	auto := &usecases.AutoResolveOnCorrelation{Cases: f.cases, Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now}}
	f.correlate = &usecases.ConsumeCorrelation{Processed: f.processed, AutoResolve: auto}
	return f
}

func TestConsumeFact_ClaimsOncePerEventID(t *testing.T) {
	f := newConsumedFixture(t)
	f.declarePolicy(t, false)
	ctx := context.Background()

	if out, err := f.consume.Execute(ctx, f.fact("ev-1")); err != nil || out != usecases.OutcomeApplied {
		t.Fatalf("first consume = %v, %v; want applied", out, err)
	}
	// The SAME event id again — even with a different payload — is a
	// duplicate: one occurrence, no second event, no second audit entry.
	if out, err := f.consume.Execute(ctx, f.fact("ev-1")); err != nil || out != usecases.OutcomeDuplicate {
		t.Fatalf("replay = %v, %v; want duplicate", out, err)
	}
	c, err := f.cases.FindOpenByDedupKey(ctx, keyOf(t, f))
	if err != nil {
		t.Fatal(err)
	}
	if c.Occurrence() != 1 {
		t.Fatalf("occurrence after the replay = %d, want 1", c.Occurrence())
	}
	if names := f.ev.CaseEventNames(); len(names) != 1 {
		t.Fatalf("case events after the replay = %v, want the single opening", names)
	}
	// A DIFFERENT event id for the same key touches the case.
	if out, err := f.consume.Execute(ctx, f.fact("ev-2")); err != nil || out != usecases.OutcomeApplied {
		t.Fatalf("second fact = %v, %v; want applied", out, err)
	}
	c, _ = f.cases.FindOpenByDedupKey(ctx, keyOf(t, f))
	if c.Occurrence() != 2 {
		t.Fatalf("occurrence after the second id = %d, want 2", c.Occurrence())
	}
}

func TestConsumeFact_UnwrappedClaimPersistsWhenTheEffectFails(t *testing.T) {
	// The memory fixture runs UNWRAPPED (nil UnitOfWork, the port's
	// contract), so neither its claim nor its case save rolls back when
	// the effect fails — the atomic claim-and-effect is a Postgres
	// property, proven on real Postgres by the consumers' integration
	// test. What this pins is the unwrapped shape: the failed fact's id
	// stays claimed (its replay is a duplicate) and a FRESH id still
	// applies.
	f := newConsumedFixture(t)
	f.declarePolicy(t, false)
	failing := &failingPublisher{Events: f.ev, broken: true}
	f.open.Events = failing
	ctx := context.Background()
	if _, err := f.consume.Execute(ctx, f.fact("ev-1")); err == nil {
		t.Fatal("the broken publisher must fail the consume")
	}
	if out, err := f.consume.Execute(ctx, f.fact("ev-1")); err != nil || out != usecases.OutcomeDuplicate {
		t.Fatalf("replay of the failed id = %v, %v; want duplicate (unwrapped claim persists)", out, err)
	}
	failing.broken = false
	if out, err := f.consume.Execute(ctx, f.fact("ev-2")); err != nil || out != usecases.OutcomeApplied {
		t.Fatalf("fresh id after the failure = %v, %v; want applied", out, err)
	}
}

func TestConsumeFact_RequiresEventID(t *testing.T) {
	f := newConsumedFixture(t)
	f.declarePolicy(t, false)
	fact := f.fact("")
	if _, err := f.consume.Execute(context.Background(), fact); !errors.Is(err, usecases.ErrNoEventID) {
		t.Fatalf("err = %v, want ErrNoEventID", err)
	}
}

func TestConsumeCorrelation_ClaimsOnceAndClosesTheCase(t *testing.T) {
	f := newConsumedFixture(t)
	f.declarePolicy(t, true) // auto-resolve on OrderRepromised within 10 minutes
	ctx := context.Background()
	if _, err := f.consume.Execute(ctx, f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}

	fact := usecases.CorrelatedFact{CorrelatedType: repromised, CorrelatedOrderID: "ord-7", CorrelatedEventID: "rep-1"}
	closed, out, err := f.correlate.Execute(ctx, fact)
	if err != nil || out != usecases.OutcomeApplied || closed != 1 {
		t.Fatalf("correlate = %d, %v, %v; want 1 applied", closed, out, err)
	}
	c, err := f.cases.FindByID(ctx, "case-0001")
	if err != nil {
		t.Fatal(err)
	}
	if c.Status() != exceptioncase.StatusResolved || c.AutoResolution() == nil {
		t.Fatalf("case = %s auto=%v, want auto-resolved", c.Status(), c.AutoResolution())
	}

	// The replay of the SAME correlation id changes nothing (terminal case,
	// duplicate claim): still exactly one AUTO_RESOLVED fact.
	closed, out, err = f.correlate.Execute(ctx, fact)
	if err != nil || out != usecases.OutcomeDuplicate || closed != 0 {
		t.Fatalf("replay = %d, %v, %v; want duplicate", closed, out, err)
	}
	if names := f.ev.CaseEventNames(); len(names) != 2 || names[1] != "ExceptionAutoResolved" {
		t.Fatalf("case events = %v, want opened then exactly one auto-resolve", names)
	}
}

func TestConsumeCorrelation_NoMatchStillClaims(t *testing.T) {
	f := newConsumedFixture(t)
	f.declarePolicy(t, true)
	ctx := context.Background()
	if _, err := f.consume.Execute(ctx, f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	fact := usecases.CorrelatedFact{CorrelatedType: repromised, CorrelatedOrderID: "ord-OTHER", CorrelatedEventID: "rep-x"}
	closed, out, err := f.correlate.Execute(ctx, fact)
	if err != nil || out != usecases.OutcomeNoMatch || closed != 0 {
		t.Fatalf("no-match = %d, %v, %v; want the fail-soft no-match", closed, out, err)
	}
	// The no-match id is claimed: its replay is a duplicate, never a
	// rescan (the fact was handled, there was nothing to close).
	if _, out, err := f.correlate.Execute(ctx, fact); err != nil || out != usecases.OutcomeDuplicate {
		t.Fatalf("replay of the no-match = %v, %v; want duplicate", out, err)
	}
}

// keyOf rebuilds the fixture fact's dedup key.
func keyOf(t *testing.T, f *consumedFixture) shared.DedupKey {
	t.Helper()
	fact := f.fact("ev-1")
	k, err := shared.NewDedupKey(fact.SourceType, fact.SourceSubject, fact.SiteID)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// failingPublisher wraps the memory Events double and fails every publish
// while broken (the effect side of the claim-and-apply unit).
type failingPublisher struct {
	*memory.Events
	broken bool
}

func (p *failingPublisher) PublishCaseEvents(ctx context.Context, events ...exceptioncase.Event) error {
	if p.broken {
		return errors.New("publisher down")
	}
	return p.Events.PublishCaseEvents(ctx, events...)
}

// PublishPolicyDeclared passes through (the fixture declares no policies
// through this wrapper).
func (p *failingPublisher) PublishPolicyDeclared(ctx context.Context, d exceptionpolicy.PolicyDeclared) error {
	return p.Events.PublishPolicyDeclared(ctx, d)
}
