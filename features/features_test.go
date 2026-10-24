// Package features_test hosts the godog (Cucumber for Go) acceptance suite
// next to the .feature files it runs (no Go file lives at the repo root).
// It drives the REAL use cases (the application layer) over the in-memory
// doubles, wired the way cmd/api will wire them over Postgres and Kafka;
// later steps of the service put the HTTP router and the Kafka consumers
// in front of the same world. The storage invariants the doubles enforce
// are the ones the Postgres adapters prove in the repocontract suite.
package features_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
)

// TestFeatures runs every Gherkin feature in this directory.
func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"."},
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}

// wall is the scenario's mutable service clock: steps set and advance it,
// every use case stamps with it.
type wall struct{ now time.Time }

func (c *wall) Now() time.Time { return c.now }

// app is the set of use cases of one scenario.
type app struct {
	declare *usecases.DeclareExceptionPolicy
	open    *usecases.OpenOrTouchException
	triage  *usecases.TriageException
	assign  *usecases.AssignException
	resolve *usecases.ResolveException
	auto    *usecases.AutoResolveOnCorrelation
	sweep   *usecases.SweepSLABreaches
	list    *usecases.ListOpenExceptions
}

// wire builds the application over fresh in-memory doubles, as cmd/api
// does over Postgres (nil UnitOfWork = the memory wiring runs unwrapped).
func wire() (app, *memory.Cases, *memory.Policies, *memory.Events, *wall, *memory.IDGen) {
	cases, policies, events := memory.NewCases(), memory.NewPolicies(), &memory.Events{}
	ids, clock := memory.NewIDGen(), &wall{now: time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)}
	return app{
		declare: &usecases.DeclareExceptionPolicy{Policies: policies, Events: events, Clock: clock},
		open:    &usecases.OpenOrTouchException{Cases: cases, Policies: policies, Events: events, IDs: ids, Clock: clock},
		triage:  &usecases.TriageException{Cases: cases, Events: events, Clock: clock},
		assign:  &usecases.AssignException{Cases: cases, Events: events, Clock: clock},
		resolve: &usecases.ResolveException{Cases: cases, Events: events, Clock: clock},
		auto:    &usecases.AutoResolveOnCorrelation{Cases: cases, Policies: policies, Events: events, Clock: clock},
		sweep:   &usecases.SweepSLABreaches{Cases: cases, Events: events, Clock: clock},
		list:    &usecases.ListOpenExceptions{Cases: cases},
	}, cases, policies, events, clock, ids
}

// world is the per-scenario state.
type world struct {
	uc     app
	cases  *memory.Cases
	pol    *memory.Policies
	ev     *memory.Events
	clock  *wall
	ctx    context.Context
	caseID string

	lastErr    error
	errChecked bool
	unchecked  []error
	closed     int
}

func (w *world) start() {
	w.uc, w.cases, w.pol, w.ev, w.clock, _ = wire()
	w.ctx = context.Background()
	w.caseID, w.lastErr, w.errChecked, w.unchecked, w.closed = "", nil, false, nil, 0
}

// run starts one command: a failure of the previous one that no step looked
// at is remembered and fails the scenario at the end.
func (w *world) run(fn func() error) {
	if w.lastErr != nil && !w.errChecked {
		w.unchecked = append(w.unchecked, w.lastErr)
	}
	w.lastErr, w.errChecked = fn(), false
}

// current re-reads the scenario's case from the repository, so assertions
// see the PERSISTED state, not a stale pointer.
func (w *world) current() *exceptioncase.Case {
	c, err := w.cases.FindByID(w.ctx, w.caseID)
	if err != nil {
		panic(fmt.Sprintf("case %s vanished: %v", w.caseID, err))
	}
	return c
}

// finish fails the scenario when a command failed and no step looked at it.
func (w *world) finish() error {
	if w.lastErr != nil && !w.errChecked {
		w.unchecked = append(w.unchecked, w.lastErr)
	}
	if len(w.unchecked) == 0 {
		return nil
	}
	return fmt.Errorf("unchecked command errors: %w", errors.Join(w.unchecked...))
}

// InitializeScenario registers every step.
func InitializeScenario(sc *godog.ScenarioContext) {
	w := &world{}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.start()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, err error) (context.Context, error) {
		if err != nil {
			return ctx, err
		}
		return ctx, w.finish()
	})
	registerSetupSteps(sc, w)
	registerFactSteps(sc, w)
	registerCommandSteps(sc, w)
	registerCaseStateSteps(sc, w)
	registerOutcomeSteps(sc, w)
	registerEventAuditSteps(sc, w)
	registerCartonSteps(sc, w)
}
