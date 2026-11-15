package telemetry_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// The acceptance path of the Tier 2 counter: the REAL open-or-touch use case
// over the in-memory repositories, wired to the REAL telemetry adapter exactly
// as cmd/api wires it, and read back through the MeterProvider's reader.
func TestOpenOrTouchExceptionIncrementsTheCounterWithTheRightAttributes(t *testing.T) {
	reader := useManualReader(t)
	metrics, err := telemetry.NewCaseMetrics()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	cases, policies, events := memory.NewCases(), memory.NewPolicies(), &memory.Events{}
	declare := &usecases.DeclareExceptionPolicy{Policies: policies, Events: events, Clock: memory.Clock{T: now}}
	const sourceType = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	if _, err := declare.Execute(context.Background(), usecases.PolicyDeclaration{
		SourceType: sourceType, Severity: shared.SeverityCritical, Queue: "ps-main", SLAMinutes: 30, Actor: "op-1",
	}); err != nil {
		t.Fatal(err)
	}
	open := &usecases.OpenOrTouchException{
		Cases: cases, Policies: policies, Events: events, IDs: memory.NewIDGen(), Clock: memory.Clock{T: now}, Metrics: metrics,
	}
	fact := func(id, subject string) usecases.Fact {
		return usecases.Fact{SourceType: sourceType, SourceSubject: subject, SiteID: "SIM1", SourceEventID: id, Payload: []byte(`{"a":1}`)}
	}

	for _, f := range []usecases.Fact{fact("ev-1", "task-1"), fact("ev-2", "task-1"), fact("ev-1", "task-1"), fact("ev-3", "task-2")} {
		if _, err := open.Execute(context.Background(), f); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := open.Execute(context.Background(), fact("ev-4", "")); err == nil {
		t.Fatal("a fact without a subject was accepted")
	}

	got := casePoints(t, reader)
	want := map[string]int64{
		"opened||CRITICAL":       2, // task-1 and task-2
		"touched||CRITICAL":      1, // ev-2 touches task-1
		"replayed||CRITICAL":     1, // ev-1 again: the opening event's replay
		"rejected|invalid-fact|": 1,
	}
	if len(got) != len(want) {
		t.Fatalf("data points = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %d, want %d (all: %v)", k, got[k], v, got)
		}
	}
}
