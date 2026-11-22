package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
)

// recordingMetrics records the ports.CaseMetrics calls of a use case as
// "outcome[/detail]" strings, in order. The detail is the severity of the
// decided case, or the problem slug of a rejection.
type recordingMetrics struct{ calls []string }

var _ ports.CaseMetrics = (*recordingMetrics)(nil)

func (m *recordingMetrics) CaseOpened(_ context.Context, severity string) {
	m.calls = append(m.calls, "opened/"+severity)
}

func (m *recordingMetrics) CaseTouched(_ context.Context, severity string) {
	m.calls = append(m.calls, "touched/"+severity)
}

func (m *recordingMetrics) CaseReplayed(_ context.Context, severity string) {
	m.calls = append(m.calls, "replayed/"+severity)
}

func (m *recordingMetrics) CaseRejected(_ context.Context, reason string) {
	m.calls = append(m.calls, "rejected/"+reason)
}

func (m *recordingMetrics) only(t *testing.T, want string) {
	t.Helper()
	if len(m.calls) != 1 || m.calls[0] != want {
		t.Fatalf("metric calls = %v, want exactly [%s]", m.calls, want)
	}
}

func TestOpenOrTouchCountsAnOpenedCaseBySeverity(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	m := &recordingMetrics{}
	f.open.Metrics = m

	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}

	m.only(t, "opened/HIGH")
}

func TestOpenOrTouchCountsATouchedCase(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	m := &recordingMetrics{}
	f.open.Metrics = m

	if _, err := f.open.Execute(context.Background(), f.fact("ev-2")); err != nil {
		t.Fatal(err)
	}

	m.only(t, "touched/HIGH")
}

// A replay of the OPENING event id is the idempotent no-op: counted on its
// own outcome, never as a second opened case.
func TestOpenOrTouchCountsAReplayOfTheOpeningEventAsReplayed(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}
	m := &recordingMetrics{}
	f.open.Metrics = m

	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatal(err)
	}

	m.only(t, "replayed/HIGH")
}

// failingSave is a case repo whose Save always fails with err.
type failingSave struct {
	ports.ExceptionCaseRepo
	err error
}

func (r failingSave) Save(context.Context, *exceptioncase.Case) error { return r.err }

// A refused fact is counted once, with the problem slug as the reason.
func TestOpenOrTouchCountsARejectionWithItsProblemSlug(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(f fixture) (usecases.Fact, *usecases.OpenOrTouchException)
		want    string
	}{
		{
			name: "a source type without a declared policy",
			arrange: func(f fixture) (usecases.Fact, *usecases.OpenOrTouchException) {
				return f.fact("ev-1"), f.open // no policy declared
			},
			want: "rejected/policy-undeclared",
		},
		{
			name: "a fact without a subject",
			arrange: func(f fixture) (usecases.Fact, *usecases.OpenOrTouchException) {
				fact := f.fact("ev-1")
				fact.SourceSubject = ""
				return fact, f.open
			},
			want: "rejected/invalid-fact",
		},
		{
			name: "a lost optimistic-concurrency race",
			arrange: func(f fixture) (usecases.Fact, *usecases.OpenOrTouchException) {
				uc := *f.open
				uc.Cases = failingSave{ExceptionCaseRepo: f.cases, err: usecases.ErrConcurrentModification}
				return f.fact("ev-1"), &uc
			},
			want: "rejected/concurrent-modification",
		},
		{
			name: "the open-case-per-key index refusing a second case",
			arrange: func(f fixture) (usecases.Fact, *usecases.OpenOrTouchException) {
				uc := *f.open
				uc.Cases = failingSave{ExceptionCaseRepo: f.cases, err: usecases.ErrOpenCaseExists}
				return f.fact("ev-1"), &uc
			},
			want: "rejected/open-case-exists",
		},
		{
			name: "a storage failure",
			arrange: func(f fixture) (usecases.Fact, *usecases.OpenOrTouchException) {
				uc := *f.open
				uc.Cases = failingSave{ExceptionCaseRepo: f.cases, err: errors.New("connection reset")}
				return f.fact("ev-1"), &uc
			},
			want: "rejected/internal-error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			if tt.want != "rejected/policy-undeclared" {
				f.declarePolicy(t, false)
			}
			fact, uc := tt.arrange(f)
			m := &recordingMetrics{}
			uc.Metrics = m

			if _, err := uc.Execute(context.Background(), fact); err == nil {
				t.Fatal("the fact was accepted, want an error")
			}

			m.only(t, tt.want)
		})
	}
}

// Metrics is optional: a use case built without it (every existing wiring and
// test) behaves exactly as before.
func TestOpenOrTouchWithoutMetricsStillWorks(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)

	if _, err := f.open.Execute(context.Background(), f.fact("ev-1")); err != nil {
		t.Fatalf("Execute without Metrics: %v", err)
	}
	if _, err := f.open.Execute(context.Background(), f.fact("ev-bad-subject")); err != nil {
		t.Fatalf("touching without Metrics: %v", err)
	}
}
