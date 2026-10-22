package report

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func at(h, m int) time.Time { return time.Date(2026, 10, 21, h, m, 0, 0, time.UTC) }

// A valid event of every kind, the baseline the mutation table edits.
func validEvent(kind Kind) Event {
	e := Event{Kind: kind, EventID: "ev-1", At: at(9, 30), SourceType: "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"}
	switch kind {
	case KindExceptionOpened:
		e.CaseID, e.SourceSubject, e.Severity, e.Queue = "case-1", "task-8a1f", "HIGH", "ps-main"
		e.SLADueAt = at(10, 30)
	case KindExceptionTriaged:
		e.CaseID, e.Severity, e.Queue = "case-1", "MEDIUM", "ps-expedite"
	case KindExceptionAssigned:
		e.CaseID, e.Assignee = "case-1", "jlee"
	case KindExceptionEscalated:
		e.CaseID, e.SLADueAt = "case-1", at(10, 30)
	case KindExceptionResolved:
		e.CaseID, e.ResolutionCode, e.RootCause = "case-1", "STOCK_FOUND", "COUNT_ERROR"
	case KindExceptionAutoResolved:
		e.CaseID, e.RuleID, e.CorrelatedType = "case-1", "src->on", "com.warehouse.wes.order-management.order.OrderRepromised"
	case KindExceptionPolicyDeclared:
		e.Version, e.Severity, e.Queue, e.SLAMinutes = 2, "HIGH", "ps-main", 30
	}
	return e
}

func TestValidEventOfEveryKindPasses(t *testing.T) {
	for _, k := range Kinds() {
		if err := validEvent(k).Validate(); err != nil {
			t.Errorf("%s: %v", k, err)
		}
	}
}

// TestValidateRejectsBrokenEvents pins one deterministic refusal per rule.
func TestValidateRejectsBrokenEvents(t *testing.T) {
	cases := []struct {
		name   string
		kinds  []Kind
		mutate func(*Event)
	}{
		{"unknown kind", []Kind{KindExceptionOpened, KindExceptionPolicyDeclared}, func(e *Event) { e.Kind = "ExceptionReopened" }},
		{"no event id", []Kind{KindExceptionOpened, KindExceptionPolicyDeclared}, func(e *Event) { e.EventID = "" }},
		{"no time", []Kind{KindExceptionOpened, KindExceptionPolicyDeclared}, func(e *Event) { e.At = time.Time{} }},
		{"case kind without case_id", []Kind{KindExceptionOpened}, func(e *Event) { e.CaseID = "" }},
		{"policy kind with case_id", []Kind{KindExceptionPolicyDeclared}, func(e *Event) { e.CaseID = "case-1" }},
		{"policy without source_type", []Kind{KindExceptionPolicyDeclared}, func(e *Event) { e.SourceType = "" }},
		{"policy version zero", []Kind{KindExceptionPolicyDeclared}, func(e *Event) { e.Version = 0 }},
		{"policy without severity", []Kind{KindExceptionPolicyDeclared}, func(e *Event) { e.Severity = "" }},
		{"policy without queue", []Kind{KindExceptionPolicyDeclared}, func(e *Event) { e.Queue = "" }},
		{"policy sla zero", []Kind{KindExceptionPolicyDeclared}, func(e *Event) { e.SLAMinutes = 0 }},
		{"policy auto rule without on_type", []Kind{KindExceptionPolicyDeclared}, func(e *Event) {
			e.AutoResolve = &AutoResolveRule{OnType: "", WithinMinutes: 5}
		}},
		{"policy auto rule window zero", []Kind{KindExceptionPolicyDeclared}, func(e *Event) {
			e.AutoResolve = &AutoResolveRule{OnType: "on", WithinMinutes: 0}
		}},
		{"opened without source_type", []Kind{KindExceptionOpened}, func(e *Event) { e.SourceType = "" }},
		{"opened without source_subject", []Kind{KindExceptionOpened}, func(e *Event) { e.SourceSubject = "" }},
		{"opened without sla_due_at", []Kind{KindExceptionOpened}, func(e *Event) { e.SLADueAt = time.Time{} }},
		{"opened without queue", []Kind{KindExceptionOpened}, func(e *Event) { e.Queue = "" }},
		{"triaged without queue", []Kind{KindExceptionTriaged}, func(e *Event) { e.Queue = "" }},
		{"assigned without assignee", []Kind{KindExceptionAssigned}, func(e *Event) { e.Assignee = "" }},
		{"escalated without sla_due_at", []Kind{KindExceptionEscalated}, func(e *Event) { e.SLADueAt = time.Time{} }},
		{"resolved without root cause", []Kind{KindExceptionResolved}, func(e *Event) { e.RootCause = "" }},
		{"resolved without code", []Kind{KindExceptionResolved}, func(e *Event) { e.ResolutionCode = "" }},
		{"auto-resolved without rule", []Kind{KindExceptionAutoResolved}, func(e *Event) { e.RuleID = "" }},
	}
	for _, c := range cases {
		for _, kind := range c.kinds {
			e := validEvent(kind)
			c.mutate(&e)
			if err := e.Validate(); !errors.Is(err, ErrInvalidEvent) {
				t.Errorf("%s (%s): want ErrInvalidEvent, got %v", c.name, kind, err)
			}
		}
	}
}

func TestMilestonesOf(t *testing.T) {
	opened := MilestonesOf(validEvent(KindExceptionOpened))
	if opened.Opened == nil || opened.SLADue == nil || opened.Resolved != nil {
		t.Fatalf("opened milestones = %+v", opened)
	}
	if !opened.SLADue.Equal(at(10, 30)) {
		t.Fatalf("sla due = %v, want the event's due time", *opened.SLADue)
	}
	esc := MilestonesOf(validEvent(KindExceptionEscalated))
	if esc.Escalated == nil || esc.SLADue == nil {
		t.Fatalf("escalated milestones = %+v", esc)
	}
	tri := MilestonesOf(validEvent(KindExceptionTriaged))
	if tri.Triaged == nil || tri.Opened != nil {
		t.Fatalf("triaged milestones = %+v", tri)
	}
	if m := MilestonesOf(validEvent(KindExceptionPolicyDeclared)); (Milestones{}) != m {
		t.Fatalf("policy milestones = %+v, want none", m)
	}
}

func TestKindPredicates(t *testing.T) {
	for _, k := range Kinds() {
		if !k.Known() {
			t.Errorf("%s not known", k)
		}
		if k.IsCase() != (k != KindExceptionPolicyDeclared) {
			t.Errorf("%s IsCase = %v", k, k.IsCase())
		}
	}
	if Kind("Nope").Known() || Kind("Nope").IsCase() != true {
		t.Error("an unknown kind is not known and not a policy declaration")
	}
}

func TestParseRange(t *testing.T) {
	now := at(12, 0)
	// Omitted bounds: the 30 days ending now.
	r, err := ParseRange("", "", now)
	if err != nil || !r.From.Equal(now.Add(-30*24*time.Hour)) || !r.To.Equal(now) {
		t.Fatalf("default range = %+v err %v", r, err)
	}
	// Only to: [to-30d, to).
	r, err = ParseRange("", "2026-10-19T12:00:00Z", now)
	wantFrom := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	if err != nil || !r.From.Equal(wantFrom) || !r.To.Equal(time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("to-anchored range = %+v err %v", r, err)
	}
	// Only from: [from, now).
	r, err = ParseRange("2026-10-21T09:00:00Z", "", now)
	if err != nil || !r.From.Equal(at(9, 0)) || !r.To.Equal(now) {
		t.Fatalf("from-anchored range = %+v err %v", r, err)
	}
	for _, c := range []struct {
		from, to string
		want     error
	}{
		{"bogus", "", ErrInvalidFrom},
		{"", "bogus", ErrInvalidTo},
		{"2026-10-21T11:00:00Z", "2026-10-21T10:00:00Z", ErrEmptyRange},
		{"2026-10-21T10:00:00Z", "2026-10-21T10:00:00Z", ErrEmptyRange},
		{"2025-10-19T00:00:00Z", "2026-10-21T00:00:00Z", ErrRangeTooLarge},
	} {
		if _, err := ParseRange(c.from, c.to, now); !errors.Is(err, c.want) {
			t.Errorf("ParseRange(%q,%q) = %v, want %v", c.from, c.to, err, c.want)
		}
	}
}

func TestComputeFreshness(t *testing.T) {
	now := at(12, 0)
	if f := ComputeFreshness(nil, now); f.AsOf != nil || f.LagSeconds != nil {
		t.Fatalf("no events: %+v, want both nil", f)
	}
	asOf := at(11, 0)
	f := ComputeFreshness(&asOf, now)
	if f.AsOf == nil || *f.LagSeconds != 3600 {
		t.Fatalf("freshness = %+v, want 3600s lag", f)
	}
	// Skew reads as zero, never negative.
	ahead := now.Add(time.Hour)
	if f := ComputeFreshness(&ahead, now); *f.LagSeconds != 0 {
		t.Fatalf("skewed lag = %v, want 0", *f.LagSeconds)
	}
}

func TestPercentile(t *testing.T) {
	if p := Percentile(nil, 0.5); p != 0 {
		t.Fatalf("empty sample percentile = %v", p)
	}
	s := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if p := Percentile(s, 0.5); p != 5.5 {
		t.Fatalf("p50 = %v", p)
	}
	if p := Percentile(s, 0.95); math.Abs(p-9.55) > 1e-9 {
		t.Fatalf("p95 = %v", p)
	}
	if p := Percentile([]float64{7}, 0.5); p != 7 {
		t.Fatalf("single = %v", p)
	}
}

func TestTimingOf(t *testing.T) {
	if m, p50, p95 := TimingOf(nil); m != nil || p50 != nil || p95 != nil {
		t.Fatal("empty sample must yield nils")
	}
	mean, p50, p95 := TimingOf([]float64{1, 2, 3})
	if *mean != 2 || *p50 != 2 || math.Abs(*p95-2.9) > 1e-9 {
		t.Fatalf("mean=%v p50=%v p95=%v", *mean, *p50, *p95)
	}
}

func TestShare(t *testing.T) {
	if s := Share(1, 4); s != 0.25 {
		t.Fatalf("Share(1,4) = %v", s)
	}
	if s := Share(3, 0); s != 0 {
		t.Fatalf("Share(3,0) = %v, want 0", s)
	}
	if s := Share(0, 5); s != 0 {
		t.Fatalf("Share(0,5) = %v", s)
	}
}

// The report rows are plain data the HTTP adapter serialises; pin that a
// freshly made row survives a reflect round trip (guards accidental
// unexported fields sneaking in).
func TestRowShapesArePlainData(t *testing.T) {
	rows := []any{
		RootCauseRow{SourceType: "s", RootCause: "r", Resolved: 2, Share: 0.5},
		QueueTimingRow{Queue: "q", Resolved: 1},
		SLABreachRow{SourceType: "s", Opened: 4, Escalated: 1, Breached: 2, BreachRate: 0.5, EscalationRate: 0.25},
	}
	for _, r := range rows {
		if !reflect.TypeOf(r).Comparable() && reflect.ValueOf(r).IsZero() {
			t.Fatalf("row %T must stay plain data", r)
		}
	}
}
