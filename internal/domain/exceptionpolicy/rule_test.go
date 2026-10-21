package exceptionpolicy

import (
	"testing"
	"time"
)

// Invariant (ADR 0002): the auto-resolve rule matches ONLY the same subject
// (through the work-unit formula) of the exact correlated type, ONLY within
// its window of the case's last occurrence — and is a silent no-op
// otherwise. This is the OrderRepromised correlation of ADR 0001.
func TestTheAutoResolveRuleMatchesOnlyTheSameSubjectWithinItsWindow(t *testing.T) {
	t.Parallel()
	r, err := NewRule(repromised, 60)
	if err != nil {
		t.Fatal(err)
	}
	last := time.Date(2026, 10, 21, 9, 0, 5, 0, time.UTC)
	base := Match{
		CorrelatedType:    repromised,
		CorrelatedOrderID: "ord-7",
		CaseSourceRef:     "ord-7-line-3",
		LastOccurrenceAt:  last,
		Now:               last.Add(30 * time.Minute),
	}
	cases := []struct {
		name string
		mut  func(*Match)
		want bool
	}{
		{"the happy path: same type, same subject, inside the window", nil, true},
		{"exactly at the window edge", func(m *Match) { m.Now = last.Add(60 * time.Minute) }, true},
		{"one nanosecond past the edge", func(m *Match) { m.Now = last.Add(60*time.Minute + time.Nanosecond) }, false},
		{"another correlated type", func(m *Match) { m.CorrelatedType = "com.warehouse.wes.order-management.order.OrderCancelled" }, false},
		{"another order (subject mismatch)", func(m *Match) { m.CorrelatedOrderID = "ord-8" }, false},
		{"a source ref that is not a work unit", func(m *Match) { m.CaseSourceRef = "ord-7" }, false},
		{"an empty source ref", func(m *Match) { m.CaseSourceRef = "" }, false},
		{"a malformed source ref", func(m *Match) { m.CaseSourceRef = "ord-7-line-" }, false},
		{"a line number mismatch still matches: only the order is the subject", func(m *Match) { m.CaseSourceRef = "ord-7-line-9" }, true},
	}
	for _, tc := range cases {
		m := base
		if tc.mut != nil {
			tc.mut(&m)
		}
		if got := r.Matches(m); got != tc.want {
			t.Errorf("%s: Matches = %v, want %v", tc.name, got, tc.want)
		}
	}
	// The matcher is pure: it never mutates its input.
	if base.Now != last.Add(30*time.Minute) || base.CaseSourceRef != "ord-7-line-3" {
		t.Error("Matches must not mutate its input")
	}
	// A different window changes the edge.
	tight, err := NewRule(repromised, 1)
	if err != nil {
		t.Fatal(err)
	}
	m := base
	m.Now = last.Add(2 * time.Minute)
	if tight.Matches(m) {
		t.Error("a 2-minute-old fact is outside a 1-minute window")
	}
	m.Now = last.Add(time.Minute)
	if !tight.Matches(m) {
		t.Error("a 1-minute-old fact is inside a 1-minute window")
	}
}
