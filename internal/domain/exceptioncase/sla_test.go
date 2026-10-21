package exceptioncase

import (
	"errors"
	"testing"
	"time"
)

// Invariant (ADR 0002): SLADueAt is openedAt + the policy's sla_minutes,
// computed from the recorded fields; BreachesAt(now) is pure — it decides at
// or after the due instant and changes nothing.
func TestSLADueIsOpenedAtPlusThePolicySLAMinutes(t *testing.T) {
	t.Parallel()
	c := openedCase(t) // 30-minute policy
	want := openedAt.Add(30 * time.Minute)
	if got := c.SLADueAt(); !got.Equal(want) {
		t.Fatalf("SLADueAt() = %v, want %v", got, want)
	}
	long, err := Open(func() OpenInput {
		in := testOpenInput()
		in.CaseID = "case-2"
		in.SLAMinutes = 1440
		return in
	}())
	if err != nil {
		t.Fatal(err)
	}
	if got := long.SLADueAt(); !got.Equal(openedAt.Add(24 * time.Hour)) {
		t.Errorf("24h policy: SLADueAt() = %v", got)
	}
}

func TestBreachesAtIsPureAndBoundariesAreExact(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	due := c.SLADueAt()
	cases := []struct {
		name string
		now  time.Time
		want bool
	}{
		{"before the due instant", due.Add(-time.Nanosecond), false},
		{"exactly at the due instant", due, true},
		{"after the due instant", due.Add(time.Nanosecond), true},
		{"far before open", openedAt.Add(-time.Hour), false},
	}
	for _, tc := range cases {
		if got := c.BreachesAt(tc.now); got != tc.want {
			t.Errorf("%s: BreachesAt(%v) = %v, want %v", tc.name, tc.now, got, tc.want)
		}
	}
	before := c.Version()
	_ = c.BreachesAt(due.Add(time.Hour))
	_ = c.BreachesAt(openedAt)
	if c.Version() != before || c.Escalated() || len(c.AuditTrail()) != 1 {
		t.Error("BreachesAt must not mutate the case (pure)")
	}
}

// Invariant: Escalate is rejected on a zero timestamp and before the SLA
// deadline, and a rejected escalation changes nothing.
func TestEscalateIsRejectedBeforeBreachOrOnZeroTime(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	if _, err := c.Escalate(time.Time{}); !errors.Is(err, ErrNoTimestamp) {
		t.Errorf("zero time: err = %v", err)
	}
	if ok, err := c.Escalate(openedAt); !errors.Is(err, ErrNotBreached) || ok {
		t.Errorf("before breach: %v %v", ok, err)
	}
	if c.Escalated() {
		t.Error("a rejected escalation must not set the flag")
	}
}

// Invariant: Escalate fires exactly one ExceptionEscalated per case, at or
// after the due instant, as a single write (+1 version).
func TestEscalateFiresOnceAtBreachAsASingleWrite(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	v := c.Version()
	if ok, err := c.Escalate(c.SLADueAt()); err != nil || !ok {
		t.Fatalf("escalate at due: %v %v", ok, err)
	}
	if c.Version() != v+1 {
		t.Error("the accepted escalation is a write (+1 version)")
	}
	evts := c.DomainEvents()
	last := evts[len(evts)-1]
	if last.EventName() != "ExceptionEscalated" {
		t.Fatalf("last event = %s", last.EventName())
	}
	esc := last.(ExceptionEscalated)
	if esc.SLADueAt != c.SLADueAt() || esc.CaseID() != c.ID() {
		t.Errorf("ExceptionEscalated = %+v", esc)
	}
}

// A one-minute SLA is the smallest valid budget — the exact boundary of the
// < 1 gate in Open AND in Restore, which must stay inclusive of 1.
func TestASingleMinuteSLAIsTheSmallestValidBudget(t *testing.T) {
	t.Parallel()
	in := testOpenInput()
	in.SLAMinutes = 1
	c, err := Open(in)
	if err != nil {
		t.Fatalf("Open with a 1-minute SLA: %v", err)
	}
	if got := c.SLADueAt(); !got.Equal(openedAt.Add(time.Minute)) {
		t.Errorf("SLADueAt = %v, want openedAt+1m", got)
	}
	if _, err := Restore(c.Snapshot()); err != nil {
		t.Errorf("Restore of a 1-minute-SLA row: %v", err)
	}
}

// Invariant: a second Escalate is a no-op (no event, no version bump), and a
// resolved case never escalates.
func TestEscalateIsANoOpAfterEscalationOrOnAResolvedCase(t *testing.T) {
	t.Parallel()
	op := operator(t)
	c := openedCase(t)
	if ok, err := c.Escalate(c.SLADueAt()); err != nil || !ok {
		t.Fatalf("escalate at due: %v %v", ok, err)
	}
	v := c.Version()
	if ok, err := c.Escalate(c.SLADueAt().Add(time.Minute)); err != nil || ok {
		t.Errorf("second escalation is a no-op: %v %v", ok, err)
	}
	if c.Version() != v {
		t.Error("a no-op escalation bumps nothing")
	}
	if n := len(c.DomainEvents()); n != 2 {
		t.Errorf("events = %d, want 2 (opened + escalated)", n)
	}
	if err := c.Resolve("WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "", op, c.SLADueAt().Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	fresh := openedCase(t)
	if err := fresh.Resolve("NO_ACTION_NEEDED", "SYSTEM_GLITCH", "", op, openedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if ok, err := fresh.Escalate(fresh.SLADueAt().Add(time.Hour)); err != nil || ok {
		t.Errorf("escalate on a resolved case: %v %v", ok, err)
	}
}
