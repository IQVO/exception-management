package exceptioncase

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// Invariant (ADR 0002): the status machine is OPEN -> TRIAGED -> ASSIGNED ->
// RESOLVED and only ever moves FORWARD. Triage moves an OPEN case to TRIAGED;
// re-triage never demotes a case that is already further along.
func TestTriageMovesAnOpenCaseToTriagedAndNeverDemotes(t *testing.T) {
	t.Parallel()
	op := operator(t)
	c := openedCase(t)
	if c.Status() != StatusOpen {
		t.Fatalf("a case opens OPEN, got %s", c.Status())
	}
	if err := c.Triage(shared.SeverityCritical, "cps-inbound", op, openedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c.Status() != StatusTriaged || c.Severity() != shared.SeverityCritical || c.Queue() != "cps-inbound" {
		t.Fatalf("after triage: %s %s %s", c.Status(), c.Severity(), c.Queue())
	}
	// Re-triage of a triaged case keeps the status.
	if err := c.Triage(shared.SeverityHigh, "cps-front", op, openedAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c.Status() != StatusTriaged || c.Severity() != shared.SeverityHigh {
		t.Error("re-triage must not demote or lose the change")
	}
}

// Invariant: assign moves the case to ASSIGNED; triage of an ASSIGNED case
// keeps ASSIGNED (a field change, never a backwards move); reassignment is
// allowed while unresolved; resolve is the final forward move.
func TestAssignMovesToAssignedAndNothingMovesBackwards(t *testing.T) {
	t.Parallel()
	op := operator(t)
	t1 := openedAt.Add(time.Minute)
	c := openedCase(t)
	if err := c.Triage(shared.SeverityCritical, "cps-inbound", op, t1); err != nil {
		t.Fatal(err)
	}
	if err := c.Assign("op-marc", op, t1.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c.Status() != StatusAssigned || c.Assignee() != "op-marc" {
		t.Fatalf("after assign: %s %s", c.Status(), c.Assignee())
	}
	if err := c.Triage(shared.SeverityLow, "cps-back", op, t1.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c.Status() != StatusAssigned || c.Severity() != shared.SeverityLow {
		t.Error("triage of an assigned case must keep the status")
	}
	// Reassignment is allowed while unresolved.
	if err := c.Assign("op-len", op, t1.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c.Status() != StatusAssigned || c.Assignee() != "op-len" {
		t.Error("reassignment must keep ASSIGNED with the new owner")
	}
	if err := c.Resolve("WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "re-ran", op, t1.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c.Status() != StatusResolved {
		t.Fatal("resolve moves the case to RESOLVED")
	}
}

// Forward skips exist on purpose: an untriaged case can be assigned, and any
// unresolved case can be resolved (auto-resolve fires from OPEN).
func TestForwardSkipsAreAllowedBackwardsNeverHappens(t *testing.T) {
	t.Parallel()
	op := operator(t)

	direct := openedCase(t)
	if err := direct.Assign("op-marc", op, openedAt.Add(time.Minute)); err != nil {
		t.Fatalf("assign from OPEN is a forward move: %v", err)
	}
	if direct.Status() != StatusAssigned {
		t.Errorf("status = %s, want ASSIGNED", direct.Status())
	}

	quick := openedCase(t)
	if err := quick.Resolve("NO_ACTION_NEEDED", "SYSTEM_GLITCH", "", op, openedAt.Add(time.Minute)); err != nil {
		t.Fatalf("resolve from OPEN is a forward move: %v", err)
	}
	if quick.Status() != StatusResolved || quick.Assignee() != "" {
		t.Errorf("status = %s assignee = %q", quick.Status(), quick.Assignee())
	}
}

// Invariant: RESOLVED requires a resolution code (and a root cause) and is
// TERMINAL — every later command fails with the 409-class ErrCaseClosed.
func TestResolvedRequiresAResolutionCodeAndIsTerminal(t *testing.T) {
	t.Parallel()
	op := operator(t)

	missing := openedCase(t)
	if err := missing.Resolve("", "OPERATOR_UNAVAILABLE", "", op, openedAt.Add(time.Minute)); !errors.Is(err, ErrBlankResolutionCode) {
		t.Errorf("blank code: err = %v", err)
	}
	if err := missing.Resolve("WORK_COMPLETED", " ", "", op, openedAt.Add(time.Minute)); !errors.Is(err, ErrBlankRootCause) {
		t.Errorf("blank root cause: err = %v", err)
	}
	if missing.Status() != StatusOpen {
		t.Error("a rejected resolve leaves the case unchanged")
	}

	c := openedCase(t)
	if err := c.Triage(shared.SeverityHigh, "cps-front", op, openedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := c.Resolve("WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "note", op, openedAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if c.ResolutionCode() != "WORK_COMPLETED" || c.RootCause() != "OPERATOR_UNAVAILABLE" || c.Note() != "note" {
		t.Error("the resolution must be recorded")
	}
	if c.ResolvedAt() != openedAt.Add(2*time.Minute) {
		t.Error("the resolution instant must be recorded")
	}
	terminal := []struct {
		name string
		call func() error
	}{
		{"triage", func() error { return c.Triage(shared.SeverityLow, "q", op, openedAt.Add(3*time.Minute)) }},
		{"assign", func() error { return c.Assign("op-x", op, openedAt.Add(3*time.Minute)) }},
		{"resolve", func() error { return c.Resolve("AGAIN", "AGAIN", "", op, openedAt.Add(3*time.Minute)) }},
		{"auto-resolve", func() error {
			return c.AutoResolve("rule", "type", "id", openedAt.Add(3*time.Minute))
		}},
	}
	for _, tc := range terminal {
		if err := tc.call(); !errors.Is(err, ErrCaseClosed) {
			t.Errorf("%s on a resolved case: err = %v, want ErrCaseClosed", tc.name, err)
		}
	}
	if c.ResolutionCode() != "WORK_COMPLETED" {
		t.Error("a terminal case keeps its first resolution")
	}
}

// Invariant: ESCALATED is a flag, never a state and never a blocker — every
// transition still works on an escalated case.
func TestEscalatedIsAFlagThatNeverBlocksATransition(t *testing.T) {
	t.Parallel()
	op := operator(t)
	c := openedCase(t)
	breach := c.SLADueAt().Add(time.Second)
	if ok, err := c.Escalate(breach); err != nil || !ok {
		t.Fatalf("Escalate at breach: %v %v", ok, err)
	}
	if !c.Escalated() || c.Status() != StatusOpen {
		t.Fatal("escalation sets the flag and no status")
	}
	if err := c.Triage(shared.SeverityCritical, "cps-inbound", op, breach.Add(time.Second)); err != nil {
		t.Errorf("triage on an escalated case: %v", err)
	}
	if err := c.Assign("op-marc", op, breach.Add(2*time.Second)); err != nil {
		t.Errorf("assign on an escalated case: %v", err)
	}
	if err := c.Resolve("WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "", op, breach.Add(3*time.Second)); err != nil {
		t.Errorf("resolve on an escalated case: %v", err)
	}
	if !c.Escalated() {
		t.Error("the flag survives the transitions")
	}
}

// Every operator command validates its actor and timestamp.
func TestCommandsRejectAnInvalidActorOrZeroTimestamp(t *testing.T) {
	t.Parallel()
	badActor := shared.Actor{Kind: shared.ActorOperator, Name: " "}
	cases := []struct {
		name string
		call func(c *Case) error
	}{
		{"triage bad actor", func(c *Case) error { return c.Triage(shared.SeverityHigh, "q", badActor, openedAt) }},
		{"triage zero time", func(c *Case) error { return c.Triage(shared.SeverityHigh, "q", operator(t), time.Time{}) }},
		{"triage bad severity", func(c *Case) error { return c.Triage("SEV9", "q", operator(t), openedAt) }},
		{"triage blank queue", func(c *Case) error { return c.Triage(shared.SeverityHigh, " ", operator(t), openedAt) }},
		{"assign bad actor", func(c *Case) error { return c.Assign("op-x", badActor, openedAt) }},
		{"assign zero time", func(c *Case) error { return c.Assign("op-x", operator(t), time.Time{}) }},
		{"assign blank assignee", func(c *Case) error { return c.Assign(" ", operator(t), openedAt) }},
		{"assign NUL assignee", func(c *Case) error { return c.Assign("op\x00", operator(t), openedAt) }},
		{"resolve bad actor", func(c *Case) error { return c.Resolve("C", "R", "", badActor, openedAt) }},
		{"resolve zero time", func(c *Case) error { return c.Resolve("C", "R", "", operator(t), time.Time{}) }},
	}
	for _, tc := range cases {
		c := openedCase(t)
		if err := tc.call(c); err == nil {
			t.Errorf("%s: expected an error", tc.name)
		} else if c.Status() != StatusOpen || c.Version() != 1 || len(c.AuditTrail()) != 1 {
			t.Errorf("%s: a rejected command changes nothing", tc.name)
		}
	}
}
