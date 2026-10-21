package exceptioncase

import (
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// Invariant (ADR 0002): every transition records its actor and time, and the
// audit list grows by EXACTLY one entry per accepted transition — nothing
// else appends to it, and no accepted transition appends more than one.
func TestEveryTransitionRecordsActorAndTimeAndTheAuditGrowsByExactlyOne(t *testing.T) {
	t.Parallel()
	op := operator(t)
	c := openedCase(t)
	steps := []struct {
		name   string
		call   func() error
		actor  shared.Actor
		at     time.Time
		action string
	}{
		{"touch", func() error { return c.Touch(openedAt.Add(time.Minute)) },
			shared.System(), openedAt.Add(time.Minute), ActionTouched},
		{"triage", func() error { return c.Triage(shared.SeverityCritical, "cps-inbound", op, openedAt.Add(2*time.Minute)) },
			op, openedAt.Add(2 * time.Minute), ActionTriaged},
		{"assign", func() error { return c.Assign("op-marc", op, openedAt.Add(3*time.Minute)) },
			op, openedAt.Add(3 * time.Minute), ActionAssigned},
		{"escalate", func() error { _, err := c.Escalate(c.SLADueAt()); return err },
			shared.System(), c.SLADueAt(), ActionEscalated},
		{"resolve", func() error {
			return c.Resolve("WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "", op, openedAt.Add(time.Hour))
		},
			op, openedAt.Add(time.Hour), ActionResolved},
	}
	wantLen := len(c.AuditTrail()) // 1: the OPENED entry
	for i, s := range steps {
		if err := s.call(); err != nil {
			t.Fatalf("step %s: %v", s.name, err)
		}
		wantLen++
		audit := c.AuditTrail()
		if len(audit) != wantLen {
			t.Fatalf("step %s: audit len = %d, want %d", s.name, len(audit), wantLen)
		}
		got := audit[len(audit)-1]
		if got.Action != s.action {
			t.Errorf("step %d (%s): action = %s, want %s", i, s.name, got.Action, s.action)
		}
		if got.Actor != s.actor {
			t.Errorf("step %s: actor = %+v, want %+v", s.name, got.Actor, s.actor)
		}
		if !got.At.Equal(s.at) {
			t.Errorf("step %s: at = %v, want %v", s.name, got.At, s.at)
		}
		if c.Version() != int64(wantLen) {
			t.Errorf("step %s: version = %d, want %d (one per accepted write)", s.name, c.Version(), wantLen)
		}
	}
	// The audit of an audit: the OPENED entry is the system's.
	if first := c.AuditTrail()[0]; first.Action != ActionOpened || first.Actor != shared.System() {
		t.Errorf("the first entry = %+v, want OPENED by system", first)
	}
	// The trail is returned as a copy.
	trail := c.AuditTrail()
	trail[0].Action = "TAMPERED"
	if c.AuditTrail()[0].Action != ActionOpened {
		t.Error("AuditTrail must return a copy")
	}
}

// A rejected command appends nothing.
func TestRejectedCommandsAppendNoAuditEntry(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	_ = c.Triage("SEV9", "q", operator(t), openedAt) // invalid severity
	_ = c.Assign(" ", operator(t), openedAt)         // invalid assignee
	_, _ = c.Escalate(openedAt)                      // before breach
	_ = c.Touch(time.Time{})                         // zero time
	if got := len(c.AuditTrail()); got != 1 {
		t.Errorf("audit len = %d, want 1 (OPENED only)", got)
	}
	if c.Version() != 1 {
		t.Errorf("version = %d, want 1", c.Version())
	}
}
