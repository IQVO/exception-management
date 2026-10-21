package exceptioncase

import (
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// assertRestoredState verifies the identity, triage fields, flags and clocks
// of a restored case against the case it came from.
func assertRestoredState(t *testing.T, name string, got, want *Case) {
	t.Helper()
	if got.ID() != want.ID() || got.Key() != want.Key() || got.Status() != want.Status() {
		t.Errorf("%s: identity mismatch: %+v", name, got.Snapshot())
	}
	if got.Severity() != want.Severity() || got.Queue() != want.Queue() || got.Assignee() != want.Assignee() {
		t.Errorf("%s: triage mismatch", name)
	}
	if got.Escalated() != want.Escalated() || got.Occurrence() != want.Occurrence() || got.Version() != want.Version() {
		t.Errorf("%s: flags/occurrence/version mismatch", name)
	}
	if !got.OpenedAt().Equal(want.OpenedAt()) || !got.LastSeenAt().Equal(want.LastSeenAt()) {
		t.Errorf("%s: clock mismatch", name)
	}
}

// assertRestoredOutcome verifies the resolution evidence, audit trail and
// pending events of a restored case.
func assertRestoredOutcome(t *testing.T, name string, got, want *Case) {
	t.Helper()
	if got.ResolutionCode() != want.ResolutionCode() || got.RootCause() != want.RootCause() || got.Note() != want.Note() {
		t.Errorf("%s: resolution mismatch", name)
	}
	if (got.AutoResolution() == nil) != (want.AutoResolution() == nil) {
		t.Errorf("%s: auto-resolution presence mismatch", name)
	}
	if len(got.AuditTrail()) != len(want.AuditTrail()) {
		t.Errorf("%s: audit length mismatch", name)
	}
	if len(got.DomainEvents()) != 0 {
		t.Errorf("%s: a restored case must have no pending events", name)
	}
}

// A snapshot of every state round-trips through Restore unchanged, with no
// pending events.
func TestSnapshotRoundTripsEveryState(t *testing.T) {
	t.Parallel()
	op := operator(t)
	states := map[string]*Case{}
	states["open"] = openedCase(t)

	escalated := openedCase(t)
	if _, err := escalated.Escalate(escalated.SLADueAt()); err != nil {
		t.Fatal(err)
	}
	states["escalated-open"] = escalated

	full := openedCase(t)
	if err := full.Triage(shared.SeverityCritical, "cps-inbound", op, openedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := full.Touch(openedAt.Add(2 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := full.Assign("op-marc", op, openedAt.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	states["assigned-touched"] = full

	resolved := openedCase(t)
	if err := resolved.Resolve("WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "note", op, openedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	states["resolved"] = resolved

	auto := openedCase(t)
	if err := auto.AutoResolve("r1", "t1", "e1", openedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	states["auto-resolved"] = auto

	for name, c := range states {
		s := c.Snapshot()
		got, err := Restore(s)
		if err != nil {
			t.Fatalf("%s: Restore: %v", name, err)
		}
		assertRestoredState(t, name, got, c)
		assertRestoredOutcome(t, name, got, c)
		// The snapshot payload is a copy: mutating it must not reach the case.
		s.SourcePayload[0] = 'X'
		if got.SourcePayload()[0] == 'X' {
			t.Errorf("%s: Snapshot must copy the payload", name)
		}
	}
}

// Restore rejects every corrupt row: a snapshot that violates an invariant
// can never become a live aggregate.
func TestRestoreRejectsEveryCorruptSnapshot(t *testing.T) {
	t.Parallel()
	base := func() Snapshot {
		c := openedCase(t)
		return c.Snapshot()
	}
	muts := []struct {
		name string
		mut  func(*Snapshot)
	}{
		{"blank id", func(s *Snapshot) { s.ID = " " }},
		{"blank source type", func(s *Snapshot) { s.Key.SourceType = "" }},
		{"invalid status", func(s *Snapshot) { s.Status = "PAUSED" }},
		{"invalid severity", func(s *Snapshot) { s.Severity = "SEV9" }},
		{"blank queue", func(s *Snapshot) { s.Queue = "" }},
		{"zero SLA", func(s *Snapshot) { s.SLAMinutes = 0 }},
		{"zero occurrence", func(s *Snapshot) { s.Occurrence = 0 }},
		{"zero version", func(s *Snapshot) { s.Version = 0 }},
		{"blank source event id", func(s *Snapshot) { s.SourceEventID = "" }},
		{"empty payload", func(s *Snapshot) { s.SourcePayload = nil }},
		{"zero openedAt", func(s *Snapshot) { s.OpenedAt = time.Time{} }},
		{"zero lastSeenAt", func(s *Snapshot) { s.LastSeenAt = time.Time{} }},
		{"lastSeen before open", func(s *Snapshot) { s.LastSeenAt = s.OpenedAt.Add(-time.Second) }},
		{"assignee on an open case", func(s *Snapshot) { s.Assignee = "op-x" }},
		{"assigned without assignee", func(s *Snapshot) { s.Status = StatusAssigned; s.Assignee = "" }},
		{"resolution fields on an unresolved case", func(s *Snapshot) { s.ResolutionCode = "C" }},
		{"auto fields on an unresolved case", func(s *Snapshot) { s.AutoResolved = true }},
		{"resolved without code", func(s *Snapshot) { s.Status = StatusResolved }},
		{"resolved without instant", func(s *Snapshot) {
			s.Status = StatusResolved
			s.ResolutionCode = "C"
			s.RootCause = "R"
		}},
		{"resolved before open", func(s *Snapshot) {
			s.Status = StatusResolved
			s.ResolutionCode = "C"
			s.RootCause = "R"
			s.ResolvedAt = s.OpenedAt.Add(-time.Second)
		}},
		{"auto-resolved without evidence", func(s *Snapshot) {
			s.Status = StatusResolved
			s.ResolutionCode = "AUTO_RESOLVED"
			s.RootCause = "t"
			s.ResolvedAt = s.OpenedAt
			s.AutoResolved = true
			s.AutoRuleID = " "
		}},
		{"empty audit", func(s *Snapshot) { s.Audit = nil }},
		{"audit not starting with OPENED", func(s *Snapshot) { s.Audit[0].Action = ActionTouched }},
		{"audit entry with an invalid actor", func(s *Snapshot) { s.Audit[0].Actor = shared.Actor{} }},
		{"audit entry with a zero instant", func(s *Snapshot) { s.Audit[0].At = time.Time{} }},
		{"audit entry before the open", func(s *Snapshot) { s.Audit[0].At = s.OpenedAt.Add(-time.Second) }},
		{"audit entry with a blank action", func(s *Snapshot) { s.Audit[0].Action = " " }},
	}
	for _, tc := range muts {
		s := base()
		tc.mut(&s)
		if _, err := Restore(s); err == nil {
			t.Errorf("%s: Restore accepted a corrupt snapshot", tc.name)
		}
	}
}
