package exceptionpolicy

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

var (
	declaredAt = time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)
	cptMissed  = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	repromised = "com.warehouse.wes.order-management.order.OrderRepromised"
)

func testRule(t *testing.T) *Rule {
	t.Helper()
	r, err := NewRule(repromised, 60)
	if err != nil {
		t.Fatal(err)
	}
	return &r
}

func testDeclareInput() DeclareInput {
	return DeclareInput{
		SourceType:  cptMissed,
		Severity:    shared.SeverityHigh,
		Queue:       "cps-front",
		SLAMinutes:  30,
		AutoResolve: nil,
		At:          declaredAt,
	}
}

// Declare validates every field and rejects the declaration whole.
func TestDeclareRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		mut  func(*DeclareInput)
		want error
	}{
		{"not a full type", func(in *DeclareInput) { in.SourceType = "cpt-missed" }, ErrInvalidSourceType},
		{"bare prefix", func(in *DeclareInput) { in.SourceType = "com.warehouse." }, ErrInvalidSourceType},
		{"one segment", func(in *DeclareInput) { in.SourceType = "com.warehouse.wes" }, ErrInvalidSourceType},
		{"lowercase event", func(in *DeclareInput) { in.SourceType = "com.warehouse.wes.fulfillment-execution.task.taskMissed" }, ErrInvalidSourceType},
		{"empty event segment", func(in *DeclareInput) { in.SourceType = "com.warehouse.wes.fulfillment-execution.task." }, ErrInvalidSourceType},
		{"unknown severity", func(in *DeclareInput) { in.Severity = "SEV1" }, ErrInvalidSeverity},
		{"blank queue", func(in *DeclareInput) { in.Queue = " " }, ErrBlankQueue},
		{"zero SLA", func(in *DeclareInput) { in.SLAMinutes = 0 }, ErrInvalidSLA},
		{"zero time", func(in *DeclareInput) { in.At = time.Time{} }, ErrNoTimestamp},
	}
	for _, tc := range cases {
		in := testDeclareInput()
		tc.mut(&in)
		if _, err := Declare(in); !errors.Is(err, tc.want) {
			t.Errorf("%s: Declare err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

// Invariant: Declare starts at version 1 recording the whole policy triage.
func TestDeclareStartsAtVersion1RecordingThePolicyTriage(t *testing.T) {
	t.Parallel()
	p, err := Declare(testDeclareInput())
	if err != nil {
		t.Fatal(err)
	}
	if p.Version() != 1 || p.SourceType() != cptMissed || p.Severity() != shared.SeverityHigh ||
		p.Queue() != "cps-front" || p.SLAMinutes() != 30 || p.AutoResolve() != nil {
		t.Fatalf("declared policy = %+v", p.Snapshot())
	}
	if !p.DeclaredAt().Equal(declaredAt) {
		t.Errorf("DeclaredAt = %v", p.DeclaredAt())
	}
}

// Invariant: versions are append-only — Redeclare returns version+1 as a NEW
// value, leaves the receiver untouched and rejects another source type.
func TestRedeclareReturnsTheNextVersionAsANewValue(t *testing.T) {
	t.Parallel()
	p, err := Declare(testDeclareInput())
	if err != nil {
		t.Fatal(err)
	}
	next, err := p.Redeclare(func() DeclareInput {
		in := testDeclareInput()
		in.Severity = shared.SeverityCritical
		in.Queue = "cps-inbound"
		in.AutoResolve = testRule(t)
		return in
	}())
	if err != nil {
		t.Fatal(err)
	}
	if next.Version() != 2 || next.Severity() != shared.SeverityCritical || next.AutoResolve() == nil {
		t.Fatalf("redeclared = %+v", next.Snapshot())
	}
	// The receiver is untouched.
	if p.Version() != 1 || p.Severity() != shared.SeverityHigh || p.AutoResolve() != nil {
		t.Error("Redeclare must not mutate the receiver")
	}
	// A redeclaration of another source type is rejected.
	other := testDeclareInput()
	other.SourceType = repromised
	if _, err := p.Redeclare(other); !errors.Is(err, ErrInvalidSourceType) {
		t.Errorf("cross-type redeclare err = %v", err)
	}
}

// The rule is data: a caller cannot mutate a policy through the pointer it
// passed in, nor through the pointer the policy hands out.
func TestTheRuleIsFrozenData(t *testing.T) {
	t.Parallel()
	r := testRule(t)
	in := testDeclareInput()
	in.AutoResolve = r
	p, err := Declare(in)
	if err != nil {
		t.Fatal(err)
	}
	held := p.AutoResolve()
	if held.OnType() != repromised || held.WithinMinutes() != 60 {
		t.Fatalf("held rule = %+v", held)
	}
	r.onType = "TAMPERED" // the caller mutates its own pointer afterwards
	if p.AutoResolve().OnType() != repromised {
		t.Error("the policy must have copied the rule at declaration")
	}
	out := p.AutoResolve()
	out.onType = "TAMPERED" // the caller mutates the handed-out pointer
	if p.AutoResolve().OnType() != repromised {
		t.Error("AutoResolve must hand out a copy")
	}
	if got := p.AutoResolve().ID(cptMissed); got != cptMissed+"->"+repromised {
		t.Errorf("ID() = %q, want the derived source->correlated form", got)
	}
}

// NewRule validates its own fields.
func TestNewRuleRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	if _, err := NewRule("not-a-type", 60); !errors.Is(err, ErrInvalidSourceType) {
		t.Errorf("bad type: err = %v", err)
	}
	if _, err := NewRule(repromised, 0); !errors.Is(err, ErrInvalidSLA) {
		t.Errorf("zero window: err = %v", err)
	}
	if r, err := NewRule(repromised, 1); err != nil || r.WithinMinutes() != 1 {
		t.Errorf("a one-minute window is valid: %v", err)
	}
}

// SLADue is openedAt + sla_minutes, UTC.
func TestPolicySLADue(t *testing.T) {
	t.Parallel()
	p, err := Declare(testDeclareInput())
	if err != nil {
		t.Fatal(err)
	}
	opened := time.Date(2026, 10, 21, 9, 0, 5, 0, time.UTC)
	if got := p.SLADue(opened); !got.Equal(opened.Add(30 * time.Minute)) {
		t.Errorf("SLADue = %v", got)
	}
}

// Snapshot round-trips; Restore rejects corrupt rows.
func TestPolicySnapshotRoundTripAndRestoreRejectsCorruption(t *testing.T) {
	t.Parallel()
	in := testDeclareInput()
	in.AutoResolve = testRule(t)
	p, err := Declare(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Restore(p.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if got.Version() != p.Version() || got.AutoResolve() == nil || !got.DeclaredAt().Equal(p.DeclaredAt()) {
		t.Errorf("restored = %+v", got.Snapshot())
	}
	corrupt := p.Snapshot()
	corrupt.Version = 0
	if _, err := Restore(corrupt); !errors.Is(err, ErrInvalidVersion) {
		t.Errorf("version 0: err = %v, want ErrInvalidVersion", err)
	}
	corrupt = p.Snapshot()
	corrupt.Severity = "SEV1"
	if _, err := Restore(corrupt); !errors.Is(err, ErrInvalidSeverity) {
		t.Errorf("bad severity: err = %v", err)
	}
	corrupt = p.Snapshot()
	corrupt.AutoResolve = func() *Rule { r, _ := NewRule(repromised, 0); return &r }()
	if _, err := Restore(corrupt); err == nil {
		t.Error("a corrupt rule inside the snapshot must be rejected")
	}
}

// Declaration is the fact behind ExceptionPolicyDeclared.
func TestDeclarationNamesTheFact(t *testing.T) {
	t.Parallel()
	p, err := Declare(testDeclareInput())
	if err != nil {
		t.Fatal(err)
	}
	d := p.Declaration()
	if d.EventName() != "ExceptionPolicyDeclared" || d.SourceType() != cptMissed ||
		d.Version() != 1 || !d.OccurredAt().Equal(declaredAt) {
		t.Errorf("Declaration = %+v", d)
	}
}

// A one-minute SLA is the smallest valid budget — the exact boundary of the
// < 1 gate, which must stay inclusive of 1.
func TestASingleMinuteSLAIsTheSmallestValidBudget(t *testing.T) {
	t.Parallel()
	in := testDeclareInput()
	in.SLAMinutes = 1
	p, err := Declare(in)
	if err != nil {
		t.Fatalf("Declare with a 1-minute SLA: %v", err)
	}
	if p.SLAMinutes() != 1 {
		t.Errorf("SLAMinutes = %d, want 1", p.SLAMinutes())
	}
}
