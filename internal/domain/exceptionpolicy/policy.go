package exceptionpolicy

import (
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// Policy is the handling policy of ONE consumed source type: the severity
// and queue a case opened under it starts with, its SLA budget, and an
// optional auto-resolve rule. Versions are append-only: Declare creates
// version 1 and Redeclare returns the NEXT version as a new value — the
// receiver is never mutated and history is never rewritten (ADR 0002).
type Policy struct {
	sourceType  string
	version     int64
	severity    shared.Severity
	queue       string
	slaMinutes  int64
	autoResolve *Rule
	declaredAt  time.Time
}

// DeclareInput is everything a first declaration needs.
type DeclareInput struct {
	SourceType  string
	Severity    shared.Severity
	Queue       string
	SLAMinutes  int64
	AutoResolve *Rule
	At          time.Time
}

// Declare creates a policy at version 1 and returns its declaration fact.
func Declare(in DeclareInput) (*Policy, error) {
	if err := validate(in); err != nil {
		return nil, err
	}
	return &Policy{
		sourceType:  in.SourceType,
		version:     1,
		severity:    in.Severity,
		queue:       in.Queue,
		slaMinutes:  in.SLAMinutes,
		autoResolve: copyRule(in.AutoResolve),
		declaredAt:  in.At.UTC(),
	}, nil
}

// Redeclare returns the next version of the policy as a NEW value; the
// receiver is left untouched (append-only versions).
func (p *Policy) Redeclare(in DeclareInput) (*Policy, error) {
	if in.SourceType != p.sourceType {
		return nil, ErrInvalidSourceType
	}
	if err := validate(in); err != nil {
		return nil, err
	}
	return &Policy{
		sourceType:  p.sourceType,
		version:     p.version + 1,
		severity:    in.Severity,
		queue:       in.Queue,
		slaMinutes:  in.SLAMinutes,
		autoResolve: copyRule(in.AutoResolve),
		declaredAt:  in.At.UTC(),
	}, nil
}

// Snapshot is the persisted form of a policy version. Restore validates it,
// so a corrupt row can never become a live policy.
type Snapshot struct {
	SourceType  string
	Version     int64
	Severity    shared.Severity
	Queue       string
	SLAMinutes  int64
	AutoResolve *Rule
	DeclaredAt  time.Time
}

// Snapshot returns the persisted form of the policy version.
func (p *Policy) Snapshot() Snapshot {
	return Snapshot{
		SourceType:  p.sourceType,
		Version:     p.version,
		Severity:    p.severity,
		Queue:       p.queue,
		SLAMinutes:  p.slaMinutes,
		AutoResolve: copyRule(p.autoResolve),
		DeclaredAt:  p.declaredAt,
	}
}

// Restore rebuilds a persisted policy version, validating every invariant.
func Restore(s Snapshot) (*Policy, error) {
	in := DeclareInput{
		SourceType:  s.SourceType,
		Severity:    s.Severity,
		Queue:       s.Queue,
		SLAMinutes:  s.SLAMinutes,
		AutoResolve: s.AutoResolve,
		At:          s.DeclaredAt,
	}
	if err := validate(in); err != nil {
		return nil, err
	}
	if err := validateRule(s.AutoResolve); err != nil {
		return nil, err
	}
	if s.Version < 1 {
		return nil, ErrInvalidVersion
	}
	return &Policy{
		sourceType:  s.SourceType,
		version:     s.Version,
		severity:    s.Severity,
		queue:       s.Queue,
		slaMinutes:  s.SLAMinutes,
		autoResolve: copyRule(s.AutoResolve),
		declaredAt:  s.DeclaredAt.UTC(),
	}, nil
}

// validate checks every invariant a declaration must satisfy.
func validate(in DeclareInput) error {
	if !isFullType(in.SourceType) {
		return ErrInvalidSourceType
	}
	if !in.Severity.Valid() {
		return ErrInvalidSeverity
	}
	if shared.IsBlank(in.Queue) {
		return ErrBlankQueue
	}
	if in.SLAMinutes < 1 {
		return ErrInvalidSLA
	}
	if in.At.IsZero() {
		return ErrNoTimestamp
	}
	return nil
}

// validateRule re-validates a rule that arrives from outside NewRule (a
// persisted snapshot): the invariants of a rule hold wherever it came from.
func validateRule(r *Rule) error {
	if r == nil {
		return nil
	}
	if _, err := NewRule(r.onType, r.withinMinutes); err != nil {
		return err
	}
	return nil
}

// copyRule returns a defensive copy so a caller cannot mutate a policy's
// rule through the pointer it passed in (the rule is data, frozen per
// version).
func copyRule(r *Rule) *Rule {
	if r == nil {
		return nil
	}
	c := *r
	return &c
}

// SourceType returns the governed full CloudEvents type.
func (p *Policy) SourceType() string { return p.sourceType }

// Version returns the policy version (1 for the first declaration).
func (p *Policy) Version() int64 { return p.version }

// Severity returns the default severity for cases opened under this policy.
func (p *Policy) Severity() shared.Severity { return p.severity }

// Queue returns the default problem-solve queue.
func (p *Policy) Queue() string { return p.queue }

// SLAMinutes returns the SLA budget of cases opened under this policy.
func (p *Policy) SLAMinutes() int64 { return p.slaMinutes }

// AutoResolve returns the auto-resolve rule, or nil when the source type
// has none. The returned pointer is a copy: callers cannot mutate history.
func (p *Policy) AutoResolve() *Rule { return copyRule(p.autoResolve) }

// DeclaredAt returns when this version was declared.
func (p *Policy) DeclaredAt() time.Time { return p.declaredAt }

// SLADue returns the SLA deadline of a case opened at openedAt under this
// policy: openedAt + slaMinutes. Pure.
func (p *Policy) SLADue(openedAt time.Time) time.Time {
	return openedAt.Add(time.Duration(p.slaMinutes) * time.Minute).UTC()
}
