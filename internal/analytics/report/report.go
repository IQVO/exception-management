// Package report is the self-contained read-model region of the analytics
// read side (ADR 0006): the kinds of the analytics stream, the shape of the
// three reports the exceptions-reports binary serves, the query rules (date
// range), the pure percentile/share/freshness logic, the validation of a
// projectable event, and the writer/reader ports the analytical store
// implements. It imports no other internal package, so the OLTP domain can
// never leak into the projection and the arch test keeps this region
// isolated (the same decision as pick-face-replenishment ADR 0008).
package report

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Kind is which of the published events a fact came from. The names are the
// CloudEvents event names (the last segment of `type`).
type Kind string

// The seven events the analytics topic carries: the six exception-case facts
// (also on the integration topic) and the policy declaration (ADR 0004).
const (
	KindExceptionOpened         Kind = "ExceptionOpened"
	KindExceptionTriaged        Kind = "ExceptionTriaged"
	KindExceptionAssigned       Kind = "ExceptionAssigned"
	KindExceptionEscalated      Kind = "ExceptionEscalated"
	KindExceptionResolved       Kind = "ExceptionResolved"
	KindExceptionAutoResolved   Kind = "ExceptionAutoResolved"
	KindExceptionPolicyDeclared Kind = "ExceptionPolicyDeclared"
)

// Kinds returns every projected kind, in catalogue order.
func Kinds() []Kind {
	return []Kind{KindExceptionOpened, KindExceptionTriaged, KindExceptionAssigned,
		KindExceptionEscalated, KindExceptionResolved, KindExceptionAutoResolved, KindExceptionPolicyDeclared}
}

// Known reports whether k is one of the seven published kinds.
func (k Kind) Known() bool {
	for _, c := range Kinds() {
		if k == c {
			return true
		}
	}
	return false
}

// IsCase reports whether k is one of the six exception-case facts (everything
// but the policy declaration).
func (k Kind) IsCase() bool {
	return k != KindExceptionPolicyDeclared
}

// Event is one decoded analytics event ready to project. Which fields are set
// depends on the Kind (hand-mirrored from apis/asyncapi.yaml, never imported
// from the domain): the case facts carry CaseID (the CloudEvents subject);
// Opened adds SourceType, SourceSubject, SiteID (nullable), Severity, Queue,
// OpenedAt and SLADueAt; Triaged Severity and Queue; Assigned Assignee;
// Escalated SLADueAt; Resolved ResolutionCode, RootCause and a nullable Note;
// AutoResolved RuleID, CorrelatedType and CorrelatedEventID; the declaration
// carries the policy version fields.
type Event struct {
	Kind    Kind
	EventID string    // CloudEvents id: the idempotency key
	At      time.Time // CloudEvents time: when the event occurred

	CaseID         string
	SourceType     string
	SourceSubject  string
	SiteID         *string
	Severity       string
	Queue          string
	SLADueAt       time.Time
	Assignee       string
	ResolutionCode string
	RootCause      string
	Note           *string
	RuleID         string
	CorrelatedType string

	Version     int64
	SLAMinutes  int64
	AutoResolve *AutoResolveRule
}

// AutoResolveRule mirrors the declaration payload's auto_resolve object.
type AutoResolveRule struct {
	OnType        string
	WithinMinutes int64
}

// ErrInvalidEvent marks an event that can never be projected (the consumer
// dead-letters it).
var ErrInvalidEvent = errors.New("report: event cannot be projected")

// Validate checks the invariants every store relies on. Every failure wraps
// ErrInvalidEvent and is deterministic: the same bytes always fail.
func (e Event) Validate() error {
	switch {
	case !e.Kind.Known():
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidEvent, e.Kind)
	case e.EventID == "":
		return e.invalid("without an id")
	case e.At.IsZero():
		return e.invalid("without a time")
	case e.Kind.IsCase() && e.CaseID == "":
		return e.invalid("without a case_id")
	case !e.Kind.IsCase() && e.CaseID != "":
		return e.invalid("carries a case_id")
	}
	if e.Kind == KindExceptionPolicyDeclared {
		return e.validatePolicy()
	}
	return nil
}

// validatePolicy checks the declaration payload: a version of at least 1, a
// severity, a queue, an SLA of at least one minute and, when the auto-resolve
// rule is present, its own type and a window of at least one minute.
func (e Event) validatePolicy() error {
	switch {
	case e.SourceType == "":
		return e.invalid("without a source_type")
	case e.Version < 1:
		return e.invalid("without a version of at least 1")
	case e.Severity == "":
		return e.invalid("without a severity")
	case e.Queue == "":
		return e.invalid("without a queue")
	case e.SLAMinutes < 1:
		return e.invalid("without an sla_minutes of at least 1")
	case e.AutoResolve != nil && (e.AutoResolve.OnType == "" || e.AutoResolve.WithinMinutes < 1):
		return e.invalid("with an incomplete auto_resolve rule")
	}
	return nil
}

func (e Event) invalid(why string) error {
	return fmt.Errorf("%w: %s %s %s", ErrInvalidEvent, e.Kind, e.EventID, why)
}

// Milestones is the "this happened at" candidate an event offers for each
// timestamp column of exception_case_facts (nil where it offers none). A
// store keeps the earliest candidate per milestone, so arrival order never
// matters; SLADue is the due time the event last saw.
type Milestones struct {
	Opened       *time.Time
	Triaged      *time.Time
	Assigned     *time.Time
	Escalated    *time.Time
	Resolved     *time.Time
	AutoResolved *time.Time
	SLADue       *time.Time
}

// MilestonesOf returns the milestones a case event marks on its own case row
// (none for the policy declaration).
func MilestonesOf(e Event) Milestones {
	at := e.At.UTC()
	var m Milestones
	switch e.Kind {
	case KindExceptionOpened:
		m.Opened = &at
		due := e.SLADueAt.UTC()
		m.SLADue = &due
	case KindExceptionTriaged:
		m.Triaged = &at
	case KindExceptionAssigned:
		m.Assigned = &at
	case KindExceptionEscalated:
		m.Escalated = &at
		due := e.SLADueAt.UTC()
		m.SLADue = &due
	case KindExceptionResolved:
		m.Resolved = &at
	case KindExceptionAutoResolved:
		m.AutoResolved = &at
	case KindExceptionPolicyDeclared:
	}
	return m
}

// Projection is the WRITER port: Apply records the event id and folds the
// event into the model in ONE transaction. applied is false when the id was
// already recorded (a replay): nothing changed. An error wrapping
// ErrRejected is deterministic (the store can never accept this event); any
// other error is transient and the same event may be retried.
type Projection interface {
	Apply(ctx context.Context, e Event) (applied bool, err error)
}

// ErrRejected marks an event the analytical store deterministically refuses
// (a validation failure or a Postgres SQLSTATE class 22/23 rejection).
var ErrRejected = errors.New("report: event rejected by the analytical store")

// Reader is the READER port of the three reports and the freshness probe.
type Reader interface {
	// RootCauseRows returns the manual resolutions of the window grouped by
	// source type and root cause, most resolved first.
	RootCauseRows(ctx context.Context, q Query) ([]RootCauseRow, error)
	// QueueTimingRows returns the opened-to-resolve timing of the window
	// per queue, alphabetical by queue.
	QueueTimingRows(ctx context.Context, q Query) ([]QueueTimingRow, error)
	// SLABreachRows returns the breach and escalation shares of the window
	// per source type, alphabetical by source type.
	SLABreachRows(ctx context.Context, q Query) ([]SLABreachRow, error)
	// LastEventAt is the CloudEvents time of the newest event the projection
	// has applied (nil while it has applied none): the freshness basis.
	LastEventAt(ctx context.Context) (*time.Time, error)
}

// Query selects a report's window. The half-open range [From, To) applies to
// the timestamp each report counts on (resolved_at for the root causes and
// the timing, opened_at for the SLA breach shares).
type Query struct {
	Range Range
}

// RootCauseRow is one line of the root-cause-frequency report: how many
// resolutions of one source type carried a given root cause in the window.
// Share is that count over ALL resolutions of the source type in the window
// (the auto-resolved ones carry no root cause, so the shares of a source type
// sum to at most 1).
type RootCauseRow struct {
	SourceType string
	RootCause  string
	Resolved   int64
	Share      float64
}

// QueueTimingRow is one line of the mean-time-to-resolve report: the
// opened-to-resolve duration of the cases resolved in the window per queue.
// Mean/P50/P95 are nil while Resolved is 0.
type QueueTimingRow struct {
	Queue       string
	Resolved    int64
	MeanSeconds *float64
	P50Seconds  *float64
	P95Seconds  *float64
}

// SLABreachRow is one line of the sla-breach-rate report, over the cases
// OPENED in the window of one source type. Breached counts the cases that
// were escalated or resolved after their due time; Escalated the ones the SLA
// sweep escalated. Rates are nil-free fractions of Opened (0 when Opened is
// 0).
type SLABreachRow struct {
	SourceType     string
	Opened         int64
	Escalated      int64
	Breached       int64
	BreachRate     float64
	EscalationRate float64
}
