package exceptioncase

import (
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// Event is a domain fact raised by the Case aggregate, to be written to the
// outbox. It carries no CloudEvents or Kafka knowledge: the outbound adapter
// maps it to the wire (ADR 0004). Every event carries the case id, the
// aggregate version AFTER the change and when it happened (the caller's
// clock — the domain never reads a clock).
type Event interface {
	EventName() string
	CaseID() string
	CaseVersion() int64
	OccurredAt() time.Time
}

// Header is embedded in every case event.
type Header struct {
	Case    string
	Version int64
	At      time.Time
}

// CaseID returns the case the event is about.
func (h Header) CaseID() string { return h.Case }

// CaseVersion returns the aggregate version after the change.
func (h Header) CaseVersion() int64 { return h.Version }

// OccurredAt returns when the change happened.
func (h Header) OccurredAt() time.Time { return h.At }

// ExceptionOpened is raised by Open: a source fact created a case (first
// occurrence of the dedup key). SourcePayload is the frozen copy of the
// source fact's data.
type ExceptionOpened struct {
	Header
	Key           shared.DedupKey
	Severity      shared.Severity
	Queue         string
	SLADueAt      time.Time
	SourceEventID string
	SourcePayload []byte
}

// EventName returns "ExceptionOpened".
func (ExceptionOpened) EventName() string { return "ExceptionOpened" }

// ExceptionTriaged is raised by Triage: severity and queue were set.
type ExceptionTriaged struct {
	Header
	Severity shared.Severity
	Queue    string
	Actor    shared.Actor
}

// EventName returns "ExceptionTriaged".
func (ExceptionTriaged) EventName() string { return "ExceptionTriaged" }

// ExceptionAssigned is raised by Assign: an owner took the case.
type ExceptionAssigned struct {
	Header
	Assignee string
	Actor    shared.Actor
}

// EventName returns "ExceptionAssigned".
func (ExceptionAssigned) EventName() string { return "ExceptionAssigned" }

// ExceptionEscalated is raised by Escalate, once per case, when the SLA
// clock breaches. The flag never blocks a transition (ADR 0002).
type ExceptionEscalated struct {
	Header
	SLADueAt time.Time
}

// EventName returns "ExceptionEscalated".
func (ExceptionEscalated) EventName() string { return "ExceptionEscalated" }

// ExceptionResolved is raised by Resolve: resolution code and root cause
// were recorded; the case is terminal.
type ExceptionResolved struct {
	Header
	ResolutionCode string
	RootCause      string
	Note           string
	Actor          shared.Actor
}

// EventName returns "ExceptionResolved".
func (ExceptionResolved) EventName() string { return "ExceptionResolved" }

// ExceptionAutoResolved is raised by AutoResolve: a declarative policy rule
// closed the case on a correlated fact.
type ExceptionAutoResolved struct {
	Header
	RuleID            string
	CorrelatedType    string
	CorrelatedEventID string
}

// EventName returns "ExceptionAutoResolved".
func (ExceptionAutoResolved) EventName() string { return "ExceptionAutoResolved" }
