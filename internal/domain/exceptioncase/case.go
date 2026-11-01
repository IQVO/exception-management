// Package exceptioncase holds the ExceptionCase aggregate (ADR 0002): one
// owned anomaly with a fixed status machine (OPEN -> TRIAGED -> ASSIGNED ->
// RESOLVED, ESCALATED a flag), a dedup key with occurrence counting, an SLA
// clock, a frozen copy of the source payload and an audit trail that grows
// by exactly one entry per accepted transition. Pure Go: no I/O, no clock
// reads; it imports nothing outside internal/domain.
package exceptioncase

import (
	"bytes"
	"slices"
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// Status is the fixed status machine. RESOLVED is terminal; ESCALATED is a
// flag (Case.Escalated), not a status.
type Status string

// The four statuses, in the only order they may appear in.
const (
	StatusOpen     Status = "OPEN"
	StatusTriaged  Status = "TRIAGED"
	StatusAssigned Status = "ASSIGNED"
	StatusResolved Status = "RESOLVED"
)

// Valid reports whether s is one of the four statuses.
func (s Status) Valid() bool {
	switch s {
	case StatusOpen, StatusTriaged, StatusAssigned, StatusResolved:
		return true
	default:
		return false
	}
}

// Audit actions. One audit entry per accepted transition (ADR 0002).
const (
	ActionOpened       = "OPENED"
	ActionTouched      = "TOUCHED"
	ActionTriaged      = "TRIAGED"
	ActionAssigned     = "ASSIGNED"
	ActionEscalated    = "ESCALATED"
	ActionResolved     = "RESOLVED"
	ActionAutoResolved = "AUTO_RESOLVED"
)

// AuditEntry records one accepted transition: what happened, who did it
// (operator id, policy id or system) and when.
type AuditEntry struct {
	Action string
	Actor  shared.Actor
	At     time.Time
}

// autoResolution is the evidence an auto-resolve rule leaves on the case.
type autoResolution struct {
	ruleID            string
	correlatedType    string
	correlatedEventID string
}

// resolution is everything a close (manual or automatic) records.
type resolution struct {
	code      string
	rootCause string
	note      string
	at        time.Time
	actor     shared.Actor
	auto      *autoResolution
}

// Case is the aggregate root: one owned anomaly.
type Case struct {
	id            shared.ID
	key           shared.DedupKey
	status        Status
	severity      shared.Severity
	queue         string
	assignee      string
	escalated     bool
	occurrence    int64
	openedAt      time.Time
	lastSeenAt    time.Time
	slaMinutes    int64
	sourceEventID string
	sourcePayload []byte
	resolution    resolution
	version       int64
	audit         []AuditEntry
	events        []Event
}

// OpenInput is everything Open needs. Severity, Queue and SLAMinutes come
// from the policy version current at open (the application layer supplies
// them); Payload is the consumed source fact's data.
type OpenInput struct {
	CaseID        string
	Key           shared.DedupKey
	Severity      shared.Severity
	Queue         string
	SLAMinutes    int64
	SourceEventID string
	Payload       []byte
	At            time.Time
}

// Open creates an OPEN case at version 1 and raises ExceptionOpened. It is
// called only when NO case exists for the dedup key (open or resolved); the
// one-open-case-per-key rule spans cases, so OpenOrTouch and the use case
// enforce it (ADR 0002).
func Open(in OpenInput) (*Case, error) {
	if err := validateOpen(in); err != nil {
		return nil, err
	}
	at := in.At.UTC()
	payload := bytes.Clone(in.Payload)
	c := &Case{
		id:            shared.ID(in.CaseID),
		key:           in.Key,
		status:        StatusOpen,
		severity:      in.Severity,
		queue:         in.Queue,
		occurrence:    1,
		openedAt:      at,
		lastSeenAt:    at,
		slaMinutes:    in.SLAMinutes,
		sourceEventID: in.SourceEventID,
		sourcePayload: payload,
		version:       1,
		audit:         []AuditEntry{{Action: ActionOpened, Actor: shared.System(), At: at}},
	}
	c.raise(ExceptionOpened{
		Header:        c.header(at),
		Key:           c.key,
		Severity:      c.severity,
		Queue:         c.queue,
		SLADueAt:      c.SLADueAt(),
		SourceEventID: c.sourceEventID,
		SourcePayload: bytes.Clone(payload),
	})
	return c, nil
}

// OpenOrTouch is the dedup decision of ADR 0002: a fact for a key whose
// case exists TOUCHES that case (occurrence + 1, lastSeenAt, one TOUCHED
// audit entry, NO new case and NO new domain event), whatever the case's
// status — a fact arriving after resolution counts as an occurrence but can
// never reopen the case. Only a key with no case at all opens one.
func OpenOrTouch(latest *Case, in OpenInput) (*Case, error) {
	if latest == nil {
		return Open(in)
	}
	if in.Key != latest.key {
		return nil, ErrKeyMismatch
	}
	if err := latest.Touch(in.At); err != nil {
		return nil, err
	}
	return latest, nil
}

// validateOpen checks every Open invariant.
func validateOpen(in OpenInput) error {
	if _, err := shared.NewID(in.CaseID); err != nil {
		return ErrBlankCaseID
	}
	if _, err := shared.NewDedupKey(in.Key.SourceType, in.Key.SourceSubject, in.Key.SiteID); err != nil {
		return ErrInvalidDedupKey
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
	if !shared.ValidText(in.SourceEventID) {
		return ErrBlankSourceEventID
	}
	if len(in.Payload) == 0 {
		return ErrEmptySourcePayload
	}
	if in.At.IsZero() {
		return ErrNoTimestamp
	}
	return nil
}

func (c *Case) raise(e Event) { c.events = append(c.events, e) }
func (c *Case) header(at time.Time) Header {
	return Header{Case: string(c.id), Version: c.version, At: at.UTC()}
}

// record appends exactly one audit entry.
func (c *Case) record(action string, actor shared.Actor, at time.Time) {
	c.audit = append(c.audit, AuditEntry{Action: action, Actor: actor, At: at})
}

// DomainEvents returns the facts raised since the aggregate was created, in
// order, for the outbox. A restored case has none. The slice is a copy.
func (c *Case) DomainEvents() []Event { return slices.Clone(c.events) }

// ID returns the case id.
func (c *Case) ID() string { return string(c.id) }

// Key returns the dedup key.
func (c *Case) Key() shared.DedupKey { return c.key }

// Status returns the lifecycle status.
func (c *Case) Status() Status { return c.status }

// Severity returns the current severity.
func (c *Case) Severity() shared.Severity { return c.severity }

// Queue returns the current problem-solve queue.
func (c *Case) Queue() string { return c.queue }

// Assignee returns the owning operator id ("" before the first assignment).
func (c *Case) Assignee() string { return c.assignee }

// Escalated reports the breach flag.
func (c *Case) Escalated() bool { return c.escalated }

// Occurrence returns how many times the source fact arrived.
func (c *Case) Occurrence() int64 { return c.occurrence }

// OpenedAt returns when the case was opened.
func (c *Case) OpenedAt() time.Time { return c.openedAt }

// LastSeenAt returns the arrival time of the most recent occurrence.
func (c *Case) LastSeenAt() time.Time { return c.lastSeenAt }

// SLAMinutes returns the policy SLA budget recorded at open.
func (c *Case) SLAMinutes() int64 { return c.slaMinutes }

// SLADueAt returns openedAt + the policy's sla_minutes, computed from the
// recorded fields. Pure.
func (c *Case) SLADueAt() time.Time {
	return c.openedAt.Add(time.Duration(c.slaMinutes) * time.Minute)
}

// BreachesAt reports whether the SLA clock has breached at now: true at or
// after the due instant. Pure — the sweeper supplies now.
func (c *Case) BreachesAt(now time.Time) bool { return !now.Before(c.SLADueAt()) }

// SourceEventID returns the CloudEvents id of the fact that opened the case.
func (c *Case) SourceEventID() string { return c.sourceEventID }

// SourcePayload returns a copy of the frozen source payload.
func (c *Case) SourcePayload() []byte { return bytes.Clone(c.sourcePayload) }

// ResolutionCode returns the resolution code ("" until resolved).
func (c *Case) ResolutionCode() string { return c.resolution.code }

// RootCause returns the root cause ("" until resolved).
func (c *Case) RootCause() string { return c.resolution.rootCause }

// Note returns the resolution note ("" when none).
func (c *Case) Note() string { return c.resolution.note }

// ResolvedAt returns the resolution instant (zero until resolved).
func (c *Case) ResolvedAt() time.Time { return c.resolution.at }

// AutoResolution returns the auto-resolution evidence (nil unless the case
// was auto-resolved). The copy is defensive.
func (c *Case) AutoResolution() *autoResolution {
	if c.resolution.auto == nil {
		return nil
	}
	a := *c.resolution.auto
	return &a
}

// Version returns the aggregate version.
func (c *Case) Version() int64 { return c.version }

// AuditTrail returns a copy of the audit entries, one per accepted
// transition, in order.
func (c *Case) AuditTrail() []AuditEntry { return slices.Clone(c.audit) }
