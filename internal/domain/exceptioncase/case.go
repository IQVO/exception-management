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

// Touch records one more arrival of the source fact: occurrence + 1 and
// lastSeenAt advanced, with a TOUCHED audit entry and no domain event (the
// occurrence count and audit trail ARE the record).
func (c *Case) Touch(at time.Time) error {
	if at.IsZero() {
		return ErrNoTimestamp
	}
	c.occurrence++
	c.lastSeenAt = at.UTC()
	c.version++
	c.record(ActionTouched, shared.System(), c.lastSeenAt)
	return nil
}

// Triage sets or changes the severity and queue. An OPEN case moves to
// TRIAGED; a triaged, assigned or escalated case keeps its status (the
// change never moves a case backwards). Rejected on a resolved case.
func (c *Case) Triage(severity shared.Severity, queue string, actor shared.Actor, at time.Time) error {
	if err := c.commandable(); err != nil {
		return err
	}
	if !severity.Valid() {
		return ErrInvalidSeverity
	}
	if shared.IsBlank(queue) {
		return ErrBlankQueue
	}
	if err := validCommand(actor, at); err != nil {
		return err
	}
	c.severity = severity
	c.queue = queue
	if c.status == StatusOpen {
		c.status = StatusTriaged
	}
	c.version++
	c.record(ActionTriaged, actor, at.UTC())
	c.raise(ExceptionTriaged{Header: c.header(at), Severity: c.severity, Queue: c.queue, Actor: actor})
	return nil
}

// Assign gives the case an owner and moves it to ASSIGNED (a forward move
// from OPEN or TRIAGED, a reassignment from ASSIGNED). Rejected on a
// resolved case.
func (c *Case) Assign(assignee string, actor shared.Actor, at time.Time) error {
	if err := c.commandable(); err != nil {
		return err
	}
	if !shared.ValidText(assignee) {
		return ErrBlankAssignee
	}
	if err := validCommand(actor, at); err != nil {
		return err
	}
	c.assignee = assignee
	c.status = StatusAssigned
	c.version++
	c.record(ActionAssigned, actor, at.UTC())
	c.raise(ExceptionAssigned{Header: c.header(at), Assignee: c.assignee, Actor: actor})
	return nil
}

// Resolve records the resolution code, the root cause and an optional note,
// and moves the case to RESOLVED — terminal: every later command fails with
// ErrCaseClosed (the 409-class error of ADR 0002).
func (c *Case) Resolve(code, rootCause, note string, actor shared.Actor, at time.Time) error {
	if err := c.commandable(); err != nil {
		return err
	}
	if !shared.ValidText(code) {
		return ErrBlankResolutionCode
	}
	if !shared.ValidText(rootCause) {
		return ErrBlankRootCause
	}
	if err := validCommand(actor, at); err != nil {
		return err
	}
	utc := at.UTC()
	c.status = StatusResolved
	c.resolution = resolution{code: code, rootCause: rootCause, note: note, at: utc, actor: actor}
	c.version++
	c.record(ActionResolved, actor, utc)
	c.raise(ExceptionResolved{
		Header:         c.header(at),
		ResolutionCode: c.resolution.code,
		RootCause:      c.resolution.rootCause,
		Note:           c.resolution.note,
		Actor:          actor,
	})
	return nil
}

// Escalate sets the ESCALATED flag and raises ExceptionEscalated exactly
// once per case. It never changes the status and never blocks a later
// transition. A no-op (false, nil) on an already-escalated or resolved
// case; an error before the SLA deadline or on a zero timestamp.
func (c *Case) Escalate(at time.Time) (bool, error) {
	if at.IsZero() {
		return false, ErrNoTimestamp
	}
	if c.status == StatusResolved || c.escalated {
		return false, nil
	}
	if !c.BreachesAt(at) {
		return false, ErrNotBreached
	}
	c.escalated = true
	c.version++
	c.record(ActionEscalated, shared.System(), at.UTC())
	c.raise(ExceptionEscalated{Header: c.header(at), SLADueAt: c.SLADueAt()})
	return true, nil
}

// AutoResolve closes the case the way a declarative policy rule does
// (ADR 0002): the rule's derived id and the correlated fact's full type and
// id are recorded, the audit actor is the policy, and the case becomes
// RESOLVED (terminal). Never agent-initiated.
func (c *Case) AutoResolve(ruleID, correlatedType, correlatedEventID string, at time.Time) error {
	if err := c.commandable(); err != nil {
		return err
	}
	if !shared.ValidText(ruleID) {
		return ErrBlankRuleID
	}
	if !shared.ValidText(correlatedType) || !shared.ValidText(correlatedEventID) {
		return ErrBlankCorrelation
	}
	if at.IsZero() {
		return ErrNoTimestamp
	}
	utc := at.UTC()
	actor := shared.Actor{Kind: shared.ActorPolicy, Name: ruleID}
	c.status = StatusResolved
	c.resolution = resolution{
		code:      "AUTO_RESOLVED",
		rootCause: correlatedType,
		at:        utc,
		actor:     actor,
		auto:      &autoResolution{ruleID: ruleID, correlatedType: correlatedType, correlatedEventID: correlatedEventID},
	}
	c.version++
	c.record(ActionAutoResolved, actor, utc)
	c.raise(ExceptionAutoResolved{
		Header:            c.header(at),
		RuleID:            ruleID,
		CorrelatedType:    correlatedType,
		CorrelatedEventID: correlatedEventID,
	})
	return nil
}

// commandable rejects every command on a terminal case.
func (c *Case) commandable() error {
	if c.status == StatusResolved {
		return ErrCaseClosed
	}
	return nil
}

// validCommand checks what every operator command needs: a valid actor and
// a timestamp.
func validCommand(actor shared.Actor, at time.Time) error {
	if !actor.Valid() {
		return ErrInvalidActor
	}
	if at.IsZero() {
		return ErrNoTimestamp
	}
	return nil
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
