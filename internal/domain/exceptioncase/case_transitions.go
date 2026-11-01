package exceptioncase

import (
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

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
