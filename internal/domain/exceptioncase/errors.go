package exceptioncase

import "errors"

// Errors of the ExceptionCase aggregate. ErrCaseClosed is the 409-class
// error of ADR 0002: RESOLVED is terminal and every later command on the
// case fails with it.
var (
	// ErrBlankCaseID is a blank or NUL-bearing case id.
	ErrBlankCaseID = errors.New("exceptioncase: case id must not be blank or contain NUL")
	// ErrInvalidDedupKey is an invalid dedup key.
	ErrInvalidDedupKey = errors.New("exceptioncase: the dedup key must be a valid (source type, source subject, site)")
	// ErrInvalidSeverity is an unknown severity.
	ErrInvalidSeverity = errors.New("exceptioncase: severity must be LOW, MEDIUM, HIGH or CRITICAL")
	// ErrBlankQueue is a blank queue.
	ErrBlankQueue = errors.New("exceptioncase: queue must not be blank")
	// ErrInvalidSLA is an SLA below one minute.
	ErrInvalidSLA = errors.New("exceptioncase: sla_minutes must be at least 1")
	// ErrBlankSourceEventID is a blank source CloudEvents id.
	ErrBlankSourceEventID = errors.New("exceptioncase: source event id must not be blank")
	// ErrEmptySourcePayload is a missing source payload.
	ErrEmptySourcePayload = errors.New("exceptioncase: the source payload must not be empty")
	// ErrNoTimestamp is a missing transition time.
	ErrNoTimestamp = errors.New("exceptioncase: a transition needs a timestamp")
	// ErrInvalidActor is an invalid actor.
	ErrInvalidActor = errors.New("exceptioncase: every transition needs a valid actor")
	// ErrCaseClosed is any command on a RESOLVED case (terminal, 409-class).
	ErrCaseClosed = errors.New("exceptioncase: a resolved case accepts no further command")
	// ErrBlankAssignee is a blank assignee.
	ErrBlankAssignee = errors.New("exceptioncase: assignee must not be blank")
	// ErrBlankResolutionCode is a blank resolution code.
	ErrBlankResolutionCode = errors.New("exceptioncase: a resolution needs a resolution code")
	// ErrBlankRootCause is a blank root cause.
	ErrBlankRootCause = errors.New("exceptioncase: a resolution needs a root cause")
	// ErrNotBreached is an escalation attempt before the SLA deadline.
	ErrNotBreached = errors.New("exceptioncase: the SLA clock has not breached yet")
	// ErrBlankRuleID is a blank auto-resolve rule id.
	ErrBlankRuleID = errors.New("exceptioncase: an auto-resolution needs its rule id")
	// ErrBlankCorrelation is a blank correlated type or event id.
	ErrBlankCorrelation = errors.New("exceptioncase: an auto-resolution needs the correlated type and event id")
	// ErrKeyMismatch is an OpenOrTouch fact whose key differs from the case's.
	ErrKeyMismatch = errors.New("exceptioncase: the fact's dedup key does not match the case")
	// ErrInvalidSnapshot is a persisted snapshot that violates an invariant.
	ErrInvalidSnapshot = errors.New("exceptioncase: the snapshot violates an invariant")
)
