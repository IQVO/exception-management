package exceptionpolicy

import "time"

// PolicyDeclared is the fact behind the ExceptionPolicyDeclared event of
// ADR 0004: a new append-only version of the handling policy of a source
// type. It is a plain domain fact; the outbound adapter maps it to the wire.
type PolicyDeclared struct {
	sourceType string
	version    int64
	at         time.Time
}

// Declaration is the fact this policy version raises.
func (p *Policy) Declaration() PolicyDeclared {
	return PolicyDeclared{sourceType: p.sourceType, version: p.version, at: p.declaredAt}
}

// EventName returns "ExceptionPolicyDeclared".
func (PolicyDeclared) EventName() string { return "ExceptionPolicyDeclared" }

// SourceType returns the governed source type (the event's subject/key).
func (e PolicyDeclared) SourceType() string { return e.sourceType }

// Version returns the declared version.
func (e PolicyDeclared) Version() int64 { return e.version }

// OccurredAt returns the declaration instant.
func (e PolicyDeclared) OccurredAt() time.Time { return e.at }
