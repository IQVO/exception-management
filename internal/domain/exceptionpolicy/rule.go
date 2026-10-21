// Package exceptionpolicy holds the ExceptionPolicy aggregate (ADR 0002):
// the per-source-type handling policy (severity, queue, SLA minutes) and its
// declarative auto-resolve rule. Policies are versioned and append-only; the
// rule is DATA, never executable code — an operator declares it through the
// API, every version is auditable, and the ops-agent may read it but never
// fire it. Pure Go: no I/O, no clock reads, imports nothing outside
// internal/domain.
package exceptionpolicy

import (
	"errors"
	"strings"
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// Errors of the policy aggregate. A declaration that fails validation is
// rejected whole; no partial policy ever exists.
var (
	// ErrInvalidSourceType is a source type that is not a full CloudEvents
	// type (com.warehouse.<subdomain>.<context>.<entity>.<Event>).
	ErrInvalidSourceType = errors.New("exceptionpolicy: source type must be a full CloudEvents type")
	// ErrInvalidSeverity is an unknown severity.
	ErrInvalidSeverity = errors.New("exceptionpolicy: severity must be LOW, MEDIUM, HIGH or CRITICAL")
	// ErrBlankQueue is a blank queue.
	ErrBlankQueue = errors.New("exceptionpolicy: queue must not be blank")
	// ErrInvalidSLA is an SLA below one minute.
	ErrInvalidSLA = errors.New("exceptionpolicy: sla_minutes must be at least 1")
	// ErrNoTimestamp is a missing declaration time.
	ErrNoTimestamp = errors.New("exceptionpolicy: a declaration needs a timestamp")
	// ErrInvalidVersion is a version below 1.
	ErrInvalidVersion = errors.New("exceptionpolicy: version must be at least 1")
)

// cloudEventTypePrefix is the fleet's type prefix; a governed source type
// always starts with it.
const cloudEventTypePrefix = "com.warehouse."

// Rule is the declarative auto-resolve rule: "close the case when a fact of
// OnType arrives whose subject matches the case's source subject through the
// work-unit formula, within WithinMinutes of the case's last occurrence".
// It is data: the matcher below is the only interpretation, and nothing
// about a rule is pluggable code.
type Rule struct {
	onType        string
	withinMinutes int64
}

// NewRule validates and wraps a rule. OnType must be a full CloudEvents
// type; WithinMinutes must be at least 1.
func NewRule(onType string, withinMinutes int64) (Rule, error) {
	if !isFullType(onType) {
		return Rule{}, ErrInvalidSourceType
	}
	if withinMinutes < 1 {
		return Rule{}, ErrInvalidSLA
	}
	return Rule{onType: onType, withinMinutes: withinMinutes}, nil
}

// OnType returns the full CloudEvents type of the correlated fact.
func (r Rule) OnType() string { return r.onType }

// WithinMinutes returns the correlation window in minutes.
func (r Rule) WithinMinutes() int64 { return r.withinMinutes }

// ID is the rule's derived identity: the governed source type and the
// correlated type. It is deterministic (no operator-chosen ids exist in the
// declared payload) and is what ExceptionAutoResolved.rule_id carries.
func (r Rule) ID(sourceType string) string {
	return sourceType + "->" + r.onType
}

// Match is the evidence a correlated fact presents to a rule.
type Match struct {
	// CorrelatedType is the FULL CloudEvents type of the correlated fact
	// (e.g. com.warehouse.wes.order-management.order.OrderRepromised).
	CorrelatedType string
	// CorrelatedOrderID is the correlated fact's order id (its subject).
	CorrelatedOrderID string
	// CaseSourceRef is the case's frozen source work-unit reference
	// (`order_ref` of the source fact, `{order_id}-line-{line_no}`).
	CaseSourceRef string
	// LastOccurrenceAt is the case's lastSeenAt when the fact arrived.
	LastOccurrenceAt time.Time
	// Now is the arrival instant, supplied by the caller (pure matcher).
	Now time.Time
}

// Matches reports whether the correlated fact satisfies the rule: the same
// type, the same subject through the work-unit formula, and within the
// window of the case's last occurrence. Anything else is a NO-OP (the case
// stays open) — the matcher fails soft, it never errors (ADR 0002).
func (r Rule) Matches(m Match) bool {
	if m.CorrelatedType != r.onType {
		return false
	}
	ref, ok := shared.ParseWorkUnitRef(m.CaseSourceRef)
	if !ok || ref.OrderID != m.CorrelatedOrderID {
		return false
	}
	deadline := m.LastOccurrenceAt.Add(time.Duration(r.withinMinutes) * time.Minute)
	return !m.Now.After(deadline)
}

// isFullType reports whether s looks like a full CloudEvents type:
// com.warehouse.<something>.<something>.<Event>, the last segment PascalCase.
func isFullType(s string) bool {
	if !strings.HasPrefix(s, cloudEventTypePrefix) {
		return false
	}
	rest := s[len(cloudEventTypePrefix):]
	dots := strings.Count(rest, ".")
	if dots < 2 {
		return false
	}
	last := rest[strings.LastIndex(rest, ".")+1:]
	if last == "" {
		return false
	}
	c := last[0]
	return c >= 'A' && c <= 'Z'
}
