package shared

// Severity is the triage outcome of a case, set by the policy default at
// open or by an operator at triage (ADR 0002). The policy package and the
// exceptioncase package share this value object, so it lives here.
type Severity string

// The four severities, lowest to highest.
const (
	SeverityLow      Severity = "LOW"
	SeverityMedium   Severity = "MEDIUM"
	SeverityHigh     Severity = "HIGH"
	SeverityCritical Severity = "CRITICAL"
)

// Valid reports whether s is one of the four severities.
func (s Severity) Valid() bool {
	switch s {
	case SeverityLow, SeverityMedium, SeverityHigh, SeverityCritical:
		return true
	default:
		return false
	}
}
