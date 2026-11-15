package usecases

import (
	"context"
	"errors"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// openOutcome is what the open-or-touch decision did with one fact.
type openOutcome int

const (
	// openedCase: no case owned the key; a new one was opened.
	openedCase openOutcome = iota
	// touchedCase: a case owned the key; its occurrence went up.
	touchedCase
	// replayedCase: the fact is the replay of the owner's opening event.
	replayedCase
)

// The problem slugs a refused fact is counted under. They reuse the slugs of
// the REST problem catalogue where one exists (concurrent-modification,
// internal-error); the rest are the consumer path's own.
const (
	reasonPolicyUndeclared       = "policy-undeclared"
	reasonInvalidFact            = "invalid-fact"
	reasonConcurrentModification = "concurrent-modification"
	reasonOpenCaseExists         = "open-case-exists"
	reasonInternalError          = "internal-error"
)

// rejectionReasons maps an OpenOrTouchException failure to its problem slug,
// in match order. Anything else is internal-error.
var rejectionReasons = []struct {
	err    error
	reason string
}{
	{ErrPolicyUndeclared, reasonPolicyUndeclared},
	{ErrConcurrentModification, reasonConcurrentModification},
	{ErrOpenCaseExists, reasonOpenCaseExists},
	{shared.ErrBlankSourceType, reasonInvalidFact},
	{shared.ErrBlankSourceSubject, reasonInvalidFact},
	{shared.ErrInvalidSite, reasonInvalidFact},
	{exceptioncase.ErrBlankSourceEventID, reasonInvalidFact},
	{exceptioncase.ErrEmptySourcePayload, reasonInvalidFact},
}

func rejectionReason(err error) string {
	for _, r := range rejectionReasons {
		if errors.Is(err, r.err) {
			return r.reason
		}
	}
	return reasonInternalError
}

// metrics returns the configured CaseMetrics or a no-op.
func (uc *OpenOrTouchException) metrics() ports.CaseMetrics {
	if uc.Metrics != nil {
		return uc.Metrics
	}
	return noCaseMetrics{}
}

// record counts one Execute: the decision when err is nil, else a rejection
// with the reason of err.
func (uc *OpenOrTouchException) record(ctx context.Context, c *exceptioncase.Case, outcome openOutcome, err error) {
	m := uc.metrics()
	if err != nil {
		m.CaseRejected(ctx, rejectionReason(err))
		return
	}
	severity := string(c.Severity())
	switch outcome {
	case openedCase:
		m.CaseOpened(ctx, severity)
	case touchedCase:
		m.CaseTouched(ctx, severity)
	case replayedCase:
		m.CaseReplayed(ctx, severity)
	}
}

// noCaseMetrics is the CaseMetrics of a use case built without one.
type noCaseMetrics struct{}

func (noCaseMetrics) CaseOpened(context.Context, string)   {}
func (noCaseMetrics) CaseTouched(context.Context, string)  {}
func (noCaseMetrics) CaseReplayed(context.Context, string) {}
func (noCaseMetrics) CaseRejected(context.Context, string) {}
