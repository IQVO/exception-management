package usecases

import (
	"context"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
)

// CorrelatedFact is the arriving fact an auto-resolve rule may react to (its
// full CloudEvents type, the order id it is about — its subject — and its
// event id). v1 ships exactly one producer: order-management OrderRepromised.
type CorrelatedFact struct {
	CorrelatedType    string
	CorrelatedOrderID string
	CorrelatedEventID string
}

// AutoResolveOnCorrelation applies the pinned FINAL semantics (00-common
// addendum): ORDER-LEVEL correlation (the case's source subject reversed
// through the work-unit formula; the line number is ignored), inclusive
// window, fail-soft — no matching open case is a no-op, never an error.
// Never agent-initiated: the actor recorded on the case is the rule's policy.
type AutoResolveOnCorrelation struct {
	Cases      ports.ExceptionCaseRepo
	Policies   ports.ExceptionPolicyRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// Execute scans the open caseload once and auto-resolves every case whose
// policy rule matches the fact. It returns how many cases closed (0 is the
// normal fail-soft outcome).
func (uc *AutoResolveOnCorrelation) Execute(ctx context.Context, fact CorrelatedFact) (int, error) {
	open, err := uc.Cases.ListOpen(ctx, "", "", "")
	if err != nil {
		return 0, err
	}
	now := uc.Clock.Now()
	closed := 0
	for _, c := range open {
		key := c.Key()
		policy, err := uc.Policies.Get(ctx, key.SourceType)
		if err != nil {
			continue // undeclared policy for this source: skip, keep scanning
		}
		rule := policy.AutoResolve()
		if rule == nil {
			continue
		}
		if !rule.Matches(exceptionpolicy.Match{
			CorrelatedType:    fact.CorrelatedType,
			CorrelatedOrderID: fact.CorrelatedOrderID,
			CaseSourceRef:     key.SourceSubject,
			LastOccurrenceAt:  c.LastSeenAt(),
			Now:               now,
		}) {
			continue
		}
		if err := c.AutoResolve(rule.ID(policy.SourceType()), fact.CorrelatedType, fact.CorrelatedEventID, now); err != nil {
			continue // fail-soft per case: a refused auto-resolve never fails the fact
		}
		if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
			if err := uc.Cases.Save(ctx, c); err != nil {
				return err
			}
			return uc.Events.PublishCaseEvents(ctx, c.DomainEvents()...)
		}); err != nil {
			return closed, err
		}
		closed++
	}
	return closed, nil
}
