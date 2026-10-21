package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/exception-management/internal/application/ports"
)

// ErrNoEventID marks a consumed fact without a CloudEvents id: deterministic,
// never retried (the consumer logs it at WARN and commits past).
var ErrNoEventID = errors.New("usecases: a consumed fact needs its CloudEvents id")

// Outcome reports what a consumer-driven use case did with one fact.
type Outcome string

// The outcomes.
const (
	// OutcomeApplied: the fact opened or touched a case, or the correlated
	// fact closed at least one case.
	OutcomeApplied Outcome = "applied"
	// OutcomeDuplicate: this CloudEvents id was already processed (the
	// claim of the same tx); nothing changed.
	OutcomeDuplicate Outcome = "duplicate"
	// OutcomeNoMatch: the correlated fact matched no open case (the normal
	// fail-soft outcome of the correlation).
	OutcomeNoMatch Outcome = "no-match"
)

// ConsumeFact is the inbound consumers' entry point for exception-class
// facts: the fleet dedupe rule made concrete. The CloudEvents id is CLAIMED
// (ports.ProcessedEvents) and the open-or-touch effect runs in ONE
// UnitOfWork, so a rollback un-claims the id and a redelivery is processed,
// not skipped — and a committed claim means a redelivered offset can never
// double-count an occurrence. OpenOrTouchException stays the pure decision;
// this wrapper owns the claim.
type ConsumeFact struct {
	Processed   ports.ProcessedEvents
	OpenOrTouch *OpenOrTouchException
	UnitOfWork  ports.UnitOfWork
}

// Execute claims the fact's event id and applies open-or-touch, returning
// the outcome for the consumer's log line.
func (uc *ConsumeFact) Execute(ctx context.Context, fact Fact) (Outcome, error) {
	if fact.SourceEventID == "" {
		return "", fmt.Errorf("%w", ErrNoEventID)
	}
	outcome := OutcomeApplied
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		claimed, err := uc.Processed.Claim(ctx, fact.SourceEventID)
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			outcome = OutcomeDuplicate
			return nil
		}
		// Joins the outer unit of work (ports.UnitOfWork is re-entrant),
		// so claim and effect commit or roll back together.
		_, err = uc.OpenOrTouch.Execute(ctx, fact)
		return err
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// ConsumeCorrelation is the order-correlation consumer's entry point: it
// claims the correlated fact's event id and runs the auto-resolve scan in
// ONE UnitOfWork. A rollback un-claims the id, so a half-applied scan is
// retried whole on redelivery; a fail-soft no-match still commits the claim
// (the fact was handled — there was nothing to close).
type ConsumeCorrelation struct {
	Processed   ports.ProcessedEvents
	AutoResolve *AutoResolveOnCorrelation
	UnitOfWork  ports.UnitOfWork
}

// Execute claims the correlated fact's id and runs the scan, returning how
// many cases closed (0 with OutcomeNoMatch is the normal fail-soft result).
func (uc *ConsumeCorrelation) Execute(ctx context.Context, fact CorrelatedFact) (int, Outcome, error) {
	if fact.CorrelatedEventID == "" {
		return 0, "", fmt.Errorf("%w", ErrNoEventID)
	}
	closed, outcome := 0, OutcomeNoMatch
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		claimed, err := uc.Processed.Claim(ctx, fact.CorrelatedEventID)
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			outcome = OutcomeDuplicate
			return nil
		}
		closed, err = uc.AutoResolve.Execute(ctx, fact)
		if err == nil && closed > 0 {
			outcome = OutcomeApplied
		}
		return err
	})
	if err != nil {
		return 0, "", err
	}
	return closed, outcome, nil
}
