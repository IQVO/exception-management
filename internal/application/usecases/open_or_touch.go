package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// ErrPolicyUndeclared is returned when a consumed source type has no declared
// policy. The consumer logs it (WARN) and commits past — an undeclared source
// is a configuration gap, never a partition blocker (fleet consumer rule).
var ErrPolicyUndeclared = errors.New("usecases: no policy declared for the source type")

// Fact is one consumed source fact: the CloudEvents envelope fields the case
// freezes at open (ADR 0003).
type Fact struct {
	SourceType    string
	SourceSubject string
	SiteID        string
	SourceEventID string
	Payload       []byte
}

// OpenOrTouchException is the consumer-driven entry point: the dedup decision
// of ADR 0002. A fact whose key has no case OPENS one (severity, queue and SLA
// from the current policy version); a fact whose key has a case TOUCHES it
// (occurrence+1, lastSeenAt), whatever its status — a resolved case stays
// resolved. A replay of the OPENING event id is a no-op returning the case.
type OpenOrTouchException struct {
	Cases      ports.ExceptionCaseRepo
	Policies   ports.ExceptionPolicyRepo
	Events     ports.EventPublisher
	IDs        ports.IDGen
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork // nil = unwrapped
}

// Execute applies the fact and returns the case it opened or touched.
func (uc *OpenOrTouchException) Execute(ctx context.Context, fact Fact) (*exceptioncase.Case, error) {
	key, err := shared.NewDedupKey(fact.SourceType, fact.SourceSubject, fact.SiteID)
	if err != nil {
		return nil, err
	}
	policy, err := uc.Policies.Get(ctx, fact.SourceType)
	if err != nil {
		if errors.Is(err, ports.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrPolicyUndeclared, fact.SourceType)
		}
		return nil, err
	}
	now := uc.Clock.Now()
	in := exceptioncase.OpenInput{
		CaseID:        uc.IDs.NewCaseID(),
		Key:           key,
		Severity:      policy.Severity(),
		Queue:         policy.Queue(),
		SLAMinutes:    policy.SLAMinutes(),
		SourceEventID: fact.SourceEventID,
		Payload:       fact.Payload,
		At:            now,
	}
	latest, err := uc.Cases.FindOpenByDedupKey(ctx, key)
	if err != nil && !errors.Is(err, ports.ErrNotFound) {
		return nil, err
	}
	var c *exceptioncase.Case
	if latest == nil {
		c, err = exceptioncase.Open(in)
	} else {
		if latest.SourceEventID() == fact.SourceEventID {
			return latest, nil // replay of the opening event: idempotent no-op
		}
		c, err = exceptioncase.OpenOrTouch(latest, in)
	}
	if err != nil {
		return nil, err
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Cases.Save(ctx, c); err != nil {
			return err
		}
		return uc.Events.PublishCaseEvents(ctx, c.DomainEvents()...)
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

// atomically runs fn inside uow when one is wired, unwrapped otherwise
// (memory wiring, tests). A nil UnitOfWork is tolerated by contract.
func atomically(ctx context.Context, uow ports.UnitOfWork, fn func(ctx context.Context) error) error {
	if uow == nil {
		return fn(ctx)
	}
	return uow.Do(ctx, fn)
}
