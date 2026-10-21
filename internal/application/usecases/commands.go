package usecases

import (
	"context"
	"errors"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// TriageException sets or changes a case's severity and queue (an operator
// command; actor is the operator id on the wire).
type TriageException struct {
	Cases      ports.ExceptionCaseRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// Execute triages the case. ErrCaseClosed maps to 409 at the HTTP edge.
func (uc *TriageException) Execute(ctx context.Context, caseID string, severity shared.Severity, queue, actorName string) (*exceptioncase.Case, error) {
	actor, err := shared.Operator(actorName)
	if err != nil {
		return nil, err
	}
	c, err := uc.Cases.FindByID(ctx, caseID)
	if err != nil {
		return nil, err
	}
	if err := c.Triage(severity, queue, actor, uc.Clock.Now()); err != nil {
		return nil, err
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Cases.Save(ctx, c); err != nil {
			return err
		}
		return uc.Events.PublishCaseEvents(ctx, c.DomainEvents()...)
	}); err != nil {
		return nil, err
	}
	return c, nil
}

// AssignException gives a case an owner (first assignment or reassignment).
type AssignException struct {
	Cases      ports.ExceptionCaseRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// Execute assigns the case to assignee.
func (uc *AssignException) Execute(ctx context.Context, caseID, assignee, actorName string) (*exceptioncase.Case, error) {
	actor, err := shared.Operator(actorName)
	if err != nil {
		return nil, err
	}
	c, err := uc.Cases.FindByID(ctx, caseID)
	if err != nil {
		return nil, err
	}
	if err := c.Assign(assignee, actor, uc.Clock.Now()); err != nil {
		return nil, err
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Cases.Save(ctx, c); err != nil {
			return err
		}
		return uc.Events.PublishCaseEvents(ctx, c.DomainEvents()...)
	}); err != nil {
		return nil, err
	}
	return c, nil
}

// ResolveException closes a case with a mandatory resolution code and root
// cause. RESOLVED is terminal: a second call fails with ErrCaseClosed.
type ResolveException struct {
	Cases      ports.ExceptionCaseRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// Execute resolves the case.
func (uc *ResolveException) Execute(ctx context.Context, caseID, code, rootCause, note, actorName string) (*exceptioncase.Case, error) {
	actor, err := shared.Operator(actorName)
	if err != nil {
		return nil, err
	}
	c, err := uc.Cases.FindByID(ctx, caseID)
	if err != nil {
		return nil, err
	}
	if err := c.Resolve(code, rootCause, note, actor, uc.Clock.Now()); err != nil {
		return nil, err
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Cases.Save(ctx, c); err != nil {
			return err
		}
		return uc.Events.PublishCaseEvents(ctx, c.DomainEvents()...)
	}); err != nil {
		return nil, err
	}
	return c, nil
}

// ErrNoCaseOpen is the fail-soft outcome of a correlation with no candidate.
var ErrNoCaseOpen = errors.New("usecases: no open case matched the correlated fact")
