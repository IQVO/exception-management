package usecases

import (
	"context"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// PolicyDeclaration is the operator input for a first declaration or a
// redeclaration (append-only versions; the actor is the operator id).
type PolicyDeclaration struct {
	SourceType    string
	Severity      shared.Severity
	Queue         string
	SLAMinutes    int64
	AutoResolveOn string // "" = no rule
	WithinMinutes int64  // ignored when AutoResolveOn is ""
	Actor         string
}

// DeclareExceptionPolicy declares or redeclares the policy of one source type
// and publishes the declaration fact (analytics-only event of ADR 0004).
type DeclareExceptionPolicy struct {
	Policies   ports.ExceptionPolicyRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// Execute returns the persisted policy version.
func (uc *DeclareExceptionPolicy) Execute(ctx context.Context, in PolicyDeclaration) (*exceptionpolicy.Policy, error) {
	actor, err := shared.Operator(in.Actor)
	if err != nil {
		return nil, err
	}
	_ = actor // the audit actor of a declaration is the operator; the fact carries the operator name
	declareIn := exceptionpolicy.DeclareInput{
		SourceType: in.SourceType,
		Severity:   in.Severity,
		Queue:      in.Queue,
		SLAMinutes: in.SLAMinutes,
		At:         uc.Clock.Now(),
	}
	if in.AutoResolveOn != "" {
		rule, err := exceptionpolicy.NewRule(in.AutoResolveOn, in.WithinMinutes)
		if err != nil {
			return nil, err
		}
		declareIn.AutoResolve = &rule
	}
	var p *exceptionpolicy.Policy
	existing, err := uc.Policies.Get(ctx, in.SourceType)
	if err == nil {
		p, err = existing.Redeclare(declareIn)
	} else {
		p, err = exceptionpolicy.Declare(declareIn)
	}
	if err != nil {
		return nil, err
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Policies.Save(ctx, p); err != nil {
			return err
		}
		return uc.Events.PublishPolicyDeclared(ctx, p.Declaration())
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}
