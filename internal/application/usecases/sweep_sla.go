package usecases

import (
	"context"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
)

// SweepSLABreaches is the bounded SLA sweeper (the pattern of
// fulfillment-execution's SweepCPTMisses): it REPORTS facts — sets the
// ESCALATED flag on breached open cases and raises ExceptionEscalated exactly
// once per case — and never mutates anything else, never blocks work. The
// ticker that calls it lives in cmd/api, not here.
type SweepSLABreaches struct {
	Cases      ports.ExceptionCaseRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// Execute sweeps at the supplied instant and returns the cases newly flagged.
func (uc *SweepSLABreaches) Execute(ctx context.Context) ([]*exceptioncase.Case, error) {
	open, err := uc.Cases.ListOpen(ctx, "", "", "")
	if err != nil {
		return nil, err
	}
	now := uc.Clock.Now()
	var flagged []*exceptioncase.Case
	for _, c := range open {
		if !c.BreachesAt(now) {
			continue
		}
		escalated, err := c.Escalate(now)
		if err != nil || !escalated {
			continue
		}
		if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
			if err := uc.Cases.Save(ctx, c); err != nil {
				return err
			}
			return uc.Events.PublishCaseEvents(ctx, c.DomainEvents()...)
		}); err != nil {
			return flagged, err
		}
		flagged = append(flagged, c)
	}
	return flagged, nil
}
