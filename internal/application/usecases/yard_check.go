package usecases

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// The yard-management and dock-scheduling types this service acts on (ADR
// 0010), byte-identical to the producers' contracts. Each is also the
// policy source type of its cases: routing is data (migration 0004), not
// code.
const (
	TypeVisitRejected         = "com.warehouse.wms.yard-management.yardvisit.VisitRejected"
	TypeSealExceptionRaised   = "com.warehouse.wms.yard-management.trailer.SealExceptionRaised"
	TypeTrailerDwellExceeded  = "com.warehouse.wms.yard-management.trailer.TrailerDwellExceeded"
	TypeYardCheckClosed       = "com.warehouse.wms.yard-management.yardcheck.YardCheckClosed"
	TypeDockAppointmentNoShow = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentNoShow"
)

// TrailerOccurrenceSubject is the dedup subject of a trailer-level yard
// exception (SealExceptionRaised, TrailerDwellExceeded): the trailer and
// the instant of the raise. A trailer can raise again on a later visit and
// a resolved case never reopens (ADR 0002), so the instant is part of the
// key; the same raise re-published carries the same instant and touches.
func TrailerOccurrenceSubject(trailerID, instant string) string {
	return trailerID + "/" + instant
}

// YardCheckDiscrepancy is one discrepancy of a closed yard check. Kind is
// MISSING, UNEXPECTED or WRONG_LOCATION today but is accepted as ANY
// non-blank string: a kind the producer adds later is never dropped.
type YardCheckDiscrepancy struct {
	Kind         string  `json:"kind"`
	LocationCode string  `json:"location_code"`
	TrailerID    *string `json:"trailer_id"`
	UnitNumber   *string `json:"unit_number"`
}

// YardCheckClosure is one consumed YardCheckClosed fact.
type YardCheckClosure struct {
	EventID       string
	YardCheckID   string
	SiteID        string
	ClosedAt      string
	Discrepancies []YardCheckDiscrepancy
}

// YardCheckSubject is the dedup subject of one discrepancy: the check, its
// kind, the location and the trailer (trailer id, else unit number, else
// "-"). Two discrepancies of one check never share a key unless they are
// the same finding listed twice.
func YardCheckSubject(yardCheckID string, d YardCheckDiscrepancy) string {
	who := "-"
	switch {
	case d.TrailerID != nil && strings.TrimSpace(*d.TrailerID) != "":
		who = *d.TrailerID
	case d.UnitNumber != nil && strings.TrimSpace(*d.UnitNumber) != "":
		who = *d.UnitNumber
	}
	return yardCheckID + "/" + d.Kind + "/" + d.LocationCode + "/" + who
}

// OpenYardCheckDiscrepancies opens (or touches) ONE case per discrepancy of
// a closed yard check. The CloudEvents id is claimed ONCE and every case
// opens in the same unit of work, so a rollback un-claims the id and a
// redelivery re-applies the whole check — never half of it. ConsumeFact
// cannot serve here: it claims per fact, so the second discrepancy of the
// same event would read as a duplicate.
type OpenYardCheckDiscrepancies struct {
	Processed   ports.ProcessedEvents
	OpenOrTouch *OpenOrTouchException
	UnitOfWork  ports.UnitOfWork
}

// Execute applies the closure and returns how many discrepancies it carried
// and the outcome for the consumer's log line. A closure without
// discrepancies is a clean check: the id is claimed and nothing opens.
func (uc *OpenYardCheckDiscrepancies) Execute(ctx context.Context, c YardCheckClosure) (int, Outcome, error) {
	if c.EventID == "" {
		return 0, "", fmt.Errorf("%w", ErrNoEventID)
	}
	if !shared.ValidText(c.YardCheckID) {
		return 0, "", fmt.Errorf("yard check id: %w", shared.ErrBlankSourceSubject)
	}
	facts := make([]Fact, 0, len(c.Discrepancies))
	for _, d := range c.Discrepancies {
		if !shared.ValidText(d.Kind) || !shared.ValidText(d.LocationCode) {
			return 0, "", fmt.Errorf("yard check discrepancy kind/location: %w", shared.ErrBlankSourceSubject)
		}
		payload, err := json.Marshal(struct {
			YardCheckID string               `json:"yard_check_id"`
			SiteID      string               `json:"site_id"`
			ClosedAt    string               `json:"closed_at"`
			Discrepancy YardCheckDiscrepancy `json:"discrepancy"`
		}{c.YardCheckID, c.SiteID, c.ClosedAt, d})
		if err != nil {
			return 0, "", fmt.Errorf("freeze yard check discrepancy: %w", err)
		}
		facts = append(facts, Fact{
			SourceType:    TypeYardCheckClosed,
			SourceSubject: YardCheckSubject(c.YardCheckID, d),
			SiteID:        c.SiteID,
			SourceEventID: c.EventID,
			Payload:       payload,
		})
	}
	outcome := OutcomeApplied
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		claimed, err := uc.Processed.Claim(ctx, c.EventID)
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			outcome = OutcomeDuplicate
			return nil
		}
		for _, f := range facts {
			if _, err := uc.OpenOrTouch.Execute(ctx, f); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, "", err
	}
	return len(facts), outcome, nil
}
