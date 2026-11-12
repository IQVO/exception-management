package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// The inventory-storage held-stock types this service acts on (ADR 0012),
// byte-identical to the producer's AsyncAPI (its ADR 0052). StockHeld is
// the CASE SOURCE type — the dedup key of a held-stock case is
// (StockHeld, held unit id, "") — and the other two are the closing facts.
const (
	TypeStockHeld          = "com.warehouse.wms.inventory-storage.stock.StockHeld"
	TypeStockReleased      = "com.warehouse.wms.inventory-storage.stock.StockReleased"
	TypeStockDispositioned = "com.warehouse.wms.inventory-storage.stock.StockDispositioned"
)

// The dispositions of the pinned contract (inventory-storage ADR 0052):
// REWORK is the only one that KEEPS the unit held; RELEASE,
// RETURN_TO_VENDOR and SCRAP end the hold. A value outside this set is a
// contract change this consumer has not been reviewed against.
const (
	dispositionRework      = "REWORK"
	dispositionsEndTheHold = "RELEASE, RETURN_TO_VENDOR or SCRAP"
)

// ErrUnknownDisposition marks a StockDispositioned whose disposition is
// not in the pinned contract: deterministic — the consumer logs it at WARN
// and commits past; it is never retried and never dead-lettered (the fact
// is well-formed; the contract moved).
var ErrUnknownDisposition = errors.New("usecases: a disposition outside the pinned held-stock contract")

// HeldStockClosure is one consumed closing fact about a held unit: a
// StockDispositioned (Disposition set) or a StockReleased (Disposition
// empty). ClosingType is the fact's FULL CloudEvents type.
type HeldStockClosure struct {
	EventID     string
	HeldUnitID  string
	ClosingType string
	Disposition string // "" for StockReleased
}

// note is the resolution note the close records (ADR 0012 point 5).
func (c HeldStockClosure) note() string {
	if c.Disposition == "" {
		return "stock released"
	}
	return "disposition " + c.Disposition
}

// endsTheHold reports whether the closure's disposition ends the hold
// (every pinned disposition except REWORK, which keeps the unit HELD).
func (c HeldStockClosure) endsTheHold() bool {
	switch c.Disposition {
	case "", "RELEASE", "RETURN_TO_VENDOR", "SCRAP":
		return true
	case dispositionRework:
		return false
	default:
		return false
	}
}

// AutoResolveHeldStock closes (or, on REWORK, touches) the case of one
// held unit when the hold's end arrives (ADR 0012): a disposition that
// ends the hold auto-resolves the case — the follow-up work the case
// exists for is done; REWORK keeps the unit HELD, so the case stays open
// and the arrival is recorded as an occurrence. The CloudEvents id is
// claimed in the SAME UnitOfWork as the effect. The rule id is derived in
// the established `<source type>-><closing type>` shape; no policy rule
// is consulted — the close is definitional, the ADR says so.
type AutoResolveHeldStock struct {
	Processed  ports.ProcessedEvents
	Cases      ports.ExceptionCaseRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// Execute applies the closure and returns the outcome for the consumer's
// log line: no-match (an unknown held unit — a pre-cutoff hold; WARN and
// commit past), duplicate (a replayed id, or a case already closed by the
// fact's pair), or applied (closed or touched).
func (uc *AutoResolveHeldStock) Execute(ctx context.Context, c HeldStockClosure) (Outcome, error) {
	if err := validateClosure(c); err != nil {
		return "", err
	}
	key, err := shared.NewDedupKey(TypeStockHeld, c.HeldUnitID, "")
	if err != nil {
		return "", err
	}
	outcome := OutcomeApplied
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		outcome, err = uc.claimAndApply(ctx, c, key)
		return err
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// validateClosure checks what every closure needs before anything is
// claimed: an event id, a held unit id, a closing type and a disposition
// of the pinned contract.
func validateClosure(c HeldStockClosure) error {
	if c.EventID == "" {
		return fmt.Errorf("%w", ErrNoEventID)
	}
	if !shared.ValidText(c.HeldUnitID) {
		return fmt.Errorf("held unit id: %w", shared.ErrBlankSourceSubject)
	}
	if !shared.ValidText(c.ClosingType) {
		return fmt.Errorf("closing type: %w", shared.ErrBlankSourceType)
	}
	switch c.Disposition {
	case "", "RELEASE", "RETURN_TO_VENDOR", "SCRAP", dispositionRework:
		return nil
	default:
		return fmt.Errorf("%w: %s (pinned: %s, or %s which keeps the hold)", ErrUnknownDisposition, c.Disposition, dispositionsEndTheHold, dispositionRework)
	}
}

// claimAndApply is the unit-of-work body: claim the fact's id, then close,
// touch or fail soft on the held unit's case.
func (uc *AutoResolveHeldStock) claimAndApply(ctx context.Context, c HeldStockClosure, key shared.DedupKey) (Outcome, error) {
	claimed, err := uc.Processed.Claim(ctx, c.EventID)
	if err != nil {
		return "", fmt.Errorf("claim processed event: %w", err)
	}
	if !claimed {
		return OutcomeDuplicate, nil
	}
	latest, err := uc.Cases.FindOpenByDedupKey(ctx, key)
	if errors.Is(err, ports.ErrNotFound) {
		// A well-formed fact about a hold this service never tracked
		// (pre-cutoff): fail-soft, the claim still commits.
		return OutcomeNoMatch, nil
	}
	if err != nil {
		return "", err
	}
	if latest.Status() == exceptioncase.StatusResolved {
		// Closed already — by this fact's pair (StockReleased rides a
		// RELEASE disposition) or an operator: terminal, nothing to do.
		return OutcomeDuplicate, nil
	}
	if !c.endsTheHold() {
		// REWORK: the unit is still held, so the case keeps owning the
		// pending rework; the arrival is an occurrence, not a close.
		if err := latest.Touch(uc.Clock.Now()); err != nil {
			return "", err
		}
		return OutcomeApplied, uc.Cases.Save(ctx, latest)
	}
	return OutcomeApplied, uc.close(ctx, latest, c)
}

// close auto-resolves the case with the derived rule id, the closing
// fact as the correlation and the outcome as the note, saving the case
// and enqueueing its facts in the caller's unit of work.
func (uc *AutoResolveHeldStock) close(ctx context.Context, latest *exceptioncase.Case, c HeldStockClosure) error {
	ruleID := TypeStockHeld + "->" + c.ClosingType
	if err := latest.AutoResolve(ruleID, c.ClosingType, c.EventID, uc.Clock.Now(), c.note()); err != nil {
		return err
	}
	if err := uc.Cases.Save(ctx, latest); err != nil {
		return err
	}
	return uc.Events.PublishCaseEvents(ctx, latest.DomainEvents()...)
}
