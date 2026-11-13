package features_test

import (
	"context"
	"errors"

	"github.com/cucumber/godog"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// heldStockSteps drives the real ConsumeFact and AutoResolveHeldStock use
// cases (event-id claim included) over the same memory doubles as the
// other facts, the way the held-stock Kafka consumer does (ADR 0012).
type heldStockSteps struct {
	w       *world
	consume *usecases.ConsumeFact
	settle  *usecases.AutoResolveHeldStock
}

// registerHeldStockSteps registers the held-stock steps (ADR 0012).
func registerHeldStockSteps(sc *godog.ScenarioContext, w *world) {
	h := &heldStockSteps{w: w}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		h.consume, h.settle = nil, nil
		return ctx, nil
	})

	sc.Step(`^a StockHeld fact for held unit (\S+) with event id (\S+) and payload (.+) arrives$`,
		h.held)
	sc.Step(`^a StockDispositioned fact with disposition (\S+) for held unit (\S+) with event id (\S+) arrives$`,
		h.dispositioned)
	sc.Step(`^a StockReleased fact for held unit (\S+) with event id (\S+) arrives$`,
		h.released)
	sc.Step(`^the case resolution note is (.+)$`, func(want string) error {
		return wantStr("resolution note", w.current().Note(), want)
	})
}

// wire builds the use cases of the scenario on first use (they share one
// processed-events set, like the production wiring).
func (h *heldStockSteps) wire() {
	if h.consume != nil {
		return
	}
	processed := memory.NewProcessedEvents()
	h.consume = &usecases.ConsumeFact{Processed: processed, OpenOrTouch: h.w.uc.open}
	h.settle = &usecases.AutoResolveHeldStock{
		Processed: processed, Cases: h.w.cases, Events: h.w.ev, Clock: h.w.clock,
	}
}

// held runs one StockHeld fact and remembers the case it opened.
func (h *heldStockSteps) held(unit, eventID, payload string) error {
	h.wire()
	h.w.run(func() error {
		if _, err := h.consume.Execute(h.w.ctx, usecases.Fact{
			SourceType:    usecases.TypeStockHeld,
			SourceSubject: unit,
			SourceEventID: eventID,
			Payload:       []byte(payload),
		}); err != nil {
			return err
		}
		return h.remember(unit)
	})
	return nil
}

// dispositioned runs one StockDispositioned closing fact.
func (h *heldStockSteps) dispositioned(disposition, unit, eventID string) error {
	h.wire()
	h.w.run(func() error {
		if _, err := h.settle.Execute(h.w.ctx, usecases.HeldStockClosure{
			EventID: eventID, HeldUnitID: unit,
			ClosingType: usecases.TypeStockDispositioned, Disposition: disposition,
		}); err != nil {
			return err
		}
		// An unknown held unit owns no case to remember: the fail-soft
		// no-op of ADR 0012 point 3, not a scenario failure.
		_ = h.remember(unit)
		return nil
	})
	return nil
}

// released runs one StockReleased closing fact.
func (h *heldStockSteps) released(unit, eventID string) error {
	h.wire()
	h.w.run(func() error {
		_, err := h.settle.Execute(h.w.ctx, usecases.HeldStockClosure{
			EventID: eventID, HeldUnitID: unit, ClosingType: usecases.TypeStockReleased,
		})
		if err != nil {
			return err
		}
		return h.remember(unit)
	})
	return nil
}

// remember points the scenario at the case owning the held unit's key.
func (h *heldStockSteps) remember(unit string) error {
	key, err := shared.NewDedupKey(usecases.TypeStockHeld, unit, "")
	if err != nil {
		return err
	}
	c, err := h.w.cases.FindOpenByDedupKey(h.w.ctx, key)
	if err != nil {
		return errors.New("no case under the held-unit key: " + err.Error())
	}
	h.w.caseID = c.ID()
	return nil
}
