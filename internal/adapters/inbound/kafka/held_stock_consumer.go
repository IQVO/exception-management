package kafka

import (
	"context"
	"errors"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// The held-stock consumer (ADR 0012) reads InventoryTopic — the SAME topic
// as the inventory exceptions consumer — under its OWN consumer group: the
// two concerns keep independent offsets and failure isolation. The types
// live in the usecases package (they are the case key's source type);
// every other type on the topic stays ignored.

// stockHeldData mirrors the pinned StockHeld v1 payload. The dedup
// subject is `held_unit_id`; the sku, quantity, reason and the receipt /
// ASN / line references stay in the frozen payload only.
type stockHeldData struct {
	HeldUnitID string `json:"held_unit_id"`
}

// stockDispositionedData mirrors the pinned StockDispositioned v1
// payload. The handler needs the held unit id and the disposition; the
// rest stays in the payload the use case never freezes (only the OPENING
// StockHeld payload is frozen on the case).
type stockDispositionedData struct {
	HeldUnitID  string `json:"held_unit_id"`
	Disposition string `json:"disposition"`
}

// stockReleasedData mirrors the pinned StockReleased v1 payload. The
// dedup subject is `held_unit_id`.
type stockReleasedData struct {
	HeldUnitID string `json:"held_unit_id"`
}

// HeldStockHandlers maps inventory-storage's held-stock types to their
// use cases (ADR 0012): StockHeld opens-or-touches the case of the held
// unit (one case per unit id, never per emission); StockDispositioned and
// StockReleased close it — except a REWORK disposition, which keeps the
// unit held and therefore the case open. Neither closing payload carries
// a site: the site part of the case key is empty (the key still dedupes).
func HeldStockHandlers(consume *usecases.ConsumeFact, settle *usecases.AutoResolveHeldStock, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		usecases.TypeStockHeld: heldOpenHandler(consume, logger),
		usecases.TypeStockDispositioned: heldCloseHandler(logger, settle, func(e ce.Event, d stockDispositionedData) usecases.HeldStockClosure {
			return usecases.HeldStockClosure{
				EventID: e.ID(), HeldUnitID: d.HeldUnitID,
				ClosingType: usecases.TypeStockDispositioned, Disposition: d.Disposition,
			}
		}),
		usecases.TypeStockReleased: heldCloseHandler(logger, settle, func(e ce.Event, d stockReleasedData) usecases.HeldStockClosure {
			return usecases.HeldStockClosure{
				EventID: e.ID(), HeldUnitID: d.HeldUnitID, ClosingType: usecases.TypeStockReleased,
			}
		}),
	}
}

// heldOpenHandler builds the StockHeld handler: open-or-touch through the
// shared ConsumeFact (event-id claim + effect in one unit of work).
func heldOpenHandler(consume *usecases.ConsumeFact, logger *slog.Logger) HandlerFunc {
	return func(ctx context.Context, e ce.Event) error {
		d, err := decodeData[stockHeldData](e)
		if err != nil {
			return err
		}
		f, err := fact(e, d.HeldUnitID, "")
		if err != nil {
			return err
		}
		return apply(ctx, logger, e, func() (usecases.Outcome, error) {
			return consume.Execute(ctx, f)
		})
	}
}

// heldCloseHandler builds a closing handler (StockDispositioned or
// StockReleased) from its payload decoder.
func heldCloseHandler[T any](logger *slog.Logger, settle *usecases.AutoResolveHeldStock, closure func(ce.Event, T) usecases.HeldStockClosure) HandlerFunc {
	return func(ctx context.Context, e ce.Event) error {
		d, err := decodeData[T](e)
		if err != nil {
			return err
		}
		c := closure(e, d)
		if !shared.ValidText(c.HeldUnitID) {
			// A payload of a handled type with no mappable id is poison,
			// like every consumer of this package.
			return &deadLetterError{cause: skipf("payload of %s carries no held_unit_id", e.Type())}
		}
		outcome, err := settle.Execute(ctx, c)
		if err != nil {
			if errors.Is(err, usecases.ErrUnknownDisposition) || errors.Is(err, usecases.ErrNoEventID) {
				return skipf("%s: %v", e.Type(), err)
			}
			return err
		}
		if outcome == usecases.OutcomeNoMatch {
			// A well-formed fact about a hold this service never tracked
			// (a pre-cutoff hold): WARN and commit past — never retried,
			// never dead-lettered (ADR 0012 point 3).
			logger.WarnContext(ctx, "no case for the held unit; committing past",
				"type", e.Type(), "event_id", e.ID(), "held_unit_id", c.HeldUnitID)
			return nil
		}
		logger.InfoContext(ctx, "fact consumed", "type", e.Type(), "event_id", e.ID(),
			"subject", e.Subject(), "outcome", string(outcome))
		return nil
	}
}

// NewHeldStockConsumer reads inventory-storage's topic under groupID
// (HELD_STOCK_EXCEPTIONS_CONSUMER_GROUP, never a literal) — a SECOND
// group on the topic, independent of INVENTORY_EXCEPTIONS_CONSUMER_GROUP
// (ADR 0012 point 1).
func NewHeldStockConsumer(brokers []string, groupID string, consume *usecases.ConsumeFact, settle *usecases.AutoResolveHeldStock, logger *slog.Logger) *Consumer {
	return newConsumer("held stock consumer", brokers, InventoryTopic, groupID,
		HeldStockHandlers(consume, settle, logger), logger)
}

// NewHeldStockConsumerForTopic is NewHeldStockConsumer on an explicit
// topic (the integration tests' unique topics).
func NewHeldStockConsumerForTopic(brokers []string, topic, groupID string, consume *usecases.ConsumeFact, settle *usecases.AutoResolveHeldStock, logger *slog.Logger) *Consumer {
	return newConsumer("held stock consumer", brokers, topic, groupID,
		HeldStockHandlers(consume, settle, logger), logger)
}
