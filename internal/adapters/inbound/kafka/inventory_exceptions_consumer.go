package kafka

import (
	"context"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// InventoryTopic is inventory-storage's integration topic; this service
// acts on the bin and stock exception facts there.
const InventoryTopic = "warehouse.inventory.events"

// The inventory-storage types this service acts on, byte-identical to the
// producer's AsyncAPI. Every other type on the topic is ignored.
const (
	TypeDiscrepancyDetected = "com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected"
	TypeItemUnlocated       = "com.warehouse.wms.inventory-storage.stock.ItemUnlocated"
)

// discrepancyDetectedData mirrors the pinned DiscrepancyDetected v1 payload.
// The dedup subject is `bin_id`; counted and system stay in the frozen
// payload only.
type discrepancyDetectedData struct {
	BinID   string `json:"bin_id"`
	Counted int64  `json:"counted"`
	System  int64  `json:"system"`
}

// itemUnlocatedData mirrors the pinned ItemUnlocated v1 payload. The dedup
// subject is `stock_unit_id`.
type itemUnlocatedData struct {
	StockUnitID string `json:"stock_unit_id"`
	SKU         string `json:"sku"`
	BinID       string `json:"bin_id"`
	Quantity    int64  `json:"quantity"`
}

// InventoryExceptionsHandlers maps inventory-storage's consumed types to
// the open-or-touch use case. Neither v1 payload carries a site: the site
// part of the dedup key is empty (the key still dedupes).
func InventoryExceptionsHandlers(consume *usecases.ConsumeFact, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypeDiscrepancyDetected: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[discrepancyDetectedData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.BinID, "")
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
		TypeItemUnlocated: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[itemUnlocatedData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.StockUnitID, "")
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
	}
}

// NewInventoryExceptionsConsumer reads inventory-storage's topic under
// groupID (INVENTORY_EXCEPTIONS_CONSUMER_GROUP, never a literal).
func NewInventoryExceptionsConsumer(brokers []string, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("inventory exceptions consumer", brokers, InventoryTopic, groupID,
		InventoryExceptionsHandlers(consume, logger), logger)
}

// NewInventoryExceptionsConsumerForTopic is NewInventoryExceptionsConsumer
// on an explicit topic (the integration tests' unique topics).
func NewInventoryExceptionsConsumerForTopic(brokers []string, topic, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("inventory exceptions consumer", brokers, topic, groupID,
		InventoryExceptionsHandlers(consume, logger), logger)
}
