package kafka

import (
	"context"
	"errors"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// OrderManagementTopic is order-management's integration topic; this
// service acts only on OrderRepromised there — the correlated fact of the
// auto-resolve rules (ADR 0002). Every other type on the topic (the order
// copy facts OrderAllocated / OrderPartiallyAllocated) is ignored: v1
// exception-management keeps no local order copy.
const OrderManagementTopic = "warehouse.order-management.events"

// TypeOrderRepromised is order-management's repromise type, byte-identical
// to its AsyncAPI.
const TypeOrderRepromised = "com.warehouse.wes.order-management.order.OrderRepromised"

// orderRepromisedData mirrors the pinned OrderRepromised v1 payload. The
// correlation is ORDER-level: `order_id` (the case's work-unit subject is
// reversed and the line ignored — 00-common's FINAL addendum). cpt_id_old,
// cpt_id_new and reason stay in the log line only; this fact opens no case
// and freezes no payload.
type orderRepromisedData struct {
	OrderID  string `json:"order_id"`
	CPTIDOld string `json:"cpt_id_old"`
	CPTIDNew string `json:"cpt_id_new"`
	Reason   string `json:"reason"`
}

// OrderCorrelationHandlers maps order-management's consumed type to the
// auto-resolve use case.
func OrderCorrelationHandlers(correlate *usecases.ConsumeCorrelation, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypeOrderRepromised: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[orderRepromisedData](e)
			if err != nil {
				return err
			}
			if d.OrderID == "" {
				return &errDeadLetter{cause: skipf("payload of %s carries no order id", e.Type())}
			}
			fact := usecases.CorrelatedFact{
				CorrelatedType:    e.Type(),
				CorrelatedOrderID: d.OrderID,
				CorrelatedEventID: e.ID(),
			}
			closed, outcome, err := correlate.Execute(ctx, fact)
			if err != nil {
				if errors.Is(err, usecases.ErrNoEventID) {
					return skipf("%s: %v", e.Type(), err)
				}
				return err
			}
			logger.InfoContext(ctx, "correlated fact consumed", "type", e.Type(), "event_id", e.ID(),
				"subject", e.Subject(), "order_id", d.OrderID, "reason", d.Reason,
				"closed", closed, "outcome", string(outcome))
			return nil
		},
	}
}

// NewOrderCorrelationConsumer reads order-management's topic under groupID
// (ORDER_CORRELATION_CONSUMER_GROUP, never a literal).
func NewOrderCorrelationConsumer(brokers []string, groupID string, correlate *usecases.ConsumeCorrelation, logger *slog.Logger) *Consumer {
	return newConsumer("order correlation consumer", brokers, OrderManagementTopic, groupID,
		OrderCorrelationHandlers(correlate, logger), logger)
}

// NewOrderCorrelationConsumerForTopic is NewOrderCorrelationConsumer on an
// explicit topic (the integration tests' unique topics).
func NewOrderCorrelationConsumerForTopic(brokers []string, topic, groupID string, correlate *usecases.ConsumeCorrelation, logger *slog.Logger) *Consumer {
	return newConsumer("order correlation consumer", brokers, topic, groupID,
		OrderCorrelationHandlers(correlate, logger), logger)
}
