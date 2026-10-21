package kafka

import (
	"context"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// ReturnsTopic is returns-processing's integration topic. It is created by
// its PRODUCER (wave 3), not by this service and not by the broker until
// the producer's first write: until then the reader retries the join
// internally (surfaced through its ErrorLogger) and the service stays ready
// — a missing topic is retry logs, never unreadiness.
const ReturnsTopic = "warehouse.returns-processing.events"

// TypeUnexpectedReturnReceived is the one returns-processing type this
// service acts on, byte-identical to the producer's AsyncAPI. Every other
// type on the topic is ignored.
const TypeUnexpectedReturnReceived = "com.warehouse.wms.returns-processing.returnreceipt.UnexpectedReturnReceived"

// unexpectedReturnReceivedData mirrors the pinned UnexpectedReturnReceived
// v1 payload. The dedup subject is `receipt_id` (the producer's subject);
// `site_id` is the site part of the key.
type unexpectedReturnReceivedData struct {
	ReceiptID  string `json:"receipt_id"`
	LineNo     int64  `json:"line_no"`
	SKU        string `json:"sku"`
	Quantity   int64  `json:"quantity"`
	SiteID     string `json:"site_id"`
	ReceivedAt string `json:"received_at"`
}

// ReturnsExceptionsHandlers maps returns-processing's consumed type to the
// open-or-touch use case.
func ReturnsExceptionsHandlers(consume *usecases.ConsumeFact, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypeUnexpectedReturnReceived: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[unexpectedReturnReceivedData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.ReceiptID, d.SiteID)
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
	}
}

// NewReturnsExceptionsConsumer reads returns-processing's topic under
// groupID (RETURNS_EXCEPTIONS_CONSUMER_GROUP, never a literal).
func NewReturnsExceptionsConsumer(brokers []string, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("returns exceptions consumer", brokers, ReturnsTopic, groupID,
		ReturnsExceptionsHandlers(consume, logger), logger)
}

// NewReturnsExceptionsConsumerForTopic is NewReturnsExceptionsConsumer on
// an explicit topic (the integration tests' unique topics).
func NewReturnsExceptionsConsumerForTopic(brokers []string, topic, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("returns exceptions consumer", brokers, topic, groupID,
		ReturnsExceptionsHandlers(consume, logger), logger)
}
