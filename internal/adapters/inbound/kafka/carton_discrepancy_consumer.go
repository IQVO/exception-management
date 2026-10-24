package kafka

import (
	"context"
	"errors"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// InboundReceivingTopic is inbound-receiving's integration topic. Only the
// CartonDiscrepancyRaised type on it is acted on (contract R12).
const InboundReceivingTopic = "warehouse.inbound-receiving.events"

// TypeCartonDiscrepancyRaised is byte-identical to the producer's AsyncAPI.
const TypeCartonDiscrepancyRaised = usecases.TypeCartonDiscrepancyRaised

// cartonDiscrepancyData mirrors the producer's snake_case v1 payload.
type cartonDiscrepancyData struct {
	ReceiptID string `json:"receipt_id"`
	SSCC      string `json:"sscc"`
	Kind      string `json:"kind"`
	Expected  string `json:"expected"`
	Actual    string `json:"actual"`
	At        string `json:"at"`
}

// CartonDiscrepancyHandlers maps the consumed type to the open-or-touch
// use case; any kind string is accepted (unknown kinds route to
// problem-solve).
func CartonDiscrepancyHandlers(open *usecases.OpenCartonDiscrepancy, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypeCartonDiscrepancyRaised: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[cartonDiscrepancyData](e)
			if err != nil {
				return err
			}
			outcome, err := open.Execute(ctx, usecases.CartonDiscrepancy{
				EventID: e.ID(), ReceiptID: d.ReceiptID, SSCC: d.SSCC, Kind: d.Kind, Payload: e.Data(),
			})
			if err != nil {
				switch {
				case errors.Is(err, shared.ErrBlankSourceSubject):
					return &errDeadLetter{cause: skipf("payload of %s carries no mappable id: %v", e.Type(), err)}
				case errors.Is(err, usecases.ErrPolicyUndeclared), errors.Is(err, usecases.ErrNoEventID):
					return skipf("%s: %v", e.Type(), err)
				}
				return err
			}
			logger.InfoContext(ctx, "fact consumed", "type", e.Type(), "event_id", e.ID(), "subject", e.Subject(), "kind", d.Kind, "outcome", string(outcome))
			return nil
		},
	}
}

// NewCartonDiscrepancyConsumer reads inbound-receiving's topic under
// groupID (CARTON_DISCREPANCY_CONSUMER_GROUP, never a literal).
func NewCartonDiscrepancyConsumer(brokers []string, groupID string, open *usecases.OpenCartonDiscrepancy, logger *slog.Logger) *Consumer {
	return newConsumer("carton discrepancy consumer", brokers, InboundReceivingTopic, groupID,
		CartonDiscrepancyHandlers(open, logger), logger)
}

// NewCartonDiscrepancyConsumerForTopic is NewCartonDiscrepancyConsumer on
// an explicit topic (the integration tests' unique topics).
func NewCartonDiscrepancyConsumerForTopic(brokers []string, topic, groupID string, open *usecases.OpenCartonDiscrepancy, logger *slog.Logger) *Consumer {
	return newConsumer("carton discrepancy consumer", brokers, topic, groupID,
		CartonDiscrepancyHandlers(open, logger), logger)
}
