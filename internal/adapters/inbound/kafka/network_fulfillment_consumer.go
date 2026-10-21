package kafka

import (
	"context"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// NetworkFulfillmentTopic is network-fulfillment's integration topic; this
// service acts on the network-order acknowledgement risk facts there.
const NetworkFulfillmentTopic = "warehouse.network-fulfillment.events"

// The network-fulfillment types this service acts on, byte-identical to the
// producer's AsyncAPI. Every other type on the topic is ignored.
const (
	TypeAcknowledgementDeadlineAtRisk = "com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk"
	TypeOrphanedHoldCancelled         = "com.warehouse.wes.network-fulfillment.networkorder.OrphanedHoldCancelled"
)

// acknowledgementDeadlineAtRiskData mirrors the pinned
// AcknowledgementDeadlineAtRisk v1 payload. The dedup subject is
// `networkRef`; `siteId` is the one consumed type that carries a site.
type acknowledgementDeadlineAtRiskData struct {
	NetworkRef    string `json:"networkRef"`
	SiteID        string `json:"siteId"`
	AcknowledgeBy string `json:"acknowledgeBy"`
	At            string `json:"at"`
}

// orphanedHoldCancelledData mirrors the pinned OrphanedHoldCancelled v1
// payload. The dedup subject is `networkRef`; `siteId` carries the site.
type orphanedHoldCancelledData struct {
	NetworkRef    string `json:"networkRef"`
	SiteID        string `json:"siteId"`
	LocalOrderID  string `json:"localOrderId"`
	AcknowledgeBy string `json:"acknowledgeBy"`
	Answer        string `json:"answer"`
	At            string `json:"at"`
}

// NetworkFulfillmentHandlers maps network-fulfillment's consumed types to
// the open-or-touch use case. Both v1 payloads carry `siteId`: it is the
// site part of the dedup key.
func NetworkFulfillmentHandlers(consume *usecases.ConsumeFact, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypeAcknowledgementDeadlineAtRisk: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[acknowledgementDeadlineAtRiskData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.NetworkRef, d.SiteID)
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
		TypeOrphanedHoldCancelled: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[orphanedHoldCancelledData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.NetworkRef, d.SiteID)
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
	}
}

// NewNetworkFulfillmentExceptionsConsumer reads network-fulfillment's topic
// under groupID (NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP, never a
// literal).
func NewNetworkFulfillmentExceptionsConsumer(brokers []string, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("network fulfillment exceptions consumer", brokers, NetworkFulfillmentTopic, groupID,
		NetworkFulfillmentHandlers(consume, logger), logger)
}

// NewNetworkFulfillmentExceptionsConsumerForTopic is
// NewNetworkFulfillmentExceptionsConsumer on an explicit topic (the
// integration tests' unique topics).
func NewNetworkFulfillmentExceptionsConsumerForTopic(brokers []string, topic, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("network fulfillment exceptions consumer", brokers, topic, groupID,
		NetworkFulfillmentHandlers(consume, logger), logger)
}
