package kafka

import (
	"context"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// OutboundShippingTopic is outbound-shipping's integration topic. It exists
// once the producer's relay first writes it; until then the reader retries
// the join internally (surfaced through its ErrorLogger) and the service
// stays ready — a missing topic is retry logs, never unreadiness.
const OutboundShippingTopic = "warehouse.outbound-shipping.events"

// TypePackageMissedLoad is the one outbound-shipping type this service acts
// on, byte-identical to the producer's AsyncAPI. Every other type on the
// topic is ignored.
const TypePackageMissedLoad = "com.warehouse.wms.outbound-shipping.shippingpackage.PackageMissedLoad"

// packageMissedLoadData mirrors the pinned PackageMissedLoad v1 payload.
// The dedup subject is `package_id` (the producer's subject — one case per
// missed package); `site_id` is the site part of the key. The load, order,
// cutoff and detection instant the operator needs stay in the frozen
// payload only, exactly as the producer shipped them.
type packageMissedLoadData struct {
	PackageID     string `json:"package_id"`
	LoadID        string `json:"load_id"`
	SiteID        string `json:"site_id"`
	SourceOrderID string `json:"source_order_id"`
	Cutoff        string `json:"cutoff"`
	DetectedAt    string `json:"detected_at"`
	Version       int64  `json:"version"`
}

// OutboundShippingHandlers maps outbound-shipping's consumed type to the
// open-or-touch use case.
func OutboundShippingHandlers(consume *usecases.ConsumeFact, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypePackageMissedLoad: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[packageMissedLoadData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.PackageID, d.SiteID)
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
	}
}

// NewOutboundShippingExceptionsConsumer reads outbound-shipping's topic
// under groupID (OUTBOUND_SHIPPING_EXCEPTIONS_CONSUMER_GROUP, never a
// literal).
func NewOutboundShippingExceptionsConsumer(brokers []string, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("outbound shipping exceptions consumer", brokers, OutboundShippingTopic, groupID,
		OutboundShippingHandlers(consume, logger), logger)
}

// NewOutboundShippingExceptionsConsumerForTopic is
// NewOutboundShippingExceptionsConsumer on an explicit topic (the
// integration tests' unique topics).
func NewOutboundShippingExceptionsConsumerForTopic(brokers []string, topic, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("outbound shipping exceptions consumer", brokers, topic, groupID,
		OutboundShippingHandlers(consume, logger), logger)
}
