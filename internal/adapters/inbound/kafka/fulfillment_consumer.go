package kafka

import (
	"context"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// FulfillmentTopic is fulfillment-execution's integration topic; this
// service acts on the task and package exception facts there.
const FulfillmentTopic = "warehouse.fulfillment.events"

// The fulfillment-execution types this service acts on, byte-identical to
// the producer's AsyncAPI. Every other type on the topic is ignored.
const (
	TypeTaskCPTMissed             = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	TypePackageDiverted           = "com.warehouse.wes.fulfillment-execution.package.PackageDiverted"
	TypeWeightDiscrepancyDetected = "com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected"
)

// taskCPTMissedData mirrors the pinned TaskCPTMissed v1 payload. The dedup
// subject is `order_ref` — the WES work unit id `{order_id}-line-{line_no}`
// (00-common's LOCAL COPY RULE keys exceptions per work unit, and the
// OrderRepromised correlation reverses exactly this field). task_type and
// cpt stay in the frozen payload only.
type taskCPTMissedData struct {
	TaskID   string `json:"task_id"`
	OrderRef string `json:"order_ref"`
	TaskType string `json:"task_type"`
	CPT      string `json:"cpt"`
}

// packageDivertedData mirrors the pinned PackageDiverted v1 payload.
type packageDivertedData struct {
	PackageID string `json:"package_id"`
}

// weightDiscrepancyDetectedData mirrors the pinned WeightDiscrepancyDetected
// v1 payload.
type weightDiscrepancyDetectedData struct {
	PackageID string `json:"package_id"`
	ExpectedG int64  `json:"expected_g"`
	ActualG   int64  `json:"actual_g"`
}

// FulfillmentHandlers maps fulfillment-execution's consumed types to the
// open-or-touch use case. None of the v1 payloads carries a site: the site
// part of the dedup key is empty (the key still dedupes).
func FulfillmentHandlers(consume *usecases.ConsumeFact, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypeTaskCPTMissed: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[taskCPTMissedData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.OrderRef, "")
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
		TypePackageDiverted: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[packageDivertedData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.PackageID, "")
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
		TypeWeightDiscrepancyDetected: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[weightDiscrepancyDetectedData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.PackageID, "")
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
	}
}

// NewFulfillmentExceptionsConsumer reads fulfillment-execution's topic under
// groupID (FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP, never a literal).
func NewFulfillmentExceptionsConsumer(brokers []string, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("fulfillment exceptions consumer", brokers, FulfillmentTopic, groupID,
		FulfillmentHandlers(consume, logger), logger)
}

// NewFulfillmentExceptionsConsumerForTopic is NewFulfillmentExceptionsConsumer
// on an explicit topic (the integration tests' unique topics).
func NewFulfillmentExceptionsConsumerForTopic(brokers []string, topic, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("fulfillment exceptions consumer", brokers, topic, groupID,
		FulfillmentHandlers(consume, logger), logger)
}
