package kafka

import (
	"context"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// WorkPlanningTopic is wes-work-planning's integration topic; this service
// acts on the workpool backlog alarm there.
const WorkPlanningTopic = "warehouse.work-planning.events"

// TypeBacklogThresholdBreached is the one work-planning type this service
// acts on, byte-identical to the producer's AsyncAPI. Every other type on
// the topic is ignored.
const TypeBacklogThresholdBreached = "com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached"

// backlogThresholdBreachedData mirrors the pinned BacklogThresholdBreached
// v1 payload: `path_id` only (the producer's AsyncAPI says consumers wanting
// the depth and rate numbers read the telemetry projection, not this
// payload).
type backlogThresholdBreachedData struct {
	PathID string `json:"path_id"`
}

// WorkPlanningHandlers maps work-planning's consumed type to the
// open-or-touch use case. The payload carries no site: the site part of the
// dedup key is empty (the key still dedupes).
func WorkPlanningHandlers(consume *usecases.ConsumeFact, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypeBacklogThresholdBreached: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[backlogThresholdBreachedData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.PathID, "")
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) {
				return consume.Execute(ctx, f)
			})
		},
	}
}

// NewWorkPlanningExceptionsConsumer reads wes-work-planning's topic under
// groupID (WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP, never a literal).
func NewWorkPlanningExceptionsConsumer(brokers []string, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("work planning exceptions consumer", brokers, WorkPlanningTopic, groupID,
		WorkPlanningHandlers(consume, logger), logger)
}

// NewWorkPlanningExceptionsConsumerForTopic is NewWorkPlanningExceptionsConsumer
// on an explicit topic (the integration tests' unique topics).
func NewWorkPlanningExceptionsConsumerForTopic(brokers []string, topic, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("work planning exceptions consumer", brokers, topic, groupID,
		WorkPlanningHandlers(consume, logger), logger)
}
