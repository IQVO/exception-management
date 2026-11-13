package kafka

import (
	"context"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// DockSchedulingTopic is dock-scheduling's integration topic (ADR 0010).
// Only DockAppointmentNoShow on it is acted on; the booking, check-in and
// door types (several analytics-only) are ignored.
const DockSchedulingTopic = "warehouse.dock-scheduling.events"

// TypeDockAppointmentNoShow is byte-identical to the producer's AsyncAPI.
const TypeDockAppointmentNoShow = usecases.TypeDockAppointmentNoShow

// dockAppointmentNoShowData mirrors the pinned DockAppointmentNoShow v1
// payload. The dedup subject is `appointment_id`; NO_SHOW is terminal in
// dock-scheduling, so an appointment is reported at most once.
type dockAppointmentNoShowData struct {
	AppointmentID string `json:"appointment_id"`
	SiteID        string `json:"site_id"`
	DoorCode      string `json:"door_code"`
	Direction     string `json:"direction"`
	CarrierSCAC   string `json:"carrier_scac"`
	WindowEnd     string `json:"window_end"`
	Version       int64  `json:"version"`
}

// DockSchedulingExceptionsHandlers maps DockAppointmentNoShow to the
// open-or-touch use case.
func DockSchedulingExceptionsHandlers(consume *usecases.ConsumeFact, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypeDockAppointmentNoShow: func(ctx context.Context, e ce.Event) error {
			d, err := decodeData[dockAppointmentNoShowData](e)
			if err != nil {
				return err
			}
			f, err := fact(e, d.AppointmentID, d.SiteID)
			if err != nil {
				return err
			}
			return apply(ctx, logger, e, func() (usecases.Outcome, error) { return consume.Execute(ctx, f) })
		},
	}
}

// NewDockSchedulingExceptionsConsumer reads dock-scheduling's topic under
// groupID (DOCK_SCHEDULING_EXCEPTIONS_CONSUMER_GROUP, never a literal).
func NewDockSchedulingExceptionsConsumer(brokers []string, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("dock scheduling exceptions consumer", brokers, DockSchedulingTopic, groupID,
		DockSchedulingExceptionsHandlers(consume, logger), logger)
}

// NewDockSchedulingExceptionsConsumerForTopic is
// NewDockSchedulingExceptionsConsumer on an explicit topic (the
// integration tests' unique topics).
func NewDockSchedulingExceptionsConsumerForTopic(brokers []string, topic, groupID string, consume *usecases.ConsumeFact, logger *slog.Logger) *Consumer {
	return newConsumer("dock scheduling exceptions consumer", brokers, topic, groupID,
		DockSchedulingExceptionsHandlers(consume, logger), logger)
}
