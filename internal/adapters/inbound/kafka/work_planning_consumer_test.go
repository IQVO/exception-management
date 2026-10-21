package kafka_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// TestWorkPlanningHandlers_OpensTheBacklogCase: the path is the dedup
// subject, no site part, and the fact freezes the producer's payload.
func TestWorkPlanningHandlers_OpensTheBacklogCase(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeBacklogThresholdBreached)
	handlers := kafka.WorkPlanningHandlers(w.consume, slog.Default())
	e := producerEvent(t, "e-backlog", "/warehouse/wes-work-planning",
		kafka.TypeBacklogThresholdBreached, "pack-singles", `{"path_id":"pack-singles"}`)
	if err := handlers[kafka.TypeBacklogThresholdBreached](context.Background(), e); err != nil {
		t.Fatalf("BacklogThresholdBreached: %v", err)
	}
	c := openCase(t, w, kafka.TypeBacklogThresholdBreached, "pack-singles", "")
	if c.SourceEventID() != "e-backlog" {
		t.Fatalf("source event = %s, want the CloudEvents id", c.SourceEventID())
	}
}
