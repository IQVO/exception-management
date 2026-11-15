package kafka_test

// Event consumer contract tests (E1-E4) for the wes-work-planning edge:
// the work planning exceptions consumer reads wes-work-planning's shared
// topic warehouse.work-planning.events (kafka.WorkPlanningTopic) and
// acts only on BacklogThresholdBreached (the payload is path_id only —
// the producer's spec says consumers wanting the depth and rate numbers
// read the telemetry projection). The contract is the PINNED copy of
// wes-work-planning's apis/asyncapi.yaml
// (testdata/wes-work-planning.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's own
// examples.

import (
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// E1: the topic the reader subscribes to is documented by the producer's
// pinned spec, and its message list documents the exact CloudEvents type
// this repo's decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_WorkPlanningEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedWorkPlanningSpec)
	documented := doc.documentedTypes(t, kafka.WorkPlanningTopic)
	if _, ok := documented[kafka.TypeBacklogThresholdBreached]; !ok {
		t.Errorf("pinned wes-work-planning spec does not document %s on %s (documented: %v)",
			kafka.TypeBacklogThresholdBreached, kafka.WorkPlanningTopic, documented)
	}
}

// E2: the producer's own BacklogThresholdBreached example decodes
// through the REAL HandleMessage decode+dispatch and opens exactly one
// case under (type, path_id, "") — the payload carries no site; a
// redelivered CloudEvents id creates nothing more.
func TestGoldenEventsDecode_BacklogThresholdBreached(t *testing.T) {
	assertGoldenFact(t, func(t *testing.T) (*consumeWorld, *kafka.Consumer) {
		w := newConsumeWorld(t, kafka.TypeBacklogThresholdBreached)
		return w, contractConsumer("work planning contract", kafka.WorkPlanningTopic,
			kafka.WorkPlanningHandlers(w.consume, contractLogger()))
	}, pinnedWorkPlanningSpec, kafka.WorkPlanningTopic,
		kafka.TypeBacklogThresholdBreached, keyBy("path_id", ""))
}

// E3: the golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — dataschema
// urn:warehouse:wes-work-planning:events:BacklogThresholdBreached:vN —
// and the pinned spec's envelope schema requires the full attribute set.
func TestCloudEventsEnvelopeInvariants_BacklogThresholdBreached(t *testing.T) {
	for _, ex := range goldenExamples(t, pinnedWorkPlanningSpec, kafka.WorkPlanningTopic, kafka.TypeBacklogThresholdBreached) {
		assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "wes-work-planning", "BacklogThresholdBreached")
	}
	doc := loadPinnedAsyncAPI(t, pinnedWorkPlanningSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	assertEnvelopeRequired(t, pinnedWorkPlanningSpec, kafka.WorkPlanningTopic, kafka.TypeBacklogThresholdBreached)
}

// E4: unknown additive data keys must not change the effect; an unknown
// event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; a BacklogThresholdBreached that lost its path_id is
// the dead-lettered poison refusal.
func TestUnknownAttributesTolerated_WorkPlanning(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeBacklogThresholdBreached)
	c := contractConsumer("work planning contract", kafka.WorkPlanningTopic,
		kafka.WorkPlanningHandlers(w.consume, contractLogger()))
	assertUnknownTolerance(t, c, w, pinnedWorkPlanningSpec, kafka.WorkPlanningTopic,
		kafka.TypeBacklogThresholdBreached, keyBy("path_id", ""),
		func(data map[string]any) { data["path_id"] = "" })
}
