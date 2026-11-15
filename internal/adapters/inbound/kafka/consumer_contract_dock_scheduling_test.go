package kafka_test

// Event consumer contract tests (E1-E4) for the dock-scheduling edge:
// the dock scheduling exceptions consumer reads dock-scheduling's shared
// topic warehouse.dock-scheduling.events (kafka.DockSchedulingTopic)
// under its own group and acts only on DockAppointmentNoShow. The
// contract is the PINNED copy of dock-scheduling's apis/asyncapi.yaml
// (testdata/dock-scheduling.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/dock-scheduling@develop). Golden fixtures are the
// producer spec's own examples.

import (
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// E1: the topic this repo's reader actually subscribes to is documented
// by the producer's pinned spec, and the message list on that channel
// documents the exact CloudEvents type this repo's decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_DockSchedulingEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedDockSchedulingSpec)
	documented := doc.documentedTypes(t, kafka.DockSchedulingTopic)
	if _, ok := documented[kafka.TypeDockAppointmentNoShow]; !ok {
		t.Errorf("pinned dock-scheduling spec does not document %s on %s (documented: %v)",
			kafka.TypeDockAppointmentNoShow, kafka.DockSchedulingTopic, documented)
	}
}

// E2: the producer's own DockAppointmentNoShow example decodes through
// the REAL HandleMessage decode+dispatch and opens exactly one case
// under (type, appointment_id, site); a redelivered CloudEvents id
// creates nothing more.
func TestGoldenEventsDecode_DockAppointmentNoShow(t *testing.T) {
	assertGoldenFact(t, func(t *testing.T) (*consumeWorld, *kafka.Consumer) {
		w := newConsumeWorld(t, kafka.TypeDockAppointmentNoShow)
		return w, contractConsumer("dock contract", kafka.DockSchedulingTopic,
			kafka.DockSchedulingExceptionsHandlers(w.consume, contractLogger()))
	}, pinnedDockSchedulingSpec, kafka.DockSchedulingTopic,
		kafka.TypeDockAppointmentNoShow, keyBy("appointment_id", "site_id"))
}

// E3: the golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, /warehouse/dock-scheduling source,
// subject, time, datacontenttype application/json, dataschema
// urn:warehouse:dock-scheduling:events:DockAppointmentNoShow:vN — and
// the pinned spec's envelope schema requires the full attribute set.
func TestCloudEventsEnvelopeInvariants_DockAppointmentNoShow(t *testing.T) {
	for _, ex := range goldenExamples(t, pinnedDockSchedulingSpec, kafka.DockSchedulingTopic, kafka.TypeDockAppointmentNoShow) {
		assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "dock-scheduling", "DockAppointmentNoShow")
	}
	doc := loadPinnedAsyncAPI(t, pinnedDockSchedulingSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	assertEnvelopeRequired(t, pinnedDockSchedulingSpec, kafka.DockSchedulingTopic, kafka.TypeDockAppointmentNoShow)
}

// E4: unknown additive data keys must not change the effect; an unknown
// event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; a handled type whose payload lost its appointment id
// is the dead-lettered poison refusal.
func TestUnknownAttributesTolerated_DockScheduling(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeDockAppointmentNoShow)
	c := contractConsumer("dock contract", kafka.DockSchedulingTopic,
		kafka.DockSchedulingExceptionsHandlers(w.consume, contractLogger()))
	assertUnknownTolerance(t, c, w, pinnedDockSchedulingSpec, kafka.DockSchedulingTopic,
		kafka.TypeDockAppointmentNoShow, keyBy("appointment_id", "site_id"),
		func(data map[string]any) { data["appointment_id"] = "" })
}
