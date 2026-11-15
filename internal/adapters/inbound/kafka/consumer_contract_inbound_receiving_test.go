package kafka_test

// Event consumer contract tests (E1-E4) for the inbound-receiving edge:
// the carton discrepancy consumer reads inbound-receiving's shared topic
// warehouse.inbound-receiving.events (kafka.InboundReceivingTopic) and
// acts only on CartonDiscrepancyRaised (contract R12). The contract is
// the PINNED copy of inbound-receiving's apis/asyncapi.yaml
// (testdata/inbound-receiving.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's own
// examples.
//
// NOTE spec shape: inbound-receiving pins the CloudEvents `type` of each
// message only in its EXAMPLES (the shared envelope schema types it as
// plain string) — the harness reads the documented type from there.

import (
	"testing"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// E1: the topic the reader subscribes to is documented by the producer's
// pinned spec, and its message list documents the exact CloudEvents type
// this repo's decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_InboundReceivingEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedInboundReceivingSpec)
	documented := doc.documentedTypes(t, kafka.InboundReceivingTopic)
	if _, ok := documented[usecases.TypeCartonDiscrepancyRaised]; !ok {
		t.Errorf("pinned inbound-receiving spec does not document %s on %s (documented: %v)",
			usecases.TypeCartonDiscrepancyRaised, kafka.InboundReceivingTopic, documented)
	}
}

// cartonKey maps a golden carton payload onto the case key
// OpenCartonDiscrepancy builds: the subject is receipt/sscc|"-"/kind, the
// source type stays the raw CloudEvents type, and the payload carries no
// site.
var cartonKey = factKey{
	subject: func(data map[string]any, _ ce.Event) string {
		return usecases.CartonSubject(dataString(data, "receipt_id"), dataString(data, "sscc"), dataString(data, "kind"))
	},
	site: func(map[string]any) string { return "" },
}

// cartonContractWorld wires the open-or-touch use case the carton
// consumer drives, declaring BOTH route policies (the case's queue/severity/
// SLA come from the kind's route, a policy source type, not the raw type).
func cartonContractWorld(t *testing.T, kinds ...string) (*consumeWorld, *kafka.Consumer) {
	t.Helper()
	routes := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		routes = append(routes, usecases.RouteForCartonKind(kind))
	}
	w := newConsumeWorld(t, routes...)
	open := &usecases.OpenCartonDiscrepancy{Consume: w.consume}
	return w, contractConsumer("carton contract", kafka.InboundReceivingTopic,
		kafka.CartonDiscrepancyHandlers(open, contractLogger()))
}

// E2: the producer's own CartonDiscrepancyRaised example decodes through
// the REAL HandleMessage decode+dispatch and opens exactly one case
// under (type, receipt/sscc|"-"/kind, "") — the payload carries no site;
// a redelivered CloudEvents id creates nothing more. The world declares
// the policies of the example kind's route.
func TestGoldenEventsDecode_CartonDiscrepancyRaised(t *testing.T) {
	kind := dataString(dataOf(t, goldenExamples(t, pinnedInboundReceivingSpec,
		kafka.InboundReceivingTopic, usecases.TypeCartonDiscrepancyRaised)[0].Payload), "kind")
	assertGoldenFact(t, func(t *testing.T) (*consumeWorld, *kafka.Consumer) {
		return cartonContractWorld(t, kind)
	}, pinnedInboundReceivingSpec, kafka.InboundReceivingTopic,
		usecases.TypeCartonDiscrepancyRaised, cartonKey)
}

// E3: the golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — dataschema urn:warehouse:inbound-receiving:events:
// CartonDiscrepancyRaised:vN — and the pinned spec's envelope schema
// requires the full attribute set.
func TestCloudEventsEnvelopeInvariants_CartonDiscrepancyRaised(t *testing.T) {
	for _, ex := range goldenExamples(t, pinnedInboundReceivingSpec, kafka.InboundReceivingTopic, usecases.TypeCartonDiscrepancyRaised) {
		assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "inbound-receiving", "CartonDiscrepancyRaised")
	}
	doc := loadPinnedAsyncAPI(t, pinnedInboundReceivingSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	assertEnvelopeRequired(t, pinnedInboundReceivingSpec, kafka.InboundReceivingTopic, usecases.TypeCartonDiscrepancyRaised)
}

// E4: unknown additive data keys must not change the effect; an unknown
// event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; a CartonDiscrepancyRaised that lost its receipt id is
// the dead-lettered poison refusal.
func TestUnknownAttributesTolerated_InboundReceiving(t *testing.T) {
	kind := dataString(dataOf(t, goldenExamples(t, pinnedInboundReceivingSpec,
		kafka.InboundReceivingTopic, usecases.TypeCartonDiscrepancyRaised)[0].Payload), "kind")
	w, c := cartonContractWorld(t, kind)
	assertUnknownTolerance(t, c, w, pinnedInboundReceivingSpec, kafka.InboundReceivingTopic,
		usecases.TypeCartonDiscrepancyRaised, cartonKey,
		func(data map[string]any) { data["receipt_id"] = "" })
}
