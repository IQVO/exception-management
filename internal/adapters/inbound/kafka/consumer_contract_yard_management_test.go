package kafka_test

// Event consumer contract tests (E1-E4) for the yard-management edge:
// the yard exceptions consumer reads yard-management's shared topic
// warehouse.yard-management.events (kafka.YardManagementTopic) and acts
// on yardvisit.VisitRejected, trailer.SealExceptionRaised,
// trailer.TrailerDwellExceeded and yardcheck.YardCheckClosed (one case
// per discrepancy of a closed check; a clean check opens none). The
// contract is the PINNED copy of yard-management's apis/asyncapi.yaml
// (testdata/yard-management.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's own
// examples.
//
// NOTE spec shape: yard-management keys its channels by NAME with the
// topic in an `address` field and lists messages as a channel-level map
// (no subscribe:/publish: operations) — the harness resolves the channel
// by address.

import (
	"context"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// yardKeys maps the three fact-per-occurrence types to their case keys.
// The trailer-level subjects carry the raise instant: SealExceptionRaised
// uses the payload's own raised_at, TrailerDwellExceeded the CloudEvents
// time (the payload has no instant of its own).
var yardKeys = map[string]factKey{
	kafka.TypeVisitRejected: keyBy("visit_id", "site_id"),
	kafka.TypeSealExceptionRaised: {
		subject: func(data map[string]any, e ce.Event) string {
			return usecases.TrailerOccurrenceSubject(dataString(data, "trailer_id"), occurredAt(dataString(data, "raised_at"), e))
		},
		site: func(data map[string]any) string { return dataString(data, "site_id") },
	},
	kafka.TypeTrailerDwellExceeded: {
		subject: func(data map[string]any, e ce.Event) string {
			return usecases.TrailerOccurrenceSubject(dataString(data, "trailer_id"), e.Time().UTC().Format(time.RFC3339Nano))
		},
		site: func(data map[string]any) string { return dataString(data, "site_id") },
	},
}

// E1: the topic the reader subscribes to is documented by the producer's
// pinned spec, and its message list documents every type this repo's
// decoder routes on (the channel carries thirteen — yard-management is
// analytics-heavy; the rest stay ignored).
func TestPinnedAsyncAPICoversSubscriptions_YardManagementEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedYardManagementSpec)
	documented := doc.documentedTypes(t, kafka.YardManagementTopic)
	for typ := range yardKeys {
		if _, ok := documented[typ]; !ok {
			t.Errorf("pinned yard-management spec does not document %s on %s (documented: %v)",
				typ, kafka.YardManagementTopic, documented)
		}
	}
	if _, ok := documented[kafka.TypeYardCheckClosed]; !ok {
		t.Errorf("pinned yard-management spec does not document %s on %s (documented: %v)",
			kafka.TypeYardCheckClosed, kafka.YardManagementTopic, documented)
	}
}

// E2: every routed fact type's own example decodes through the REAL
// HandleMessage decode+dispatch and opens exactly one case under its
// occurrence subject; a redelivered CloudEvents id creates nothing more.
func TestGoldenEventsDecode_YardExceptions(t *testing.T) {
	for typ, key := range yardKeys {
		assertGoldenFact(t, func(t *testing.T) (*consumeWorld, *kafka.Consumer) {
			w, handlers := yardWorld(t, typ)
			return w, contractConsumer("yard contract", kafka.YardManagementTopic, handlers)
		}, pinnedYardManagementSpec, kafka.YardManagementTopic, typ, key)
	}
}

// E2 (yard check): the producer's own YardCheckClosed example opens one
// case per discrepancy it carries — the check, its kind, the location
// and the trailer keying each case; a redelivered CloudEvents id
// re-applies the whole check and never doubles it.
func TestGoldenEventsDecode_YardCheckClosed(t *testing.T) {
	w, handlers := yardWorld(t, kafka.TypeYardCheckClosed)
	c := contractConsumer("yard contract", kafka.YardManagementTopic, handlers)
	for _, ex := range goldenExamples(t, pinnedYardManagementSpec, kafka.YardManagementTopic, kafka.TypeYardCheckClosed) {
		t.Run(ex.Name, func(t *testing.T) {
			raw := goldenJSON(t, ex.Payload)
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			data := dataOf(t, ex.Payload)
			site := dataString(data, "site_id")
			discrepancies, _ := data["discrepancies"].([]any)
			for _, raw := range discrepancies {
				d, _ := raw.(map[string]any)
				findCase(t, w, kafka.TypeYardCheckClosed,
					usecases.YardCheckSubject(dataString(data, "yard_check_id"), yardCheckDiscrepancy(t, d)), site)
			}
			before := openCaseCount(t, w)
			if before != len(discrepancies) {
				t.Errorf("the check opened %d cases, its payload carries %d discrepancies", before, len(discrepancies))
			}
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("redelivery: %v", err)
			}
			if after := openCaseCount(t, w); after != before {
				t.Errorf("replayed CloudEvents id changed the open-case set: %d -> %d", before, after)
			}
		})
	}
}

// yardCheckDiscrepancy maps one golden discrepancy object onto the
// producer's documented shape (nullable trailer_id / unit_number).
func yardCheckDiscrepancy(t *testing.T, d map[string]any) usecases.YardCheckDiscrepancy {
	t.Helper()
	out := usecases.YardCheckDiscrepancy{
		Kind:         dataString(d, "kind"),
		LocationCode: dataString(d, "location_code"),
	}
	if v, ok := d["trailer_id"].(string); ok {
		out.TrailerID = &v
	}
	if v, ok := d["unit_number"].(string); ok {
		out.UnitNumber = &v
	}
	return out
}

// E3: every routed type's golden fixture carries the fleet-mandatory
// CloudEvents 1.0 envelope, and the pinned spec's envelope schemas
// require the full attribute set.
func TestCloudEventsEnvelopeInvariants_YardManagement(t *testing.T) {
	for _, typ := range []string{kafka.TypeVisitRejected, kafka.TypeSealExceptionRaised, kafka.TypeTrailerDwellExceeded, kafka.TypeYardCheckClosed} {
		for _, ex := range goldenExamples(t, pinnedYardManagementSpec, kafka.YardManagementTopic, typ) {
			assertEnvelope(t, ex.Name, goldenJSON(t, ex.Payload), "yard-management", eventNameOf(typ))
		}
		assertEnvelopeRequired(t, pinnedYardManagementSpec, kafka.YardManagementTopic, typ)
	}
	doc := loadPinnedAsyncAPI(t, pinnedYardManagementSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
}

// E4: unknown additive data keys must not change the effect; an unknown
// event type is silently skipped; a non-CloudEvent is WARNed and
// committed past; a VisitRejected that lost its visit_id is the
// dead-lettered poison refusal.
func TestUnknownAttributesTolerated_YardManagement(t *testing.T) {
	w, handlers := yardWorld(t, kafka.TypeVisitRejected)
	c := contractConsumer("yard contract", kafka.YardManagementTopic, handlers)
	assertUnknownTolerance(t, c, w, pinnedYardManagementSpec, kafka.YardManagementTopic,
		kafka.TypeVisitRejected, keyBy("visit_id", "site_id"),
		func(data map[string]any) { data["visit_id"] = "" })
}
