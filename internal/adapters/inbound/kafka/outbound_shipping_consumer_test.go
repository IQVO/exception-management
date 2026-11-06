package kafka_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// missedLoadPayload is the pinned PackageMissedLoad v1 payload of one
// package, as outbound-shipping ships it (snake_case; source_order_id
// omitted when the manifest carried none).
func missedLoadPayload(pkg, load, site, order string) string {
	orderField := ""
	if order != "" {
		orderField = `"source_order_id":"` + order + `",`
	}
	return `{"package_id":"` + pkg + `","load_id":"` + load + `","site_id":"` + site + `",` +
		orderField + `"cutoff":"2026-11-06T16:00:00Z","detected_at":"2026-11-06T16:00:00Z","version":2}`
}

// openCaseCount is the number of open cases the world holds (the memory
// repo's whole-list view).
func openCaseCount(t *testing.T, w *consumeWorld) int {
	t.Helper()
	open, err := w.cases.ListOpen(context.Background(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	return len(open)
}

// missedCase fetches the case of (type, package, site) without the
// first-occurrence assertions of openCase (the touch test reuses it).
func missedCase(t *testing.T, w *consumeWorld, pkg, site string) *exceptioncase.Case {
	t.Helper()
	key, err := shared.NewDedupKey(kafka.TypePackageMissedLoad, pkg, site)
	if err != nil {
		t.Fatal(err)
	}
	c, err := w.cases.FindOpenByDedupKey(context.Background(), key)
	if err != nil {
		t.Fatalf("no case for (%s, %s, %q): %v", kafka.TypePackageMissedLoad, pkg, site, err)
	}
	return c
}

// TestOutboundShippingHandlers_OpenOneCasePerMissedPackage: one
// PackageMissedLoad opens one problem-solve case keyed on (type,
// package_id, site_id), freezing the producer payload — the package, load
// and order references the operator works from.
func TestOutboundShippingHandlers_OpenOneCasePerMissedPackage(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypePackageMissedLoad)
	handlers := kafka.OutboundShippingHandlers(w.consume, slog.Default())

	e := producerEvent(t, "m-1", "/warehouse/outbound-shipping",
		kafka.TypePackageMissedLoad, "pkg-2055", missedLoadPayload("pkg-2055", "load-7f3a", "SITE-1", "order-88423"))
	if err := handlers[kafka.TypePackageMissedLoad](context.Background(), e); err != nil {
		t.Fatalf("PackageMissedLoad: %v", err)
	}
	c := openCase(t, w, kafka.TypePackageMissedLoad, "pkg-2055", "SITE-1")
	if c.SourceEventID() != "m-1" {
		t.Fatalf("frozen source event = %s, want m-1", c.SourceEventID())
	}
	for _, ref := range []string{`"package_id":"pkg-2055"`, `"load_id":"load-7f3a"`, `"source_order_id":"order-88423"`, `"cutoff":"2026-11-06T16:00:00Z"`} {
		if !strings.Contains(string(c.SourcePayload()), ref) {
			t.Fatalf("frozen payload %q lacks the operator reference %s", c.SourcePayload(), ref)
		}
	}
	if n := openCaseCount(t, w); n != 1 {
		t.Fatalf("open cases = %d, want 1", n)
	}
}

// TestOutboundShippingHandlers_DuplicateDeliveryOpensNoSecondCase: the
// same CloudEvents id again (even tampered) is deduped on the id, and a
// re-flag of the same package under a NEW id (another load) TOUCHES the
// open case — never a second case.
func TestOutboundShippingHandlers_DuplicateDeliveryOpensNoSecondCase(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypePackageMissedLoad)
	handlers := kafka.OutboundShippingHandlers(w.consume, slog.Default())
	ctx := context.Background()

	first := producerEvent(t, "m-1", "/warehouse/outbound-shipping",
		kafka.TypePackageMissedLoad, "pkg-2055", missedLoadPayload("pkg-2055", "load-7f3a", "SITE-1", "order-88423"))
	if err := handlers[kafka.TypePackageMissedLoad](ctx, first); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	replay := producerEvent(t, "m-1", "/warehouse/outbound-shipping",
		kafka.TypePackageMissedLoad, "pkg-2055", missedLoadPayload("pkg-2055", "load-TAMPERED", "SITE-1", ""))
	if err := handlers[kafka.TypePackageMissedLoad](ctx, replay); err != nil {
		t.Fatalf("replayed id: %v", err)
	}
	c := missedCase(t, w, "pkg-2055", "SITE-1")
	if c.Occurrence() != 1 || !strings.Contains(string(c.SourcePayload()), `"load_id":"load-7f3a"`) {
		t.Fatalf("replayed id changed the case: occ=%d payload=%s", c.Occurrence(), c.SourcePayload())
	}

	// The same package misses its NEXT load: a new id, the same natural
	// key — a touch, not a second case.
	reflag := producerEvent(t, "m-2", "/warehouse/outbound-shipping",
		kafka.TypePackageMissedLoad, "pkg-2055", missedLoadPayload("pkg-2055", "load-9999", "SITE-1", "order-88423"))
	if err := handlers[kafka.TypePackageMissedLoad](ctx, reflag); err != nil {
		t.Fatalf("re-flag under a new id: %v", err)
	}
	c = missedCase(t, w, "pkg-2055", "SITE-1")
	if c.Occurrence() != 2 {
		t.Fatalf("re-flag occurrence = %d, want 2 (a touch)", c.Occurrence())
	}
	if n := openCaseCount(t, w); n != 1 {
		t.Fatalf("open cases = %d, want 1 (a touch, never a second case)", n)
	}
}

// TestOutboundShippingHandlers_IgnoreUnknownTypes: another type on
// outbound-shipping's topic (ShipmentConfirmed) has no handler, so it is
// ignored — no case, no error.
func TestOutboundShippingHandlers_IgnoreUnknownTypes(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypePackageMissedLoad)
	c := &kafka.Consumer{
		Name: "outbound shipping consumer", Topic: kafka.OutboundShippingTopic,
		Handlers: kafka.OutboundShippingHandlers(w.consume, slog.Default()), Logger: slog.Default(),
	}
	other := producerEvent(t, "s-1", "/warehouse/outbound-shipping",
		"com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed", "load-7f3a",
		`{"load_id":"load-7f3a","site_id":"SITE-1"}`)
	if err := c.HandleMessage(context.Background(), encode(t, other)); err != nil {
		t.Fatalf("unknown type = %v, want nil", err)
	}
	if n := openCaseCount(t, w); n != 0 {
		t.Fatalf("open cases = %d, want 0 (an unknown type is ignored)", n)
	}
}

// TestOutboundShippingHandlers_MalformedFactsAreDeadLettered: a payload
// of the handled type that can never be applied — data that does not
// decode, or no package_id to key the case — is poison: returned to the
// run loop, which dead-letters it and commits past (the integration test
// proves the DLQ round trip). Neither opens a case.
func TestOutboundShippingHandlers_MalformedFactsAreDeadLettered(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypePackageMissedLoad)
	dlq := &recordingDLQ{}
	c := &kafka.Consumer{
		Name: "outbound shipping consumer", Topic: kafka.OutboundShippingTopic,
		Handlers: kafka.OutboundShippingHandlers(w.consume, slog.Default()), Logger: slog.Default(), DLQ: dlq,
	}

	// A `data` that is a JSON string cannot decode into the payload
	// struct: poison.
	undecodable := []byte(`{"specversion":"1.0","id":"m-bad","source":"/warehouse/outbound-shipping","type":"` +
		kafka.TypePackageMissedLoad + `","subject":"pkg-x","datacontenttype":"application/json","data":"not-an-object"}`)
	if err := c.HandleMessage(context.Background(), undecodable); err == nil {
		t.Fatal("an undecodable payload must be returned to the run loop for the dead letter")
	}

	// A payload with no package_id has no natural id to key a case:
	// poison.
	noPackage := producerEvent(t, "m-blank", "/warehouse/outbound-shipping",
		kafka.TypePackageMissedLoad, "pkg-x",
		`{"load_id":"load-7f3a","site_id":"SITE-1","cutoff":"2026-11-06T16:00:00Z","detected_at":"2026-11-06T16:00:00Z","version":2}`)
	if err := c.HandleMessage(context.Background(), encode(t, noPackage)); err == nil {
		t.Fatal("a payload with no package_id must be returned to the run loop for the dead letter")
	}
	if n := openCaseCount(t, w); n != 0 {
		t.Fatalf("open cases = %d, want 0", n)
	}
}
