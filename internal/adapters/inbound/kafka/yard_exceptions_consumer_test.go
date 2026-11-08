package kafka_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

const yardSource = "/warehouse/yard-management"

var yardTypes = []string{
	kafka.TypeVisitRejected, kafka.TypeSealExceptionRaised, kafka.TypeTrailerDwellExceeded,
	kafka.TypeYardCheckClosed, kafka.TypeDockAppointmentNoShow,
}

// findCase fetches the case owning (sourceType, subject, site) whatever its
// occurrence count (openCase insists on a first occurrence).
func findCase(t *testing.T, w *consumeWorld, sourceType, subject, site string) *exceptioncase.Case {
	t.Helper()
	key, err := shared.NewDedupKey(sourceType, subject, site)
	if err != nil {
		t.Fatal(err)
	}
	c, err := w.cases.FindOpenByDedupKey(context.Background(), key)
	if err != nil {
		t.Fatalf("no case for (%s, %s, %q): %v", sourceType, subject, site, err)
	}
	return c
}

func yardWorld(t *testing.T, declared ...string) (*consumeWorld, map[string]kafka.HandlerFunc) {
	t.Helper()
	if len(declared) == 0 {
		declared = yardTypes
	}
	w := newConsumeWorld(t, declared...)
	checks := &usecases.OpenYardCheckDiscrepancies{Processed: w.consume.Processed, OpenOrTouch: w.consume.OpenOrTouch}
	return w, kafka.YardExceptionsHandlers(w.consume, checks, slog.Default())
}

// TestYardTypes_AreTheContractStrings pins the consumed type strings to
// contracts-delta.md: a rename is a contract change, not a refactor.
func TestYardTypes_AreTheContractStrings(t *testing.T) {
	want := map[string]string{
		kafka.TypeVisitRejected:         "com.warehouse.wms.yard-management.yardvisit.VisitRejected",
		kafka.TypeSealExceptionRaised:   "com.warehouse.wms.yard-management.trailer.SealExceptionRaised",
		kafka.TypeTrailerDwellExceeded:  "com.warehouse.wms.yard-management.trailer.TrailerDwellExceeded",
		kafka.TypeYardCheckClosed:       "com.warehouse.wms.yard-management.yardcheck.YardCheckClosed",
		kafka.TypeDockAppointmentNoShow: "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentNoShow",
	}
	for got, w := range want {
		if got != w {
			t.Errorf("type = %q, want %q", got, w)
		}
	}
	if kafka.YardManagementTopic != "warehouse.yard-management.events" || kafka.DockSchedulingTopic != "warehouse.dock-scheduling.events" {
		t.Errorf("topics = %q, %q", kafka.YardManagementTopic, kafka.DockSchedulingTopic)
	}
}

func TestYardHandlers_VisitRejectedOpensOnePerVisit(t *testing.T) {
	w, handlers := yardWorld(t)
	h := handlers[kafka.TypeVisitRejected]
	ctx := context.Background()
	payload := `{"visit_id":"visit-1","site_id":"SITE-1","carrier_scac":"ABCD","appointment_id":null,"reason":"SEAL_MISMATCH","rejected_at":"2026-11-05T10:00:00Z"}`
	send := func(id string) {
		t.Helper()
		if err := h(ctx, producerEvent(t, id, yardSource, kafka.TypeVisitRejected, "visit-1", payload)); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	send("e-1")
	send("e-2") // another id for the same visit: touch
	send("e-1") // replay: nothing
	c := findCase(t, w, kafka.TypeVisitRejected, "visit-1", "SITE-1")
	if c.Occurrence() != 2 || c.Queue() != "ps-main" || c.SourceEventID() != "e-1" {
		t.Fatalf("case = occ %d queue %s source %s, want 2 / ps-main / e-1", c.Occurrence(), c.Queue(), c.SourceEventID())
	}
	if !strings.Contains(string(c.SourcePayload()), `"reason":"SEAL_MISMATCH"`) {
		t.Fatalf("frozen payload = %s", c.SourcePayload())
	}
}

func TestYardHandlers_SealAndDwellKeyOnTheTrailerAndTheInstant(t *testing.T) {
	w, handlers := yardWorld(t)
	ctx := context.Background()
	seal := func(id, raisedAt string) {
		t.Helper()
		p := `{"trailer_id":"trl-1","site_id":"SITE-1","appointment_id":"appt-1","expected_seal_number":"S1","found_seal_number":"S2","raised_at":"` + raisedAt + `"}`
		if err := handlers[kafka.TypeSealExceptionRaised](ctx, producerEvent(t, id, yardSource, kafka.TypeSealExceptionRaised, "trl-1", p)); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	seal("s-1", "2026-11-05T10:00:00Z")
	seal("s-2", "2026-11-05T10:00:00Z") // same raise re-published: touch
	seal("s-3", "2026-11-06T08:00:00Z") // a later visit: its own case
	if c := findCase(t, w, kafka.TypeSealExceptionRaised, usecases.TrailerOccurrenceSubject("trl-1", "2026-11-05T10:00:00Z"), "SITE-1"); c.Occurrence() != 2 {
		t.Fatalf("seal occurrence = %d, want 2", c.Occurrence())
	}
	findCase(t, w, kafka.TypeSealExceptionRaised, usecases.TrailerOccurrenceSubject("trl-1", "2026-11-06T08:00:00Z"), "SITE-1")

	// Dwell has no instant in its payload: the CloudEvents time (the
	// helper's 2026-10-21T09:00:00Z) stands in.
	dwell := `{"trailer_id":"trl-1","site_id":"SITE-1","kind":"DRY_VAN","dwell_minutes":3000,"threshold_minutes":2880,"location_code":"SPOT-04"}`
	if err := handlers[kafka.TypeTrailerDwellExceeded](ctx, producerEvent(t, "d-1", yardSource, kafka.TypeTrailerDwellExceeded, "trl-1", dwell)); err != nil {
		t.Fatalf("dwell: %v", err)
	}
	findCase(t, w, kafka.TypeTrailerDwellExceeded, usecases.TrailerOccurrenceSubject("trl-1", "2026-10-21T09:00:00Z"), "SITE-1")
}

// TestYardHandlers_YardCheckClosedOpensOneCasePerDiscrepancy: the id is
// claimed once, every discrepancy opens its own case, a replay of the event
// changes nothing, a clean check opens nothing.
func TestYardHandlers_YardCheckClosedOpensOneCasePerDiscrepancy(t *testing.T) {
	w, handlers := yardWorld(t)
	h := handlers[kafka.TypeYardCheckClosed]
	ctx := context.Background()
	payload := `{"yard_check_id":"yck-1","site_id":"SITE-1","closed_at":"2026-11-05T11:00:00Z","discrepancies":[
		{"kind":"MISSING","location_code":"SPOT-01","trailer_id":"trl-1","unit_number":"TR100"},
		{"kind":"UNEXPECTED","location_code":"SPOT-02","trailer_id":null,"unit_number":"TR200"},
		{"kind":"WRONG_LOCATION","location_code":"SPOT-03","trailer_id":"trl-3","unit_number":null},
		{"kind":"SOMETHING_NEW","location_code":"SPOT-04"}]}`
	for i := 0; i < 2; i++ { // the second delivery is a replay of the same id
		if err := h(ctx, producerEvent(t, "y-1", yardSource, kafka.TypeYardCheckClosed, "yck-1", payload)); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	for _, sub := range []string{
		"yck-1/MISSING/SPOT-01/trl-1", "yck-1/UNEXPECTED/SPOT-02/TR200",
		"yck-1/WRONG_LOCATION/SPOT-03/trl-3", "yck-1/SOMETHING_NEW/SPOT-04/-",
	} {
		c := findCase(t, w, kafka.TypeYardCheckClosed, sub, "SITE-1")
		if c.Occurrence() != 1 || c.SourceEventID() != "y-1" {
			t.Fatalf("%s: occ %d source %s, want 1 / y-1", sub, c.Occurrence(), c.SourceEventID())
		}
		if !strings.Contains(string(c.SourcePayload()), `"yard_check_id":"yck-1"`) || !strings.Contains(string(c.SourcePayload()), `"location_code"`) {
			t.Fatalf("%s: frozen payload = %s", sub, c.SourcePayload())
		}
	}
	if n := len(w.events.CaseEventNames()); n != 4 {
		t.Fatalf("events = %d, want 4 ExceptionOpened (replay opens none)", n)
	}

	clean := `{"yard_check_id":"yck-2","site_id":"SITE-1","closed_at":"2026-11-05T12:00:00Z","discrepancies":[]}`
	if err := h(ctx, producerEvent(t, "y-2", yardSource, kafka.TypeYardCheckClosed, "yck-2", clean)); err != nil {
		t.Fatalf("clean check: %v", err)
	}
	if n := len(w.events.CaseEventNames()); n != 4 {
		t.Fatalf("a clean check opened cases: %d events", n)
	}
}

func TestYardHandlers_PoisonAndUndeclared(t *testing.T) {
	_, handlers := yardWorld(t)
	ctx := context.Background()
	poison := map[string]string{
		kafka.TypeVisitRejected:        `{"site_id":"SITE-1","reason":"SITE_REQUEST"}`,
		kafka.TypeSealExceptionRaised:  `{"site_id":"SITE-1","found_seal_number":"S2"}`,
		kafka.TypeTrailerDwellExceeded: `{"site_id":"SITE-1","dwell_minutes":1}`,
		kafka.TypeYardCheckClosed:      `{"site_id":"SITE-1","discrepancies":[]}`,
	}
	for typ, p := range poison {
		if err := handlers[typ](ctx, producerEvent(t, "e-bad", yardSource, typ, "x", p)); err == nil {
			t.Errorf("%s: a payload without its natural id must be refused", typ)
		}
	}
	// A discrepancy without a kind or location poisons the WHOLE check.
	bad := `{"yard_check_id":"yck-9","site_id":"SITE-1","discrepancies":[{"kind":"MISSING","location_code":"SPOT-1"},{"kind":"","location_code":"SPOT-2"}]}`
	if err := handlers[kafka.TypeYardCheckClosed](ctx, producerEvent(t, "e-bad2", yardSource, kafka.TypeYardCheckClosed, "yck-9", bad)); err == nil {
		t.Error("a discrepancy without a kind must be refused")
	}

	_, bare := yardWorld(t, "com.warehouse.wms.other.thing.Declared")
	good := `{"visit_id":"visit-1","site_id":"SITE-1","reason":"SITE_REQUEST"}`
	if err := bare[kafka.TypeVisitRejected](ctx, producerEvent(t, "e-1", yardSource, kafka.TypeVisitRejected, "visit-1", good)); err == nil {
		t.Error("an undeclared policy must be a skip error, not success")
	}
	check := `{"yard_check_id":"yck-1","site_id":"SITE-1","discrepancies":[{"kind":"MISSING","location_code":"SPOT-1"}]}`
	if err := bare[kafka.TypeYardCheckClosed](ctx, producerEvent(t, "e-2", yardSource, kafka.TypeYardCheckClosed, "yck-1", check)); err == nil {
		t.Error("an undeclared yard check policy must be a skip error, not success")
	}
}

// TestYardCheckClosed_AllDiscrepanciesValidatedBeforeTheClaim: a closure with
// one bad discrepancy is refused whole BEFORE the id is claimed, so a
// corrected redelivery of the same id still opens every case. (The rollback
// of a mid-way failure is proved over Postgres in the integration test.)
func TestYardCheckClosed_AllDiscrepanciesValidatedBeforeTheClaim(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeYardCheckClosed)
	uc := &usecases.OpenYardCheckDiscrepancies{Processed: w.consume.Processed, OpenOrTouch: w.consume.OpenOrTouch}
	_, _, err := uc.Execute(context.Background(), usecases.YardCheckClosure{
		EventID: "y-1", YardCheckID: "yck-1", SiteID: "SITE-1",
		Discrepancies: []usecases.YardCheckDiscrepancy{{Kind: "MISSING", LocationCode: "SPOT-1"}, {Kind: "MISSING", LocationCode: " "}},
	})
	if err == nil {
		t.Fatal("a blank location must be refused")
	}
	n, outcome, err := uc.Execute(context.Background(), usecases.YardCheckClosure{
		EventID: "y-1", YardCheckID: "yck-1", SiteID: "SITE-1",
		Discrepancies: []usecases.YardCheckDiscrepancy{{Kind: "MISSING", LocationCode: "SPOT-1"}},
	})
	if err != nil || n != 1 || outcome != usecases.OutcomeApplied {
		t.Fatalf("retry = %d %s %v, want 1 applied (the refusal must not have claimed the id)", n, outcome, err)
	}
}

func TestDockSchedulingHandlers_NoShowOpensOneCasePerAppointment(t *testing.T) {
	w := newConsumeWorld(t, kafka.TypeDockAppointmentNoShow)
	h := kafka.DockSchedulingExceptionsHandlers(w.consume, slog.Default())[kafka.TypeDockAppointmentNoShow]
	ctx := context.Background()
	payload := `{"appointment_id":"appt-1","site_id":"SITE-1","door_code":"DOOR-03","direction":"INBOUND","carrier_scac":"ABCD","window_end":"2026-11-05T10:00:00Z","version":4}`
	for _, id := range []string{"n-1", "n-2", "n-1"} {
		if err := h(ctx, producerEvent(t, id, "/warehouse/dock-scheduling", kafka.TypeDockAppointmentNoShow, "appt-1", payload)); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	c := findCase(t, w, kafka.TypeDockAppointmentNoShow, "appt-1", "SITE-1")
	if c.Occurrence() != 2 || c.SourceEventID() != "n-1" {
		t.Fatalf("case = occ %d source %s, want 2 / n-1", c.Occurrence(), c.SourceEventID())
	}
	bad := producerEvent(t, "n-bad", "/warehouse/dock-scheduling", kafka.TypeDockAppointmentNoShow, "x", `{"site_id":"SITE-1"}`)
	if err := h(ctx, bad); err == nil {
		t.Fatal("a payload without appointment_id must be refused")
	}
	bare := newConsumeWorld(t)
	hb := kafka.DockSchedulingExceptionsHandlers(bare.consume, slog.Default())[kafka.TypeDockAppointmentNoShow]
	if err := hb(ctx, producerEvent(t, "n-9", "/warehouse/dock-scheduling", kafka.TypeDockAppointmentNoShow, "appt-9", payload)); err == nil {
		t.Fatal("an undeclared policy must be a skip error")
	}
}

// Other types on the two topics are ignored, and a legacy flat message is
// committed past (fleet rule: a legacy-flat-message-rejected test per consumer).
func TestYardAndDockConsumers_IgnoreOtherTypesAndRejectFlatMessages(t *testing.T) {
	w, handlers := yardWorld(t)
	dock := kafka.DockSchedulingExceptionsHandlers(w.consume, slog.Default())
	for name, hs := range map[string]map[string]kafka.HandlerFunc{"yard": handlers, "dock": dock} {
		c := consumerWith(hs)
		other := producerEvent(t, "o-1", yardSource, "com.warehouse.wms.yard-management.trailer.TrailerParked", "trl-1", `{"trailer_id":"trl-1"}`)
		if err := c.HandleMessage(context.Background(), encode(t, other)); err != nil {
			t.Fatalf("%s: other type = %v, want nil", name, err)
		}
		flat := []byte(`{"event_id":"e-flat","event_type":"VisitRejected","occurred_at":"2026-11-05T09:00:00Z"}`)
		if err := c.HandleMessage(context.Background(), flat); err != nil {
			t.Fatalf("%s: flat envelope = %v, want nil (WARN + commit past)", name, err)
		}
	}
	if n := len(w.events.CaseEventNames()); n != 0 {
		t.Fatalf("ignored messages opened %d cases", n)
	}
}
