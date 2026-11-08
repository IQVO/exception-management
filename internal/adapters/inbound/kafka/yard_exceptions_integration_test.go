//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/testing/kafkatest"
)

// TestYardExceptions_RealKafkaAndPostgres feeds REAL CloudEvents (the
// yard-management envelope and snake_case payloads) through a real Kafka
// topic into the consumer over real Postgres (migrated, so the routing
// policies come from migration 0004, not from the test):
//  1. VisitRejected opens a yard-supervisor case; its replay (tampered) is deduped on the id
//  2. SealExceptionRaised opens a HIGH case; the same raise under another id touches it
//  3. TrailerDwellExceeded opens a case
//  4. YardCheckClosed opens ONE case per discrepancy under ONE claimed id; a clean check opens none
//  5. another type of the topic and a legacy flat message are ignored
//  6. poison payloads (no visit_id; a discrepancy without a kind) are dead-lettered, opening nothing
func TestYardExceptions_RealKafkaAndPostgres(t *testing.T) {
	w := newPGWorld(t)
	topic := kafkatest.Topic(t, kafka.YardManagementTopic)
	dlq := topic + kafka.DLQSuffix
	kafkatest.CreateTopic(t, dlq)

	const src = "/warehouse/yard-management"
	visit, trailer, check, cleanCheck, sentinel := itID("visit"), itID("trl"), itID("yck"), itID("yck"), itID("visit")
	const raisedAt = "2026-11-05T08:30:00Z"
	rejected := func(v string) string {
		return `{"visit_id":"` + v + `","site_id":"SITE-1","carrier_scac":"ABCD","appointment_id":null,"reason":"SEAL_MISMATCH","rejected_at":"2026-11-05T08:00:00Z"}`
	}
	seal := `{"trailer_id":"` + trailer + `","site_id":"SITE-1","appointment_id":"appt-1","expected_seal_number":"S1","found_seal_number":"S2","raised_at":"` + raisedAt + `"}`
	dwell := `{"trailer_id":"` + trailer + `","site_id":"SITE-1","kind":"DRY_VAN","dwell_minutes":3000,"threshold_minutes":2880,"location_code":"SPOT-04"}`
	closed := `{"yard_check_id":"` + check + `","site_id":"SITE-1","closed_at":"2026-11-05T11:00:00Z","discrepancies":[
		{"kind":"MISSING","location_code":"SPOT-01","trailer_id":"trl-a","unit_number":"TR100"},
		{"kind":"UNEXPECTED","location_code":"SPOT-02","trailer_id":null,"unit_number":"TR200"},
		{"kind":"WRONG_LOCATION","location_code":"SPOT-03","trailer_id":"trl-c","unit_number":null}]}`
	clean := `{"yard_check_id":"` + cleanCheck + `","site_id":"SITE-1","closed_at":"2026-11-05T12:00:00Z","discrepancies":[]}`
	messages := keyed("k",
		produced(t, "y-1", src, kafka.TypeVisitRejected, visit, rejected(visit)),
		produced(t, "y-1", src, kafka.TypeVisitRejected, visit, rejected("tampered")),
		produced(t, "y-2", src, kafka.TypeSealExceptionRaised, trailer, seal),
		produced(t, "y-3", src, kafka.TypeSealExceptionRaised, trailer, seal),
		produced(t, "y-4", src, kafka.TypeTrailerDwellExceeded, trailer, dwell),
		produced(t, "y-5", src, kafka.TypeYardCheckClosed, check, closed),
		produced(t, "y-6", src, kafka.TypeYardCheckClosed, cleanCheck, clean),
		produced(t, "y-7", src, "com.warehouse.wms.yard-management.trailer.TrailerParked", trailer, `{"trailer_id":"`+trailer+`"}`),
		[]byte(legacyFlat),
		produced(t, "y-8", src, kafka.TypeVisitRejected, "x", `{"site_id":"SITE-1","reason":"SITE_REQUEST"}`),
		produced(t, "y-9", src, kafka.TypeYardCheckClosed, "yck-bad", `{"yard_check_id":"yck-bad","site_id":"SITE-1","discrepancies":[{"kind":"","location_code":"SPOT-1"}]}`),
		produced(t, "y-10", src, kafka.TypeVisitRejected, sentinel, rejected(sentinel)), // sentinel
	)
	kafkatest.Produce(t, topic, messages...)

	group := theGroup("yard-exceptions")
	checks := &usecases.OpenYardCheckDiscrepancies{Processed: w.consume.Processed, OpenOrTouch: w.consume.OpenOrTouch, UnitOfWork: w.consume.UnitOfWork}
	runConsumer(t, kafka.NewYardExceptionsConsumerForTopic(kafkatest.Brokers(t), topic, group, w.consume, checks, itLogger(t)))

	eventually(t, "the sentinel case to open", func() bool {
		_, err := w.lookupCase(t, usecases.TypeVisitRejected, sentinel, "SITE-1")
		return err == nil
	})
	assertCommittedPast(t, group, topic, int64(len(messages)))

	rej := w.openCase(t, usecases.TypeVisitRejected, visit, "SITE-1")
	if rej.Status() != exceptioncase.StatusOpen || rej.Queue() != "yard-supervisor" || rej.SLAMinutes() != 120 || string(rej.Severity()) != "MEDIUM" || rej.Occurrence() != 1 {
		t.Fatalf("VisitRejected case = %s %s sla=%d %s occ=%d, want OPEN yard-supervisor 120 MEDIUM 1", rej.Status(), rej.Queue(), rej.SLAMinutes(), rej.Severity(), rej.Occurrence())
	}
	if strings.Contains(string(rej.SourcePayload()), "tampered") {
		t.Fatalf("the replayed id leaked into the frozen payload: %s", rej.SourcePayload())
	}
	sealCase, err := w.lookupCase(t, usecases.TypeSealExceptionRaised, usecases.TrailerOccurrenceSubject(trailer, raisedAt), "SITE-1")
	if err != nil || string(sealCase.Severity()) != "HIGH" || sealCase.SLAMinutes() != 30 || sealCase.Occurrence() != 2 {
		t.Fatalf("seal case = %v %v, want HIGH 30 occurrence 2", sealCase, err)
	}
	// Dwell carries no instant in its payload: the CloudEvents time of the
	// helper's envelope is the instant in the key.
	dwellCase, err := w.lookupCase(t, usecases.TypeTrailerDwellExceeded, usecases.TrailerOccurrenceSubject(trailer, "2026-10-21T09:00:00Z"), "SITE-1")
	if err != nil || dwellCase.Queue() != "yard-supervisor" || dwellCase.SLAMinutes() != 240 {
		t.Fatalf("dwell case = %v %v, want yard-supervisor 240", dwellCase, err)
	}
	for _, sub := range []string{
		check + "/MISSING/SPOT-01/trl-a", check + "/UNEXPECTED/SPOT-02/TR200", check + "/WRONG_LOCATION/SPOT-03/trl-c",
	} {
		c := w.openCase(t, usecases.TypeYardCheckClosed, sub, "SITE-1")
		if c.Queue() != "yard-supervisor" || c.SLAMinutes() != 480 || c.SourceEventID() != "y-5" {
			t.Fatalf("%s: %s sla=%d source=%s, want yard-supervisor 480 y-5", sub, c.Queue(), c.SLAMinutes(), c.SourceEventID())
		}
	}
	// visit, sentinel, seal, dwell, three discrepancies: seven cases, none from the clean check or the poison.
	if n := w.countTable("exception_cases"); n != 7 {
		t.Fatalf("cases = %d, want 7", n)
	}
	// y-1, y-2, y-3, y-4, y-5, y-6, y-10 claimed (the clean check claims too);
	// the replay, the ignored type, the flat message and both poisons claim nothing.
	if n := w.countTable("processed_events"); n != 7 {
		t.Fatalf("processed_events = %d, want 7", n)
	}
	dead := kafkatest.ReadN(t, dlq, 2, 90*time.Second)
	got := string(dead[0].Value) + string(dead[1].Value)
	if !strings.Contains(got, `"id":"y-8"`) || !strings.Contains(got, `"id":"y-9"`) || dead[0].Headers["x-dlq-source-topic"] != topic {
		t.Fatalf("dead letters = %s %v, want the poisons y-8 and y-9", got, dead[0].Headers)
	}
}

// TestDockSchedulingNoShow_RealKafkaAndPostgres: DockAppointmentNoShow opens
// a dock-planner case from migration 0004's policy; the same appointment
// under another id touches it; the other types of dock-scheduling's topic
// are ignored; a payload without appointment_id is dead-lettered.
func TestDockSchedulingNoShow_RealKafkaAndPostgres(t *testing.T) {
	w := newPGWorld(t)
	topic := kafkatest.Topic(t, kafka.DockSchedulingTopic)
	dlq := topic + kafka.DLQSuffix
	kafkatest.CreateTopic(t, dlq)

	const src = "/warehouse/dock-scheduling"
	appt, sentinel := itID("appt"), itID("appt")
	noShow := func(a string) string {
		return `{"appointment_id":"` + a + `","site_id":"SITE-1","door_code":"DOOR-03","direction":"INBOUND","carrier_scac":"ABCD","window_end":"2026-11-05T10:00:00Z","version":4}`
	}
	messages := keyed("k",
		produced(t, "n-1", src, kafka.TypeDockAppointmentNoShow, appt, noShow(appt)),
		produced(t, "n-2", src, kafka.TypeDockAppointmentNoShow, appt, noShow(appt)),
		produced(t, "n-1", src, kafka.TypeDockAppointmentNoShow, appt, noShow("tampered")),
		produced(t, "n-3", src, "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentBooked", appt, `{"appointment_id":"`+appt+`"}`),
		produced(t, "n-4", src, kafka.TypeDockAppointmentNoShow, "x", `{"site_id":"SITE-1","door_code":"DOOR-03"}`),
		produced(t, "n-5", src, kafka.TypeDockAppointmentNoShow, sentinel, noShow(sentinel)),
	)
	kafkatest.Produce(t, topic, messages...)

	group := theGroup("dock-scheduling-exceptions")
	runConsumer(t, kafka.NewDockSchedulingExceptionsConsumerForTopic(kafkatest.Brokers(t), topic, group, w.consume, itLogger(t)))
	eventually(t, "the sentinel case to open", func() bool {
		_, err := w.lookupCase(t, usecases.TypeDockAppointmentNoShow, sentinel, "SITE-1")
		return err == nil
	})
	assertCommittedPast(t, group, topic, int64(len(messages)))

	c := w.openCase(t, usecases.TypeDockAppointmentNoShow, appt, "SITE-1")
	if c.Status() != exceptioncase.StatusOpen || c.Queue() != "dock-planner" || c.SLAMinutes() != 240 || string(c.Severity()) != "MEDIUM" || c.Occurrence() != 2 {
		t.Fatalf("no-show case = %s %s sla=%d %s occ=%d, want OPEN dock-planner 240 MEDIUM 2", c.Status(), c.Queue(), c.SLAMinutes(), c.Severity(), c.Occurrence())
	}
	if c.SourceEventID() != "n-1" || strings.Contains(string(c.SourcePayload()), "tampered") {
		t.Fatalf("frozen source = %s %s, want n-1 untampered", c.SourceEventID(), c.SourcePayload())
	}
	// appointment + sentinel: two cases; n-1, n-2, n-5 claimed.
	if n := w.countTable("exception_cases"); n != 2 {
		t.Fatalf("cases = %d, want 2", n)
	}
	if n := w.countTable("processed_events"); n != 3 {
		t.Fatalf("processed_events = %d, want 3", n)
	}
	dead := kafkatest.ReadN(t, dlq, 1, 90*time.Second)
	if !strings.Contains(string(dead[0].Value), `"id":"n-4"`) || dead[0].Headers["x-dlq-source-topic"] != topic {
		t.Fatalf("dead letter = %s %v, want the poison n-4", dead[0].Value, dead[0].Headers)
	}
}

// TestYardCheckClosure_RollsBackWhole: when a discrepancy cannot open (here:
// the policy of the type was never declared) the unit of work rolls back, so
// the CloudEvents id is NOT left claimed and no case is half-opened: a
// redelivery after the policy is declared opens every case.
func TestYardCheckClosure_RollsBackWhole(t *testing.T) {
	w := newPGWorld(t)
	ctx := context.Background()
	if _, err := w.pool.Exec(ctx, `DELETE FROM exception_policies WHERE source_type = $1`, usecases.TypeYardCheckClosed); err != nil {
		t.Fatal(err)
	}
	uc := &usecases.OpenYardCheckDiscrepancies{Processed: w.consume.Processed, OpenOrTouch: w.consume.OpenOrTouch, UnitOfWork: w.consume.UnitOfWork}
	closure := usecases.YardCheckClosure{
		EventID: "y-1", YardCheckID: "yck-1", SiteID: "SITE-1",
		Discrepancies: []usecases.YardCheckDiscrepancy{{Kind: "MISSING", LocationCode: "SPOT-1"}, {Kind: "UNEXPECTED", LocationCode: "SPOT-2"}},
	}
	if _, _, err := uc.Execute(ctx, closure); !errors.Is(err, usecases.ErrPolicyUndeclared) {
		t.Fatalf("err = %v, want ErrPolicyUndeclared", err)
	}
	if c, p := w.countTable("exception_cases"), w.countTable("processed_events"); c != 0 || p != 0 {
		t.Fatalf("after the rollback: cases=%d processed=%d, want 0 / 0", c, p)
	}
	w.declarePolicy(t, usecases.TypeYardCheckClosed, "", 0)
	n, outcome, err := uc.Execute(ctx, closure)
	if err != nil || n != 2 || outcome != usecases.OutcomeApplied {
		t.Fatalf("redelivery = %d %s %v, want 2 applied", n, outcome, err)
	}
	if c := w.countTable("exception_cases"); c != 2 {
		t.Fatalf("cases = %d, want 2", c)
	}
}
