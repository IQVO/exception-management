//go:build integration

package kafka_test

import (
	"strings"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/testing/kafkatest"
)

// TestCartonDiscrepancy_RealKafkaAndPostgres feeds REAL CloudEvents (the
// inbound-receiving envelope and snake_case payload) through a real Kafka
// topic into the consumer over real Postgres (migrated, so the two route
// policies come from migration 0003, not from the test):
//  1. MissingCarton for a carton opens a problem-solve case
//  2. the same key under another id TOUCHES it
//  3. the first id again (tampered payload) is deduped on the id
//  4. CartonOverLimit routes to receiving-lead (informational)
//  5. an unknown kind routes to problem-solve
//  6. another type of the topic is ignored
//  7. a payload without receipt_id is dead-lettered
func TestCartonDiscrepancy_RealKafkaAndPostgres(t *testing.T) {
	w := newPGWorld(t)
	topic := kafkatest.Topic(t, kafka.InboundReceivingTopic)
	dlq := topic + kafka.DLQSuffix
	kafkatest.CreateTopic(t, dlq)

	const src = "/warehouse/inbound-receiving"
	const sscc = "340123450000000013"
	receipt := itID("rcpt")
	payload := func(kind, s string) string {
		sf := ""
		if s != "" {
			sf = `"sscc":"` + s + `",`
		}
		return `{"receipt_id":"` + receipt + `",` + sf + `"kind":"` + kind + `","expected":"12","actual":"10","at":"2026-10-24T09:00:00Z"}`
	}
	messages := keyed("k",
		produced(t, "c-1", src, kafka.TypeCartonDiscrepancyRaised, receipt, payload("MissingCarton", sscc)),
		produced(t, "c-2", src, kafka.TypeCartonDiscrepancyRaised, receipt, payload("MissingCarton", sscc)),
		produced(t, "c-1", src, kafka.TypeCartonDiscrepancyRaised, receipt, payload("MissingCarton", "tampered")),
		produced(t, "c-3", src, kafka.TypeCartonDiscrepancyRaised, receipt, payload("CartonOverLimit", sscc)),
		produced(t, "c-4", src, kafka.TypeCartonDiscrepancyRaised, receipt, payload("SomeFutureKind", "")),
		produced(t, "c-5", src, "com.warehouse.wms.inbound-receiving.receipt.ReceiptClosed", receipt, `{"receipt_id":"`+receipt+`"}`),
		produced(t, "c-6", src, kafka.TypeCartonDiscrepancyRaised, receipt, `{"kind":"MissingCarton","at":"2026-10-24T09:00:00Z"}`),
		produced(t, "c-7", src, kafka.TypeCartonDiscrepancyRaised, receipt, payload("DuplicateSscc", sscc)), // sentinel
	)
	kafkatest.Produce(t, topic, messages...)

	group := theGroup("carton-discrepancy")
	open := &usecases.OpenCartonDiscrepancy{Consume: w.consume}
	runConsumer(t, kafka.NewCartonDiscrepancyConsumerForTopic(kafkatest.Brokers(t), topic, group, open, itLogger(t)))

	eventually(t, "the sentinel case to open", func() bool {
		_, err := w.lookupCase(t, usecases.TypeCartonDiscrepancyRaised, usecases.CartonSubject(receipt, sscc, "DuplicateSscc"), "")
		return err == nil
	})
	assertCommittedPast(t, group, topic, int64(len(messages)))

	missing := w.openCase(t, usecases.TypeCartonDiscrepancyRaised, usecases.CartonSubject(receipt, sscc, "MissingCarton"), "")
	if missing.Status() != exceptioncase.StatusOpen || missing.Queue() != "problem-solve" || missing.SLAMinutes() != 60 || missing.Occurrence() != 2 {
		t.Fatalf("MissingCarton case = %s %s sla=%d occ=%d, want OPEN problem-solve 60 / 2", missing.Status(), missing.Queue(), missing.SLAMinutes(), missing.Occurrence())
	}
	if missing.SourceEventID() != "c-1" || !strings.Contains(string(missing.SourcePayload()), `"expected"`) {
		t.Fatalf("frozen source = %s %q", missing.SourceEventID(), missing.SourcePayload())
	}
	over := w.openCase(t, usecases.TypeCartonDiscrepancyRaised, usecases.CartonSubject(receipt, sscc, "CartonOverLimit"), "")
	if over.Queue() != "receiving-lead" || string(over.Severity()) != "LOW" {
		t.Fatalf("CartonOverLimit routed to %s/%s, want receiving-lead/LOW", over.Queue(), over.Severity())
	}
	future := w.openCase(t, usecases.TypeCartonDiscrepancyRaised, usecases.CartonSubject(receipt, "", "SomeFutureKind"), "")
	if future.Queue() != "problem-solve" {
		t.Fatalf("unknown kind routed to %s, want problem-solve", future.Queue())
	}
	// missing, over-limit, future-kind, duplicate-sscc: four cases, four openings.
	if n := w.countTable("exception_cases"); n != 4 {
		t.Fatalf("cases = %d, want 4", n)
	}
	// c-1, c-2, c-3, c-4, c-7 claimed; the replay, the other type and the poison claim nothing.
	if n := w.countTable("processed_events"); n != 5 {
		t.Fatalf("processed_events = %d, want 5", n)
	}
	dead := kafkatest.ReadN(t, dlq, 1, 90*time.Second)
	if !strings.Contains(string(dead[0].Value), `"id":"c-6"`) || dead[0].Headers["x-dlq-source-topic"] != topic {
		t.Fatalf("dead letter = %s %v, want the poison c-6", dead[0].Value, dead[0].Headers)
	}
}
