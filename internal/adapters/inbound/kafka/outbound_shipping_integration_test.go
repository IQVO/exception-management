//go:build integration

package kafka_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/testing/kafkatest"
)

// TestOutboundShipping_RealKafkaAndPostgres feeds REAL CloudEvents (the
// outbound-shipping envelope and snake_case payload) through a real Kafka
// topic into the consumer over real Postgres (migrated, so the
// PackageMissedLoad policy comes from migration 0004, not from the test):
//  1. a missed package opens ONE problem-solve case
//  2. the same CloudEvents id again (tampered) opens no second case
//  3. the same package under a NEW id (its next load) TOUCHES the case
//  4. another type of the topic (ShipmentConfirmed) is ignored
//  5. a payload without package_id is dead-lettered
//  6. a second missed package opens its own case (the sentinel)
func TestOutboundShipping_RealKafkaAndPostgres(t *testing.T) {
	w := newPGWorld(t)
	topic := kafkatest.Topic(t, kafka.OutboundShippingTopic)
	dlq := topic + kafka.DLQSuffix
	kafkatest.CreateTopic(t, dlq)

	const src = "/warehouse/outbound-shipping"
	const site = "SITE-1"
	pkg := itID("pkg")
	sentinelPkg := itID("pkg")
	missed := func(id, load, order string) []byte {
		orderField := ""
		if order != "" {
			orderField = `"source_order_id":"` + order + `",`
		}
		return produced(t, id, src, kafka.TypePackageMissedLoad, pkg,
			`{"package_id":"`+pkg+`","load_id":"`+load+`","site_id":"`+site+`",`+orderField+
				`"cutoff":"2026-11-06T16:00:00Z","detected_at":"2026-11-06T16:00:00Z","version":2}`)
	}
	sentinel := produced(t, "m-3", src, kafka.TypePackageMissedLoad, sentinelPkg,
		`{"package_id":"`+sentinelPkg+`","load_id":"load-7f3a","site_id":"`+site+
			`","cutoff":"2026-11-06T16:00:00Z","detected_at":"2026-11-06T16:00:00Z","version":3}`)
	values := [][]byte{
		missed("m-1", "load-7f3a", "order-88423"),
		missed("m-1", "load-TAMPERED", ""), // replayed id: deduped on the id
		missed("m-2", "load-9999", "order-88423"),
		produced(t, "s-1", src, "com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed", pkg,
			`{"load_id":"load-7f3a","site_id":"`+site+`"}`),
		produced(t, "m-blank", src, kafka.TypePackageMissedLoad, pkg,
			`{"load_id":"load-7f3a","site_id":"`+site+`","cutoff":"2026-11-06T16:00:00Z","detected_at":"2026-11-06T16:00:00Z","version":2}`),
		sentinel,
	}
	messages := make([]kafkatest.Message, len(values))
	for i, v := range values {
		key := pkg
		if i == len(values)-1 {
			key = sentinelPkg // the producer keys by the package id
		}
		messages[i] = kafkatest.Message{Key: key, Value: v}
	}
	kafkatest.Produce(t, topic, messages...)

	group := theGroup("outbound-shipping")
	runConsumer(t, kafka.NewOutboundShippingExceptionsConsumerForTopic(kafkatest.Brokers(t), topic, group, w.consume, itLogger(t)))

	eventually(t, "the sentinel case to open", func() bool {
		_, err := w.lookupCase(t, kafka.TypePackageMissedLoad, sentinelPkg, site)
		return err == nil
	})
	assertCommittedPast(t, group, topic, int64(len(messages)))

	c := w.openCase(t, kafka.TypePackageMissedLoad, pkg, site)
	if c.Status() != exceptioncase.StatusOpen || c.Queue() != "problem-solve" || c.SLAMinutes() != 60 || c.Occurrence() != 2 {
		t.Fatalf("missed-load case = %s %s sla=%d occ=%d, want OPEN problem-solve 60 / 2", c.Status(), c.Queue(), c.SLAMinutes(), c.Occurrence())
	}
	if c.SourceEventID() != "m-1" || c.SourcePayload() == nil {
		t.Fatalf("frozen source = %s %s", c.SourceEventID(), c.SourcePayload())
	}
	// The frozen payload carries the operator's references (format-agnostic:
	// the CloudEvents round trip may reorder keys and respaced colons).
	var frozen map[string]any
	if err := json.Unmarshal(c.SourcePayload(), &frozen); err != nil {
		t.Fatalf("frozen payload does not parse: %v", err)
	}
	for k, want := range map[string]any{"package_id": pkg, "load_id": "load-7f3a", "site_id": site, "source_order_id": "order-88423", "cutoff": "2026-11-06T16:00:00Z"} {
		if frozen[k] != want {
			t.Fatalf("frozen payload[%s] = %v, want %v (%s)", k, frozen[k], want, c.SourcePayload())
		}
	}
	// pkg and the sentinel: two packages, two cases — never a second for pkg.
	if n := w.countTable("exception_cases"); n != 2 {
		t.Fatalf("cases = %d, want 2", n)
	}
	// m-1, m-2, m-3 claimed; the replay, the other type and the poison claim nothing.
	if n := w.countTable("processed_events"); n != 3 {
		t.Fatalf("processed_events = %d, want 3", n)
	}
	dead := kafkatest.ReadN(t, dlq, 1, 90*time.Second)
	if !strings.Contains(string(dead[0].Value), `"id":"m-blank"`) || dead[0].Headers["x-dlq-source-topic"] != topic {
		t.Fatalf("dead letter = %s %v, want the poison m-blank", dead[0].Value, dead[0].Headers)
	}
}
