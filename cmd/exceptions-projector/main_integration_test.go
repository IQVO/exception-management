//go:build integration

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/kafka/cloudevents"

	inboundhttp "github.com/claudioed/exception-management/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/exception-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/exception-management/internal/adapters/outbound/outbox"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
	"github.com/claudioed/exception-management/internal/testing/kafkatest"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	kafkatest.Shutdown()
	pgtest.Shutdown()
	os.Exit(code)
}

const (
	taskCE   = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	invCE    = "com.warehouse.wes.inventory-storage.bin.DiscrepancyDetected"
	repromCE = "com.warehouse.wes.order-management.order.OrderRepromised"
)

func iat(h, m int) time.Time { return time.Date(2026, 10, 21, h, m, 0, 0, time.UTC) }

func mustKey(t *testing.T, sourceType, subject string) shared.DedupKey {
	t.Helper()
	k, err := shared.NewDedupKey(sourceType, subject, "")
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func mustActor(t *testing.T, name string) shared.Actor {
	t.Helper()
	a, err := shared.Operator(name)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// The full analytics read side on real infrastructure (ADR 0006's proof):
// REAL domain facts are enqueued through the REAL transactional outbox
// (postgres.OutboxPublisher) on the OLTP database, the REAL relay drains them
// to Kafka (which now writes every fact to BOTH topics), the projector
// consumer folds the analytics twins into a real analytical Postgres migrated
// by the projector's own boot step, and the reports binary's router answers
// the three reports and the freshness probe from a READ-ONLY pool. A tampered
// message lands on the analytics DLQ and the offsets move past it.
func TestProjectorFoldsTheRealOutboxStreamAndTheReportsServeIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// The OLTP side: real outbox rows on a migrated Postgres.
	oltpURL := pgtest.URL(t)
	oltp, err := postgres.NewPool(ctx, oltpURL)
	if err != nil {
		t.Fatal(err)
	}
	defer oltp.Close()

	// The analytical side: migrated by the projector's own boot step.
	analyticsURL := pgtest.EmptyURL(t)
	analyticsPool, err := openAnalyticsPool(ctx, logger, analyticsURL)
	if err != nil {
		t.Fatalf("openAnalyticsPool: %v", err)
	}
	defer analyticsPool.Close()

	// Kafka: isolated per-test topics; the relay writes the integration twin
	// to the events topic and the analytics twin to the analytics topic.
	brokers := kafkatest.Brokers(t)
	eventsTopic := kafkatest.Topic(t, "exceptions.analytics.events")
	analyticsTopic := kafkatest.Topic(t, "exceptions.analytics.stream")
	dlqTopic := inboundkafka.DeadLetterTopic(analyticsTopic)
	kafkatest.CreateTopic(t, dlqTopic)

	// Enqueue the case lifecycle the way the service does: through the REAL
	// OutboxPublisher. case-1 breaches (triaged into ps-expedite, escalated,
	// resolved after 75 minutes); case-2 auto-resolves in 5 minutes.
	publisher := postgres.NewOutboxPublisher(oltp)
	case1 := "case-" + time.Now().Format("150405.000000000")
	case2 := case1 + "-auto"
	events := []exceptioncase.Event{
		exceptioncase.ExceptionOpened{Header: exceptioncase.Header{Case: case1, Version: 1, At: iat(9, 0)},
			Key: mustKey(t, taskCE, "task-8a1f"), Severity: shared.Severity("HIGH"), Queue: "ps-main",
			SLADueAt: iat(10, 0), SourceEventID: "src-1", SourcePayload: []byte(`{"task_id":"task-8a1f"}`)},
		exceptioncase.ExceptionTriaged{Header: exceptioncase.Header{Case: case1, Version: 2, At: iat(9, 10)},
			Severity: shared.Severity("MEDIUM"), Queue: "ps-expedite", Actor: mustActor(t, "ops-amy")},
		exceptioncase.ExceptionAssigned{Header: exceptioncase.Header{Case: case1, Version: 3, At: iat(9, 20)},
			Assignee: "jlee", Actor: mustActor(t, "ops-amy")},
		exceptioncase.ExceptionEscalated{Header: exceptioncase.Header{Case: case1, Version: 4, At: iat(10, 5)}, SLADueAt: iat(10, 0)},
		exceptioncase.ExceptionResolved{Header: exceptioncase.Header{Case: case1, Version: 5, At: iat(10, 15)},
			ResolutionCode: "STOCK_FOUND", RootCause: "COUNT_ERROR", Actor: mustActor(t, "jlee")},
		exceptioncase.ExceptionOpened{Header: exceptioncase.Header{Case: case2, Version: 1, At: iat(9, 45)},
			Key: mustKey(t, taskCE, "task-9b2c"), Severity: shared.Severity("LOW"), Queue: "ps-main",
			SLADueAt: iat(13, 45), SourceEventID: "src-2", SourcePayload: []byte(`{"task_id":"task-9b2c"}`)},
		exceptioncase.ExceptionAutoResolved{Header: exceptioncase.Header{Case: case2, Version: 2, At: iat(9, 50)},
			RuleID: taskCE + "->" + repromCE, CorrelatedType: repromCE, CorrelatedEventID: "corr-1"},
	}
	if err := publisher.PublishCaseEvents(ctx, events...); err != nil {
		t.Fatalf("enqueue through the real outbox: %v", err)
	}

	// The REAL relay over the per-test topics.
	sink := outboundkafka.NewRelaySink(brokers)
	sink.EventsTopic, sink.AnalyticsTopic = eventsTopic, analyticsTopic
	relay := outbox.NewOutboxRelay(postgres.NewOutboxStore(oltp), sink, logger, outbox.WithInterval(50*time.Millisecond))
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		_ = relay.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-relayDone:
		case <-time.After(10 * time.Second):
		}
		_ = sink.Close()
	})

	// A poison message directly on the analytics topic: a known type whose
	// payload contradicts its subject can never be projected.
	tampered := kafkatest.Message{Key: case1, Value: tamperedEvent(t, case1+"-OTHER")}
	kafkatest.Produce(t, analyticsTopic, tampered)

	// The projector: the consumer of the projector binary over the real
	// analytical store.
	group := "exceptions-projector-" + analyticsTopic
	consumer := inboundkafka.NewAnalyticsConsumer(brokers, analyticsTopic, group, analyticsstore.NewProjection(analyticsPool), logger)
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-consumerDone:
		case <-time.After(10 * time.Second):
			t.Error("the analytics consumer did not stop")
		}
		_ = consumer.Close()
	})

	// The reports binary's router over a read-only pool.
	ro, err := analyticsstore.NewReadOnlyPool(ctx, analyticsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	reports := httptest.NewServer(inboundhttp.NewReportsRouter(&inboundhttp.ReportsServer{
		Reader: analyticsstore.NewReader(ro), Now: func() time.Time { return iat(12, 0) },
	}))
	defer reports.Close()

	// Wait for the relay to hand every fact to BOTH topics (7 facts, one
	// twin each) and for the projection to answer.
	waitFor(t, 90*time.Second, "the analytics stream to hold the seven twins", func() bool {
		return kafkatest.EndOffset(t, analyticsTopic) >= 8 // 7 twins + the poison
	})
	from, to := iat(8, 0).Format(time.RFC3339), iat(20, 0).Format(time.RFC3339)

	var mttr struct {
		Queues []struct {
			Queue  string `json:"queue"`
			Timing struct {
				Resolved    int      `json:"resolved"`
				MeanSeconds *float64 `json:"mean_seconds"`
			} `json:"timing"`
		} `json:"queues"`
	}
	// The projection lands asynchronously (group join, fetch, fold): poll
	// the report until both queues answered or the bound expires.
	waitFor(t, 60*time.Second, "the mean-time-to-resolve report to answer", func() bool {
		getJSON(t, reports.URL+"/reports/mean-time-to-resolve?from="+from+"&to="+to, &mttr)
		return len(mttr.Queues) == 2
	})
	if len(mttr.Queues) != 2 {
		t.Fatalf("mttr queues = %+v, want ps-expedite and ps-main", mttr.Queues)
	}
	byQueue := map[string]float64{}
	counts := map[string]int{}
	for _, q := range mttr.Queues {
		byQueue[q.Queue] = *q.Timing.MeanSeconds
		counts[q.Queue] = q.Timing.Resolved
	}
	if counts["ps-expedite"] != 1 || byQueue["ps-expedite"] != 4500 { // opened 09:00 -> resolved 10:15
		t.Errorf("ps-expedite = %d %v, want 1 4500s", counts["ps-expedite"], byQueue["ps-expedite"])
	}
	if counts["ps-main"] != 1 || byQueue["ps-main"] != 300 { // opened 09:45 -> auto-resolved 09:50
		t.Errorf("ps-main = %d %v, want 1 300s", counts["ps-main"], byQueue["ps-main"])
	}

	var rc struct {
		RootCauses []struct {
			SourceType string  `json:"source_type"`
			RootCause  string  `json:"root_cause"`
			Resolved   int64   `json:"resolved"`
			Share      float64 `json:"share"`
		} `json:"root_causes"`
	}
	getJSON(t, reports.URL+"/reports/root-cause-frequency?from="+from+"&to="+to, &rc)
	if len(rc.RootCauses) != 1 || rc.RootCauses[0].RootCause != "COUNT_ERROR" || rc.RootCauses[0].Resolved != 1 {
		t.Fatalf("root causes = %+v", rc.RootCauses)
	}
	if rc.RootCauses[0].Share != 0.5 { // 1 manual of taskCE's 2 resolutions
		t.Errorf("share = %v, want 0.5", rc.RootCauses[0].Share)
	}

	var breach struct {
		SourceTypes []struct {
			SourceType     string  `json:"source_type"`
			Opened         int64   `json:"opened"`
			Escalated      int64   `json:"escalated"`
			Breached       int64   `json:"breached"`
			BreachRate     float64 `json:"breach_rate"`
			EscalationRate float64 `json:"escalation_rate"`
		} `json:"source_types"`
	}
	getJSON(t, reports.URL+"/reports/sla-breach-rate?from="+from+"&to="+to, &breach)
	if len(breach.SourceTypes) != 1 || breach.SourceTypes[0].SourceType != taskCE ||
		breach.SourceTypes[0].Opened != 2 || breach.SourceTypes[0].Breached != 1 || breach.SourceTypes[0].BreachRate != 0.5 {
		t.Fatalf("sla breach rows = %+v", breach.SourceTypes)
	}

	var fresh struct {
		AsOf       string  `json:"as_of"`
		LagSeconds float64 `json:"lag_seconds"`
	}
	getJSON(t, reports.URL+"/reports/freshness", &fresh)
	if fresh.AsOf != iat(10, 15).Format(time.RFC3339) || fresh.LagSeconds != 6300 {
		t.Fatalf("freshness = %+v, want the 10:15 resolution and 6300s lag", fresh)
	}

	// The poison message was dead-lettered byte for byte with its context,
	// and the offsets moved past it (the projection above already proves the
	// stream kept flowing).
	dead := kafkatest.ReadN(t, dlqTopic, 1, 30*time.Second)
	if string(dead[0].Value) != string(tampered.Value) || dead[0].Headers["x-dlq-cause"] == "" {
		t.Fatalf("dlq = %+v", dead[0])
	}

	// The integration twins left too: same count, same ids.
	if n := kafkatest.EndOffset(t, eventsTopic); n != 7 {
		t.Fatalf("%s holds %d messages, want 7", eventsTopic, n)
	}
}

// tamperedEvent builds a valid CloudEvent of a known kind whose payload names
// another case than its subject.
func tamperedEvent(t *testing.T, subject string) []byte {
	t.Helper()
	return cloudeventsValue(t, "exceptioncase", "ExceptionResolved", subject, map[string]any{
		"case_id": "case-someone-else", "resolution_code": "STOCK_FOUND", "root_cause": "COUNT_ERROR",
		"note": nil, "actor": "jlee", "at": iat(10, 15).Format(time.RFC3339),
	})
}

// cloudeventsValue builds the structured-mode JSON value of one analytics
// event exactly the way the publisher does.
func cloudeventsValue(t *testing.T, entity, name, subject string, data any) []byte {
	t.Helper()
	value, err := cloudevents.New(cloudevents.Spec{
		ID: "ev-tampered", Entity: entity, EventName: name, Subject: subject, Time: iat(10, 15),
		Stream: cloudevents.StreamAnalytics, Version: 1, Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func getJSON(t *testing.T, url string, dest any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
}
