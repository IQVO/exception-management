//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/adapters/outbound/system"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
	"github.com/claudioed/exception-management/internal/testing/kafkatest"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// The infrastructure comes from testcontainers, never from the environment:
// internal/testing/pgtest boots ONE Postgres and internal/testing/kafkatest
// ONE Kafka per test binary (each lazily, on first use); TestMain only tears
// them down at the end.
func TestMain(m *testing.M) {
	code := m.Run()
	kafkatest.Shutdown()
	pgtest.Shutdown()
	os.Exit(code)
}

// The kafka module import stays reachable from the Kafka-touching test
// files (TestKafkaIntegrationTestsUseTestcontainers).
var _ = tckafka.Run

// itSeq mints unique ids (event ids, group ids) per test binary.
var itSeq atomic.Int64

func itID(prefix string) string { return fmt.Sprintf("%s-%d", prefix, itSeq.Add(1)) }

// pgWorld is the production Postgres wiring of the consumers over a fresh,
// migrated database of the shared testcontainers Postgres.
type pgWorld struct {
	pool       *pgxpool.Pool
	uow        *postgres.UnitOfWork
	cases      *postgres.CaseRepo
	policies   *postgres.PolicyRepo
	processed  *postgres.ProcessedEventRepo
	consume    *usecases.ConsumeFact
	correlate  *usecases.ConsumeCorrelation
	declare    *usecases.DeclareExceptionPolicy
	countTable func(table string) int
}

func newPGWorld(t *testing.T) *pgWorld {
	t.Helper()
	pool := pgtest.NewPool(t)
	uow := postgres.NewUnitOfWork(pool)
	cases := postgres.NewCaseRepo(pool)
	policies := postgres.NewPolicyRepo(pool)
	events := postgres.NewOutboxPublisher(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	clock := system.Clock{}
	consume := &usecases.ConsumeFact{
		Processed: processed,
		OpenOrTouch: &usecases.OpenOrTouchException{
			Cases: cases, Policies: policies, Events: events,
			IDs: system.CaseIDs{}, Clock: clock, UnitOfWork: uow,
		},
		UnitOfWork: uow,
	}
	correlate := &usecases.ConsumeCorrelation{
		Processed: processed,
		AutoResolve: &usecases.AutoResolveOnCorrelation{
			Cases: cases, Policies: policies, Events: events, Clock: clock, UnitOfWork: uow,
		},
		UnitOfWork: uow,
	}
	return &pgWorld{
		pool: pool, uow: uow, cases: cases, policies: policies, processed: processed,
		consume: consume, correlate: correlate,
		declare:    &usecases.DeclareExceptionPolicy{Policies: policies, Events: events, Clock: clock, UnitOfWork: uow},
		countTable: func(table string) int { return pgtest.Count(t, pool, table) },
	}
}

// declarePolicy declares the policy of sourceType (auto-resolve rule when
// onType is not empty).
func (w *pgWorld) declarePolicy(t *testing.T, sourceType, onType string, withinMinutes int64) {
	t.Helper()
	in := usecases.PolicyDeclaration{
		SourceType: sourceType, Severity: shared.SeverityHigh, Queue: "ps-main",
		SLAMinutes: 30, Actor: "op-itest",
	}
	if onType != "" {
		in.AutoResolveOn, in.WithinMinutes = onType, withinMinutes
	}
	if _, err := w.declare.Execute(context.Background(), in); err != nil {
		t.Fatalf("declare policy for %s: %v", sourceType, err)
	}
}

// case returns the case that owns (sourceType, subject, site), or the
// lookup error (a poll-friendly miss: ErrNotFound while the consumer has
// not caught up yet).
func (w *pgWorld) lookupCase(t *testing.T, sourceType, subject, site string) (*exceptioncase.Case, error) {
	t.Helper()
	key, err := shared.NewDedupKey(sourceType, subject, site)
	if err != nil {
		t.Fatal(err)
	}
	return w.cases.FindOpenByDedupKey(context.Background(), key)
}

// openCase is lookupCase with the fatal on miss (the final assertions).
func (w *pgWorld) openCase(t *testing.T, sourceType, subject, site string) *exceptioncase.Case {
	t.Helper()
	c, err := w.lookupCase(t, sourceType, subject, site)
	if err != nil {
		t.Fatalf("no case for (%s, %s, %q): %v", sourceType, subject, site, err)
	}
	return c
}

// itLogger sends the consumer's log lines to the test output.
type itLogWriter struct{ t *testing.T }

func (w itLogWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

func itLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(itLogWriter{t}, nil))
}

// runConsumer starts c and stops it (and waits for it) when the test ends.
func runConsumer(t *testing.T, c *kafka.Consumer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Errorf("the %s did not stop after the context cancellation", c.Name)
			return
		}
		if runErr != nil && runErr != context.Canceled {
			t.Errorf("the %s died during the test: %v", c.Name, runErr)
		}
		if err := c.Close(); err != nil {
			t.Errorf("close the %s: %v", c.Name, err)
		}
	})
}

// eventually polls cond until it holds or the bound expires.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// assertCommittedPast waits until the group committed `want` offsets of
// topic: every message, the skipped ones (invalid envelope, unknown type)
// and the dead-lettered poison included.
func assertCommittedPast(t *testing.T, group, topic string, want int64) {
	t.Helper()
	eventually(t, fmt.Sprintf("%d committed offsets of %s", want, topic), func() bool {
		return kafkatest.CommittedOffset(t, group, topic) == want
	})
}

// produced builds the structured-mode JSON bytes of a producer-side
// occurrence (the exact envelope a sibling context publishes).
func produced(t *testing.T, id, source, typ, subject string, data string) []byte {
	t.Helper()
	raw, err := json.Marshal(producerEvent(t, id, source, typ, subject, data))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// keyed wraps raw values as keyed messages.
func keyed(key string, values ...[]byte) []kafkatest.Message {
	out := make([]kafkatest.Message, len(values))
	for i, v := range values {
		out[i] = kafkatest.Message{Key: key, Value: v}
	}
	return out
}

// theGroup mints a unique consumer group id per topic (like the environment
// variable a deployment sets).
func theGroup(name string) string { return fmt.Sprintf("itest-%s-%d", name, itSeq.Add(1)) }
