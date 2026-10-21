//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/exception-management/internal/testing/kafkatest"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// The real-wiring proof: the test boots the REAL entry point (run,
// in-process) against testcontainers Postgres and Kafka with nothing but the
// environment a deployment sets, and drives it ONLY through REST (and reads
// the produced topic through Kafka). The test broker has topic
// auto-creation OFF, so the test controls when each topic appears — and
// readiness, which is reachability only, never depends on them.
//
// The case lifecycle (triage/assign/resolve) cannot be driven over HTTP
// here BY DESIGN: no POST /exceptions exists and the inbound consumers that
// open cases are the next build step — the handler tests over the memory
// doubles cover the lifecycle; this proof covers the composition root, the
// probes, the Idempotency-Key store on real Postgres and the outbox relay
// to Kafka.

const (
	topicEvents = "warehouse.exception-management.events"

	typePolicyDeclared = "com.warehouse.wes.exception-management.exceptionpolicy.ExceptionPolicyDeclared"
	sourceType         = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
)

var seq atomic.Int64

// freeAddr returns a TCP address with a listener already closed (the OS
// keeps the port unused for the window the service needs to bind it).
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// service is one running instance of the real entry point. Shutdown is
// signalled by CLOSING stopped (observable by every waiter, nothing to
// consume): a buffered done channel is racy here — a second armed reader
// (the boot poll) can steal the single value before stop() sees it.
type service struct {
	t       *testing.T
	base    string
	cancel  context.CancelFunc
	stopped chan struct{}
	err     error
}

// startService boots run in-process with the environment a deployment sets
// and waits until the listener answers /healthz.
func startService(t *testing.T, dbURL, brokers string) *service {
	t.Helper()
	addr := freeAddr(t)
	t.Setenv("DATABASE_URL", dbURL)
	t.Setenv("KAFKA_BROKERS", brokers)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("SHUTDOWN_DRAIN_DELAY", "0")
	t.Setenv("OUTBOX_RELAY_INTERVAL", "100ms")
	t.Setenv("LOG_LEVEL", "warn")
	for _, name := range consumerGroupEnvs {
		t.Setenv(name, fmt.Sprintf("%s-%d", strings.ToLower(strings.TrimSuffix(name, "_CONSUMER_GROUP")), seq.Add(1)))
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &service{t: t, base: "http://" + addr, cancel: cancel, stopped: make(chan struct{})}
	go func() {
		s.err = run(ctx, slogDiscard)
		close(s.stopped)
	}()
	t.Cleanup(func() {
		s.cancel()
		select {
		case <-s.stopped:
		case <-time.After(60 * time.Second):
		}
	})
	waitFor(t, 60*time.Second, "the service to listen on "+addr, func() bool {
		select {
		case <-s.stopped:
			t.Fatalf("run returned while booting: %v", s.err)
		default:
		}
		status, _, _ := s.try(http.MethodGet, "/healthz", "", nil)
		return status == http.StatusOK
	})
	return s
}

// slogDiscard keeps the service's boot logs out of the test output (the
// real main() passes a stdout logger).
var slogDiscard = newLogger("error")

// stop cancels run and requires a clean shutdown within a bound.
func (s *service) stop() {
	s.t.Helper()
	s.cancel()
	select {
	case <-s.stopped:
		if s.err != nil {
			s.t.Fatalf("shutdown returned %v", s.err)
		}
	case <-time.After(25 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		s.t.Fatalf("run(%s) did not return after the shutdown\n\n%s", s.base, buf[:n])
	}
}

// try performs a request and returns -1 when the connection fails.
func (s *service) try(method, path, body string, header map[string]string) (int, string, http.Header) {
	req, err := http.NewRequestWithContext(context.Background(), method, s.base+path, strings.NewReader(body))
	if err != nil {
		return -1, err.Error(), nil
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return -1, err.Error(), nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw), resp.Header
}

func (s *service) call(method, path, body string, want int, header map[string]string) string {
	s.t.Helper()
	status, raw, _ := s.try(method, path, body, header)
	if status != want {
		s.t.Fatalf("%s %s = %d %s, want %d", method, path, status, raw, want)
	}
	return raw
}

// idemKey is the Idempotency-Key header set of a well-formed command.
func idemKey(key string) map[string]string { return map[string]string{"Idempotency-Key": key} }

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// topicMissing asks the broker directly whether topic exists (the test
// broker has auto-creation OFF, so a metadata request naming it cannot bring
// it into being).
func topicMissing(t *testing.T, brokers []string, topic string) bool {
	t.Helper()
	client := &kafkago.Client{Addr: kafkago.TCP(brokers...)}
	resp, err := client.Metadata(context.Background(), &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("metadata answered %d topics for one asked", len(resp.Topics))
	}
	return resp.Topics[0].Error != nil || len(resp.Topics[0].Partitions) == 0
}

// TestRealEntryPointServesRestAndRelaysToKafka: boot the real root against
// real Postgres and Kafka, prove the probes, the problem map, the
// idempotency store and the policy surface over HTTP, and the declaration
// fact leaving through the real outbox relay.
func TestRealEntryPointServesRestAndRelaysToKafka(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	dbURL := pgtest.EmptyURL(t)
	s := startService(t, dbURL, strings.Join(brokers, ","))

	// Phase 1: no topic exists yet. Readiness needs Postgres plus a broker
	// answering an EMPTY-topic metadata request, NOT topics — and the
	// service creates none itself (the broker here has auto-creation OFF
	// and the service never calls an admin API).
	t.Run("readyz is 200 while every topic is missing", func(t *testing.T) {
		waitFor(t, 60*time.Second, "/readyz to be 200 although no topic exists", func() bool {
			status, _, _ := s.try(http.MethodGet, "/readyz", "", nil)
			return status == http.StatusOK
		})
		if !topicMissing(t, brokers, topicEvents) {
			t.Fatalf("topic %s exists: the service must never create topics through an admin API", topicEvents)
		}
	})

	// Phase 2: the REST surface over the real Postgres wiring.
	t.Run("the rest surface answers over real postgres", func(t *testing.T) {
		if raw := s.call(http.MethodGet, "/exceptions", "", http.StatusOK, nil); strings.TrimSpace(raw) != "[]" {
			t.Fatalf("empty list = %q, want []", raw)
		}
		raw := s.call(http.MethodGet, "/exceptions/case-unknown", "", http.StatusNotFound, nil)
		if !strings.Contains(raw, "case-not-found") {
			t.Fatalf("unknown case = %s, want the case-not-found problem", raw)
		}
		raw = s.call(http.MethodPost, "/exceptions", `{}`, http.StatusMethodNotAllowed, nil)
		if !strings.Contains(raw, "no POST /exceptions") {
			t.Fatalf("POST /exceptions = %s, want the explaining 405", raw)
		}
		raw = s.call(http.MethodGet, "/exceptions?status=CLOSED", "", http.StatusBadRequest, nil)
		if !strings.Contains(raw, "invalid-status") {
			t.Fatalf("bad status filter = %s", raw)
		}
		body := `{"severity":"HIGH","queue":"cps-front","slaMinutes":30}`
		raw = s.call(http.MethodPut, "/exception-policies/"+sourceType, body, http.StatusBadRequest, nil)
		if !strings.Contains(raw, "idempotency-key-required") {
			t.Fatalf("PUT without a key = %s, want idempotency-key-required", raw)
		}
	})

	// Phase 3: declare a policy (idempotent), list it back, and read the
	// declaration fact from the relay's topic.
	t.Run("policy declare replays and relays", func(t *testing.T) {
		kafkatest.CreateTopic(t, topicEvents)
		body := `{"severity":"HIGH","queue":"cps-front","slaMinutes":30}`
		target := "/exception-policies/" + sourceType
		first := s.call(http.MethodPut, target, body, http.StatusCreated, idemKey("k-1"))
		replay := s.call(http.MethodPut, target, body, http.StatusCreated, idemKey("k-1"))
		if first != replay {
			t.Fatalf("replay = %s, want the stored %s", replay, first)
		}
		if raw := s.call(http.MethodPut, target, `{"severity":"LOW","queue":"cps-back","slaMinutes":90}`, http.StatusConflict, idemKey("k-1")); !strings.Contains(raw, "idempotency-key-reused") {
			t.Fatalf("key reuse with another body = %s, want idempotency-key-reused", raw)
		}
		if raw := s.call(http.MethodGet, "/exception-policies", "", http.StatusOK, nil); !strings.Contains(raw, `"sourceType":"`+sourceType+`"`) || !strings.Contains(raw, `"version":1`) {
			t.Fatalf("GET /exception-policies = %s", raw)
		}

		// The declaration fact left through the real outbox relay.
		waitFor(t, 90*time.Second, "the declaration fact on "+topicEvents, func() bool {
			return kafkatest.EndOffset(t, topicEvents) >= 1
		})
		msg := kafkatest.ReadN(t, topicEvents, 1, 30*time.Second)[0]
		var env struct {
			SpecVersion     string          `json:"specversion"`
			ID              string          `json:"id"`
			Source          string          `json:"source"`
			Type            string          `json:"type"`
			Subject         string          `json:"subject"`
			Time            string          `json:"time"`
			DataContentType string          `json:"datacontenttype"`
			DataSchema      string          `json:"dataschema"`
			Data            json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			t.Fatalf("not a CloudEvent: %v: %s", err, msg.Value)
		}
		if env.SpecVersion != "1.0" || env.Type != typePolicyDeclared || env.Source != "/warehouse/exception-management" ||
			env.Subject != sourceType || env.DataSchema != "urn:warehouse:exception-management:events:ExceptionPolicyDeclared:v1" ||
			env.DataContentType != "application/json" || env.ID == "" {
			t.Fatalf("envelope = %+v", env)
		}
		if _, err := time.Parse(time.RFC3339Nano, env.Time); err != nil {
			t.Fatalf("time %q is not RFC 3339: %v", env.Time, err)
		}
		if msg.Key != sourceType {
			t.Fatalf("kafka key = %q, want the source type %q", msg.Key, sourceType)
		}
		if got := msg.Headers["content-type"]; got != "application/cloudevents+json; charset=UTF-8" {
			t.Fatalf("content-type header = %q", got)
		}
		var data struct {
			SourceType  string `json:"source_type"`
			Version     int64  `json:"version"`
			Severity    string `json:"severity"`
			Queue       string `json:"queue"`
			SLAMinutes  int64  `json:"sla_minutes"`
			AutoResolve any    `json:"auto_resolve"`
		}
		if err := json.Unmarshal(env.Data, &data); err != nil {
			t.Fatal(err)
		}
		if data.SourceType != sourceType || data.Version != 1 || data.Severity != "HIGH" ||
			data.Queue != "cps-front" || data.SLAMinutes != 30 || data.AutoResolve != nil {
			t.Fatalf("declaration data = %+v", data)
		}

		// The restart proof: nothing lost, still exactly one row per key. The
		// replacement instance is registered on the OUTER test (a service
		// started on the subtest's t would be cancelled by the subtest's own
		// cleanup the moment this closure returns).
		outer := t
		s.stop()
		s = startService(outer, dbURL, strings.Join(brokers, ","))
		waitFor(outer, 60*time.Second, "/readyz after the restart", func() bool {
			status, _, _ := s.try(http.MethodGet, "/readyz", "", nil)
			return status == http.StatusOK
		})
		if raw := s.call(http.MethodPut, target, body, http.StatusCreated, idemKey("k-1")); raw != first {
			outer.Fatalf("replay across a restart = %s, want the stored answer (the idempotency table survives)", raw)
		}
		if n := kafkatest.EndOffset(outer, topicEvents); n != 1 {
			outer.Fatalf("%s holds %d messages after the restart replay, want still 1", topicEvents, n)
		}
	})

	s.stop()
}

// A Kafka outage never crashes the service: it stays alive, reports
// not-ready naming kafka, and keeps its Postgres-backed API up (the same
// dependency-unavailable proof as stopping the broker, driven the way the
// fleet's e2e suites drive it: a broker address nothing answers).
func TestServiceStaysAliveButNotReadyWhileKafkaIsUnreachable(t *testing.T) {
	dbURL := pgtest.EmptyURL(t)
	s := startService(t, dbURL, "127.0.0.1:1")
	if status, _, _ := s.try(http.MethodGet, "/healthz", "", nil); status != http.StatusOK {
		t.Fatalf("healthz = %d: liveness must not depend on Kafka", status)
	}
	waitFor(t, 60*time.Second, "/readyz to report kafka down", func() bool {
		status, body, _ := s.try(http.MethodGet, "/readyz", "", nil)
		return status == http.StatusServiceUnavailable && strings.Contains(body, "not ready: kafka") && !strings.Contains(body, "postgres")
	})
	// The Postgres-backed surface keeps serving while Kafka is unreachable.
	body := `{"severity":"HIGH","queue":"cps-front","slaMinutes":30}`
	if raw := s.call(http.MethodPut, "/exception-policies/"+sourceType, body, http.StatusCreated, idemKey("k-outage")); !strings.Contains(raw, `"version":1`) {
		t.Fatalf("declare during the outage = %s", raw)
	}
	s.stop()
}
