// Command api is exception-management's composition root. It wires the whole
// service: the REST surface (every route of apis/openapi.yaml on chi, the
// Idempotency-Key middleware over the Postgres store, the NUL guard, the
// readiness gate), the use cases over the real Postgres adapters, the six
// inbound Kafka fact consumers (each under its pinned consumer-group
// variable), the SLA sweep ticker and the transactional-outbox relay (the
// event side leaves the service ONLY through it, ADR 0004/0005). A consumer
// that dies fails run — the process exits non-zero and the pod restarts.
//
// Runtime: real Postgres + real Kafka only (no memory adapters); the
// embedded migrations run at boot before anything serves. A missing
// mandatory variable is a boot error naming it, raised before the listener
// binds. The service never creates topics through an admin API: the cluster
// broker auto-creates them on first write (the relay's writer sets
// AllowAutoTopicCreation) and infrastructure declares none.
//
// Environment:
//
//	DATABASE_URL              REQUIRED. Postgres DSN
//	KAFKA_BROKERS             REQUIRED. Comma-separated brokers
//	FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP
//	WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP
//	NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP
//	ORDER_CORRELATION_CONSUMER_GROUP
//	INVENTORY_EXCEPTIONS_CONSUMER_GROUP
//	RETURNS_EXCEPTIONS_CONSUMER_GROUP
//	CARTON_DISCREPANCY_CONSUMER_GROUP
//	                          REQUIRED. Each consumer's group id
//	                          (.claude/rules/integration-events.md)
//	HTTP_ADDR                 listen address (default :8080)
//	OUTBOX_RELAY_INTERVAL     relay poll interval (Go duration or seconds, default 1s)
//	OUTBOX_MAX_ATTEMPTS       failures before the relay excludes a row (default 20, 0 = unlimited)
//	SLA_SWEEP_INTERVAL        SLA sweep ticker interval (Go duration or seconds, default 1m)
//	SHUTDOWN_DRAIN_DELAY      wait after flipping /readyz before closing (default 5s, 0 = off)
//	LOG_LEVEL                 debug | info | warn | error (default info)
//
// /readyz is the drain gate AND a Postgres ping AND a Kafka metadata check
// (a broker answers a metadata request that names NO topic: reachability
// only, never topics — a consumed topic that does not exist yet, like the
// wave-3 returns topic, is the reader's internal retry, never unreadiness),
// cached for a few seconds. /healthz is liveness only.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/exception-management/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/exception-management/internal/adapters/outbound/kafka"
	outboxrelay "github.com/claudioed/exception-management/internal/adapters/outbound/outbox"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/adapters/outbound/system"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// shutdownTimeout bounds the HTTP server's graceful drain.
const shutdownTimeout = 10 * time.Second

// relayDrainTimeout bounds how long shutdown waits for the outbox relay to
// return after its context is cancelled.
const relayDrainTimeout = 10 * time.Second

func main() {
	logger := newLogger(os.Getenv("LOG_LEVEL"))
	if err := run(context.Background(), logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("service stopped cleanly")
}

// run boots the service and serves until SIGINT/SIGTERM or until ctx is
// cancelled (the in-process integration test stops it that way), then shuts
// down in the fleet order. It reads the whole configuration from the
// environment.
func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := loadBootConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// closeAdapters (pool.Close) is deferred FIRST so it runs LAST, after
	// the HTTP drain and the relay have stopped.
	pool, err := postgres.NewPool(ctx, cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer pool.Close()
	if err := postgres.RunMigrations(cfg.databaseURL); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	clock := system.Clock{}
	uow := postgres.NewUnitOfWork(pool)
	readiness := inboundhttp.NewReadiness(0, 0,
		inboundhttp.Dependency{Name: "postgres", Check: pool.Ping},
		inboundhttp.Dependency{Name: "kafka", Check: outboundkafka.NewBrokerCheck(cfg.brokers).Check},
	)
	handler := inboundhttp.NewRouter(buildServer(pool, uow, clock, readiness))
	httpServer := &http.Server{
		Addr: cfg.httpAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second,
	}

	// The relay is not optional: events leave only through it. Kafka is
	// dialled lazily on the first send, never here. WithMaxAttempts is
	// passed inline at this composition root on purpose (fitness test
	// TestOutboxRelaysPassAttemptCap): the attempt cap is a deliberate
	// setting, never the unlimited default by omission.
	sink, relay := newRelay(cfg, pool, logger)

	// The inbound side: six consumers (each under its pinned group id) and
	// the SLA sweep ticker. A consumer death lands on errCh: run returns it,
	// the process exits non-zero, the pod restarts.
	errCh := make(chan error, 8)
	go func() {
		logger.Info("http server listening", "addr", cfg.httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	inbound := startInbound(ctx, cfg, pool, uow, clock, logger, errCh)

	select {
	case err := <-errCh:
		awaitRelay(logger, relayDone)
		inbound.await(logger)
		inbound.close(logger)
		closeSinkBounded(logger, sink)
		return err
	case <-ctx.Done():
	}

	// The fleet shutdown order: (1) /readyz flips to 503 so no new traffic
	// arrives, (2) wait the drain delay, (3) drain HTTP, (4) await the
	// relay LAST so nothing committed above is stranded; the caller's
	// deferred pool.Close runs after all of it. The inbound consumers stop
	// with the cancelled context between (3) and (4): their offsets only
	// move after a durable effect, so an interrupted message is simply
	// redelivered after the restart.
	readiness.SetNotReady()
	if delay := shutdownDrainDelay(logger); delay > 0 {
		logger.Info("shutdown: readiness flipped to not-ready; waiting for traffic to drain", "drain_delay", delay.String())
		time.Sleep(delay)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("drain http: %w", err)
	}
	awaitRelay(logger, relayDone)
	inbound.await(logger)
	inbound.close(logger)
	closeSinkBounded(logger, sink)
	return nil
}

// newRelay builds the relay, its sink and the attempt-capped wiring, and logs
// the running configuration once. Split from run to keep each function under
// the complexity gate.
func newRelay(cfg bootConfig, pool *pgxpool.Pool, logger *slog.Logger) (*outboundkafka.RelaySink, *outboxrelay.Relay) {
	interval := parseRelayInterval(os.Getenv("OUTBOX_RELAY_INTERVAL"), logger)
	maxAttempts := outboxMaxAttempts(os.Getenv("OUTBOX_MAX_ATTEMPTS"), logger)
	sink := outboundkafka.NewRelaySink(cfg.brokers)
	relay := outboxrelay.NewOutboxRelay(postgres.NewOutboxStore(pool), sink, logger,
		outboxrelay.WithInterval(interval),
		outboxrelay.WithMaxAttempts(maxAttempts))
	logger.Info("exception-management api running",
		"addr", cfg.httpAddr, "relay_interval", interval, "relay_max_attempts", maxAttempts,
		"events_topic", outboundkafka.TopicEvents, "dlq_topic", outboundkafka.DeadLetterTopic,
		"brokers", cfg.brokers)
	return sink, relay
}

const sinkCloseTimeout = 5 * time.Second

// closeSinkBounded closes the Kafka writer with a hard deadline: a Close
// concurrent with an in-flight write can block on the writer's mutex until
// the write resolves, and shutdown must stay bounded (fleet rule: run always
// returns). A timed-out close leaks the writer to GC — nothing writes after
// the relay has stopped.
func closeSinkBounded(logger *slog.Logger, sink *outboundkafka.RelaySink) {
	done := make(chan error, 1)
	go func() { done <- sink.Close() }()
	select {
	case err := <-done:
		if err != nil {
			logger.Error("kafka relay sink close failed", "error", err)
		}
	case <-time.After(sinkCloseTimeout):
		logger.Warn("kafka relay sink close timed out; writer left to GC", "timeout", sinkCloseTimeout.String())
	}
}

// awaitRelay waits (bounded) for the relay goroutine to return.
func awaitRelay(logger *slog.Logger, done <-chan struct{}) {
	select {
	case <-done:
	case <-time.After(relayDrainTimeout):
		logger.Warn("the outbox relay did not stop before the shutdown drain deadline")
	}
}

// buildServer wires the Postgres adapters into the use cases and the use
// cases into the HTTP adapter: the ONLY place the two sides meet.
func buildServer(pool *pgxpool.Pool, uow ports.UnitOfWork, clock ports.Clock, readiness *inboundhttp.Readiness) *inboundhttp.Server {
	cases := postgres.NewCaseRepo(pool)
	policies := postgres.NewPolicyRepo(pool)
	events := postgres.NewOutboxPublisher(pool)
	idempotency := inboundhttp.NewIdempotency(postgres.NewIdempotencyStore(pool), uow, clock)
	return &inboundhttp.Server{
		List:         &usecases.ListOpenExceptions{Cases: cases},
		Get:          &usecases.GetException{Cases: cases},
		Triage:       &usecases.TriageException{Cases: cases, Events: events, Clock: clock, UnitOfWork: uow},
		Assign:       &usecases.AssignException{Cases: cases, Events: events, Clock: clock, UnitOfWork: uow},
		Resolve:      &usecases.ResolveException{Cases: cases, Events: events, Clock: clock, UnitOfWork: uow},
		Declare:      &usecases.DeclareExceptionPolicy{Policies: policies, Events: events, Clock: clock, UnitOfWork: uow},
		ListPolicies: &usecases.ListExceptionPolicies{Policies: policies},
		Idempotency:  idempotency.Middleware,
		Readiness:    readiness,
	}
}
