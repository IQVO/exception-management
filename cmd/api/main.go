// Command api is exception-management's composition root. This build step
// wires the transactional-outbox path end to end — the event side leaves the
// service ONLY through it (ADR 0004/0005): the use cases' facts are enqueued
// by postgres.OutboxPublisher inside the UnitOfWork, and the outbox relay
// drains them to Kafka as CloudEvents 1.0 through the kafka.RelaySink. The
// REST surface and the inbound consumers are later build steps on this same
// binary; nothing here pre-wires them.
//
// Runtime: real Postgres + real Kafka only (no memory adapters); the
// embedded migrations run at boot before the relay starts.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	outboundkafka "github.com/claudioed/exception-management/internal/adapters/outbound/kafka"
	outboxrelay "github.com/claudioed/exception-management/internal/adapters/outbound/outbox"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
)

func main() {
	logger := newLogger(os.Getenv("LOG_LEVEL"))
	if err := run(logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("service stopped cleanly")
}

func run(logger *slog.Logger) error {
	cfg, err := loadBootConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer pool.Close()
	if err := postgres.RunMigrations(cfg.databaseURL); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	// The relay is not optional: events leave only through it. Kafka is
	// dialled lazily on the first send, never here. WithMaxAttempts is
	// passed inline at this composition root on purpose (fitness test
	// TestOutboxRelaysPassAttemptCap): the attempt cap is a deliberate
	// setting, never the unlimited default by omission.
	sink := outboundkafka.NewRelaySink(cfg.brokers)
	interval := parseRelayInterval(os.Getenv("OUTBOX_RELAY_INTERVAL"), logger)
	maxAttempts := outboxMaxAttempts(os.Getenv("OUTBOX_MAX_ATTEMPTS"), logger)
	relay := outboxrelay.NewOutboxRelay(postgres.NewOutboxStore(pool), sink, logger,
		outboxrelay.WithInterval(interval),
		outboxrelay.WithMaxAttempts(maxAttempts))

	logger.Info("exception-management outbox relay running",
		"interval", interval, "max_attempts", maxAttempts,
		"events_topic", outboundkafka.TopicEvents, "dlq_topic", outboundkafka.DeadLetterTopic,
		"brokers", cfg.brokers)
	err = relay.Run(ctx)
	if cerr := sink.Close(); cerr != nil {
		logger.Error("kafka relay sink close failed", "error", cerr)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
