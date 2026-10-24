package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	outboxrelay "github.com/claudioed/exception-management/internal/adapters/outbound/outbox"
)

// The service's boot configuration, from the environment. Everything is
// mandatory-fail-fast except the tuning knobs, which have fleet defaults.

// getenv returns the value of key when set (non-empty), else fallback.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// newLogger builds the JSON logger; LOG_LEVEL debug|info|warn|warning|error
// (case-insensitive; "warning" is accepted like "warn" fleet-wide).
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// brokersFromEnv reads KAFKA_BROKERS (comma separated); empty when unset.
func brokersFromEnv() []string {
	var brokers []string
	for _, b := range strings.Split(os.Getenv("KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

// consumerGroupEnvs are the six pinned consumer-group variables of
// .claude/rules/integration-events.md. They are read and validated at boot
// even though the consumers themselves are a later build step on this
// binary: fail-closed now, so a deployment cannot go live missing a group id
// the step-6 consumers will need.
var consumerGroupEnvs = []string{
	"FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP",
	"WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP",
	"NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP",
	"ORDER_CORRELATION_CONSUMER_GROUP",
	"INVENTORY_EXCEPTIONS_CONSUMER_GROUP",
	"RETURNS_EXCEPTIONS_CONSUMER_GROUP",
	"CARTON_DISCREPANCY_CONSUMER_GROUP",
}

// parseRelayInterval reads OUTBOX_RELAY_INTERVAL (a Go duration or plain
// seconds), defaulting when unset, malformed or non-positive.
func parseRelayInterval(raw string, logger *slog.Logger) time.Duration {
	if raw == "" {
		return outboxrelay.DefaultInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		if secs, serr := strconv.ParseFloat(raw, 64); serr == nil {
			d, err = time.Duration(secs*float64(time.Second)), nil
		}
	}
	if err != nil || d <= 0 {
		logger.Warn("invalid OUTBOX_RELAY_INTERVAL; using the default", "value", raw, "default", outboxrelay.DefaultInterval)
		return outboxrelay.DefaultInterval
	}
	return d
}

// outboxMaxAttempts reads OUTBOX_MAX_ATTEMPTS: how many times an outbox row
// may fail before the relay excludes it (ADR 0005). Unset = the fleet
// default (20); 0 = unlimited; negative or unparsable values log a warning
// and use the default, so a typo never silently removes the cap. Every
// relay a cmd builds passes it (fitness test
// TestOutboxRelaysPassAttemptCap).
func outboxMaxAttempts(raw string, logger *slog.Logger) int {
	if raw == "" {
		return outboxrelay.DefaultMaxAttempts
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 0 {
		logger.Warn("invalid OUTBOX_MAX_ATTEMPTS; using the default", "value", raw, "default", outboxrelay.DefaultMaxAttempts)
		return outboxrelay.DefaultMaxAttempts
	}
	return n
}

// DefaultShutdownDrainDelay is how long shutdown waits, after flipping
// /readyz to not-ready, before closing the listener (SHUTDOWN_DRAIN_DELAY
// overrides it; "0" disables it).
const DefaultShutdownDrainDelay = 5 * time.Second

// DefaultSLASweepInterval is how often the SLA sweeper ticks
// (SLA_SWEEP_INTERVAL overrides it).
const DefaultSLASweepInterval = time.Minute

// slaSweepInterval reads SLA_SWEEP_INTERVAL (a Go duration or plain
// seconds). Unset, malformed or non-positive values use the default, so a
// typo never disables the sweeper silently.
func slaSweepInterval(raw string, logger *slog.Logger) time.Duration {
	if raw == "" {
		return DefaultSLASweepInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		if secs, serr := strconv.ParseFloat(raw, 64); serr == nil {
			d, err = time.Duration(secs*float64(time.Second)), nil
		}
	}
	if err != nil || d <= 0 {
		logger.Warn("invalid SLA_SWEEP_INTERVAL; using the default", "value", raw, "default", DefaultSLASweepInterval)
		return DefaultSLASweepInterval
	}
	return d
}

// shutdownDrainDelay reads SHUTDOWN_DRAIN_DELAY ("0" disables; unset,
// negative or unparsable = the default).
func shutdownDrainDelay(logger *slog.Logger) time.Duration {
	raw := os.Getenv("SHUTDOWN_DRAIN_DELAY")
	if raw == "" {
		return DefaultShutdownDrainDelay
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		logger.Warn("ignoring invalid SHUTDOWN_DRAIN_DELAY", "value", raw, "default", DefaultShutdownDrainDelay.String())
		return DefaultShutdownDrainDelay
	}
	return d
}

// bootConfig is the mandatory configuration: real Postgres, real Kafka and
// the six pinned consumer groups. There is no default and no degraded mode
// behind any of it.
type bootConfig struct {
	databaseURL string
	brokers     []string
	httpAddr    string
	// consumerGroups carries each pinned variable's group id; the consumers
	// that use them are the next build step on this binary.
	consumerGroups map[string]string
}

// loadBootConfig reads and validates the mandatory environment, failing fast
// before any connection is opened. Every problem is reported at once, each
// naming its variable.
func loadBootConfig() (bootConfig, error) {
	cfg := bootConfig{
		databaseURL: os.Getenv("DATABASE_URL"), brokers: brokersFromEnv(),
		httpAddr: getenv("HTTP_ADDR", ":8080"),
	}
	var errs []error
	if cfg.databaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required: this context runs on real Postgres"))
	}
	if len(cfg.brokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required: this context runs on real Kafka"))
	}
	groups := make(map[string]string, len(consumerGroupEnvs))
	for _, name := range consumerGroupEnvs {
		if id := os.Getenv(name); id == "" {
			errs = append(errs, fmt.Errorf("%s is required: the consumers of this context run on real Kafka with pinned group ids (integration-events.md)", name))
			continue
		}
		groups[name] = os.Getenv(name)
	}
	cfg.consumerGroups = groups
	if err := errors.Join(errs...); err != nil {
		return bootConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}
