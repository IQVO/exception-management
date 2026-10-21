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
// may fail before the relay excludes it from claims (ADR 0005). Unset = the
// fleet default (20); 0 = unlimited; negative or unparsable values log a
// warning and use the default, so a typo never silently removes the cap.
// Every relay a cmd builds passes it (fitness test
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

// bootConfig is the mandatory configuration: real Postgres and real Kafka;
// there is no degraded mode behind any of it.
type bootConfig struct {
	databaseURL string
	brokers     []string
}

// loadBootConfig reads and validates the mandatory environment, failing fast
// before any connection is opened.
func loadBootConfig() (bootConfig, error) {
	cfg := bootConfig{databaseURL: os.Getenv("DATABASE_URL"), brokers: brokersFromEnv()}
	var errs []error
	if cfg.databaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required: this context runs on real Postgres"))
	}
	if len(cfg.brokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required: this context runs on real Kafka"))
	}
	if err := errors.Join(errs...); err != nil {
		return bootConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}
