package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	outboxrelay "github.com/claudioed/exception-management/internal/adapters/outbound/outbox"
)

// setMandatoryEnv sets every mandatory variable (unique per test via t.Setenv
// semantics); individual tests unset the one they prove is required.
func setMandatoryEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("KAFKA_BROKERS", "b1:9092, b2:9092")
	for _, name := range consumerGroupEnvs {
		t.Setenv(name, strings.ToLower(strings.TrimSuffix(name, "_CONSUMER_GROUP"))+"-cg")
	}
}

func TestLoadBootConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("KAFKA_BROKERS", "")
	for _, name := range consumerGroupEnvs {
		t.Setenv(name, "")
	}
	if _, err := loadBootConfig(); err == nil {
		t.Fatal("want the missing-env error")
	}

	setMandatoryEnv(t)
	t.Setenv("DATABASE_URL", "")
	if _, err := loadBootConfig(); err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("err = %v, want it to name DATABASE_URL", err)
	}

	setMandatoryEnv(t)
	t.Setenv("KAFKA_BROKERS", "")
	if _, err := loadBootConfig(); err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Fatalf("err = %v, want it to name KAFKA_BROKERS", err)
	}

	setMandatoryEnv(t)
	cfg, err := loadBootConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.databaseURL != "postgres://x" || len(cfg.brokers) != 2 || cfg.brokers[0] != "b1:9092" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.httpAddr != ":8080" {
		t.Fatalf("httpAddr = %q, want the default :8080", cfg.httpAddr)
	}
	if len(cfg.consumerGroups) != len(consumerGroupEnvs) {
		t.Fatalf("consumer groups = %d, want %d", len(cfg.consumerGroups), len(consumerGroupEnvs))
	}
}

// Every pinned consumer-group variable is boot-mandatory on its own, and the
// error names it (fail-closed now; the consumers themselves are step 6).
func TestLoadBootConfigNamesEachMissingConsumerGroup(t *testing.T) {
	for _, missing := range consumerGroupEnvs {
		setMandatoryEnv(t)
		t.Setenv(missing, "")
		_, err := loadBootConfig()
		if err == nil || !strings.Contains(err.Error(), missing) {
			t.Fatalf("missing %s: err = %v, want it named", missing, err)
		}
	}
}

func TestLoadBootConfigHTTPAddr(t *testing.T) {
	setMandatoryEnv(t)
	t.Setenv("HTTP_ADDR", "127.0.0.1:9099")
	cfg, err := loadBootConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.httpAddr != "127.0.0.1:9099" {
		t.Fatalf("httpAddr = %q", cfg.httpAddr)
	}
}

func TestOutboxMaxAttempts(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	for _, tc := range []struct {
		raw  string
		want int
	}{
		{"", outboxrelay.DefaultMaxAttempts},
		{"5", 5},
		{"0", 0}, // unlimited is an explicit operator choice
		{"-1", outboxrelay.DefaultMaxAttempts},
		{"junk", outboxrelay.DefaultMaxAttempts},
	} {
		if got := outboxMaxAttempts(tc.raw, logger); got != tc.want {
			t.Errorf("outboxMaxAttempts(%q) = %d, want %d", tc.raw, got, tc.want)
		}
	}
}

func TestParseRelayInterval(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	for _, tc := range []struct {
		raw  string
		want time.Duration
	}{
		{"", outboxrelay.DefaultInterval},
		{"250ms", 250 * time.Millisecond},
		{"2", 2 * time.Second}, // plain seconds
		{"0", outboxrelay.DefaultInterval},
		{"junk", outboxrelay.DefaultInterval},
	} {
		if got := parseRelayInterval(tc.raw, logger); got != tc.want {
			t.Errorf("parseRelayInterval(%q) = %s, want %s", tc.raw, got, tc.want)
		}
	}
}

func TestShutdownDrainDelay(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	t.Setenv("SHUTDOWN_DRAIN_DELAY", "")
	if got := shutdownDrainDelay(logger); got != DefaultShutdownDrainDelay {
		t.Errorf("unset = %s, want the default", got)
	}
	t.Setenv("SHUTDOWN_DRAIN_DELAY", "0")
	if got := shutdownDrainDelay(logger); got != 0 {
		t.Errorf("0 = %s, want disabled", got)
	}
	t.Setenv("SHUTDOWN_DRAIN_DELAY", "250ms")
	if got := shutdownDrainDelay(logger); got != 250*time.Millisecond {
		t.Errorf("250ms = %s", got)
	}
	t.Setenv("SHUTDOWN_DRAIN_DELAY", "junk")
	if got := shutdownDrainDelay(logger); got != DefaultShutdownDrainDelay {
		t.Errorf("junk = %s, want the default", got)
	}
}

func TestSLASweepInterval(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	for _, tc := range []struct {
		raw  string
		want time.Duration
	}{
		{"", DefaultSLASweepInterval},
		{"30s", 30 * time.Second},
		{"5", 5 * time.Second}, // plain seconds
		{"0", DefaultSLASweepInterval},
		{"-1s", DefaultSLASweepInterval},
		{"junk", DefaultSLASweepInterval},
	} {
		if got := slaSweepInterval(tc.raw, logger); got != tc.want {
			t.Errorf("slaSweepInterval(%q) = %s, want %s", tc.raw, got, tc.want)
		}
	}
}

func TestNewLoggerLevels(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "warning", "error", "junk"} {
		if newLogger(level) == nil {
			t.Fatalf("newLogger(%q) = nil", level)
		}
	}
}
