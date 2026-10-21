package main

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	outboxrelay "github.com/claudioed/exception-management/internal/adapters/outbound/outbox"
)

func TestLoadBootConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("KAFKA_BROKERS", "")
	if _, err := loadBootConfig(); err == nil {
		t.Fatal("want the missing-env error")
	}
	t.Setenv("DATABASE_URL", "postgres://x")
	if _, err := loadBootConfig(); err == nil {
		t.Fatal("want the missing-brokers error")
	}
	t.Setenv("KAFKA_BROKERS", "b1:9092, b2:9092")
	cfg, err := loadBootConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.databaseURL != "postgres://x" || len(cfg.brokers) != 2 || cfg.brokers[0] != "b1:9092" {
		t.Fatalf("cfg = %+v", cfg)
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

func TestNewLoggerLevels(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "warning", "error", "junk"} {
		if newLogger(level) == nil {
			t.Fatalf("newLogger(%q) = nil", level)
		}
	}
}
