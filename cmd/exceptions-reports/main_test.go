package main

import (
	"strings"
	"testing"
)

func TestGetenvFallsBack(t *testing.T) {
	t.Setenv("X_PRESENT", "value")
	if got := getenv("X_PRESENT", "fallback"); got != "value" {
		t.Fatalf("present = %q", got)
	}
	if got := getenv("X_ABSENT", "fallback"); got != "fallback" {
		t.Fatalf("absent = %q", got)
	}
}

// A missing ANALYTICS_DATABASE_URL is the named boot error; run never gets
// as far as dialling anything. The wired behaviours are proven by the
// reports router tests and the projector's end-to-end integration proof.
func TestRunRequiresTheAnalyticsURL(t *testing.T) {
	t.Setenv("ANALYTICS_DATABASE_URL", "")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	err := run()
	if err == nil || !strings.Contains(err.Error(), "ANALYTICS_DATABASE_URL") {
		t.Fatalf("run = %v, want the named boot error", err)
	}
}
