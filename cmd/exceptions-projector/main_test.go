package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestLoadConfigReadsTheEnvironment(t *testing.T) {
	full := map[string]string{
		"ANALYTICS_DATABASE_URL":   "postgres://u:p@h:5432/analytics",
		"KAFKA_BROKERS":            "b1:9092, b2:9092",
		"ANALYTICS_CONSUMER_GROUP": "exception-management-analytics",
		"ADMIN_ADDR":               ":8099",
		"LOG_LEVEL":                "debug",
	}
	cfg, err := loadConfig(func(k string) string { return full[k] })
	if err != nil {
		t.Fatalf("full environment: %v", err)
	}
	if cfg.analyticsURL == "" || cfg.group == "" || cfg.adminAddr != ":8099" || len(cfg.brokers) != 2 {
		t.Fatalf("cfg = %+v", cfg)
	}
}

// Every missing mandatory variable is named in one joined boot error.
func TestLoadConfigNamesEveryMissingVariable(t *testing.T) {
	_, err := loadConfig(func(string) string { return "" })
	if err == nil {
		t.Fatal("empty environment: want the joined boot error")
	}
	for _, name := range []string{"ANALYTICS_DATABASE_URL", "KAFKA_BROKERS", "ANALYTICS_CONSUMER_GROUP"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("boot error %q does not name %s", err, name)
		}
	}
}

// /healthz never flips; /readyz flips to 503 as the first step of shutdown.
func TestAdminMuxFlipsReadinessOnly(t *testing.T) {
	var notReady atomic.Bool
	srv := httptest.NewServer(newAdminMux(&notReady))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz = %d", resp.StatusCode)
	}
	notReady.Store(true)
	resp, err = http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz after the flip = %d, want 503", resp.StatusCode)
	}
	resp, err = http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz after the flip = %d, want 200", resp.StatusCode)
	}
}
