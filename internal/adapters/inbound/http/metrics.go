package http

import (
	"net/http"
	"strings"
	"unicode/utf8"
)

// utf8Len is the rune count of s (the OpenAPI bounds count characters, not
// bytes).
func utf8Len(s string) int { return utf8.RuneCountInString(s) }

// isBlank reports whether s is blank in the ECMA-262 sense (the domain's own
// blank rule: only whitespace is blank).
func isBlank(s string) bool { return strings.TrimSpace(s) == "" }

// handleMetrics backs GET /metrics: the Prometheus scrape endpoint. v1 wires
// the route with the process's presence gauge (the contract declares the
// endpoint's text exposition shape; a full metrics registry is a later,
// fleet-wide instrumentation step — no prometheus client is in the module).
func handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(`# HELP exception_management_up The exception-management process is up.
# TYPE exception_management_up gauge
exception_management_up 1
`))
}
