package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

func req(target string) *http.Request { return httptest.NewRequest(http.MethodGet, target, nil) }

func withParam(r *http.Request, name, value string) *http.Request {
	r = r.Clone(r.Context())
	// The way chi injects a matched route parameter.
	ctx := chi.RouteContext(r.Context())
	if ctx == nil {
		ctx = chi.NewRouteContext()
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, ctx))
	}
	ctx.URLParams.Add(name, value)
	return r
}

func TestCaseIDParam(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"case-1", true},
		{"case-1d2e3f4a-5b6c", true},
		{"nope", false},
		{"case-", false},           // under the 6-character bound
		{"case-a b", false},        // whitespace
		{"case-a/b", false},        // '/'
		{"case-\x00a", false},      // NUL
		{"case-" + pad65(), false}, // over the 64-character bound
	} {
		r := withParam(req("/exceptions/{caseId}"), "caseId", tc.raw)
		_, err := caseIDParam(r)
		if (err == nil) != tc.want {
			t.Errorf("caseIDParam(%q) err = %v, want valid=%v", tc.raw, err, tc.want)
		}
	}
}

func pad65() string {
	b := make([]byte, 65)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

func TestSourceTypeParam(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed", true},
		{"com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected", true},
		{"cpt-missed", false},
		{"com.warehouse.wes", false},
		// The CONTRACT pattern is looser than the domain's isFullType: a
		// lowercase event segment passes the parameter and the domain's own
		// ErrInvalidSourceType answers it at the use case (the same
		// invalid-source-type problem).
		{"com.warehouse.wes.fulfillment-execution.task.taskMissed", true},
		{"nope", false},
	} {
		r := withParam(req("/exception-policies/{sourceType}"), "sourceType", tc.raw)
		_, err := sourceTypeParam(r)
		if (err == nil) != tc.want {
			t.Errorf("sourceTypeParam(%q) err = %v, want valid=%v", tc.raw, err, tc.want)
		}
	}
}

func TestStatusFilter(t *testing.T) {
	for raw, want := range map[string]bool{"": true, "OPEN": true, "TRIAGED": true, "ASSIGNED": true, "RESOLVED": true, "CLOSED": false, "open": false} {
		if _, err := statusFilter(raw); (err == nil) != want {
			t.Errorf("statusFilter(%q) err = %v, want valid=%v", raw, err, want)
		}
	}
}

func TestSeverityOf(t *testing.T) {
	for raw, want := range map[string]bool{"LOW": true, "MEDIUM": true, "HIGH": true, "CRITICAL": true, "SEV1": false, "": false} {
		_, err := severityOf(raw)
		if (err == nil) != want {
			t.Errorf("severityOf(%q) err = %v, want valid=%v", raw, err, want)
		}
	}
	if s, err := severityOf("HIGH"); err != nil || s != shared.SeverityHigh {
		t.Fatalf("severityOf(HIGH) = %v %v", s, err)
	}
}

func TestQueueOf(t *testing.T) {
	if _, err := queueOf("cps-front"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "   ", pad65()} {
		if _, err := queueOf(bad); err == nil {
			t.Errorf("queueOf(%q) accepted an invalid queue", bad)
		}
	}
}

func TestCodeOf(t *testing.T) {
	if _, err := codeOf("WORK_COMPLETED"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "not a code", "lower", pad65()} {
		if _, err := codeOf(bad); err == nil {
			t.Errorf("codeOf(%q) accepted an invalid code", bad)
		}
	}
}

func TestNoteOf(t *testing.T) {
	if _, err := noteOf("moved by hand"); err != nil {
		t.Fatal(err)
	}
	if _, err := noteOf(""); err != nil {
		t.Fatal("an empty note is valid (resolved without a note)")
	}
	if _, err := noteOf(string(make([]byte, 2001))); err == nil {
		t.Fatal("a 2001-character note must be rejected")
	}
}
