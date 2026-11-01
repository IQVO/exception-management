package http

import (
	"net/http"
	"testing"
)

// TestRetiredRoutesAreGone: the verb sub-paths of the pre-guideline API answer
// 404 or 405 with a key and without one — no alias, no redirect, no toggle.
func TestRetiredRoutesAreGone(t *testing.T) {
	f := newFixture(t)
	h, _ := f.withIdempotency(t)
	for _, route := range []string{
		"/exceptions/case-0001/triage",
		"/exceptions/case-0001/assign",
		"/exceptions/case-0001/resolve",
	} {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodGet} {
			t.Run(method+" "+route, func(t *testing.T) {
				rec := call(t, h, method, route, `{"assignee":"op-a"}`, idemKey("k-retired"))
				if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
					t.Fatalf("%s %s = %d %s, want 404 or 405", method, route, rec.Code, rec.Body.String())
				}
			})
		}
	}
}
