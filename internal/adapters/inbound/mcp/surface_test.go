package mcp_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestSLAReportResource covers the one resource (AS-3): it is listed, reads
// the SAME bounded read model the get_sla_report tool returns, and serves it
// as JSON text under the pinned URI.
func TestSLAReportResource(t *testing.T) {
	h := newHarness(t)
	h.declarePolicy(t, testSourceType, 30, "", 0)
	h.openCase(t, "task-old", "SITE-1", -2*time.Hour) // breached 90 minutes ago
	h.openCase(t, "task-new", "SITE-2", 0)            // 30 minutes of SLA left

	res, err := h.session.ListResources(t.Context(), nil)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}
	r, ok := exactlyOneResource(t, res)
	if !ok {
		return
	}

	read, err := h.session.ReadResource(t.Context(), &sdk.ReadResourceParams{URI: r.URI})
	if err != nil {
		t.Fatalf("read resource: %v", err)
	}
	body, ok := oneJSONContent(t, read, r.URI)
	if !ok {
		return
	}
	var report map[string]any
	if err := json.Unmarshal([]byte(body), &report); err != nil {
		t.Fatalf("resource body is not JSON: %v (%s)", err, body)
	}
	// The same numbers the tool returns: one open caseload of 2, one breach.
	if report["open_cases"] != float64(2) || report["breached"] != float64(1) {
		t.Errorf("resource body = %v, want open_cases 2 and breached 1", report)
	}
	if _, has := report["generated_at"]; !has {
		t.Error("resource body has no generated_at")
	}
	if strings.Contains(body, "items") {
		t.Error("the resource must be a bounded read model, never a table dump")
	}
}

// exactlyOneResource asserts the server lists exactly the one SLA-report
// resource with complete metadata, returning it.
func exactlyOneResource(t *testing.T, res *sdk.ListResourcesResult) (*sdk.Resource, bool) {
	t.Helper()
	if len(res.Resources) != 1 {
		t.Fatalf("listed %d resources, want exactly 1 (the SLA report)", len(res.Resources))
	}
	r := res.Resources[0]
	if r.URI != "warehouse://exception-management/sla-report" {
		t.Errorf("resource uri = %q, want warehouse://exception-management/sla-report", r.URI)
	}
	if r.Name == "" || r.Description == "" || r.MIMEType != "application/json" {
		t.Errorf("resource metadata incomplete: %+v", r)
	}
	return r, true
}

// oneJSONContent asserts the read returned one content carrying the pinned URI
// and JSON MIME type, returning its text.
func oneJSONContent(t *testing.T, read *sdk.ReadResourceResult, uri string) (string, bool) {
	t.Helper()
	if len(read.Contents) != 1 {
		t.Fatalf("resource returned %d contents, want 1", len(read.Contents))
	}
	c := read.Contents[0]
	if c.URI != uri || c.MIMEType != "application/json" {
		t.Errorf("content identity = %+v, want the pinned URI and JSON MIME type", c)
	}
	return c.Text, true
}

// TestTriageCaseloadPrompt covers the one prompt (AS-3): it is listed and
// get returns the SOP text, which names the read tools but never performs a
// write.
func TestTriageCaseloadPrompt(t *testing.T) {
	h := newHarness(t)

	list, err := h.session.ListPrompts(t.Context(), nil)
	if err != nil {
		t.Fatalf("list prompts: %v", err)
	}
	if len(list.Prompts) != 1 {
		t.Fatalf("listed %d prompts, want exactly 1 (the operator SOP)", len(list.Prompts))
	}
	if list.Prompts[0].Name != "triage_exception_caseload" {
		t.Errorf("prompt name = %q, want triage_exception_caseload", list.Prompts[0].Name)
	}
	if list.Prompts[0].Description == "" {
		t.Error("prompt has no description")
	}

	got, err := h.session.GetPrompt(t.Context(), &sdk.GetPromptParams{Name: "triage_exception_caseload"})
	if err != nil {
		t.Fatalf("get prompt: %v", err)
	}
	if len(got.Messages) != 1 || got.Messages[0].Role != "user" {
		t.Fatalf("prompt messages = %+v, want one user message", got.Messages)
	}
	content, ok := got.Messages[0].Content.(*sdk.TextContent)
	if !ok {
		t.Fatalf("prompt content = %T, want text", got.Messages[0].Content)
	}
	sop := content.Text
	for _, needle := range []string{"get_sla_report", "list_open_exceptions", "get_exception", "get_exception_policies"} {
		if !strings.Contains(sop, needle) {
			t.Errorf("the SOP never names %s", needle)
		}
	}
	if !strings.Contains(sop, "REST operations") {
		t.Error("the SOP must say the write operations stay on REST")
	}
}
