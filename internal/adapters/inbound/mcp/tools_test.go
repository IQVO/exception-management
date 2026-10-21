package mcp_test

import (
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// TestListOpenExceptionsTools covers the listing tool: the default call
// returns every case (open and resolved alike — resolve activity is part of
// the record), each filter narrows, and the newest-opened-first order holds.
func TestListOpenExceptionsTools(t *testing.T) {
	h := newHarness(t)
	h.declarePolicy(t, testSourceType, 30, "", 0)
	c1 := h.openCase(t, "task-1", "SITE-1", -2*time.Hour)
	c2 := h.openCase(t, "task-2", "SITE-2", -1*time.Hour)
	c3 := h.openCase(t, "task-3", "SITE-1", 0)

	// The default listing: every status, newest opened first.
	page := h.ok(t, "list_open_exceptions", nil)
	if got := strs(page, "items", "case_id"); len(got) != 3 || got[0] != c3 || got[1] != c2 || got[2] != c1 {
		t.Fatalf("default listing = %v, want %v newest first", got, []string{c3, c2, c1})
	}

	// One item carries the full read shape.
	first := page["items"].([]any)[0].(map[string]any)
	for key, want := range map[string]any{
		"source_type": testSourceType, "source_subject": "task-3", "site_id": "SITE-1",
		"status": "OPEN", "severity": "HIGH", "queue": testQueue,
		"occurrence_count": float64(1), "escalated": false, "sla_minutes": float64(30),
		"source_event_id": "evt-task-3", "version": float64(1),
	} {
		if first[key] != want {
			t.Errorf("item field %q = %#v, want %#v", key, first[key], want)
		}
	}
	payload, isMap := first["source_payload"].(map[string]any)
	if !isMap || payload["task_id"] != "task-8a1f" {
		t.Errorf("source_payload = %#v, want the frozen fact object", first["source_payload"])
	}
	if due, _ := first["sla_due_at"].(string); due == "" {
		t.Error("sla_due_at is absent")
	}
	if _, has := first["assignee"]; has {
		t.Error("assignee must be omitted while unassigned")
	}

	// The filters narrow.
	filters := []struct {
		name string
		args map[string]any
		want []string
	}{
		{"status OPEN keeps every open case", map[string]any{"status": "OPEN"}, []string{c3, c2, c1}},
		{"site narrows", map[string]any{"site_id": "SITE-2"}, []string{c2}},
		{"source type narrows", map[string]any{"source_type": "com.warehouse.wes.other.Type"}, nil},
		{"queue narrows", map[string]any{"queue": testQueue}, []string{c3, c2, c1}},
		{"a status nothing holds", map[string]any{"status": "RESOLVED"}, nil},
	}
	for _, tc := range filters {
		got := strs(h.ok(t, "list_open_exceptions", tc.args), "items", "case_id")
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
				break
			}
		}
	}
}

// TestGetExceptionTool covers one-case reads: the full audit-bearing view,
// the resolution fields once resolved, and the two error slugs.
func TestGetExceptionTool(t *testing.T) {
	h := newHarness(t)
	h.declarePolicy(t, testSourceType, 30, "", 0)
	id := h.openCase(t, "task-1", "SITE-1", 0)

	got := h.ok(t, "get_exception", map[string]any{"case_id": id})
	if got["case_id"] != id || got["status"] != "OPEN" {
		t.Fatalf("get_exception = %#v", got)
	}
	if _, has := got["resolution_code"]; has {
		t.Error("resolution_code must be omitted while open")
	}
	audit, isList := got["audit"].([]any)
	if !isList || len(audit) != 1 || audit[0].(map[string]any)["action"] != "OPENED" {
		t.Errorf("audit = %#v, want the OPENED entry", got["audit"])
	}

	// Resolve through the use case the REST adapter uses, then re-read.
	resolve := &usecases.ResolveException{Cases: h.cases, Events: &memory.Events{}, Clock: memory.Clock{T: now.Add(time.Minute)}}
	must(t, func() error {
		_, err := resolve.Execute(t.Context(), id, "WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "", "op-7")
		return err
	}())
	resolved := h.ok(t, "get_exception", map[string]any{"case_id": id})
	if resolved["status"] != "RESOLVED" || resolved["resolution_code"] != "WORK_COMPLETED" {
		t.Fatalf("resolved view = %#v", resolved)
	}
	if audit, _ := resolved["audit"].([]any); len(audit) != 2 || audit[1].(map[string]any)["action"] != "RESOLVED" {
		t.Errorf("audit after resolve = %#v, want OPENED then RESOLVED", resolved["audit"])
	}

	failWith(t, h.session, "get_exception", map[string]any{"case_id": "case-nope"}, "case-not-found")
	failWith(t, h.session, "get_exception", map[string]any{"case_id": ""}, "invalid-case-id")
}

// TestGetExceptionPoliciesTool covers the policy listing: the latest version
// per source type, ordered by source type, auto_resolve null or the rule.
func TestGetExceptionPoliciesTool(t *testing.T) {
	h := newHarness(t)
	h.declarePolicy(t, "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed", 30, "", 0)
	h.declarePolicy(t, "com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached", 120,
		"com.warehouse.wes.order-management.order.OrderRepromised", 15)

	page := h.ok(t, "get_exception_policies", nil)
	got := strs(page, "items", "source_type")
	want := []string{
		"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed",
		"com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached",
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("policies = %v, want %v ordered by source type", got, want)
	}
	items := page["items"].([]any)
	plain := items[0].(map[string]any)
	if plain["auto_resolve"] != nil || plain["sla_minutes"] != float64(30) || plain["severity"] != "HIGH" {
		t.Errorf("plain policy = %#v, want auto_resolve null, sla 30, HIGH", plain)
	}
	withRule := items[1].(map[string]any)
	rule, isMap := withRule["auto_resolve"].(map[string]any)
	if !isMap || rule["on_type"] != "com.warehouse.wes.order-management.order.OrderRepromised" || rule["within_minutes"] != float64(15) {
		t.Errorf("auto_resolve = %#v, want the OrderRepromised 15-minute rule", withRule["auto_resolve"])
	}
}

// TestGetSLAReportTool covers the operations read: open counts, breaches
// against the report clock, and the by-severity/by-queue breakdowns.
func TestGetSLAReportTool(t *testing.T) {
	h := newHarness(t)
	h.declarePolicy(t, testSourceType, 30, "", 0)
	h.openCase(t, "task-old", "SITE-1", -2*time.Hour) // breached 90 minutes ago
	h.openCase(t, "task-new", "SITE-2", 0)            // 30 minutes of SLA left

	rep := h.ok(t, "get_sla_report", nil)
	if rep["open_cases"] != float64(2) || rep["breached"] != float64(1) || rep["escalated"] != float64(0) {
		t.Fatalf("report = %#v, want 2 open, 1 breached, 0 escalated", rep)
	}
	if rep["by_severity"].(map[string]any)["HIGH"] != float64(2) {
		t.Errorf("by_severity = %#v, want HIGH:2", rep["by_severity"])
	}
	if rep["by_queue"].(map[string]any)[testQueue] != float64(2) {
		t.Errorf("by_queue = %#v, want %s:2", rep["by_queue"], testQueue)
	}
	if rep["generated_at"] == "" {
		t.Error("generated_at is absent")
	}
}
