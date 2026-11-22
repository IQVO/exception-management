//go:build integration

// Integration proof of cmd/mcp: the real buildDeps (embedded migrations, the
// Postgres repositories and the four read use cases) answers the documented
// read-only tool surface (docs/docs/mcp/tools.md) over in-memory MCP
// transports, and those reads add no outbox row — this binary wires no write
// use case, so it can never emit an event (AGENTS.md: agents only read;
// events leave only through the transactional outbox).
package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/exception-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// taskCPTMissedType is the consumed fact type of the seeded case (the
// CloudEvents catalogue, ADR 0004).
const taskCPTMissedType = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"

// packageMissedLoadPolicy is one policy the embedded migrations seed
// (migrations/0004), pinned as a stable row of the documented policy listing.
const packageMissedLoadPolicy = "com.warehouse.wms.outbound-shipping.shippingpackage.PackageMissedLoad"

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// callTool invokes a documented read tool and returns its structured content;
// a transport error or an isError result fails the test.
func callTool(t *testing.T, session *sdk.ClientSession, name string, args map[string]any) map[string]any {
	t.Helper()
	res, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: transport error %v", name, err)
	}
	if res.IsError {
		t.Fatalf("%s returned a tool error: %s", name, resultText(res))
	}
	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("%s: structured content = %#v, want an object", name, res.StructuredContent)
	}
	return sc
}

// errorTool invokes a tool and returns the text of its isError result (a
// handler error is a tool result, never a transport failure).
func errorTool(t *testing.T, session *sdk.ClientSession, name string, args map[string]any) string {
	t.Helper()
	res, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: transport error %v", name, err)
	}
	if !res.IsError {
		t.Fatalf("%s succeeded, want the documented tool error: %#v", name, res.StructuredContent)
	}
	return resultText(res)
}

// resultText joins the text parts of a tool result.
func resultText(res *sdk.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, isText := c.(*sdk.TextContent); isText {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

// seedBreachedCase opens case-mcp-it-1 the documented way — the same domain
// Open the fact consumers drive, persisted through the Postgres repository
// the command side owns — HIGH, 30-minute SLA, opened two hours ago, so its
// SLA clock is breached. It returns the opening instant (whole seconds, so
// the exact SLA deadline survives the database round trip).
func seedBreachedCase(t *testing.T, url string) time.Time {
	t.Helper()
	key, err := shared.NewDedupKey(taskCPTMissedType, "task-mcp-1", "SIM1")
	if err != nil {
		t.Fatal(err)
	}
	openedAt := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	c, err := exceptioncase.Open(exceptioncase.OpenInput{
		CaseID: "case-mcp-it-1", Key: key, Severity: shared.SeverityHigh, Queue: "ps-main",
		SLAMinutes: 30, SourceEventID: "evt-mcp-1", Payload: []byte(`{"task_id":"task-mcp-1"}`),
		At: openedAt,
	})
	if err != nil {
		t.Fatalf("open case: %v", err)
	}
	if err := postgres.NewCaseRepo(pgtest.Open(t, url)).Save(context.Background(), c); err != nil {
		t.Fatalf("save case: %v", err)
	}
	return openedAt
}

// TestMCPAgainstPostgresAnswersTheDocumentedReadsAndNeverWrites proves the
// composition root end to end: buildDeps over a fresh testcontainers
// Postgres, its four read-only tools answering over in-memory transports,
// and the outbox untouched by every read.
func TestMCPAgainstPostgresAnswersTheDocumentedReadsAndNeverWrites(t *testing.T) {
	url := pgtest.URL(t) // a fresh, already migrated database
	t.Setenv("MIGRATIONS_DATABASE_URL", url)
	openedAt := seedBreachedCase(t, url)
	pool := pgtest.Open(t, url)
	outboxBefore := pgtest.Count(t, pool, "outbox")

	deps, closeAdapters, err := buildDeps(context.Background(), quietLogger(), url)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	defer closeAdapters()

	ct, st := sdk.NewInMemoryTransports()
	serverSession, err := inboundmcp.NewServer(deps).Connect(context.Background(), st, nil)
	if err != nil {
		t.Fatalf("connect server session: %v", err)
	}
	defer serverSession.Close()
	session, err := sdk.NewClient(&sdk.Implementation{Name: "integration-test", Version: "0"}, nil).Connect(context.Background(), ct, nil)
	if err != nil {
		t.Fatalf("connect client session: %v", err)
	}
	defer session.Close()

	assertCaseRead(t, session, openedAt)
	assertListFilters(t, session)
	assertPoliciesRead(t, session)
	assertSLAReportRead(t, session)
	assertDocumentedToolErrors(t, session)

	if after := pgtest.Count(t, pool, "outbox"); after != outboxBefore {
		t.Fatalf("outbox rows %d -> %d: the read-only MCP binary wrote an event", outboxBefore, after)
	}
}

// assertCaseRead reads the seeded case back through get_exception and the
// unfiltered listing: the dedup identity, the triage defaults, the exact SLA
// clock, the frozen source payload and the audit trail (docs/docs/mcp/tools.md).
func assertCaseRead(t *testing.T, session *sdk.ClientSession, openedAt time.Time) {
	t.Helper()
	one := callTool(t, session, "get_exception", map[string]any{"case_id": "case-mcp-it-1"})
	if one["case_id"] != "case-mcp-it-1" || one["status"] != "OPEN" || one["version"] != float64(1) {
		t.Fatalf("get_exception identity = %v", one)
	}
	if one["source_type"] != taskCPTMissedType || one["source_subject"] != "task-mcp-1" || one["site_id"] != "SIM1" {
		t.Fatalf("get_exception dedup key = %v", one)
	}
	if one["severity"] != "HIGH" || one["queue"] != "ps-main" || one["occurrence_count"] != float64(1) || one["escalated"] != false {
		t.Fatalf("get_exception triage = %v", one)
	}
	due := openedAt.Add(30 * time.Minute)
	if one["sla_minutes"] != float64(30) || one["sla_due_at"] != due.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("get_exception SLA clock = %v %v, want 30 minutes due at %s", one["sla_minutes"], one["sla_due_at"], due.Format(time.RFC3339Nano))
	}
	payload, _ := one["source_payload"].(map[string]any)
	if payload["task_id"] != "task-mcp-1" {
		t.Fatalf("frozen source_payload = %v", one["source_payload"])
	}
	audit, _ := one["audit"].([]any)
	if len(audit) != 1 {
		t.Fatalf("audit = %v, want the accepted OPENED transition", one["audit"])
	}
	first := audit[0].(map[string]any)
	if first["action"] != "OPENED" || first["actor"] != "system" {
		t.Fatalf("audit entry = %v", first)
	}

	listed := callTool(t, session, "list_open_exceptions", map[string]any{})
	items, _ := listed["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["case_id"] != "case-mcp-it-1" {
		t.Fatalf("list_open_exceptions = %v", listed)
	}
	if listed["next_cursor"] != nil {
		t.Fatalf("a single case is the last page, got cursor %v", listed["next_cursor"])
	}
}

// assertListFilters proves every documented narrowing of the listing.
func assertListFilters(t *testing.T, session *sdk.ClientSession) {
	t.Helper()
	narrowed := callTool(t, session, "list_open_exceptions", map[string]any{
		"status": "OPEN", "queue": "ps-main", "site_id": "SIM1", "source_type": taskCPTMissedType,
	})
	items, _ := narrowed["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("every filter matches the seeded case: %v", narrowed)
	}
	none := callTool(t, session, "list_open_exceptions", map[string]any{"queue": "ps-nope"})
	if items, _ = none["items"].([]any); len(items) != 0 {
		t.Fatalf("a queue nothing sits in must answer an empty page: %v", none)
	}
}

// assertPoliciesRead proves the policy listing answers the policies the
// embedded migrations seed, pinning one stable row (migrations/0004).
func assertPoliciesRead(t *testing.T, session *sdk.ClientSession) {
	t.Helper()
	pols := callTool(t, session, "get_exception_policies", map[string]any{})
	items, _ := pols["items"].([]any)
	if len(items) == 0 {
		t.Fatalf("get_exception_policies = %v, want the migration-seeded policies", pols)
	}
	var found map[string]any
	for _, item := range items {
		if p, _ := item.(map[string]any); p["source_type"] == packageMissedLoadPolicy {
			found = p
		}
	}
	if found == nil {
		t.Fatalf("get_exception_policies = %v, want %s", pols, packageMissedLoadPolicy)
	}
	if found["version"] != float64(1) || found["severity"] != "HIGH" || found["queue"] != "problem-solve" || found["sla_minutes"] != float64(60) {
		t.Fatalf("policy %s = %v", packageMissedLoadPolicy, found)
	}
}

// assertSLAReportRead proves the SLA report read model over the seeded
// caseload: one open case, breached (opened two hours ago on a 30-minute
// clock), none escalated, counted by severity and queue.
func assertSLAReportRead(t *testing.T, session *sdk.ClientSession) {
	t.Helper()
	sla := callTool(t, session, "get_sla_report", map[string]any{})
	if sla["open_cases"] != float64(1) || sla["breached"] != float64(1) || sla["escalated"] != float64(0) {
		t.Fatalf("get_sla_report = %v, want 1 open breached case", sla)
	}
	if sla["generated_at"] == "" {
		t.Fatalf("get_sla_report generated_at = %v", sla)
	}
	bySeverity, _ := sla["by_severity"].(map[string]any)
	if bySeverity["HIGH"] != float64(1) {
		t.Fatalf("by_severity = %v", bySeverity)
	}
	byQueue, _ := sla["by_queue"].(map[string]any)
	if byQueue["ps-main"] != float64(1) {
		t.Fatalf("by_queue = %v", byQueue)
	}
}

// assertDocumentedToolErrors proves the two documented error paths of
// get_exception: case-not-found for an unknown id, invalid-case-id for a
// blank one (docs/docs/mcp/tools.md).
func assertDocumentedToolErrors(t *testing.T, session *sdk.ClientSession) {
	t.Helper()
	if got := errorTool(t, session, "get_exception", map[string]any{"case_id": "case-nope-123"}); !strings.HasPrefix(got, "case-not-found: ") {
		t.Fatalf("unknown case error = %q, want case-not-found", got)
	}
	if got := errorTool(t, session, "get_exception", map[string]any{"case_id": ""}); !strings.HasPrefix(got, "invalid-case-id: ") {
		t.Fatalf("blank case error = %q, want invalid-case-id", got)
	}
}
