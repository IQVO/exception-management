//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"

	inbounda2a "github.com/claudioed/exception-management/internal/adapters/inbound/a2a"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// ask sends one skill request over the real JSON-RPC wire and returns the
// envelope's data part as generic JSON.
func ask(t *testing.T, client *a2aclient.Client, skill string, args map[string]any) map[string]any {
	t.Helper()
	resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args})),
	})
	if err != nil {
		t.Fatalf("SendMessage %s: %v", skill, err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) == 0 {
		t.Fatalf("reply = %T", resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first part = %T, want data", msg.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	return env
}

// dig walks nested JSON objects by key.
func dig(t *testing.T, v map[string]any, keys ...string) any {
	t.Helper()
	var cur any = v
	for _, k := range keys {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("%v: %q is not inside an object: %v", keys, k, cur)
		}
		cur = m[k]
	}
	return cur
}

// seedOverdueCase stores one HIGH case opened two hours ago (30 minute SLA)
// straight into Postgres, without touching the outbox.
func seedOverdueCase(t *testing.T, url string) {
	t.Helper()
	key, err := shared.NewDedupKey("com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed", "task-9", "SIM1")
	if err != nil {
		t.Fatal(err)
	}
	c, err := exceptioncase.Open(exceptioncase.OpenInput{
		CaseID: "case-a2a-1", Key: key, Severity: shared.SeverityHigh, Queue: "ps-main",
		SLAMinutes: 30, SourceEventID: "evt-9", Payload: []byte(`{"task_id":"t-9"}`),
		At: time.Now().Add(-2 * time.Hour).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewCaseRepo(pgtest.Open(t, url)).Save(context.Background(), c); err != nil {
		t.Fatal(err)
	}
}

// problemStatus returns the in-band problem status of an answer, or 0.
func problemStatus(env map[string]any) float64 {
	p, _ := env["problem"].(map[string]any)
	st, _ := p["status"].(float64)
	return st
}

// TestA2AAnswersFromRealPostgresAndNeverWritesTheOutbox proves the real
// wiring end to end: buildDeps (embedded migrations + the Postgres repos) ->
// the router -> the SDK client. A case seeded straight into Postgres is read
// back by both skills, and the read-only agent leaves the outbox untouched:
// A2A here carries delegation of reads, never facts.
func TestA2AAnswersFromRealPostgresAndNeverWritesTheOutbox(t *testing.T) {
	url := pgtest.URL(t)
	t.Setenv("MIGRATIONS_DATABASE_URL", url)
	deps, closeAdapters, err := buildDeps(context.Background(), quietLogger(), url)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	defer closeAdapters()

	seedOverdueCase(t, url)
	pool := pgtest.Open(t, url)
	outboxBefore := pgtest.Count(t, pool, "outbox")

	srv := httptest.NewServer(newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)),
		inbounda2a.Card(inbounda2a.Config{AgentURL: testAgentURL, Version: serviceVersion})))
	defer srv.Close()
	client, err := a2aclient.NewFromCard(context.Background(),
		inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + "/", Version: "test"}))
	if err != nil {
		t.Fatal(err)
	}

	sla := ask(t, client, "sla_status", map[string]any{"includePolicies": true})
	status := dig(t, sla, "data", "slaStatus").(map[string]any)
	if status["openCases"] != float64(1) || status["breached"] != float64(1) {
		t.Errorf("sla_status = %v, want 1 open and 1 breached case", status)
	}
	if pols, _ := status["policies"].([]any); len(pols) == 0 {
		t.Errorf("sla_status policies = %v, want the migration-seeded policies", status["policies"])
	}

	one := ask(t, client, "find_exceptions", map[string]any{"caseId": "case-a2a-1"})
	cases, _ := dig(t, one, "data", "exceptions", "cases").([]any)
	if len(cases) != 1 || cases[0].(map[string]any)["sourceSubject"] != "task-9" {
		t.Errorf("find_exceptions by id = %v", one)
	}
	list := ask(t, client, "find_exceptions", map[string]any{"status": "OPEN", "siteId": "SIM1"})
	if got := dig(t, list, "data", "exceptions", "count"); got != float64(1) {
		t.Errorf("find_exceptions list count = %v, want 1", got)
	}
	if st := problemStatus(ask(t, client, "find_exceptions", map[string]any{"caseId": "case-nope-123"})); st != 404 {
		t.Errorf("unknown case status = %v, want an in-band 404 problem", st)
	}
	// A NUL would abort a Postgres statement; the skill refuses it as a 400
	// before the database sees it.
	if st := problemStatus(ask(t, client, "find_exceptions", map[string]any{"queue": "ps\u0000main"})); st != 400 {
		t.Errorf("NUL queue status = %v, want an in-band 400 problem", st)
	}

	if after := pgtest.Count(t, pool, "outbox"); after != outboxBefore {
		t.Errorf("outbox rows %d -> %d: the read-only A2A agent must never write an event", outboxBefore, after)
	}
}
