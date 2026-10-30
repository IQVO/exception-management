package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"

	inbounda2a "github.com/claudioed/exception-management/internal/adapters/inbound/a2a"
)

const testAgentURL = "http://exception-management-a2a.warehouse-systems.svc.cluster.local:8093/"

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// The a2a binary has no in-memory mode: a missing DATABASE_URL is a boot
// error naming it.
func TestBuildDepsRefusesWithoutADatabase(t *testing.T) {
	_, _, err := buildDeps(context.Background(), quietLogger(), "")
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("buildDeps without a database = %v, want a boot error naming DATABASE_URL", err)
	}
}

// An unreachable database is a boot error (the migrations step dials it).
func TestBuildDepsBadDatabaseFailsBoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, _, err := buildDeps(ctx, quietLogger(), "postgres://u@127.0.0.1:1/db"); err == nil {
		t.Fatal("an unreachable database must fail boot")
	}
}

// Each missing or rejected value is a boot error that names its variable.
func TestRunRefusesEachMissingOrRejectedValue(t *testing.T) {
	cases := []struct {
		name, database, agentURL, want, variable string
	}{
		{"database unset", "", testAgentURL, "DATABASE_URL is required: this context runs on real Postgres", "DATABASE_URL"},
		{"agent url unset", "postgres://u@127.0.0.1:1/db", "", "A2A_AGENT_URL is required: the Agent Card must advertise", "A2A_AGENT_URL"},
		{"agent url blank", "postgres://u@127.0.0.1:1/db", "   ", "A2A_AGENT_URL is required", "A2A_AGENT_URL"},
		{"agent url relative", "postgres://u@127.0.0.1:1/db", "/a2a", `A2A_AGENT_URL="/a2a" is not an absolute http(s) URL`, "A2A_AGENT_URL"},
		{"agent url schemeless", "postgres://u@127.0.0.1:1/db", "exception-management-a2a:8093", "is not an absolute http(s) URL", "A2A_AGENT_URL"},
		{"agent url wrong scheme", "postgres://u@127.0.0.1:1/db", "ftp://host/", "is not an absolute http(s) URL", "A2A_AGENT_URL"},
		{"agent url without a host", "postgres://u@127.0.0.1:1/db", "http:///a2a", "is not an absolute http(s) URL", "A2A_AGENT_URL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", tc.database)
			t.Setenv("A2A_AGENT_URL", tc.agentURL)
			err := run(context.Background(), quietLogger())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run = %v, want a boot error containing %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), tc.variable) {
				t.Fatalf("the error does not name %s: %v", tc.variable, err)
			}
		})
	}
}

func TestCheckAgentURLAcceptsAbsoluteHTTPAndHTTPS(t *testing.T) {
	for _, ok := range []string{testAgentURL, "https://gw.example/api/a2a/exception-management/", "http://localhost:8093", " http://x.example/ "} {
		if err := checkAgentURL(ok); err != nil {
			t.Errorf("checkAgentURL(%q) = %v, want accepted", ok, err)
		}
	}
}

// run fails fast on a bad configuration: before any connection and before the
// listener binds.
func TestRunFailsFastBeforeBindingTheListener(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u@127.0.0.1:1/db?sslmode=disable")
	t.Setenv("A2A_AGENT_URL", "not-a-url")
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	t.Setenv("A2A_ADDR", addr)

	start := time.Now()
	if err := run(context.Background(), quietLogger()); err == nil || !strings.Contains(err.Error(), "A2A_AGENT_URL") {
		t.Fatalf("run = %v, want a boot error naming A2A_AGENT_URL", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("a configuration error must not wait on any dependency")
	}
	if c, derr := (&net.Dialer{Timeout: 200 * time.Millisecond}).DialContext(t.Context(), "tcp", addr); derr == nil {
		_ = c.Close()
		t.Fatal("the listener was bound although the configuration is invalid")
	}
}

// newTestRouter builds the router exactly as run() does: the real executor
// (empty deps are fine, no skill that touches a use case is invoked here) and
// the real Agent Card.
func newTestRouter() http.Handler {
	card := inbounda2a.Card(inbounda2a.Config{AgentURL: testAgentURL, Version: serviceVersion})
	return newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(inbounda2a.Deps{})), card)
}

func TestRouterHealthzIsOpenAndServesOK(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestRouter().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != `{"status":"ok"}` {
		t.Fatalf("GET /healthz = %d %q, want 200 {\"status\":\"ok\"}", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

func TestRouterServesTheAgentCardAtTheWellKnownPath(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestRouter().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/agent-card.json", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET agent card: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{`"name":"exception-management"`, testAgentURL, `"sla_status"`, `"find_exceptions"`} {
		if !strings.Contains(body, want) {
			t.Errorf("agent card lacks %s: %s", want, body)
		}
	}
	for _, banned := range []string{"securitySchemes", "triage_exception", "resolve_exception"} {
		if strings.Contains(body, banned) {
			t.Errorf("agent card mentions %s: %s", banned, body)
		}
	}
}

// TestRouterBothMountsReachTheJSONRPCHandler pins the fleet's
// <CTX>_A2A_ENDPOINT convention: the endpoint answers at "/" and at "/a2a".
func TestRouterBothMountsReachTheJSONRPCHandler(t *testing.T) {
	h := newTestRouter()
	for _, path := range []string{"/", "/a2a"} {
		t.Run(path, func(t *testing.T) {
			// An unknown skill is answered in-band before any use case runs, so
			// a JSON-RPC result proves the request reached the SDK handler.
			body := `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"data":{"skill":"launch_rocket","arguments":{}}}]}}}`
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("A2A-Version", "1.0")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			raw, _ := io.ReadAll(rec.Body)
			if rec.Code == http.StatusNotFound || !strings.Contains(string(raw), `"jsonrpc"`) {
				t.Fatalf("POST %s: %d %q does not look like a JSON-RPC reply", path, rec.Code, raw)
			}
			if !strings.Contains(string(raw), "unknown-skill") {
				t.Fatalf("POST %s: reply %q lacks the in-band unknown-skill problem", path, raw)
			}
		})
	}
}

// The router serves the real wire for the SDK client at both mounts: a client
// built from a card whose URL carries the mount reaches the agent.
func TestRouterAgentIsDialableFromItsCardAtBothMounts(t *testing.T) {
	srv := httptest.NewServer(newTestRouter())
	defer srv.Close()
	for _, mount := range []string{"/", "/a2a"} {
		t.Run(mount, func(t *testing.T) {
			card := inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + mount, Version: "test"})
			client, err := a2aclient.NewFromCard(context.Background(), card)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": "nope"})),
			})
			if err != nil || resp == nil {
				t.Fatalf("SendMessage = %v, %v", resp, err)
			}
		})
	}
}
