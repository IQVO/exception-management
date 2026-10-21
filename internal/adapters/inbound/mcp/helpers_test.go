package mcp_test

import (
	"context"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/exception-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// now is the fixture clock the seeded cases are stamped with.
var now = time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)

const (
	testSourceType = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	testQueue      = "ps-main"
)

// harness is a connected in-memory MCP client over the real server wired to
// the in-memory repositories, exactly as cmd/mcp wires them (minus
// Postgres). The write use cases exist ONLY here, to seed data the way the
// REST adapter would: the MCP surface itself has none.
type harness struct {
	session *sdk.ClientSession
	cases   *memory.Cases
	pol     *memory.Policies
	ids     *memory.IDGen

	declare *usecases.DeclareExceptionPolicy
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	cases, pol, ev := memory.NewCases(), memory.NewPolicies(), &memory.Events{}
	ids := memory.NewIDGen()
	deps := inboundmcp.Deps{
		List:      &usecases.ListOpenExceptions{Cases: cases},
		Get:       &usecases.GetException{Cases: cases},
		Policies:  &usecases.ListExceptionPolicies{Policies: pol},
		SLAReport: &usecases.GetSLAReport{Cases: cases, Clock: memory.Clock{T: now}},
	}
	return &harness{
		session: connectSession(t, deps),
		cases:   cases,
		pol:     pol,
		ids:     ids,
		declare: &usecases.DeclareExceptionPolicy{Policies: pol, Events: ev, Clock: memory.Clock{T: now}},
	}
}

func connectSession(t *testing.T, deps inboundmcp.Deps) *sdk.ClientSession {
	t.Helper()
	ctx := context.Background()
	clientTransport, serverTransport := sdk.NewInMemoryTransports()
	serverSession, err := inboundmcp.NewServer(deps).Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// declarePolicy seeds one policy version through the use case (severity
// HIGH, queue ps-main, the given SLA) and returns nothing; the list tools
// read it back.
func (h *harness) declarePolicy(t *testing.T, sourceType string, sla int64, autoOn string, within int64) {
	t.Helper()
	_, err := h.declare.Execute(context.Background(), usecases.PolicyDeclaration{
		SourceType: sourceType, Severity: "HIGH", Queue: testQueue, SLAMinutes: sla,
		AutoResolveOn: autoOn, WithinMinutes: within, Actor: "op-1",
	})
	if err != nil {
		t.Fatal(err)
	}
}

// openCase seeds one case from a consumed fact (opened at now + offset) and
// returns its id.
func (h *harness) openCase(t *testing.T, subject, site string, offset time.Duration) string {
	t.Helper()
	at := now.Add(offset)
	uc := &usecases.OpenOrTouchException{
		Cases: h.cases, Policies: h.pol, Events: &memory.Events{}, IDs: h.ids, Clock: memory.Clock{T: at},
	}
	c, err := uc.Execute(context.Background(), usecases.Fact{
		SourceType: testSourceType, SourceSubject: subject, SiteID: site,
		SourceEventID: "evt-" + subject, Payload: []byte(`{"task_id":"task-8a1f","order_ref":"wu-8a1f"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return c.ID()
}

// must fails the test on err.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// call invokes a tool and fails the test on a TRANSPORT/protocol error;
// tool-level failures come back as res.IsError.
func call(t *testing.T, session *sdk.ClientSession, name string, args map[string]any) *sdk.CallToolResult {
	t.Helper()
	res, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: transport/protocol error (want a tool result): %v", name, err)
	}
	return res
}

// ok calls a tool, requires success, and returns its structured content.
func (h *harness) ok(t *testing.T, name string, args map[string]any) map[string]any {
	t.Helper()
	res := call(t, h.session, name, args)
	if res.IsError {
		t.Fatalf("%s returned a tool error: %s", name, text(res))
	}
	sc, isMap := res.StructuredContent.(map[string]any)
	if !isMap {
		t.Fatalf("%s: structured content = %#v, want an object", name, res.StructuredContent)
	}
	return sc
}

// failWith calls a tool and requires an isError result whose text starts with
// the slug want.
func failWith(t *testing.T, session *sdk.ClientSession, name string, args map[string]any, want string) {
	t.Helper()
	res := call(t, session, name, args)
	if !res.IsError {
		t.Fatalf("%s: expected an isError tool result, got %#v", name, res.StructuredContent)
	}
	if got := text(res); !strings.HasPrefix(got, want+": ") {
		t.Fatalf("%s: error text %q does not start with %q", name, got, want+": ")
	}
}

func text(res *sdk.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, isText := c.(*sdk.TextContent); isText {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

// strs returns the string field key of every item of a list result, in order.
func strs(page map[string]any, listKey, key string) []string {
	var out []string
	items, _ := page[listKey].([]any)
	for _, item := range items {
		out = append(out, item.(map[string]any)[key].(string))
	}
	return out
}
