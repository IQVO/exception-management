// Package mcp is the inbound Model Context Protocol adapter: it exposes this
// bounded context to the AI ecosystem as a second driving adapter over the
// SAME application-layer read use cases the REST adapter uses. It is built
// on the official MCP Go SDK and served over Streamable HTTP only.
//
// The surface is READ-ONLY (the ops-agent surface of the plan): every tool
// reads exception data — list_open_exceptions, get_exception,
// get_exception_policies, get_sla_report; triaging, assigning and resolving
// a case and declaring a policy stay on REST, and governance_test.go fails
// the build on a write-verb tool name. This package depends inward on the
// application layer and the domain only — never on an outbound adapter or
// the REST adapter — and nothing else may depend on it
// (internal/architecture's TestMCPAdapterDependencyRule). The composition
// root (cmd/mcp) wires concrete repositories into the use cases. There is no
// auth of any kind (fleet-wide revert 2026-09-11;
// TestNoAuthMiddlewareReintroduced): access control is the in-cluster
// ClusterIP boundary.
package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// serverName and serverVersion identify this server in the MCP initialize
// handshake.
const (
	serverName    = "exception-management-mcp"
	serverVersion = "1.0.0"
)

// instructions is what an MCP client is told about this server at initialize.
const instructions = "Exception management of the warehouse (read-only): see the exception cases a source " +
	"fact opened (status OPEN, TRIAGED, ASSIGNED or RESOLVED) with list_open_exceptions and get_exception, the " +
	"severity/queue/SLA policy declared per source type with get_exception_policies, and how the open caseload " +
	"sits against its SLA clocks (breached, escalated, by severity and queue) with get_sla_report. Cases are " +
	"deduplicated per source stream, so occurrence_count is how often the same exception happened. Nothing here " +
	"changes data: triaging, assigning or resolving a case and declaring a policy are REST operations of " +
	"exception-management. The warehouse://exception-management/sla-report resource carries the same " +
	"SLA report, and the triage_exception_caseload prompt is the operator SOP to start from."

// NewServer builds the MCP server for this bounded context with every tool,
// resource and prompt registered.
func NewServer(deps Deps) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: serverName, Version: serverVersion},
		&mcp.ServerOptions{Instructions: instructions},
	)
	deps.registerTools(server)
	deps.registerResources(server)
	deps.registerPrompts(server)
	return server
}

// Handler returns the Streamable HTTP handler for the MCP server.
func Handler(server *mcp.Server) http.Handler {
	// Stateless (Agentic Standard v2, AS-4): no Mcp-Session-Id is issued, so
	// the server also answers the 2026-07-28 revision's per-request handshake
	// (server/discover) while legacy initialize clients keep working. Nothing
	// here relies on cross-call session state.
	return mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: true},
	)
}
