// Package a2a is the inbound Agent2Agent adapter: it exposes this bounded
// context to the fleet's agent ecosystem as a third driving adapter over the
// SAME application-layer read use cases the REST and MCP adapters use. It is
// built on the official A2A Go SDK (a2aproject/a2a-go) and speaks JSON-RPC 2.0
// over HTTP, with the Agent Card served at the well-known path.
//
// A2A is the DELEGATION channel between bounded contexts (ADR 0007): an agent
// client sends a goal-shaped skill request as a structured message part and
// this agent answers from its own state through its own use cases. It is not
// a data-sync channel; facts between contexts still travel exclusively as
// CloudEvents on Kafka.
//
// The surface is READ-ONLY, exactly like the MCP adapter's charter:
// Deps carries no write use case at all, so a skill cannot triage, assign or
// resolve a case or declare a policy; those stay on REST. This package
// depends inward on the application layer and the domain only, never on an
// outbound adapter, and nothing else may depend on it
// (internal/architecture's TestA2AAdapterDependencyRule). The composition root
// (cmd/a2a) wires concrete repositories into the use cases. There is no auth
// of any kind (fleet-wide revert 2026-09-11): access control is the
// in-cluster ClusterIP boundary. Domain structs never leak across the message
// boundary.
package a2a

import (
	"context"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent
// Card) and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the SAME read use cases the REST and MCP adapters use and holds
// no write use case at all (TestDepsHoldNoWriteUseCase pins it); the adapter
// never constructs an outbound adapter itself.
type Deps struct {
	List      *usecases.ListOpenExceptions
	Get       *usecases.GetException
	Policies  *usecases.ListExceptionPolicies
	SLAReport *usecases.GetSLAReport
}

// Card builds this agent's Agent Card (ADR 0007). The surface is two curated,
// goal-shaped READ skills, not one per endpoint, the same curation discipline
// the MCP adapter applies to its tools: sla_status answers "how does the open
// caseload sit against its SLA clocks" and find_exceptions answers "show me
// the cases". The card declares no security schemes on purpose: this surface
// is unauthenticated by the same fleet-wide decision as REST and MCP.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "exception-management",
		Description: "Exception Management agent for the warehouse (read-only): answers how the open exception caseload sits against its SLA clocks (open, breached, escalated, by severity and queue, optionally with the severity/queue/SLA policy declared per source type) and finds exception cases, one by id or a filtered list by status, queue, site and source type, with their SLA clock, occurrence count, assignee, resolution and audit trail. It reads this context's own state only; triaging, assigning and resolving a case and declaring a policy are REST operations and are not offered here.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/exception-management",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillSLAStatus,
				Name:        "SLA status of the open exception caseload",
				Description: "How the open caseload sits against its SLA clocks at generatedAt: openCases, breached (open cases past their SLA due time), escalated (cases the SLA sweeper flagged) and the open counts bySeverity and byQueue. With includePolicies=true it also returns the exception policy declared for every source type (latest version, severity, queue, slaMinutes and the auto-resolve rule or null). Read-only; takes no required argument.",
				Tags:        []string{"exceptions", "sla", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"sla_status","arguments":{}}`,
					`{"skill":"sla_status","arguments":{"includePolicies":true}}`,
				},
			},
			{
				ID:          skillFindExceptions,
				Name:        "Find exception cases",
				Description: "Reads exception cases, newest opened first. Give caseId (starts with case-) to read exactly one case, or any of status (OPEN, TRIAGED, ASSIGNED or RESOLVED; omitted means every status, open and resolved alike), queue, siteId and sourceType (the full CloudEvents type that opened the case) to list the matching cases; caseId cannot be combined with the filters, and no argument lists every case. Each case carries its dedup identity, severity, queue, occurrenceCount, escalated flag, assignee, SLA clock (slaMinutes, slaDueAt), the frozen sourcePayload of the opening fact, the resolution once resolved and the audit trail. Read-only; an unknown caseId is answered as a 404 problem.",
				Tags:        []string{"exceptions", "cases", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"find_exceptions","arguments":{"status":"OPEN","queue":"ps-main"}}`,
					`{"skill":"find_exceptions","arguments":{"caseId":"case-1b4e28ba"}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are transactional request/response reads, so
// there is no long-running Task lifecycle to track. The response carries the
// caller's contextId (and taskId when the client referenced one) so a
// delegating agent can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(answer(ctx, deps, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler, the only transport this
// agent ships (ADR 0007), mirroring the MCP adapter's "Streamable HTTP only"
// minimalism.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
