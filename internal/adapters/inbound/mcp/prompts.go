package mcp

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// triageCaseloadSOP is the operational standard-operating-procedure the
// triage_exception_caseload prompt hands to the model. Per the charter,
// prompts encode how to interpret results, when to escalate, and what "done"
// means — they standardise agent behaviour across clients rather than leaving
// procedure implicit. The prompt may NAME the tools but never calls them, and
// it never writes: triaging, assigning and resolving a case and declaring a
// policy are REST operations of exception-management.
const triageCaseloadSOP = `You are helping an operator triage the exception caseload of a warehouse. Use only the MCP tools; never assume state. This context surfaces the caseload and its SLA clocks — deciding what to do with a case is a human call, so report and recommend, do not act: the write operations (triage, assign, resolve, declare a policy) are REST operations and are not offered to you.

Procedure:
1. Call get_sla_report to see how the open caseload sits against its SLA clocks: open_cases, breached (open cases past their sla_due_at), escalated, and the by_severity / by_queue breakdowns. Breached and escalated counts are where attention is most urgent.
2. For the queues or severities that carry the breaches, call list_open_exceptions narrowed by queue (and status) to see the individual cases, newest opened first. occurrence_count tells you how often the same exception happened — a high count on one case is a repeating problem, not many problems.
3. Read the cases that matter with get_exception (the case id starts with case-): the frozen source_payload of the opening fact, the assignee when one exists, and the audit trail of accepted transitions.
4. Call get_exception_policies to check what the declared policy says per source type: the severity and queue a new case opens with, sla_minutes, and the auto_resolve rule — a case may resolve itself when its correlated event arrives within the window, so do not recommend manual work on a case that is about to auto-resolve.

Interpretation:
- A breached case with no assignee is the top triage candidate: it is late and nobody owns it.
- A case already TRIAGED or ASSIGNED is owned; escalate only when its clock keeps running against the SLA.
- A repeating case (high occurrence_count) points at the upstream source, not the case: say so instead of proposing the same resolution again.

Escalate to a human operator when: a case is breached and unassigned, a queue's breaches exceed what its assignees can clear, or the policies themselves look wrong for the source types actually arriving (declaring a policy is a REST operation).

Done means: you have reported the SLA position, listed the cases behind the breaches with their owners and occurrence counts, checked the policies that apply, and recommended which cases a human should triage, assign or resolve first — with a one-line reason each.`

// registerPrompts adds the workflow prompt (the operator SOP).
func (d Deps) registerPrompts(server *mcp.Server) {
	server.AddPrompt(&mcp.Prompt{
		Name:        "triage_exception_caseload",
		Description: "Standard operating procedure for reading the exception caseload against its SLA clocks and recommending which cases a human triages, assigns or resolves first.",
	}, func(ctx context.Context, _ *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return &mcp.GetPromptResult{
			Description: "How to triage the exception caseload: read the SLA report, list the cases behind the breaches, check the declared policies, and know when to escalate to a human.",
			Messages: []*mcp.PromptMessage{{
				Role:    "user",
				Content: &mcp.TextContent{Text: triageCaseloadSOP},
			}},
		}, nil
	})
}
