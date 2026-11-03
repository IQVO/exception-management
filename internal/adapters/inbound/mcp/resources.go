package mcp

import (
	"context"
	"encoding/json"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// mimeTypeJSON is the MIME type of every resource this server serves.
const mimeTypeJSON = "application/json"

// slaReportURI is the one resource this server serves (Agentic Standard v2,
// AS-3): the SLA report read model — how the open caseload sits against its
// SLA clocks — as a bounded JSON document, not a dump of a table. It is
// backed by the same GetSLAReport use case the get_sla_report tool and the
// REST reports endpoint read, so the resource and the tool can never
// disagree.
const slaReportURI = "warehouse://exception-management/sla-report"

// registerResources adds the scoped read-model resources. Per the charter the
// surface stays read-only; resources are context data an agent can load
// without a tool call.
func (d Deps) registerResources(server *mcp.Server) {
	server.AddResource(&mcp.Resource{
		URI:         slaReportURI,
		Name:        "sla-report",
		Description: "How the open exception caseload sits against its SLA clocks at generated_at: open_cases, breached (open cases past their sla_due_at), escalated (cases the SLA sweeper flagged) and the open counts by_severity and by_queue. The same bounded read model the get_sla_report tool returns.",
		MIMEType:    mimeTypeJSON,
	}, func(ctx context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		report, err := d.SLAReport.Execute(ctx)
		if err != nil {
			return nil, mapError(err)
		}
		body, err := json.Marshal(toSLAReport(report))
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      slaReportURI,
				MIMEType: mimeTypeJSON,
				Text:     string(body),
			}},
		}, nil
	})
}
