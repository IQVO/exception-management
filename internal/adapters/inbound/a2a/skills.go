package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/views"
)

// tracerName is the OTel instrumentation scope for A2A skill spans, mirroring
// the MCP adapter's mcp.tool spans.
const tracerName = "github.com/claudioed/exception-management/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card).
const (
	skillSLAStatus      = "sla_status"
	skillFindExceptions = "find_exceptions"
)

// The OpenAPI bounds of a case id (apis/openapi.yaml, mirrored from the REST
// adapter: `case-` plus a suffix with no whitespace or '/', 6 to 64 chars).
const (
	minCaseIDLen = 6
	maxCaseIDLen = 64
)

var caseIDPattern = regexp.MustCompile(`^case-[^\s/]+$`)

// caseStatuses are the values of the status filter (the fixed status machine
// of ADR 0002), the same set the REST adapter accepts.
var caseStatuses = map[string]bool{"OPEN": true, "TRIAGED": true, "ASSIGNED": true, "RESOLVED": true}

// skillRequest is the wire contract of every skill call: the first message
// part carries (as a Data part, or a Text part holding the same JSON) the
// skill id and its arguments. Strict on purpose: an agent client is
// machine-to-machine, so unknown fields are rejected, never ignored (a
// misspelled filter such as "statu" must not silently list every case). Go's
// encoding/json matches field names case-insensitively, so "STATUS" still
// reaches status; only genuinely unknown names are refused.
type skillRequest struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

// slaStatusArgs and findExceptionsArgs are the per-skill argument shapes.
type slaStatusArgs struct {
	IncludePolicies bool `json:"includePolicies"`
}

type findExceptionsArgs struct {
	CaseID     *string `json:"caseId"`
	Status     string  `json:"status"`
	Queue      string  `json:"queue"`
	SiteID     string  `json:"siteId"`
	SourceType string  `json:"sourceType"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: this is a message-only agent, so a
// skill run is exactly one request mapped to one answer.
func answer(ctx context.Context, deps Deps, ec *a2asrv.ExecutorContext) a2a.Event {
	req, prob := parseSkillRequest(ec)
	if prob != nil {
		return reply(ec, envelope{Skill: "", Problem: prob}, prob.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	dto, prob := run(ctx, deps, req)
	if prob != nil {
		span.SetStatus(codes.Error, prob.Detail)
		return reply(ec, envelope{Skill: req.Skill, Problem: prob}, prob.summary())
	}
	return reply(ec, envelope{Skill: req.Skill, Data: dto}, dto.Summary())
}

// run dispatches one validated request to its skill.
func run(ctx context.Context, deps Deps, req skillRequest) (skillDTO, *problem) {
	switch req.Skill {
	case skillSLAStatus:
		data, prob := deps.slaStatus(ctx, req.Arguments)
		if prob != nil {
			return skillDTO{}, prob
		}
		return skillDTO{SLAStatus: &data, summary: data.summary()}, nil
	case skillFindExceptions:
		data, prob := deps.findExceptions(ctx, req.Arguments)
		if prob != nil {
			return skillDTO{}, prob
		}
		return skillDTO{Exceptions: &data, summary: data.summary()}, nil
	default:
		return skillDTO{}, problemUnknownSkill(req.Skill)
	}
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first part.
// A Data part (or a Text part holding the same JSON object) is accepted;
// anything else is a request-shape problem, answered, not raised, because a
// malformed delegation is a business answer for the calling agent, not a
// transport failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := decodeStrict(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// decodeStrict decodes one JSON document into dst, rejecting unknown fields
// and trailing data. Absent arguments decode as the empty object so each
// skill's own checks produce the specific problem.
func decodeStrict(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

// slaStatus is the SLA read skill: the SLA report and, when asked for, the
// declared policies, both from this context's own state.
func (d Deps) slaStatus(ctx context.Context, rawArgs json.RawMessage) (slaStatus, *problem) {
	var args slaStatusArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return slaStatus{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	report, err := d.SLAReport.Execute(ctx)
	if err != nil {
		return slaStatus{}, problemFromUseCase(err)
	}
	var policies []views.PolicyView
	if args.IncludePolicies {
		if policies, err = d.Policies.Execute(ctx); err != nil {
			return slaStatus{}, problemFromUseCase(err)
		}
	}
	return toSLAStatus(report, policies, args.IncludePolicies), nil
}

// findExceptions is the case-finding read skill: one case by id, or the
// filtered list, through the same use cases as GET /exceptions/{caseId} and
// GET /exceptions.
func (d Deps) findExceptions(ctx context.Context, rawArgs json.RawMessage) (exceptionsList, *problem) {
	var args findExceptionsArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return exceptionsList{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if prob := validateFindArgs(args); prob != nil {
		return exceptionsList{}, prob
	}
	if args.CaseID != nil {
		v, err := d.Get.Execute(ctx, *args.CaseID)
		if err != nil {
			return exceptionsList{}, problemFromUseCase(err)
		}
		return exceptionsList{Count: 1, Cases: []caseDTO{toCase(v)}}, nil
	}
	found, err := d.List.Execute(ctx, args.Status, args.Queue, args.SiteID, args.SourceType)
	if err != nil {
		return exceptionsList{}, problemFromUseCase(err)
	}
	out := exceptionsList{Count: len(found), Cases: make([]caseDTO, 0, len(found))}
	for _, v := range found {
		out.Cases = append(out.Cases, toCase(v))
	}
	return out, nil
}

// validateFindArgs applies the REST adapter's request validation to the
// find_exceptions arguments, with the REST problem slugs and details.
func validateFindArgs(args findExceptionsArgs) *problem {
	if prob := rejectNUL(args); prob != nil {
		return prob
	}
	if args.CaseID != nil {
		if args.Status != "" || args.Queue != "" || args.SiteID != "" || args.SourceType != "" {
			return problemInvalidArgument("caseId reads exactly one case and cannot be combined with status, queue, siteId or sourceType")
		}
		if !validCaseID(*args.CaseID) {
			return newProblem(400, "invalid-case-id", "Case id is invalid",
				"case id must start with case- and have no whitespace, control characters or '/'")
		}
		return nil
	}
	if args.Status != "" && !caseStatuses[args.Status] {
		return newProblem(400, "invalid-status", "Status is invalid", "status must be one of OPEN, TRIAGED, ASSIGNED, RESOLVED")
	}
	return nil
}

// rejectNUL refuses a U+0000 in any string argument. PostgreSQL text cannot
// hold it and a statement carrying one aborts the transaction, so without
// this guard a hostile filter becomes a 500 on the real runtime; the REST
// adapter's rejectNUL middleware does the same as a 400.
func rejectNUL(args findExceptionsArgs) *problem {
	fields := []string{args.Status, args.Queue, args.SiteID, args.SourceType}
	if args.CaseID != nil {
		fields = append(fields, *args.CaseID)
	}
	for _, f := range fields {
		if strings.ContainsRune(f, 0) {
			return newProblem(400, "malformed-request-body", "Malformed request body",
				"a NUL character (U+0000) is not allowed in a skill argument")
		}
	}
	return nil
}

// validCaseID mirrors the REST adapter's {caseId} rule.
func validCaseID(id string) bool {
	n := utf8.RuneCountInString(id)
	if n < minCaseIDLen || n > maxCaseIDLen || !caseIDPattern.MatchString(id) {
		return false
	}
	for _, r := range id {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// problemFromUseCase maps a use case error to a problem answer with the SAME
// classification as the REST adapter (internal/adapters/inbound/http
// problemCatalogue), so a delegating agent sees the outcome a REST caller
// would: a missing case is a 404, anything unrecognised a 500 whose detail
// never carries infrastructure text (DSNs, SQL). The read path raises no
// write conflict (409) and the REST adapter reports no 503 outside its
// readiness probe, so neither is mapped here.
func problemFromUseCase(err error) *problem {
	if errors.Is(err, ports.ErrNotFound) {
		return newProblem(404, "case-not-found", "Exception case not found", "exception case not found")
	}
	slog.Error("a2a skill failed", "error", err)
	return newProblem(500, "internal-error", "Internal server error", "an unexpected error occurred")
}
