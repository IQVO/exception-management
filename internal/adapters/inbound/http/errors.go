// Package http is the REST inbound adapter: every route of apis/openapi.yaml
// (.claude/rules/rest-api.md) on a chi router, RFC 7807 problem documents
// for every error, the Idempotency-Key middleware on the POSTs and the PUT,
// the NUL guard and the liveness/readiness/metrics probes. It has no auth
// middleware (fleet decision 2026-09-11;
// internal/architecture's TestNoAuthMiddlewareReintroduced).
package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/exception-management/internal/adapters/inbound/http/page"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
)

// problemBaseURI is the namespace of this service's RFC 7807 `type` URIs.
const problemBaseURI = "https://errors.exception-management.warehouse-systems.dev/"

// Errors the adapter itself raises, before or besides the use cases. Their
// text never reaches a client: the catalogue pairs each with a fixed
// (status, slug, title, detail).
var (
	// errMalformedRequest marks a body the adapter rejects (bad JSON, unknown
	// field, wrong type, trailing data, empty required body).
	errMalformedRequest = errors.New("request body is not valid JSON for this operation")
	// errInvalidCaseID marks a case id outside the OpenAPI bound (it must be
	// `case-` plus a suffix with no whitespace, control characters or '/').
	errInvalidCaseID = errors.New("case id is invalid")
	// errInvalidSourceType marks a source type that is not a full
	// CloudEvents type (the OpenAPI pattern of {sourceType}).
	errInvalidSourceType = errors.New("source type is invalid")
	// errInvalidStatus marks an unknown status filter value.
	errInvalidStatus = errors.New("status is invalid")
	// errInvalidSeverity marks an unknown severity.
	errInvalidSeverity = errors.New("severity is invalid")
	// errInvalidQueue marks a blank or over-long queue.
	errInvalidQueue = errors.New("queue is invalid")
	// errInvalidAssignee marks a blank or over-long assignee.
	errInvalidAssignee = errors.New("assignee is invalid")
	// errInvalidResolution marks a missing/invalid resolution code or root
	// cause, or an over-long note.
	errInvalidResolution = errors.New("resolution is invalid")
	// errInvalidPolicy marks policy values out of range (slaMinutes, or the
	// autoResolve rule).
	errInvalidPolicy = errors.New("policy is invalid")
)

// problem is the fixed (status, slug, title) of one error category. detail,
// when set, replaces the error's own message (used for domain errors, whose
// text carries a package prefix and is never sent to a client as is).
type problem struct {
	status int
	slug   string
	title  string
	detail string
}

var internalProblem = problem{status: http.StatusInternalServerError, slug: "internal-error", title: "Internal server error"}

// Problems the middleware and the probe/router handlers write themselves,
// before any use case runs.
var (
	idempotencyKeyRequired = problem{status: http.StatusBadRequest, slug: "idempotency-key-required", title: "Idempotency-Key header is required"}
	idempotencyKeyInvalid  = problem{status: http.StatusBadRequest, slug: "idempotency-key-invalid", title: "Idempotency-Key header is invalid"}
	idempotencyKeyReused   = problem{status: http.StatusConflict, slug: "idempotency-key-reused", title: "Idempotency-Key was already used with a different request"}
	notFoundProblem        = problem{status: http.StatusNotFound, slug: "not-found", title: "Not found"}
	methodNotAllowed       = problem{status: http.StatusMethodNotAllowed, slug: "method-not-allowed", title: "Method not allowed"}
	dependencyUnavailable  = problem{status: http.StatusServiceUnavailable, slug: "dependency-unavailable", title: "A runtime dependency is unavailable"}
)

// The adapter's own validation problems (detail pins the rule that was
// broken, the wording of the contract's examples).
var (
	invalidCaseIDProblem = problem{http.StatusBadRequest, "invalid-case-id", "Case id is invalid",
		"case id must start with case- and have no whitespace, control characters or '/'"}
	invalidSourceTypeProblem = problem{http.StatusBadRequest, "invalid-source-type", "Source type is invalid",
		"sourceType must be a full CloudEvents type this service consumes"}
	invalidStatusProblem = problem{http.StatusBadRequest, "invalid-status", "Status is invalid",
		"status must be one of OPEN, TRIAGED, ASSIGNED, RESOLVED"}
	invalidSeverityProblem = problem{http.StatusBadRequest, "invalid-severity", "Severity is invalid",
		"severity must be one of LOW, MEDIUM, HIGH, CRITICAL"}
	invalidQueueProblem = problem{http.StatusBadRequest, "invalid-queue", "Queue is invalid",
		"queue must not be blank"}
	invalidAssigneeProblem = problem{http.StatusBadRequest, "invalid-assignee", "Assignee is invalid",
		"assignee must not be blank"}
	invalidResolutionProblem = problem{http.StatusBadRequest, "invalid-resolution", "Resolution is invalid",
		"resolutionCode and rootCause must not be blank"}
	invalidPolicyProblem = problem{http.StatusBadRequest, "invalid-policy", "Policy is invalid",
		"slaMinutes must be at least 1 and autoResolve.onType, when present, must be a full CloudEvents type"}
)

// problemCatalogue maps every typed error to its problem, in match order.
// Slugs are the catalogue of .claude/rules/rest-api.md and
// apis/openapi.yaml; contract_internal_test.go pins both directions.
var problemCatalogue = []struct {
	err error
	p   problem
}{
	{errMalformedRequest, problem{status: http.StatusBadRequest, slug: "malformed-request-body", title: "Malformed request body",
		detail: "request body is not valid JSON for this operation"}},
	{errInvalidCaseID, invalidCaseIDProblem},
	{errInvalidSourceType, invalidSourceTypeProblem},
	{errInvalidStatus, invalidStatusProblem},
	{errInvalidSeverity, invalidSeverityProblem},
	{errInvalidQueue, invalidQueueProblem},
	{errInvalidAssignee, invalidAssigneeProblem},
	{errInvalidResolution, invalidResolutionProblem},
	{errInvalidPolicy, invalidPolicyProblem},

	// The paging parameters of a collection GET (REST guideline 3 and 4.2).
	{page.ErrInvalidLimit, problem{status: http.StatusBadRequest, slug: "invalid-limit", title: "Limit is invalid",
		detail: page.ErrInvalidLimit.Error()}},
	{page.ErrInvalidCursor, problem{status: http.StatusBadRequest, slug: "invalid-cursor", title: "Cursor is invalid",
		detail: page.ErrInvalidCursor.Error()}},
	{errUnknownQueryParameter, problem{status: http.StatusBadRequest, slug: "unknown-query-parameter", title: "Query parameter is not supported"}},

	// The domain's own validation is the backstop behind the adapter's; the
	// same slugs, so a rule missed here cannot become a 500.
	{exceptioncase.ErrInvalidSeverity, invalidSeverityProblem},
	{exceptionpolicy.ErrInvalidSeverity, invalidSeverityProblem},
	{exceptioncase.ErrBlankQueue, invalidQueueProblem},
	{exceptionpolicy.ErrBlankQueue, invalidQueueProblem},
	{exceptioncase.ErrBlankAssignee, invalidAssigneeProblem},
	{exceptioncase.ErrBlankResolutionCode, invalidResolutionProblem},
	{exceptioncase.ErrBlankRootCause, invalidResolutionProblem},
	{exceptionpolicy.ErrInvalidSourceType, invalidSourceTypeProblem},
	{exceptionpolicy.ErrInvalidSLA, invalidPolicyProblem},

	// The miss of a named case.
	{ports.ErrNotFound, problem{status: http.StatusNotFound, slug: "case-not-found", title: "Exception case not found",
		detail: "exception case not found"}},

	// Terminal and write-conflict states.
	{exceptioncase.ErrCaseClosed, problem{status: http.StatusConflict, slug: "case-already-resolved", title: "The case is already resolved",
		detail: "a resolved case accepts no further command"}},
	{usecases.ErrConcurrentModification, problem{status: http.StatusConflict, slug: "concurrent-modification",
		title: "The case was modified by another request; re-fetch the latest version and retry"}},
}

// problemFor returns the problem of the first catalogue entry err matches,
// or the internal-error problem.
func problemFor(err error) problem {
	for _, entry := range problemCatalogue {
		if errors.Is(err, entry.err) {
			return entry.p
		}
	}
	return internalProblem
}
