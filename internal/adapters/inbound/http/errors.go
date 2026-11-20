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
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// problemBaseURI is the namespace of this service's RFC 7807 `type` URIs.
const problemBaseURI = "https://errors.exception-management.warehouse-systems.dev/"

// slugMalformedRequestBody is the slug of every malformed-body problem (the
// catalogue entry and the NUL guard's three variants share it).
const slugMalformedRequestBody = "malformed-request-body"

// replayClass is what the Idempotency-Key middleware does with the answer of
// a problem type (ADR 0015, the fleet rule of 2026-11-18): FINAL and OUTCOME
// answers are stored and replayed, a RETRYABLE one is not stored and its key
// is released, so a retry executes again. A 2xx is stored and a 5xx never is,
// whatever the class. The empty class is "unclassified" and is released like
// RETRYABLE (the fail-safe direction: no slug list and no status range
// beyond 2xx and 5xx lives in the middleware).
type replayClass string

const (
	// replayFinal: the answer is a function of the request (or of state that
	// only ever moves one way) alone; stored and replayed.
	replayFinal replayClass = "FINAL"
	// replayOutcome: the command committed state and then refused; the writes
	// would commit with the stored answer. No problem type of this service is
	// one: this store rolls the whole unit of work back on any refusal
	// (idempotency.go serveFresh), so no committed state can back a 4xx —
	// OUTCOME cannot exist here (ADR 0015).
	replayOutcome replayClass = "OUTCOME"
	// replayRetryable: nothing committed and the answer depends on state that
	// can change; not stored, the key is released, a retry executes again.
	replayRetryable replayClass = "RETRYABLE"
)

// stored reports whether an error answer of this class is remembered under
// its key. An unclassified answer is not: releasing is the safe fallback.
func (c replayClass) stored() bool { return c == replayFinal || c == replayOutcome }

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

// problem is the fixed (status, slug, title) of one error category and the
// replay class of that category (ADR 0015; a test holds the ADR's table
// equal to this catalogue). detail, when set, replaces the error's own
// message (used for domain errors, whose text carries a package prefix and
// is never sent to a client as is).
type problem struct {
	status int
	slug   string
	title  string
	detail string
	class  replayClass
}

var internalProblem = problem{status: http.StatusInternalServerError, slug: "internal-error", title: "Internal server error", class: replayRetryable}

// Problems the middleware and the probe/router handlers write themselves,
// before any use case runs.
var (
	idempotencyKeyRequired = problem{status: http.StatusBadRequest, slug: "idempotency-key-required", title: "Idempotency-Key header is required", class: replayFinal}
	idempotencyKeyInvalid  = problem{status: http.StatusBadRequest, slug: "idempotency-key-invalid", title: "Idempotency-Key header is invalid", class: replayFinal}
	idempotencyKeyReused   = problem{status: http.StatusConflict, slug: "idempotency-key-reused", title: "Idempotency-Key was already used with a different request", class: replayFinal}
	notFoundProblem        = problem{status: http.StatusNotFound, slug: "not-found", title: "Not found", class: replayFinal}
	methodNotAllowed       = problem{status: http.StatusMethodNotAllowed, slug: "method-not-allowed", title: "Method not allowed", class: replayFinal}
	dependencyUnavailable  = problem{status: http.StatusServiceUnavailable, slug: "dependency-unavailable", title: "A runtime dependency is unavailable", class: replayRetryable}
)

// The NUL guard's malformed-body variants (nul.go answers them before the
// idempotency middleware runs, so they never sit under a key; the slug and
// the class are the catalogue entry's).
var (
	nulMalformedBody  = problem{status: http.StatusBadRequest, slug: slugMalformedRequestBody, title: "Malformed request body", class: replayFinal}
	nulBodyUnreadable = problem{status: http.StatusBadRequest, slug: slugMalformedRequestBody, title: "The request body could not be read", class: replayFinal}
	nulBodyHasNUL     = problem{status: http.StatusBadRequest, slug: slugMalformedRequestBody, title: "The request body contains a NUL character", class: replayFinal}
)

// The adapter's own validation problems (detail pins the rule that was
// broken, the wording of the contract's examples). Each is a function of
// the request alone: FINAL.
var (
	invalidCaseIDProblem = problem{http.StatusBadRequest, "invalid-case-id", "Case id is invalid",
		"case id must start with case- and have no whitespace, control characters or '/'", replayFinal}
	invalidSourceTypeProblem = problem{http.StatusBadRequest, "invalid-source-type", "Source type is invalid",
		"sourceType must be a full CloudEvents type this service consumes", replayFinal}
	invalidStatusProblem = problem{http.StatusBadRequest, "invalid-status", "Status is invalid",
		"status must be one of OPEN, TRIAGED, ASSIGNED, RESOLVED", replayFinal}
	invalidSeverityProblem = problem{http.StatusBadRequest, "invalid-severity", "Severity is invalid",
		"severity must be one of LOW, MEDIUM, HIGH, CRITICAL", replayFinal}
	invalidQueueProblem = problem{http.StatusBadRequest, "invalid-queue", "Queue is invalid",
		"queue must not be blank", replayFinal}
	invalidAssigneeProblem = problem{http.StatusBadRequest, "invalid-assignee", "Assignee is invalid",
		"assignee must not be blank", replayFinal}
	invalidActorProblem = problem{http.StatusBadRequest, "invalid-actor", "Actor is invalid",
		"the X-Actor header must name the operator performing the command, non-blank with no NUL byte", replayFinal}
	invalidResolutionProblem = problem{http.StatusBadRequest, "invalid-resolution", "Resolution is invalid",
		"resolutionCode and rootCause must not be blank", replayFinal}
	invalidPolicyProblem = problem{http.StatusBadRequest, "invalid-policy", "Policy is invalid",
		"slaMinutes must be at least 1 and autoResolve.onType, when present, must be a full CloudEvents type", replayFinal}
)

// problemCatalogue maps every typed error to its problem, in match order.
// Slugs are the catalogue of .claude/rules/rest-api.md and
// apis/openapi.yaml; contract_internal_test.go pins both directions. The
// class of every entry is ADR 0015's table; a new entry without one fails
// TestEveryProblemTypeDeclaresAReplayClass. When unsure the class is
// RETRYABLE: a released refusal costs one more execution, a wrongly stored
// one pins a stale answer.
var problemCatalogue = []struct {
	err error
	p   problem
}{
	{errMalformedRequest, problem{status: http.StatusBadRequest, slug: slugMalformedRequestBody, title: "Malformed request body",
		detail: "request body is not valid JSON for this operation", class: replayFinal}},
	{errInvalidCaseID, invalidCaseIDProblem},
	{errInvalidSourceType, invalidSourceTypeProblem},
	{errInvalidStatus, invalidStatusProblem},
	{errInvalidSeverity, invalidSeverityProblem},
	{errInvalidQueue, invalidQueueProblem},
	{errInvalidAssignee, invalidAssigneeProblem},
	{errInvalidResolution, invalidResolutionProblem},
	{errInvalidPolicy, invalidPolicyProblem},

	// The paging parameters of a collection GET (REST guideline 3 and 4.2);
	// request-only, classified for completeness.
	{page.ErrInvalidLimit, problem{status: http.StatusBadRequest, slug: "invalid-limit", title: "Limit is invalid",
		detail: page.ErrInvalidLimit.Error(), class: replayFinal}},
	{page.ErrInvalidCursor, problem{status: http.StatusBadRequest, slug: "invalid-cursor", title: "Cursor is invalid",
		detail: page.ErrInvalidCursor.Error(), class: replayFinal}},
	{errUnknownQueryParameter, problem{status: http.StatusBadRequest, slug: "unknown-query-parameter", title: "Query parameter is not supported", class: replayFinal}},

	// The domain's own validation is the backstop behind the adapter's; the
	// same slugs, so a rule missed here cannot become a 500.
	{exceptioncase.ErrInvalidSeverity, invalidSeverityProblem},
	{exceptionpolicy.ErrInvalidSeverity, invalidSeverityProblem},
	{exceptioncase.ErrBlankQueue, invalidQueueProblem},
	{exceptionpolicy.ErrBlankQueue, invalidQueueProblem},
	{exceptioncase.ErrBlankAssignee, invalidAssigneeProblem},
	{shared.ErrBlankActor, invalidActorProblem},
	{exceptioncase.ErrBlankResolutionCode, invalidResolutionProblem},
	{exceptioncase.ErrBlankRootCause, invalidResolutionProblem},
	{exceptioncase.ErrInvalidResolutionCode, invalidResolutionProblem},
	{exceptioncase.ErrInvalidRootCause, invalidResolutionProblem},
	{exceptionpolicy.ErrInvalidSourceType, invalidSourceTypeProblem},
	{exceptionpolicy.ErrInvalidSLA, invalidPolicyProblem},

	// The miss of a named case. A case opens only from a consumed fact
	// (ADR 0001; POST /exceptions is the permanent 405), so the missing case
	// is a fact this service has not consumed yet — it can arrive moments
	// later: RETRYABLE.
	{ports.ErrNotFound, problem{status: http.StatusNotFound, slug: "case-not-found", title: "Exception case not found",
		detail: "exception case not found", class: replayRetryable}},

	// Terminal and write-conflict states. RESOLVED is terminal and nothing
	// moves it back, so the refusal can never clear: FINAL. The version
	// guard loses only to a live competing writer: RETRYABLE.
	{exceptioncase.ErrCaseClosed, problem{status: http.StatusConflict, slug: "case-already-resolved", title: "The case is already resolved",
		detail: "a resolved case accepts no further command", class: replayFinal}},
	{usecases.ErrConcurrentModification, problem{status: http.StatusConflict, slug: "concurrent-modification",
		title: "The case was modified by another request; re-fetch the latest version and retry", class: replayRetryable}},
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
