package http

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// The OpenAPI bounds of the wire parameters (apis/openapi.yaml).
const (
	maxCaseIDLen     = 64
	minCaseIDLen     = 6
	maxSourceTypeLen = 200
	maxQueueLen      = 64
	maxCodeLen       = 64
	maxNoteLen       = 2000
)

// caseIDPattern and sourceTypePattern are the OpenAPI patterns of {caseId}
// and {sourceType}; codePattern is the pattern of resolutionCode/rootCause.
var (
	caseIDPattern     = regexp.MustCompile(`^case-[^\s/]+$`)
	sourceTypePattern = regexp.MustCompile(`^com\.warehouse\.(wms|wes)\.[a-z][a-z-]*\.[a-z][a-z-]*\.[A-Za-z0-9.]+$`)
	codePattern       = regexp.MustCompile(`^[A-Z0-9_]+$`)
)

// caseStatuses are the values of the status query filter (and the fixed
// status machine of ADR 0002).
var caseStatuses = map[string]bool{
	string(exceptioncase.StatusOpen): true, string(exceptioncase.StatusTriaged): true,
	string(exceptioncase.StatusAssigned): true, string(exceptioncase.StatusResolved): true,
}

// pathParam returns a path segment, unescaped (an encoded '/' or space
// reaches the validators and is rejected there). invalid is returned for a
// segment that is not valid percent-encoding.
func pathParam(r *http.Request, name string, invalid error) (string, error) {
	v, err := url.PathUnescape(chi.URLParam(r, name))
	if err != nil {
		return "", invalid
	}
	return v, nil
}

// controlRune reports whether s carries a control character (the
// invalid-case-id contract text: "no whitespace, control characters or '/'";
// NUL foremost, because Postgres text cannot hold it).
func controlRune(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

// caseIDParam reads and validates {caseId}: `case-` plus a suffix with no
// whitespace, control characters or '/', 6 to 64 characters.
func caseIDParam(r *http.Request) (string, error) {
	id, err := pathParam(r, "caseId", errInvalidCaseID)
	if err != nil {
		return "", err
	}
	n := utf8.RuneCountInString(id)
	if n < minCaseIDLen || n > maxCaseIDLen || !caseIDPattern.MatchString(id) || controlRune(id) {
		return "", errInvalidCaseID
	}
	return id, nil
}

// sourceTypeParam reads and validates {sourceType}: the full CloudEvents
// type of the governed source fact (its dots are significant, so it arrives
// URL-encoded), 5 to 200 characters.
func sourceTypeParam(r *http.Request) (string, error) {
	typ, err := pathParam(r, "sourceType", errInvalidSourceType)
	if err != nil {
		return "", err
	}
	n := utf8.RuneCountInString(typ)
	if n < 5 || n > maxSourceTypeLen || !sourceTypePattern.MatchString(typ) {
		return "", errInvalidSourceType
	}
	return typ, nil
}

// statusFilter validates the status query parameter of GET /exceptions
// (absent/empty = every status).
func statusFilter(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if !caseStatuses[raw] {
		return "", errInvalidStatus
	}
	return raw, nil
}

// severityOf validates a severity string from a body (LOW, MEDIUM, HIGH,
// CRITICAL — the domain's own rule).
func severityOf(raw string) (shared.Severity, error) {
	s := shared.Severity(raw)
	if !s.Valid() {
		return "", errInvalidSeverity
	}
	return s, nil
}

// queueOf validates a queue from a body: non-blank, at most 64 characters.
func queueOf(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" || utf8.RuneCountInString(raw) > maxQueueLen {
		return "", errInvalidQueue
	}
	return raw, nil
}

// codeOf validates a resolution code or root cause: A-Z, 0-9 and _, 1 to 64
// characters (a code, not prose).
func codeOf(raw string) (string, error) {
	if raw == "" || utf8.RuneCountInString(raw) > maxCodeLen || !codePattern.MatchString(raw) {
		return "", errInvalidResolution
	}
	return raw, nil
}

// noteOf validates the optional resolve note: at most 2000 characters.
func noteOf(raw string) (string, error) {
	if utf8.RuneCountInString(raw) > maxNoteLen {
		return "", errInvalidResolution
	}
	return raw, nil
}
