package http

import "net/http"

// CatalogueRow is one problem type of the catalogue as the replay-class
// tests read it.
type CatalogueRow struct {
	Err    error
	Slug   string
	Status int
	Class  string
}

// Catalogue exposes every problem type the adapter can answer — the
// catalogue entries and the middleware/router/readiness problems outside the
// catalogue — with the replay class its entry declares ("" when it declares
// none; ADR 0015).
func Catalogue() []CatalogueRow {
	entries := make([]CatalogueRow, 0, len(problemCatalogue)+7)
	for _, e := range problemCatalogue {
		entries = append(entries, CatalogueRow{Err: e.err, Slug: e.p.slug, Status: e.p.status, Class: string(e.p.class)})
	}
	for _, p := range []problem{
		internalProblem, idempotencyKeyRequired, idempotencyKeyInvalid, idempotencyKeyReused,
		notFoundProblem, methodNotAllowed, dependencyUnavailable,
		invalidReportRange, reportStoreError,
	} {
		entries = append(entries, CatalogueRow{Slug: p.slug, Status: p.status, Class: string(p.class)})
	}
	return entries
}

// WriteError is writeError for the tests of the Idempotency-Key middleware
// over a store: it answers err as its problem through the production
// writer, so the replay class of the problem marks the capturing writer the
// way a handler's answer does.
func WriteError(w http.ResponseWriter, r *http.Request, err error) { writeError(w, r, err) }

// WriteProblemFor answers, through the production writer, the standalone
// problem (middleware, router, readiness — the Catalogue rows that carry no
// typed error of their own) whose slug is slug.
func WriteProblemFor(w http.ResponseWriter, r *http.Request, slug string) {
	for _, p := range []problem{
		internalProblem, idempotencyKeyRequired, idempotencyKeyInvalid, idempotencyKeyReused,
		notFoundProblem, methodNotAllowed, dependencyUnavailable, invalidReportRange, reportStoreError,
	} {
		if p.slug == slug {
			writeProblem(w, r, p, p.detail)
			return
		}
	}
	panic("no standalone problem for slug " + slug)
}
