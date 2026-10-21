// ErrConcurrentModification and friends: the typed write-conflict errors the
// repositories (Postgres and the in-memory double) return so use cases and
// the HTTP edge can map them instead of guessing at a generic 500.
package usecases

import "errors"

var (
	// ErrConcurrentModification is returned by a repo's Save when the
	// version-guarded write affected zero rows against an EXISTING row:
	// another writer already modified (and incremented the version of)
	// the same aggregate since this caller last read it (optimistic
	// concurrency, the inventory-storage ADR 0018/0019 pattern; here the
	// guard is exception_cases.version = excluded.version - 1). The caller
	// must re-fetch and retry rather than treat this as success or as a
	// generic internal error — the inbound HTTP adapter maps it to 409
	// Conflict.
	ErrConcurrentModification = errors.New("aggregate was concurrently modified by another writer; re-fetch and retry")

	// ErrOpenCaseExists is returned when the one-open-case-per-dedup-key
	// invariant (ADR 0002) is enforced by STORAGE instead of the use
	// case: the losing writer of an open-or-touch race tried to store a
	// second OPEN case for the same (source type, source subject, site)
	// and the partial unique index uq_exception_cases_one_open_per_key
	// rejected it. The caller re-fetches the owner and touches it.
	ErrOpenCaseExists = errors.New("an open case already exists for the dedup key")
)
