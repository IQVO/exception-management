// Package repository holds the non-interface vocabulary of the application's
// ports: value types the ports trade in that are not interfaces. It is
// separate from package ports because ports may contain interfaces only
// (internal/architecture's "ports package only contains interfaces" rule).
package repository

import "time"

// IdempotencyTTL is how long a stored answer is replayed: the fleet default
// of 24 hours (ADR 0015, addendum 2026-11-22). It is the ONE age the whole
// retention shares — the age guard on the store's Lookup (a row older than
// the TTL never answers a replay), the refresh predicate of Save (a key past
// its window is free again, so the new answer replaces the stale one) and the
// purge's delete predicate — so the read check, the write and the sweep can
// never disagree about which rows are live.
const IdempotencyTTL = 24 * time.Hour

// IdempotencyRecord is the stored answer of a POST or PUT made with an
// Idempotency-Key: 2xx and 4xx only, never 5xx. Fingerprint identifies the
// request (method, path, body) so the same key with a different request can
// be told apart. Body is the encoded stored answer (headers plus payload) so
// a replay is byte-identical.
type IdempotencyRecord struct {
	Key         string
	Fingerprint string
	Status      int
	ContentType string
	Body        []byte
	CreatedAt   time.Time
}
