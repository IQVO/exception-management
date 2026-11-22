package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/exception-management/internal/application/repository"
)

// IdempotencyStore is the pgxpool-backed ports.IdempotencyStore over the
// idempotency_keys table (migration 0002). Every statement runs on the
// transaction carried by ctx when one is open (the Idempotency-Key
// middleware's unit of work) and on the pool otherwise. The retention lives
// here too (ADR 0015, addendum 2026-11-22): a row older than
// repository.IdempotencyTTL never answers a Lookup, a Save past the window
// replaces the expired answer, and PurgeExpired deletes the dead rows the
// read already ignores.
type IdempotencyStore struct {
	pool *pgxpool.Pool
}

// NewIdempotencyStore constructs an IdempotencyStore over pool.
func NewIdempotencyStore(pool *pgxpool.Pool) *IdempotencyStore {
	return &IdempotencyStore{pool: pool}
}

// Lookup implements ports.IdempotencyStore. The age guard —
// created_at > now() - TTL — is the exact complement of the purge's and the
// Save refresh's predicate, over the one repository.IdempotencyTTL: an
// expired row is invisible, so it can never answer a replay between expiry
// and the sweep (and every reader of the port — the middleware's up-front
// check, the post-save re-read, finishRejected's re-read — shares it).
func (s *IdempotencyStore) Lookup(ctx context.Context, key string) (repository.IdempotencyRecord, bool, error) {
	rec := repository.IdempotencyRecord{Key: key}
	err := queryFor(ctx, s.pool).QueryRow(ctx, `
		SELECT fingerprint, status, content_type, body, created_at
		FROM idempotency_keys
		WHERE key = $1 AND created_at > now() - make_interval(secs => $2)
	`, key, repository.IdempotencyTTL.Seconds()).Scan(&rec.Fingerprint, &rec.Status, &rec.ContentType, &rec.Body, &rec.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return repository.IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return repository.IdempotencyRecord{}, false, fmt.Errorf("lookup idempotency key: %w", err)
	}
	rec.CreatedAt = rec.CreatedAt.UTC()
	return rec, true, nil
}

// Save implements ports.IdempotencyStore: the first writer of a LIVE key
// wins, and the answer of a key whose previous answer has expired replaces
// it (the key is free again — created_at is the NEW answer's stamp, so the
// post-save re-read compares equal down to the microsecond). Under Postgres
// a concurrent Save of the same key blocks on the primary key until the
// other transaction ends, so the middleware's post-save re-read sees the
// committed winner; the WHERE clause is re-checked under that row lock, and
// a purge deletes an expired row just before a Save only turns the refresh
// into a plain INSERT.
func (s *IdempotencyStore) Save(ctx context.Context, rec repository.IdempotencyRecord) error {
	_, err := queryFor(ctx, s.pool).Exec(ctx, `
		INSERT INTO idempotency_keys (key, fingerprint, status, content_type, body, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (key) DO UPDATE SET
			fingerprint = EXCLUDED.fingerprint, status = EXCLUDED.status,
			content_type = EXCLUDED.content_type, body = EXCLUDED.body,
			created_at = EXCLUDED.created_at
		WHERE idempotency_keys.created_at <= now() - make_interval(secs => $7)
	`, rec.Key, rec.Fingerprint, rec.Status, rec.ContentType, nonNilBytes(rec.Body), rec.CreatedAt.UTC(), repository.IdempotencyTTL.Seconds())
	if err != nil {
		return fmt.Errorf("save idempotency key: %w", err)
	}
	return nil
}

// PurgeExpired implements ports.IdempotencyStore: it deletes at most limit
// rows older than repository.IdempotencyTTL, oldest first (the
// idx_idempotency_keys_created_at index), and returns how many it deleted. It
// runs on the pool only (the background worker's call), never inside a
// request's unit of work. FOR UPDATE SKIP LOCKED leaves a row another
// transaction is writing — a request whose Save is refreshing an expired
// key — and the age test is repeated on the DELETE itself, so a row a
// request just refreshed is never deleted. It takes no advisory lock and
// never waits on a row lock: a row it cannot take is skipped, not blocked
// on, and an expired row is invisible to Lookup anyway, so deleting it
// changes nothing a request can observe.
func (s *IdempotencyStore) PurgeExpired(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM idempotency_keys WHERE key IN (
			SELECT key FROM idempotency_keys
			WHERE created_at <= now() - make_interval(secs => $1)
			ORDER BY created_at LIMIT $2 FOR UPDATE SKIP LOCKED
		) AND created_at <= now() - make_interval(secs => $1)
	`, repository.IdempotencyTTL.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge idempotency keys: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// nonNilBytes keeps a nil body from becoming SQL NULL (the column is NOT NULL).
func nonNilBytes(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}
