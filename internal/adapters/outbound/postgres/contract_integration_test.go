//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
	"github.com/claudioed/exception-management/internal/testing/repocontract"
)

// The same behavioural contract the in-memory doubles pass, run against a
// real Postgres: every contract case gets its own database.

func TestCaseRepoContract(t *testing.T) {
	repocontract.CaseRepo(t, func(t *testing.T) ports.ExceptionCaseRepo {
		return postgres.NewCaseRepo(pgtest.NewPool(t))
	})
}

func TestPolicyRepoContract(t *testing.T) {
	repocontract.PolicyRepo(t, func(t *testing.T) ports.ExceptionPolicyRepo {
		pool := pgtest.NewPool(t)
		// Migration 0003 seeds the two CartonDiscrepancyRaised routes into
		// every database cloned from the migrated template. The contract
		// cases assume an empty policy table (they must hold for the
		// in-memory doubles too, which start empty), so the seed rows are
		// cleared for the contract run only.
		if _, err := pool.Exec(context.Background(), `DELETE FROM exception_policies`); err != nil {
			t.Fatalf("clearing the seeded policies: %v", err)
		}
		return postgres.NewPolicyRepo(pool)
	})
}

func TestIdempotencyStoreContract(t *testing.T) {
	repocontract.IdempotencyStore(t, func(t *testing.T) ports.IdempotencyStore {
		return postgres.NewIdempotencyStore(pgtest.NewPool(t))
	})
}

// TestIdempotencyRetentionContract: the 24-hour window of
// repository.IdempotencyTTL over a real Postgres — the age guard on Lookup,
// the refresh of an expired key by Save, the bounded purge — the same cases
// the memory double runs (the fixture's Age moves created_at back).
func TestIdempotencyRetentionContract(t *testing.T) {
	repocontract.IdempotencyRetention(t, func(t *testing.T) repocontract.IdempotencyRetentionFixture {
		pool := pgtest.NewPool(t)
		store := postgres.NewIdempotencyStore(pool)
		return repocontract.IdempotencyRetentionFixture{
			Store: store,
			Put: func(t *testing.T, key, answer string) {
				t.Helper()
				rec := repository.IdempotencyRecord{Key: key, Fingerprint: "fp-" + key, Status: 200,
					ContentType: "application/json", Body: []byte(answer),
					CreatedAt: time.Now().UTC().Truncate(time.Microsecond)}
				if err := store.Save(context.Background(), rec); err != nil {
					t.Fatalf("storing %s: %v", key, err)
				}
			},
			Age: func(t *testing.T, d time.Duration) {
				t.Helper()
				if _, err := pool.Exec(context.Background(),
					`UPDATE idempotency_keys SET created_at = created_at - make_interval(secs => $1)`, d.Seconds()); err != nil {
					t.Fatalf("ageing the stored answers by %v: %v", d, err)
				}
			},
		}
	})
}

func TestProcessedEventsContract(t *testing.T) {
	repocontract.ProcessedEvents(t, func(t *testing.T) ports.ProcessedEvents {
		return postgres.NewProcessedEventRepo(pgtest.NewPool(t))
	})
}
