//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/application/ports"
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

func TestProcessedEventsContract(t *testing.T) {
	repocontract.ProcessedEvents(t, func(t *testing.T) ports.ProcessedEvents {
		return postgres.NewProcessedEventRepo(pgtest.NewPool(t))
	})
}
