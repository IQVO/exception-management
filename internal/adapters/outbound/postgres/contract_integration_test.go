//go:build integration

package postgres_test

import (
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
		return postgres.NewPolicyRepo(pgtest.NewPool(t))
	})
}
