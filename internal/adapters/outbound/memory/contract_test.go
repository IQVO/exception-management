package memory_test

import (
	"testing"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/testing/repocontract"
)

// The behavioural contract the Postgres adapters pass against a real
// database (internal/adapters/outbound/postgres, integration tag): the same
// cases here, over the doubles.

func TestCaseRepoContract(t *testing.T) {
	repocontract.CaseRepo(t, func(*testing.T) ports.ExceptionCaseRepo { return memory.NewCases() })
}

func TestPolicyRepoContract(t *testing.T) {
	repocontract.PolicyRepo(t, func(*testing.T) ports.ExceptionPolicyRepo { return memory.NewPolicies() })
}

func TestIdempotencyStoreContract(t *testing.T) {
	repocontract.IdempotencyStore(t, func(*testing.T) ports.IdempotencyStore { return memory.NewIdempotencyStore() })
}
