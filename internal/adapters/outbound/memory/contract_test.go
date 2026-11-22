package memory_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
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

func TestProcessedEventsContract(t *testing.T) {
	repocontract.ProcessedEvents(t, func(*testing.T) ports.ProcessedEvents { return memory.NewProcessedEvents() })
}

// advanceableClock is the scenario clock the retention suite ages answers
// with: every stored row is stamped with it, so advancing it ages them all.
type advanceableClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *advanceableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *advanceableClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// TestIdempotencyRetentionContract: the 24-hour window — the age guard, the
// refresh of an expired key and the bounded purge — over the double, the same
// cases the Postgres store runs against a real database.
func TestIdempotencyRetentionContract(t *testing.T) {
	repocontract.IdempotencyRetention(t, func(t *testing.T) repocontract.IdempotencyRetentionFixture {
		clock := &advanceableClock{t: time.Now().UTC()}
		store := memory.NewIdempotencyStoreClock(clock)
		return repocontract.IdempotencyRetentionFixture{
			Store: store,
			Put: func(t *testing.T, key, answer string) {
				t.Helper()
				rec := repository.IdempotencyRecord{Key: key, Fingerprint: "fp-" + key, Status: 200,
					ContentType: "application/json", Body: []byte(answer), CreatedAt: clock.Now().UTC()}
				if err := store.Save(context.Background(), rec); err != nil {
					t.Fatalf("storing %s: %v", key, err)
				}
			},
			Age: func(t *testing.T, d time.Duration) {
				t.Helper()
				clock.advance(d)
			},
		}
	})
}
