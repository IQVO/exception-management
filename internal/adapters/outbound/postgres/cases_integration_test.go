//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// This file proves the two storage invariants at the DATABASE level, not
// just through the repo's SQL: optimistic concurrency under real row
// locking, and the partial unique index behind the one-open-case rule —
// including for writers that bypass the repository entirely.

func integrationCase(t *testing.T, id, subject string, at time.Time) *exceptioncase.Case {
	t.Helper()
	key, err := shared.NewDedupKey(
		"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed", subject, "SIM1")
	if err != nil {
		t.Fatal(err)
	}
	c, err := exceptioncase.Open(exceptioncase.OpenInput{
		CaseID: id, Key: key, Severity: shared.SeverityHigh, Queue: "ps-main",
		SLAMinutes: 30, SourceEventID: "evt-" + id, Payload: []byte(`{"task_id":"t-1"}`), At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestOptimisticConcurrencyIsReal proves the version guard against the
// database itself: two writers load the same version on two independent
// connections, both save; exactly one wins and the loser gets the typed
// ErrConcurrentModification (the second UPDATE blocks on the row lock,
// re-evaluates version = excluded.version - 1 after the winner commits, and
// affects zero rows). The loser then re-fetches and succeeds.
func TestOptimisticConcurrencyIsReal(t *testing.T) {
	pool := pgtest.NewPool(t)
	repo := postgres.NewCaseRepo(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seed := integrationCase(t, "case-1", "ord-7-line-1", time.Now().UTC())
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatal(err)
	}

	// Barrier: both writers LOAD version 1 first; only then may either
	// save, so the overlap the guard exists for actually happens.
	loadedBoth := make(chan struct{})
	saveNow := make(chan struct{})
	var wg sync.WaitGroup
	var wins, losses atomic.Int64
	var loserErr error
	var mu sync.Mutex
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			loaded, err := repo.FindByID(ctx, "case-1")
			if err != nil {
				t.Errorf("writer %d: load: %v", i, err)
				return
			}
			if loaded.Version() != 1 {
				t.Errorf("writer %d: loaded version %d, want 1 (the barrier failed)", i, loaded.Version())
				return
			}
			loadedBoth <- struct{}{}
			<-saveNow
			if err := loaded.Touch(time.Now().UTC()); err != nil {
				t.Errorf("writer %d: touch: %v", i, err)
				return
			}
			err = repo.Save(ctx, loaded)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, usecases.ErrConcurrentModification):
				losses.Add(1)
				loserErr = err
			default:
				t.Errorf("writer %d: unexpected save error: %v", i, err)
			}
		}(i)
	}
	<-loadedBoth
	<-loadedBoth
	close(saveNow)
	wg.Wait()

	if wins.Load() != 1 || losses.Load() != 1 {
		t.Fatalf("wins=%d losses=%d, want exactly one of each", wins.Load(), losses.Load())
	}
	if loserErr == nil {
		t.Fatal("the losing writer must surface ErrConcurrentModification")
	}

	// The stored row carries the winner's version 2, not the loser's.
	stored, err := repo.FindByID(ctx, "case-1")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Version() != 2 || stored.Occurrence() != 2 {
		t.Fatalf("stored version=%d occurrence=%d, want 2/2 (the winner's write)", stored.Version(), stored.Occurrence())
	}

	// The documented recovery: re-fetch, mutate, save.
	if err := stored.Touch(time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, stored); err != nil {
		t.Fatalf("retry after re-fetch: %v", err)
	}
}

// TestOneOpenCaseIndexRejectsAtDBLevel proves the one-open-case invariant
// holds in the INDEX, not in repo logic: a raw INSERT that bypasses the
// repository is rejected with the partial index's unique violation, a
// RESOLVED row for the same key is not (the index is partial), and the
// repository maps the violation to usecases.ErrOpenCaseExists.
func TestOneOpenCaseIndexRejectsAtDBLevel(t *testing.T) {
	pool := pgtest.NewPool(t)
	repo := postgres.NewCaseRepo(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := repo.Save(ctx, integrationCase(t, "case-1", "ord-7-line-1", time.Now().UTC())); err != nil {
		t.Fatal(err)
	}

	// A raw SQL writer (no repo, no use case) cannot store a second OPEN
	// case for the key: the index itself refuses.
	_, err := pool.Exec(ctx, `
		INSERT INTO exception_cases
			(case_id, source_type, source_subject, site_id, status, severity, queue,
			 occurrence, opened_at, last_seen_at, sla_minutes, source_event_id,
			 source_payload, version, audit)
		VALUES
			('case-raw', 'com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed',
			 'ord-7-line-1', 'SIM1', 'OPEN', 'HIGH', 'ps-main',
			 1, now(), now(), 30, 'evt-raw', '{"raw":true}', 1, '[{"action":"OPENED"}]')
	`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "uq_exception_cases_one_open_per_key" {
		t.Fatalf("raw duplicate open insert err = %v, want 23505 on uq_exception_cases_one_open_per_key", err)
	}

	// The repository surfaces the same index as the typed error.
	if err := repo.Save(ctx, integrationCase(t, "case-2", "ord-7-line-1", time.Now().UTC())); !errors.Is(err, usecases.ErrOpenCaseExists) {
		t.Fatalf("repo duplicate open err = %v, want ErrOpenCaseExists", err)
	}
	if n := pgtest.Count(t, pool, "exception_cases"); n != 1 {
		t.Fatalf("exception_cases rows = %d, want 1 (every refused write left nothing)", n)
	}

	// RESOLVED rows do not hold the key: two of them for the same key are
	// fine, and the index still guards the OPEN one.
	for _, id := range []string{"case-r1", "case-r2"} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO exception_cases
				(case_id, source_type, source_subject, site_id, status, severity, queue,
				 occurrence, opened_at, last_seen_at, sla_minutes, source_event_id,
				 source_payload, version, audit, resolution_code, root_cause, resolved_at)
			VALUES
				($1, 'com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed',
				 'ord-9-line-1', 'SIM1', 'RESOLVED', 'HIGH', 'ps-main',
				 1, now(), now(), 30, 'evt-x', '{"x":true}', 2, '[{"action":"OPENED"}]',
				 'DONE', 'handled', now())
		`, id); err != nil {
			t.Fatalf("resolved row %s must not hold the key: %v", id, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO exception_cases
			(case_id, source_type, source_subject, site_id, status, severity, queue,
			 occurrence, opened_at, last_seen_at, sla_minutes, source_event_id,
			 source_payload, version, audit)
		VALUES
			('case-open-9', 'com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed',
			 'ord-9-line-1', 'SIM1', 'OPEN', 'HIGH', 'ps-main',
			 1, now(), now(), 30, 'evt-y', '{"y":true}', 1, '[{"action":"OPENED"}]')
	`); err != nil {
		t.Fatalf("an open case for an otherwise-resolved key must be storable: %v", err)
	}
}

// TestRepoBootsAndDrives proves the repo layer boots over a real pool and
// drives the Save/Find round trip the use cases rely on, including the
// owner lookup through the dedup key.
func TestRepoBootsAndDrives(t *testing.T) {
	pool := pgtest.NewPool(t)
	repo := postgres.NewCaseRepo(pool)
	ctx := context.Background()

	c := integrationCase(t, "case-1", "ord-7-line-1", time.Now().UTC())
	if err := repo.Save(ctx, c); err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindByID(ctx, "case-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != "case-1" || got.Status() != exceptioncase.StatusOpen || got.Queue() != "ps-main" {
		t.Fatalf("round trip = %+v", got.Snapshot())
	}
	key := got.Key()
	owner, err := repo.FindOpenByDedupKey(ctx, key)
	if err != nil || owner.ID() != "case-1" {
		t.Fatalf("owner = %v %v, want case-1", owner, err)
	}
	if _, err := repo.FindOpenByDedupKey(ctx, shared.DedupKey{SourceType: key.SourceType, SourceSubject: "ord-none", SiteID: key.SiteID}); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("unknown key err = %v, want ports.ErrNotFound", err)
	}
}
