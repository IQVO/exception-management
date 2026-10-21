//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// This file proves the UnitOfWork really is a unit: a case save and its
// outbox rows commit together, roll back together, and a nested Do joins
// the outer transaction instead of committing early. It also proves the
// outbox row of a policy declaration carries the full declared wire shape
// (read back through the SAME transaction).

func unitCase(t *testing.T, id string) *exceptioncase.Case {
	t.Helper()
	return integrationCase(t, id, "ord-uow-line-1", time.Now().UTC())
}

func TestUnitOfWorkCommitsCaseAndOutboxTogether(t *testing.T) {
	pool := pgtest.NewPool(t)
	repo := postgres.NewCaseRepo(pool)
	pub := postgres.NewOutboxPublisher(pool)
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()

	c := unitCase(t, "case-1")
	err := uow.Do(ctx, func(ctx context.Context) error {
		if err := repo.Save(ctx, c); err != nil {
			return err
		}
		return pub.PublishCaseEvents(ctx, c.DomainEvents()...)
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := pgtest.Count(t, pool, "exception_cases"); n != 1 {
		t.Fatalf("exception_cases = %d, want 1", n)
	}
	if n := pgtest.Count(t, pool, "outbox"); n != 1 {
		t.Fatalf("outbox = %d, want 1 (ExceptionOpened)", n)
	}
}

func TestUnitOfWorkRollbackUnWritesEverything(t *testing.T) {
	pool := pgtest.NewPool(t)
	repo := postgres.NewCaseRepo(pool)
	pub := postgres.NewOutboxPublisher(pool)
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()

	burst := errors.New("consumer burst error")
	c := unitCase(t, "case-1")
	err := uow.Do(ctx, func(ctx context.Context) error {
		if err := repo.Save(ctx, c); err != nil {
			return err
		}
		if err := pub.PublishCaseEvents(ctx, c.DomainEvents()...); err != nil {
			return err
		}
		return burst
	})
	if !errors.Is(err, burst) {
		t.Fatalf("Do returned %v, want the fn error unchanged", err)
	}
	if n := pgtest.Count(t, pool, "exception_cases"); n != 0 {
		t.Fatalf("exception_cases = %d, want 0 (rolled back)", n)
	}
	if n := pgtest.Count(t, pool, "outbox"); n != 0 {
		t.Fatalf("outbox = %d, want 0 (rolled back with the case)", n)
	}
}

func TestNestedDoJoinsTheOuterTransaction(t *testing.T) {
	pool := pgtest.NewPool(t)
	repo := postgres.NewCaseRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()

	c := unitCase(t, "case-1")
	err := uow.Do(ctx, func(outer context.Context) error {
		if err := uow.Do(outer, func(inner context.Context) error { // must JOIN, not begin+commit
			return repo.Save(inner, c)
		}); err != nil {
			return err
		}
		// Still inside the OUTER transaction: another connection must not
		// see the row yet, and the outer unit can still roll it all back.
		other, err := pool.Acquire(ctx)
		if err != nil {
			return err
		}
		defer other.Release()
		var visible int
		if err := other.QueryRow(ctx, `SELECT count(*) FROM exception_cases WHERE case_id = 'case-1'`).Scan(&visible); err != nil {
			return err
		}
		if visible != 0 {
			return errors.New("the nested Do committed early: the row is visible before the outer commit")
		}
		return errors.New("roll the outer unit back on purpose")
	})
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("Do = %v, want the deliberate rollback error", err)
	}
	if n := pgtest.Count(t, pool, "exception_cases"); n != 0 {
		t.Fatalf("exception_cases = %d, want 0: a joined nested unit must not survive the outer rollback", n)
	}
}

func TestPolicyDeclarationOutboxRowCarriesTheWireShape(t *testing.T) {
	pool := pgtest.NewPool(t)
	repo := postgres.NewPolicyRepo(pool)
	pub := postgres.NewOutboxPublisher(pool)
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()

	rule, err := exceptionpolicy.NewRule("com.warehouse.wes.order-management.order.OrderRepromised", 10)
	if err != nil {
		t.Fatal(err)
	}
	p, err := exceptionpolicy.Declare(exceptionpolicy.DeclareInput{
		SourceType: "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed",
		Severity:   shared.SeverityHigh, Queue: "ps-main", SLAMinutes: 30,
		AutoResolve: &rule, At: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = uow.Do(ctx, func(ctx context.Context) error {
		if err := repo.Save(ctx, p); err != nil {
			return err
		}
		return pub.PublishPolicyDeclared(ctx, p.Declaration())
	})
	if err != nil {
		t.Fatal(err)
	}

	var aggregType, payload string
	if err := pool.QueryRow(ctx, `SELECT aggreg_type, payload::text FROM outbox LIMIT 1`).Scan(&aggregType, &payload); err != nil {
		t.Fatal(err)
	}
	if aggregType != "exceptionpolicy" {
		t.Fatalf("aggreg_type = %s", aggregType)
	}
	for _, want := range []string{
		`"event": "ExceptionPolicyDeclared"`,
		`"source_type": "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"`,
		`"version": 1`,
		`"severity": "HIGH"`,
		`"queue": "ps-main"`,
		`"sla_minutes": 30`,
		`"on_type": "com.warehouse.wes.order-management.order.OrderRepromised"`,
		`"within_minutes": 10`,
	} {
		if !strings.Contains(payload, want) {
			t.Fatalf("outbox payload misses %s:\n%s", want, payload)
		}
	}
}
