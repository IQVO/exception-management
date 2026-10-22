//go:build integration

package analyticsstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/analytics/report"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// The SAME contract as the memory twin, against a real Postgres migrated by
// the projector's own boot step (RunMigrations), plus the atomicity proof:
// the dedupe claim and the fold commit or roll back together.
func TestPostgresStoreSatisfiesTheContract(t *testing.T) {
	url := pgtest.EmptyURL(t) // no OLTP migrations: the analytical schema is its own
	if err := RunMigrations(url); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	pool, err := NewPool(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	p := NewProjection(pool)
	runStoreContract(t, contractStore{name: "postgres", projection: p, reader: NewReader(pool)})
}

// A fold that the database refuses leaves NOTHING behind: the claimed event
// id is rolled back with the failed fold, so a fixed redelivery applies
// afresh. The refusal is raised by a trigger with an integrity-violation
// SQLSTATE, which classify maps to report.ErrRejected.
func TestPostgresFoldFailureRollsTheClaimBack(t *testing.T) {
	url := pgtest.EmptyURL(t)
	if err := RunMigrations(url); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	pool, err := NewPool(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION reject_case_facts() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'projection refused' USING ERRCODE = '23000';
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER reject_case_fold BEFORE INSERT ON exception_case_facts
		FOR EACH ROW EXECUTE FUNCTION reject_case_facts();
	`); err != nil {
		t.Fatalf("install the refusing trigger: %v", err)
	}

	e := report.Event{Kind: report.KindExceptionOpened, EventID: "boom-1", At: time.Now().UTC(),
		CaseID: "case-boom", SourceType: sourceTaskCE, SourceSubject: "task-x", Severity: "HIGH",
		Queue: "ps-main", SLADueAt: time.Now().UTC().Add(time.Hour)}
	if _, err := NewProjection(pool).Apply(ctx, e); !errors.Is(err, report.ErrRejected) {
		t.Fatalf("apply under the refusing trigger: %v, want ErrRejected", err)
	}

	// The claim rolled back with the fold: the id is not in the dedupe set.
	var claimed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM analytics_processed_events WHERE event_id = $1`, e.EventID).Scan(&claimed); err != nil {
		t.Fatal(err)
	}
	if claimed != 0 {
		t.Fatal("the event id was claimed although the fold failed: the claim and the effect must commit together")
	}
}

// The read-only pool really is read-only: a write through it fails, so a bug
// in the reports binary cannot mutate the model.
func TestReadOnlyPoolCannotWrite(t *testing.T) {
	url := pgtest.EmptyURL(t)
	if err := RunMigrations(url); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	ro, err := NewReadOnlyPool(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if _, err := ro.Exec(context.Background(), `INSERT INTO analytics_processed_events (event_id, event_type, occurred_at) VALUES ('x','Y',now())`); err == nil {
		t.Fatal("a write through the read-only pool succeeded")
	}
}
