//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/application/outbox"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// The OutboxStore's drain contract against real Postgres: a published row is
// removed in the same pass, a failed row records attempts/backoff and stops
// the pass (later rows never overtake a row still being retried), the cap
// excludes a row from claims, and a payload that is not the publisher's
// envelope is handed over raw for dead-lettering instead of failing the pass.

// insertRow inserts one outbox row with the given payload (the envelope
// OutboxPublisher would have written, unless the test says otherwise) and
// returns its id.
func insertRow(t *testing.T, pool *pgxpool.Pool, event, aggregID, payload string) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(), `
		INSERT INTO outbox (event_id, aggreg_type, aggreg_id, payload, occurred_at)
		VALUES ($1, $2, $3, $4::jsonb, $5)
		RETURNING id
	`, "evt-"+event+"-"+aggregID, "exceptioncase", aggregID, payload, time.Now().UTC()).Scan(&id)
	if err != nil {
		t.Fatalf("insert outbox row: %v", err)
	}
	return id
}

func envelope(event, aggregID string) string {
	return `{"event":"` + event + `","aggregate_id":"` + aggregID + `","occurred_at":"2026-10-21T09:30:00Z","data":{"case_id":"` + aggregID + `"}}`
}

// rowState reads one row's attempts and whether its retry is still due.
func rowState(t *testing.T, pool *pgxpool.Pool, id int64) (attempts int, due bool, nextAttemptAt time.Time) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT attempts, next_attempt_at <= now(), next_attempt_at FROM outbox WHERE id = $1`, id,
	).Scan(&attempts, &due, &nextAttemptAt)
	if err != nil {
		t.Fatalf("read outbox row %d: %v", id, err)
	}
	return attempts, due, nextAttemptAt
}

func rearmRow(t *testing.T, pool *pgxpool.Pool, id int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE outbox SET next_attempt_at = now() WHERE id = $1`, id); err != nil {
		t.Fatalf("make row %d due: %v", id, err)
	}
}

// okHandler accepts every row and records it.
type okHandler struct{ seen []outbox.Row }

func (h *okHandler) Handle(_ context.Context, row outbox.Row) error {
	h.seen = append(h.seen, row)
	return nil
}

// failOnHandler fails rows whose fact event is in fail and accepts the rest.
type failOnHandler struct {
	fail map[string]bool
	seen []outbox.Row
}

func (h *failOnHandler) Handle(_ context.Context, row outbox.Row) error {
	h.seen = append(h.seen, row)
	if h.fail[row.Fact.Event] {
		return errors.New("kafka: simulated broker failure")
	}
	return nil
}

// A row the publisher really wrote round-trips: Drain hands the handler the
// decoded envelope (entity, event, aggregate id, the wire data), and a
// handled row is DELETED in the same pass — nothing else removes it.
func TestOutboxStoreDrainDecodesAndRemovesPublishedRows(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewOutboxStore(pool)
	pub := postgres.NewOutboxPublisher(pool)
	ctx := context.Background()

	c := integrationCase(t, "case-drain-1", "ord-drain-line-1", time.Now().UTC())
	if err := postgres.NewUnitOfWork(pool).Do(ctx, func(ctx context.Context) error {
		return pub.PublishCaseEvents(ctx, c.DomainEvents()...)
	}); err != nil {
		t.Fatal(err)
	}
	if n := pgtest.Count(t, pool, "outbox"); n != 1 {
		t.Fatalf("outbox = %d rows, want 1", n)
	}

	h := &okHandler{}
	published, err := store.Drain(ctx, 10, 0, h.Handle)
	if err != nil || published != 1 {
		t.Fatalf("Drain = %d, %v; want 1, nil", published, err)
	}
	if n := pgtest.Count(t, pool, "outbox"); n != 0 {
		t.Fatalf("outbox = %d rows after the pass, want 0 (published rows are removed)", n)
	}
	row := h.seen[0]
	if row.Fact.Entity != "exceptioncase" || row.Fact.Event != "ExceptionOpened" || row.Fact.AggregateID != "case-drain-1" {
		t.Fatalf("fact = %+v", row.Fact)
	}
	if row.Fact.EventID == "" || row.Fact.OccurredAt.IsZero() || len(row.Fact.Data) == 0 || len(row.RawPayload) == 0 {
		t.Fatalf("fact not fully decoded: %+v", row)
	}
	if row.Fact.Data[0] != '{' {
		t.Fatalf("data is not the envelope's JSON object: %s", row.Fact.Data)
	}
}

// The first failure stops the pass at that row: it records attempts+1 and a
// future next_attempt_at (the exponential backoff), the rows behind it are
// left untouched, and the error is an *outbox.SendFailure with the new count.
func TestOutboxStoreFailureRecordsBackoffAndStopsThePass(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewOutboxStore(pool)
	ctx := context.Background()
	poison := insertRow(t, pool, "Poison", "case-1", envelope("Poison", "case-1"))
	good := insertRow(t, pool, "Good", "case-1", envelope("Good", "case-1"))

	h := &failOnHandler{fail: map[string]bool{"Poison": true}}
	published, err := store.Drain(ctx, 10, 0, h.Handle)
	if err == nil || published != 0 {
		t.Fatalf("Drain = %d, %v; want 0 and the failure", published, err)
	}
	var sf *outbox.SendFailure
	if !errors.As(err, &sf) || sf.RowID != poison || sf.Attempts != 1 {
		t.Fatalf("err = %v, want *SendFailure{row %d, attempts 1}", err, poison)
	}
	attempts, due, next := rowState(t, pool, poison)
	if attempts != 1 || due || !next.After(time.Now()) {
		t.Fatalf("poison row: attempts=%d due=%v next=%s, want 1, not due, in the future", attempts, due, next)
	}
	if n := pgtest.Count(t, pool, "outbox"); n != 2 {
		t.Fatalf("outbox = %d rows, want 2 (nothing deleted: the pass stopped at the failure)", n)
	}
	if got, _, _ := rowState(t, pool, good); got != 0 {
		t.Fatalf("good row attempts = %d, want 0 (never claimed)", got)
	}
	if len(h.seen) != 1 || h.seen[0].Fact.Event != "Poison" {
		t.Fatalf("handler saw %+v, want only the poison row", h.seen)
	}

	// The backoff holds the poison row only: the next pass does not reclaim
	// it (attempts stay 1) and the good row behind it overtakes it — the
	// deliberate per-key trade-off of the backoff design (ADR 0005).
	published, err = store.Drain(ctx, 10, 0, h.Handle)
	if err != nil || published != 1 {
		t.Fatalf("pass during backoff = %d, %v; want 1 (the good row overtakes), nil", published, err)
	}
	if attempts, _, _ := rowState(t, pool, poison); attempts != 1 {
		t.Fatalf("poison attempts during backoff = %d, want 1 (not reclaimed)", attempts)
	}
	// Once the retry is due again the row is claimed and fails again.
	rearmRow(t, pool, poison)
	if published, err := store.Drain(ctx, 10, 0, h.Handle); err == nil || published != 0 {
		t.Fatalf("due pass = %d, %v; want 0 and the failure", published, err)
	}
	if attempts, _, _ := rowState(t, pool, poison); attempts != 2 {
		t.Fatalf("attempts after second failure = %d, want 2", attempts)
	}
}

// With maxAttempts > 0 a row that reached the cap is excluded from claims:
// the pass skips it and publishes the good row behind it — the deliberate
// overtake of ADR 0005. The runbook's re-arm (attempts = 0) claims it again.
func TestOutboxStoreCapExcludesCappedRowAndReArmClaimsIt(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewOutboxStore(pool)
	ctx := context.Background()
	poison := insertRow(t, pool, "Poison", "case-cap-1", envelope("Poison", "case-cap-1"))
	good := insertRow(t, pool, "Good", "case-cap-1", envelope("Good", "case-cap-1"))

	h := &failOnHandler{fail: map[string]bool{"Poison": true}}
	for i := 1; i <= 2; i++ { // cap 2: two failures exclude the row
		if _, err := store.Drain(ctx, 10, 2, h.Handle); err == nil {
			t.Fatalf("pass %d: want the failure", i)
		}
		if i == 1 {
			rearmRow(t, pool, poison)
		}
	}
	if attempts, _, _ := rowState(t, pool, poison); attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}

	// The capped row is skipped even when due; the good row overtakes it.
	rearmRow(t, pool, poison)
	published, err := store.Drain(ctx, 10, 2, h.Handle)
	if err != nil || published != 1 {
		t.Fatalf("pass after the cap = %d, %v; want 1 (the good row), nil", published, err)
	}
	if last := h.seen[len(h.seen)-1]; last.Fact.Event != "Good" || last.ID != good {
		t.Fatalf("the overtaking row = %+v, want the good row %d", last.Fact, good)
	}
	if n := pgtest.Count(t, pool, "outbox"); n != 1 {
		t.Fatalf("outbox = %d rows, want 1 (only the capped poison row)", n)
	}

	// The runbook re-arm: attempts = 0 makes the row claimable again.
	if _, err := pool.Exec(ctx, `UPDATE outbox SET attempts = 0, next_attempt_at = now() WHERE id = $1`, poison); err != nil {
		t.Fatal(err)
	}
	h.fail = nil // the broker healed
	if published, err := store.Drain(ctx, 10, 2, h.Handle); err != nil || published != 1 {
		t.Fatalf("re-armed pass = %d, %v; want 1, nil", published, err)
	}
	if n := pgtest.Count(t, pool, "outbox"); n != 0 {
		t.Fatalf("outbox = %d rows after the re-armed pass, want 0", n)
	}
}

// With maxAttempts 0 (unlimited) a failed row is claimed forever: the good
// row behind it never publishes. This is why every cmd relay must pass a cap
// (fitness test TestOutboxRelaysPassAttemptCap).
func TestOutboxStoreUnlimitedRetriesBlockLaterRows(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewOutboxStore(pool)
	ctx := context.Background()
	insertRow(t, pool, "Poison", "case-nocap", envelope("Poison", "case-nocap"))
	insertRow(t, pool, "Good", "case-nocap", envelope("Good", "case-nocap"))

	h := &failOnHandler{fail: map[string]bool{"Poison": true}}
	for range 3 {
		if _, err := store.Drain(ctx, 10, 0, h.Handle); err == nil {
			t.Fatal("want the failure every pass")
		}
		if n := pgtest.Count(t, pool, "outbox"); n != 2 {
			t.Fatalf("outbox = %d rows mid-retries, want 2", n)
		}
		// make every row due again (the scheduler's tick; backoff elided)
		if _, err := pool.Exec(ctx, `UPDATE outbox SET next_attempt_at = now()`); err != nil {
			t.Fatal(err)
		}
	}
	if len(h.seen) != 3 || h.seen[2].Fact.Event != "Poison" {
		t.Fatalf("handler saw %d rows, want the poison row 3 times (good row never claimed)", len(h.seen))
	}
}

// A payload that is not the publisher's envelope does not fail the pass: the
// row is handed over with an empty event name and its raw bytes, the send
// path dead-letters it, and the handled row is removed.
func TestOutboxStoreForeignPayloadIsHandedOverRaw(t *testing.T) {
	pool := pgtest.NewPool(t)
	store := postgres.NewOutboxStore(pool)
	ctx := context.Background()
	raw := `{"not":"the envelope"}`
	id := insertRow(t, pool, "", "case-raw", raw)

	h := &okHandler{}
	published, err := store.Drain(ctx, 10, 0, h.Handle)
	if err != nil || published != 1 {
		t.Fatalf("Drain = %d, %v; want 1, nil (a foreign payload is not a pass failure)", published, err)
	}
	row := h.seen[0]
	if row.ID != id || row.Fact.Event != "" {
		t.Fatalf("row = %+v, want the empty event name of an undecodable payload", row)
	}
	// jsonb re-renders the payload (its own spacing/key order), so compare
	// the JSON, not the bytes: the dead-letter value is exactly what the
	// table holds.
	var gotPayload, wantPayload any
	if err := json.Unmarshal(row.RawPayload, &gotPayload); err != nil {
		t.Fatalf("raw payload is not JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(raw), &wantPayload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotPayload, wantPayload) {
		t.Fatalf("raw payload = %s, want the inserted %s", row.RawPayload, raw)
	}
	if row.Fact.Entity != "exceptioncase" || row.Fact.AggregateID != "case-raw" {
		t.Fatalf("fact = %+v, want the columns carried over", row.Fact)
	}
	if n := pgtest.Count(t, pool, "outbox"); n != 0 {
		t.Fatalf("outbox = %d rows, want 0 (the dead-lettered row is removed)", n)
	}
}
