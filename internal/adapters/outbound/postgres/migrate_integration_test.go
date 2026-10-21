//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// This file proves the migration runner over the EMBEDDED files: apply, be
// idempotent, carry the fleet's shared tables and the partial unique index
// exactly as designed, and down-recreate the previous (empty) schema.

func TestMigrationsApplyAndAreIdempotent(t *testing.T) {
	url := pgtest.EmptyURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := postgres.RunMigrations(url); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := postgres.RunMigrations(url); err != nil {
		t.Fatalf("second run must be a no-op: %v", err)
	}

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()

	for _, table := range []string{"exception_cases", "exception_policies", "processed_events", "outbox", "idempotency_keys"} {
		var reg string
		if err := conn.QueryRow(ctx, `SELECT to_regclass($1)::text`, "public."+table).Scan(&reg); err != nil || reg == "" {
			t.Fatalf("table %s missing after migration (reg=%q err=%v)", table, reg, err)
		}
	}
	var version int
	if err := conn.QueryRow(ctx, `SELECT version FROM schema_migrations`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("schema_migrations version = %d %v, want 2", version, err)
	}
}

func TestMigrationsCarryTheDesignedIndexes(t *testing.T) {
	url := pgtest.URL(t) // applied by the shared template
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()

	cases := []struct {
		index string
		want  string // a fragment of the index definition
	}{
		// The one-open invariant, as a PARTIAL unique index over the three
		// dedup-key columns.
		{"uq_exception_cases_one_open_per_key", "WHERE (status <> 'RESOLVED'::text)"},
		{"uq_exception_cases_one_open_per_key", "UNIQUE INDEX"},
		// The relay's claim scan.
		{"idx_outbox_attempts_next_attempt", "(attempts, next_attempt_at)"},
	}
	for _, c := range cases {
		var def string
		if err := conn.QueryRow(ctx, `SELECT indexdef FROM pg_indexes WHERE indexname = $1`, c.index).Scan(&def); err != nil {
			t.Fatalf("index %s missing: %v", c.index, err)
		}
		if !strings.Contains(def, c.want) {
			t.Fatalf("index %s = %s, want it to contain %q", c.index, def, c.want)
		}
	}
}

func TestMigrationsDownRecreatesThePreviousSchema(t *testing.T) {
	url := pgtest.EmptyURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := postgres.RunMigrations(url); err != nil {
		t.Fatal(err)
	}
	m, err := postgres.Migrator(url)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Down(); err != nil {
		t.Fatalf("down: %v", err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, table := range []string{"exception_cases", "exception_policies", "processed_events", "outbox", "idempotency_keys"} {
		var reg *string
		if err := conn.QueryRow(ctx, `SELECT to_regclass($1)::text`, "public."+table).Scan(&reg); err != nil {
			t.Fatalf("probe %s: %v", table, err)
		}
		if reg != nil {
			t.Fatalf("table %s survived the down migration", table)
		}
	}
}
