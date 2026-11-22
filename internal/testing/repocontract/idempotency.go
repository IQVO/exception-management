package repocontract

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
)

// IdempotencyStore runs the ports.IdempotencyStore contract: first-writer-wins
// saves, lookup misses and round trips. newStore must return an EMPTY store
// on every call. Records are stamped "now" — the replay window
// (repository.IdempotencyTTL) is exercised by IdempotencyRetention below.
func IdempotencyStore(t *testing.T, newStore func(t *testing.T) ports.IdempotencyStore) {
	t.Helper()
	cases := map[string]func(t *testing.T, s ports.IdempotencyStore){
		"an unknown key has no answer":              idemNotFound,
		"save then lookup round trips the answer":   idemRoundTrip,
		"the first writer of a key wins":            idemFirstWriterWins,
		"keys are independent":                      idemKeysIndependent,
		"the stored body may be empty but not null": idemEmptyBody,
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { fn(t, newStore(t)) })
	}
}

// idemStamp is a fresh, microsecond-truncated stamp (the precision Postgres
// keeps, so a round trip compares equal) — a record stored "now" is live for
// the whole repository.IdempotencyTTL, on every store and every clock.
func idemStamp() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// idemJSON is the stored answers' content type (a goconst, not a magic
// string three times over).
const idemJSON = "application/json"

func idemRecord(key, fp string, status int, body []byte) repository.IdempotencyRecord {
	return repository.IdempotencyRecord{Key: key, Fingerprint: fp, Status: status, ContentType: idemJSON, Body: body, CreatedAt: idemStamp()}
}

func idemNotFound(t *testing.T, s ports.IdempotencyStore) {
	rec, found, err := s.Lookup(context.Background(), "nope")
	must(t, err)
	if found || rec.Key != "" {
		t.Fatalf("lookup of an unknown key = (%+v, %v), want the zero record and not-found", rec, found)
	}
}

func idemRoundTrip(t *testing.T, s ports.IdempotencyStore) {
	at := idemStamp()
	rec := repository.IdempotencyRecord{Key: "k1", Fingerprint: "fp-rt", Status: 200, ContentType: idemJSON, Body: []byte(`{"body":"ok"}`), CreatedAt: at}
	must(t, s.Save(context.Background(), rec))
	got, found, err := s.Lookup(context.Background(), "k1")
	must(t, err)
	if !found {
		t.Fatal("the saved key was not found")
	}
	if got.Fingerprint != "fp-rt" || got.Status != 200 || got.ContentType != idemJSON ||
		string(got.Body) != `{"body":"ok"}` || !got.CreatedAt.Equal(at) {
		t.Fatalf("round trip = %+v, want %+v", got, rec)
	}
}

func idemFirstWriterWins(t *testing.T, s ports.IdempotencyStore) {
	must(t, s.Save(context.Background(), idemRecord("k1", "fp1", 200, []byte("first"))))
	must(t, s.Save(context.Background(), idemRecord("k1", "fp2", 409, []byte("second"))))
	got, found, err := s.Lookup(context.Background(), "k1")
	must(t, err)
	if !found || got.Fingerprint != "fp1" || got.Status != 200 || string(got.Body) != "first" {
		t.Fatalf("after two saves = (%+v, %v), want the first writer's answer", got, found)
	}
}

func idemKeysIndependent(t *testing.T, s ports.IdempotencyStore) {
	must(t, s.Save(context.Background(), idemRecord("k1", "fp1", 200, []byte("one"))))
	must(t, s.Save(context.Background(), idemRecord("k2", "fp2", 201, []byte("two"))))
	for key, want := range map[string]int{"k1": 200, "k2": 201} {
		got, found, err := s.Lookup(context.Background(), key)
		must(t, err)
		if !found || got.Status != want {
			t.Fatalf("lookup %s = (%d, %v), want (%d, true)", key, got.Status, found, want)
		}
	}
}

func idemEmptyBody(t *testing.T, s ports.IdempotencyStore) {
	// A 204-class answer has no body; the stored row must still round trip
	// (as empty bytes, never as an error).
	must(t, s.Save(context.Background(), idemRecord("k1", "fp1", 204, nil)))
	got, found, err := s.Lookup(context.Background(), "k1")
	must(t, err)
	if !found || got.Status != 204 || len(got.Body) != 0 {
		t.Fatalf("empty-body round trip = (%+v, %v), want the 204 back", got, found)
	}
}

// IdempotencyRetentionFixture is one ports.IdempotencyStore over ONE empty
// database, with the hooks that store and age answers on ITS clock: Put
// stamps with the same clock Age advances (the memory double's clock, the
// Postgres now()), so a case's ages stay comparable as the clock moves.
type IdempotencyRetentionFixture struct {
	Store ports.IdempotencyStore
	// Put stores a 200 answer for key under a fresh stamp of the fixture's
	// clock.
	Put func(t *testing.T, key, answer string)
	// Age makes every answer stored so far d older (the memory double
	// advances its clock, Postgres moves created_at back): the only way a
	// test reaches an expired row without waiting a day. A negative d ages
	// them back down.
	Age func(t *testing.T, d time.Duration)
}

// IdempotencyRetentionFactory returns a fresh, empty fixture and registers
// its own cleanup with t.
type IdempotencyRetentionFactory func(t *testing.T) IdempotencyRetentionFixture

// IdempotencyRetention runs the retention contract of the
// ports.IdempotencyStore — the 24-hour replay window of
// repository.IdempotencyTTL (ADR 0015, addendum 2026-11-22): the memory
// double and the Postgres store both run it.
func IdempotencyRetention(t *testing.T, factory IdempotencyRetentionFactory) {
	t.Helper()
	cases := []struct {
		name string
		run  func(*testing.T, IdempotencyRetentionFixture)
	}{
		{"an expired answer is not found, a live one still is", retentionAgeGuard},
		{"the purge deletes expired keeps live", retentionPurgeKeepsLive},
		{"one batch per pass", retentionPurgeBatches},
		{"oldest first", retentionPurgeOldestFirst},
		{"a save past the window refreshes the key", retentionSaveRefreshes},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.run(t, factory(t)) })
	}
}

// retentionAgeGuard: an answer a minute past the TTL is invisible to Lookup,
// one stored after it (a minute old) still answers — the read guard that
// keeps an expired row from answering between expiry and the sweep.
func retentionAgeGuard(t *testing.T, f IdempotencyRetentionFixture) {
	f.Put(t, "gone", "v-gone")
	f.Age(t, repository.IdempotencyTTL+time.Minute)
	f.Put(t, "edge", "v-edge")
	f.Age(t, time.Minute)
	wantNotFound(t, f.Store, "gone")
	wantFound(t, f.Store, "edge", "v-edge")
}

// retentionPurgeKeepsLive: a row older than the TTL goes, a row a minute
// short of it and a fresh one stay and answer; a second pass has nothing
// left to delete.
func retentionPurgeKeepsLive(t *testing.T, f IdempotencyRetentionFixture) {
	ctx := context.Background()
	f.Put(t, "old", "v-old")
	f.Age(t, repository.IdempotencyTTL+time.Minute)
	f.Put(t, "edge", "v-edge")
	f.Age(t, repository.IdempotencyTTL-time.Minute)
	f.Put(t, "live", "v-live")
	if n, err := f.Store.PurgeExpired(ctx, 100); err != nil || n != 1 {
		t.Fatalf("purge = %d %v, want 1 (only the expired row)", n, err)
	}
	wantNotFound(t, f.Store, "old")
	wantFound(t, f.Store, "edge", "v-edge")
	wantFound(t, f.Store, "live", "v-live")
	if n, err := f.Store.PurgeExpired(ctx, 100); err != nil || n != 0 {
		t.Fatalf("second purge = %d %v, want 0", n, err)
	}
}

// retentionPurgeBatches: one pass deletes at most limit rows, the next ones
// finish the backlog.
func retentionPurgeBatches(t *testing.T, f IdempotencyRetentionFixture) {
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		f.Put(t, k, "v-"+k)
	}
	f.Age(t, repository.IdempotencyTTL+time.Hour)
	for _, want := range []int{2, 2, 1, 0} {
		if n, err := f.Store.PurgeExpired(context.Background(), 2); err != nil || n != want {
			t.Fatalf("purge = %d %v, want %d", n, err, want)
		}
	}
}

// retentionPurgeOldestFirst: with two expired rows and a limit of one, the
// older goes. Ageing everything back by more than both ages makes the
// survivor live again, which is what tells the two apart (an expired row
// never answers).
func retentionPurgeOldestFirst(t *testing.T, f IdempotencyRetentionFixture) {
	f.Put(t, "older", "v-older")
	f.Age(t, time.Hour)
	f.Put(t, "newer", "v-newer")
	f.Age(t, repository.IdempotencyTTL)
	if n, err := f.Store.PurgeExpired(context.Background(), 1); err != nil || n != 1 {
		t.Fatalf("purge = %d %v, want 1", n, err)
	}
	f.Age(t, -(repository.IdempotencyTTL + time.Hour)) // both would be live again were they still stored
	wantFound(t, f.Store, "newer", "v-newer")
	wantNotFound(t, f.Store, "older")
}

// retentionSaveRefreshes: the answer of a key whose previous answer has
// expired replaces it (the key is free again), so the purge keeps the row and
// the new answer is the one that replays — the double of the Postgres
// ON CONFLICT DO UPDATE ... WHERE created_at <= now() - TTL.
func retentionSaveRefreshes(t *testing.T, f IdempotencyRetentionFixture) {
	f.Put(t, "k", "stale")
	f.Age(t, repository.IdempotencyTTL+time.Hour)
	f.Put(t, "k", "fresh")
	if n, err := f.Store.PurgeExpired(context.Background(), 100); err != nil || n != 0 {
		t.Fatalf("purge after a refresh = %d %v, want 0 (the refreshed row is live)", n, err)
	}
	wantFound(t, f.Store, "k", "fresh")
}

func wantFound(t *testing.T, s ports.IdempotencyStore, key, answer string) {
	t.Helper()
	got, found, err := s.Lookup(context.Background(), key)
	must(t, err)
	if !found || string(got.Body) != answer {
		t.Fatalf("lookup %s = (%q, %v), want the stored %q", key, got.Body, found, answer)
	}
}

func wantNotFound(t *testing.T, s ports.IdempotencyStore, key string) {
	t.Helper()
	got, found, err := s.Lookup(context.Background(), key)
	must(t, err)
	if found {
		t.Fatalf("lookup %s = (%q, %v), want not-found (outside the replay window)", key, got.Body, found)
	}
}
