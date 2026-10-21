package http

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The readiness gate's own mechanics (the HTTP behaviour is health_test.go).

func TestReadinessCachesTheVerdictForTheTTL(t *testing.T) {
	calls := 0
	g := NewReadiness(10*time.Minute, time.Second,
		Dependency{Name: "db", Check: func(context.Context) error { calls++; return nil }})

	ctx := context.Background()
	if failing := g.Failing(ctx); len(failing) != 0 {
		t.Fatalf("failing = %v", failing)
	}
	for range 3 {
		if failing := g.Failing(ctx); len(failing) != 0 {
			t.Fatalf("failing = %v", failing)
		}
	}
	if calls != 1 {
		t.Fatalf("the check ran %d times inside the ttl, want 1", calls)
	}
}

func TestReadinessRechecksAfterTheTTL(t *testing.T) {
	failing := true
	g := NewReadiness(time.Nanosecond, time.Second,
		Dependency{Name: "db", Check: func(context.Context) error {
			if failing {
				return errors.New("down")
			}
			return nil
		}})
	ctx := context.Background()
	if got := g.Failing(ctx); len(got) != 1 || got[0] != "db" {
		t.Fatalf("first verdict = %v, want [db]", got)
	}
	time.Sleep(time.Millisecond)
	failing = false
	if got := g.Failing(ctx); len(got) != 0 {
		t.Fatalf("second verdict = %v, want none", got)
	}
}

func TestReadinessCheckIsBoundedByTheTimeout(t *testing.T) {
	g := NewReadiness(time.Nanosecond, 10*time.Millisecond,
		Dependency{Name: "hangs", Check: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Minute):
				return nil
			}
		}})
	start := time.Now()
	if got := g.Failing(context.Background()); len(got) != 1 {
		t.Fatalf("failing = %v, want the hung check reported", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the check was not bounded: %s", elapsed)
	}
}

func TestReadinessDetachedContextSurvivesCallerCancel(t *testing.T) {
	g := NewReadiness(time.Hour, time.Second,
		Dependency{Name: "db", Check: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
				return nil
			}
		}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the caller hung up before the check ran
	if got := g.Failing(ctx); len(got) != 0 {
		t.Fatalf("a cancelled caller poisoned the verdict: %v", got)
	}
}
