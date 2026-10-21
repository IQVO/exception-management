package kafka

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// fakeReader programmes the run loop without a broker: it hands out the
// queued messages one at a time and records every commit.
type fakeReader struct {
	mu         sync.Mutex
	fetched    int
	msgs       []kafkago.Message
	commits    []kafkago.Message
	fetchErr   error
	commitErr  error
	commitErrN int // how many commits fail before succeeding
}

func (r *fakeReader) FetchMessage(context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fetched >= len(r.msgs) {
		if r.fetchErr != nil {
			return kafkago.Message{}, r.fetchErr
		}
		return kafkago.Message{}, errors.New("no more messages")
	}
	if r.fetchErr != nil && r.fetched == 0 && len(r.msgs) == 0 {
		return kafkago.Message{}, r.fetchErr
	}
	m := r.msgs[r.fetched]
	r.fetched++
	return m, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, msgs...)
	if r.commitErrN > 0 {
		r.commitErrN--
		return r.commitErr
	}
	return nil
}

func (*fakeReader) Close() error { return nil }

func msg(offset int, value string) kafkago.Message {
	return kafkago.Message{Partition: 0, Offset: int64(offset), Value: []byte(value)}
}

func (r *fakeReader) commitsN() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.commits)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestConsumeLoop_CommitsOnlyAfterSuccess: while handling keeps failing the
// loop keeps retrying the SAME message and nothing is committed; the commit
// lands only once handling succeeded, and only then is the next message
// fetched.
func TestConsumeLoop_CommitsOnlyAfterSuccess(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{msg(0, "m0"), msg(1, "m1")}, fetchErr: context.Canceled}
	var mu sync.Mutex
	attempts := 0
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	loop := consumeLoop{
		reader: reader, logger: slog.Default(), name: "test",
		sleep: func(context.Context, time.Duration) error { return nil },
		handle: func(context.Context, kafkago.Message) error {
			mu.Lock()
			attempts++
			n := attempts
			mu.Unlock()
			if n < 3 {
				return errors.New("transient")
			}
			if n == 3 {
				<-release // hold the success until the test has looked
			}
			return nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- loop.run(ctx) }()

	// m0 failed twice and the third attempt is held: no commit may exist.
	waitFor(t, 5*time.Second, "the third handling attempt", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts >= 3
	})
	if n := reader.commitsN(); n != 0 {
		t.Fatalf("%d commits while handling still failed; the offset must move only after success", n)
	}
	close(release)
	waitFor(t, 5*time.Second, "the first commit after success", func() bool { return reader.commitsN() >= 1 })
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want ctx cancellation", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts < 3 {
		t.Fatalf("attempts = %d, want the same message retried", attempts)
	}
}

// TestConsumeLoop_RetriesWithCappedBackoff pins the backoff shape:
// 200ms, 400ms, 800ms ... capped at 5s.
func TestConsumeLoop_RetriesWithCappedBackoff(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{msg(0, "m0")}}
	var slept []time.Duration
	var mu sync.Mutex
	loop := consumeLoop{
		reader: reader, logger: slog.Default(), name: "test",
		handle: func(context.Context, kafkago.Message) error { return errors.New("down") },
		sleep: func(ctx context.Context, d time.Duration) error {
			mu.Lock()
			slept = append(slept, d)
			n := len(slept)
			mu.Unlock()
			if n >= 7 {
				return context.Canceled // stop the loop under test
			}
			return nil
		},
	}
	if err := loop.run(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want ctx.Canceled after the recorded sleeps", err)
	}
	want := []time.Duration{
		200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
		1600 * time.Millisecond, 3200 * time.Millisecond, 5 * time.Second, 5 * time.Second,
	}
	if len(slept) != len(want) {
		t.Fatalf("slept %v, want %v", slept, want)
	}
	for i := range want {
		if slept[i] != want[i] {
			t.Fatalf("slept %v, want %v", slept, want)
		}
	}
}

// TestConsumeLoop_FetchFailureStopsRun: only a reader failure (or ctx
// cancellation) ends the loop.
func TestConsumeLoop_FetchFailureStopsRun(t *testing.T) {
	wantErr := errors.New("reader closed")
	loop := consumeLoop{
		reader: &fakeReader{fetchErr: wantErr}, logger: slog.Default(), name: "test",
		handle: func(context.Context, kafkago.Message) error { return nil },
		sleep:  func(context.Context, time.Duration) error { return nil },
	}
	if err := loop.run(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("run = %v, want the fetch error", err)
	}
}

// TestConsumeLoop_DeadLettersPoisonAtOnce: a poison marker is published to
// the dead letter topic without a handling retry, and the offset then moves
// past the message.
func TestConsumeLoop_DeadLettersPoisonAtOnce(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{msg(0, "poison"), msg(1, "good")},
		fetchErr: context.Canceled}
	var dead []string
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	loop := consumeLoop{
		reader: reader, logger: slog.Default(), name: "test",
		sleep: func(context.Context, time.Duration) error { return nil },
		handle: func(_ context.Context, m kafkago.Message) error {
			if string(m.Value) == "poison" {
				return &errDeadLetter{cause: errors.New("no mappable id")}
			}
			return nil
		},
		deadLetter: func(_ context.Context, m kafkago.Message, cause error) error {
			mu.Lock()
			dead = append(dead, string(m.Value)+" ("+cause.Error()+")")
			mu.Unlock()
			return nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- loop.run(ctx) }()
	waitFor(t, 5*time.Second, "the poison and the good message committed", func() bool {
		return reader.commitsN() == 2
	})
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dead) != 1 || dead[0] != "poison (no mappable id)" {
		t.Fatalf("dead letters = %v, want the poison only", dead)
	}
}

// TestConsumeLoop_NeverDeadLettersACancelledMessage: the fleet escape-18b
// guard — a handler that fails BECAUSE the process is stopping is never
// dead-lettered; the offset stays uncommitted and the message is
// redelivered on restart.
func TestConsumeLoop_NeverDeadLettersACancelledMessage(t *testing.T) {
	reader := &fakeReader{msgs: []kafkago.Message{msg(0, "good")}, fetchErr: context.Canceled}
	dead := 0
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	loop := consumeLoop{
		reader: reader, logger: slog.Default(), name: "test",
		sleep: func(context.Context, time.Duration) error { return nil },
		handle: func(context.Context, kafkago.Message) error {
			cancel() // the shutdown lands mid-handle
			return context.Canceled
		},
		deadLetter: func(context.Context, kafkago.Message, error) error {
			mu.Lock()
			dead++
			mu.Unlock()
			return nil
		},
	}
	if err := loop.run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want the cancellation", err)
	}
	if reader.commitsN() != 0 {
		t.Fatalf("%d commits of a message interrupted by shutdown", reader.commitsN())
	}
	mu.Lock()
	defer mu.Unlock()
	if dead != 0 {
		t.Fatal("a message interrupted by shutdown was dead-lettered")
	}
}

// TestConsumeLoop_CommitFailureIsRetried: the effect is durable (the
// processed-events claim), so a failed commit is retried until it succeeds.
func TestConsumeLoop_CommitFailureIsRetried(t *testing.T) {
	reader := &fakeReader{
		msgs: []kafkago.Message{msg(0, "m0")}, commitErr: errors.New("commit down"), commitErrN: 2,
		fetchErr: context.Canceled, // ends the loop once the stream is drained
	}
	ctx, cancel := context.WithCancel(context.Background())
	loop := consumeLoop{
		reader: reader, logger: slog.Default(), name: "test",
		handle: func(context.Context, kafkago.Message) error { return nil },
		sleep:  func(context.Context, time.Duration) error { return nil },
	}
	done := make(chan error, 1)
	go func() { done <- loop.run(ctx) }()
	// Two failed commits then the third lands: 3 commit calls in total.
	waitFor(t, 5*time.Second, "the retried commit", func() bool { return reader.commitsN() == 3 })
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
}
