// Package kafka holds this service's INBOUND Kafka consumers: the six fact
// consumers of .claude/rules/integration-events.md (fulfillment, work
// planning, network fulfillment, inventory, returns — the exception-class
// facts — and order correlation for auto-resolve). Every consumer decodes
// CloudEvents 1.0 only, via internal/adapters/kafka/cloudevents, and
// dispatches on the FULL `type` string. The consumers only decode and map:
// every effect is a use case of internal/application/usecases.
//
// Delivery guarantee: AT-LEAST-ONCE with an ATOMIC effect. Each message is
// handled inside ONE ports.UnitOfWork (the processed_events claim of the
// CloudEvents id and the effect commit or roll back together, see
// usecases.ConsumeFact / ConsumeCorrelation) and its offset is committed
// only AFTER handling returned nil (FetchMessage + CommitMessages).
//
// Deterministic problems — a message that is not a valid CloudEvent, a type
// this consumer has no handler for, a payload of a handled type that can
// never be applied (unmappable/blank natural id, no declared policy) — are
// logged at WARN and committed past: they can never succeed, so retrying
// would wedge the partition. A TRANSIENT failure (Postgres down, a failed
// commit) is retried on the SAME message with capped exponential backoff,
// forever: this service has no inbound dead-letter queue because every
// effect is idempotent under the claimed event id — a redelivery after a
// crash is always safe, and a dropped message would be a silently missed
// exception. A topic that does not exist yet (returns-processing's topic is
// created by its producer, not by this service) is kafka-go's internal
// retry, surfaced through the reader's ErrorLogger: retry logs, never
// unreadiness, never a crash.
package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Reader is the subset of *kafkago.Reader a consumer needs, so unit tests
// never need a live broker. FetchMessage/CommitMessages (not ReadMessage)
// because ReadMessage with a GroupID auto-commits the offset the moment it
// returns, i.e. before the message has been handled: a failed handling would
// then never be redelivered. Offsets are committed explicitly, after success
// only.
type Reader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// readerConfig builds the kafka-go reader configuration shared by the
// consumers. GroupID always comes from the caller (an environment variable
// at the composition root, never a literal here), satisfying
// internal/architecture's TestKafkaConsumerGroupNeverHardcodedInline.
//
// CommitInterval is deliberately left UNSET: with a GroupID kafka-go then
// commits synchronously inside CommitMessages, which the run loop calls only
// after a message was handled successfully. Setting it would batch the commit
// and reintroduce a window where an offset moves without the work being
// durable. A group without a committed offset starts at the first offset, so
// a fresh deployment replays the topic and the caseload converges.
//
// kafkago.NewReader does not dial the broker: the connection is opened by
// the first FetchMessage inside the consumer's own goroutine, so a missing
// topic or an unreachable broker never blocks process startup.
func readerConfig(brokers []string, topic, groupID string) kafkago.ReaderConfig {
	return kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: groupID}
}

func defaultLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}

// readerErrorLogger adapts logger to kafka-go's ErrorLogger: without it
// kafka-go silently retries a failing group join, a missing topic (the
// cluster broker auto-creates topics on first write; a consumed topic exists
// once its producer ran) or an unreachable broker — surface every such
// retry as an ERROR log line so it is visible.
func readerErrorLogger(logger *slog.Logger, consumer, topic string) kafkago.LoggerFunc {
	return func(format string, args ...any) {
		logger.Error("kafka reader error; retrying", "consumer", consumer, "topic", topic, "detail", fmt.Sprintf(format, args...))
	}
}

// RetryPolicy is the capped exponential backoff the run loop applies while
// the SAME message keeps failing with a transient error. Zero fields fall
// back to DefaultRetryInitial / DefaultRetryMax.
type RetryPolicy struct {
	Initial time.Duration
	Max     time.Duration
}

// Default backoff: 200ms, 400ms, 800ms ... capped at 5s.
const (
	DefaultRetryInitial = 200 * time.Millisecond
	DefaultRetryMax     = 5 * time.Second
)

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.Initial <= 0 {
		p.Initial = DefaultRetryInitial
	}
	if p.Max <= 0 {
		p.Max = DefaultRetryMax
	}
	return p
}

// next doubles d, capped at p.Max.
func (p RetryPolicy) next(d time.Duration) time.Duration {
	d *= 2
	if d > p.Max {
		return p.Max
	}
	return d
}

// sleepFunc waits d or until ctx is done (returning ctx.Err()). A field so
// tests can record the backoff sequence without really sleeping.
type sleepFunc func(ctx context.Context, d time.Duration) error

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// consumeLoop is the at-least-once run loop shared by the consumers.
type consumeLoop struct {
	reader Reader
	// handle applies one message; non-nil means transient failure => retry.
	handle func(ctx context.Context, msg kafkago.Message) error
	logger *slog.Logger
	name   string
	retry  RetryPolicy
	sleep  sleepFunc
}

// run fetches one message at a time and does not fetch the next until the
// current one was handled successfully AND its offset committed. It returns
// only when ctx is cancelled (shutdown drain) or FetchMessage itself fails.
func (l *consumeLoop) run(ctx context.Context) error {
	policy := l.retry.withDefaults()
	sleep := l.sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	for {
		msg, err := l.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := l.retryUntilOK(ctx, policy, sleep, "handling", msg, func() error {
			return l.handle(ctx, msg)
		}); err != nil {
			return err
		}
		// Commit ONLY now. A commit that fails is retried too: the work is
		// already durable (the processed-events claim), and if the process
		// dies first the redelivery is skipped by that same claim.
		if err := l.retryUntilOK(ctx, policy, sleep, "offset commit", msg, func() error {
			return l.reader.CommitMessages(ctx, msg)
		}); err != nil {
			return err
		}
	}
}

// retryUntilOK runs op until it returns nil, sleeping with capped
// exponential backoff between attempts. It gives up (returning ctx.Err())
// only when ctx is cancelled — a message interrupted by shutdown is
// redelivered on restart, never dropped. It NEVER skips: a transiently
// unhandleable message blocks its partition (and is logged at ERROR every
// attempt) rather than being silently lost.
func (l *consumeLoop) retryUntilOK(ctx context.Context, p RetryPolicy, sleep sleepFunc, what string, msg kafkago.Message, op func() error) error {
	delay := p.Initial
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		l.logger.ErrorContext(ctx, l.name+" "+what+" failed; retrying the same message",
			"error", err, "partition", msg.Partition, "offset", msg.Offset,
			"attempt", attempt, "retry_in", delay)
		if err := sleep(ctx, delay); err != nil {
			return err
		}
		delay = p.next(delay)
	}
}
