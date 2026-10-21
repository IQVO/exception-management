package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/exception-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// errSkip marks a DETERMINISTIC refusal that is NOT the message's fault (no
// policy declared for the source type): WARN and commit past — an undeclared
// source is a configuration gap, never a partition blocker, and the message
// is not dead-lettered (a later declaration makes the NEXT occurrence apply;
// parking this one in the DLQ would hide exactly that recovery).
var errSkip = errors.New("kafka: event skipped deterministically")

// skipf builds a deterministic-skip error wrapping the cause.
func skipf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errSkip, fmt.Sprintf(format, args...))
}

// HandlerFunc applies one decoded CloudEvent. nil means done (or
// deterministic and skippable); an error wrapping errDeadLetter means a
// poison message of a handled type (dead-lettered and committed past); any
// other non-nil error is transient and retried.
type HandlerFunc func(ctx context.Context, e ce.Event) error

// Consumer is one fact consumer: it reads Topic under a stable consumer
// group and dispatches each message on the FULL CloudEvents `type` to its
// handler. Types it has no handler for are ignored.
type Consumer struct {
	Name     string
	Topic    string
	Reader   Reader
	Handlers map[string]HandlerFunc
	Logger   *slog.Logger
	Retry    RetryPolicy

	// DLQ is the dead-letter writer (topic Topic+DLQSuffix): a message of a
	// handled type that can never be applied (a payload that never decodes,
	// a blank natural id) is moved there at once and committed past instead
	// of blocking the partition. Not-a-CloudEvent, unknown types and the
	// policy-undeclared skip never reach it.
	DLQ DeadLetterWriter

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// newConsumer builds a Consumer reading topic from brokers under groupID
// (from an environment variable at the composition root, never a literal).
// Constructing the reader and the dead-letter writer does not dial.
func newConsumer(name string, brokers []string, topic, groupID string, handlers map[string]HandlerFunc, logger *slog.Logger) *Consumer {
	cfg := readerConfig(brokers, topic, groupID)
	cfg.ErrorLogger = readerErrorLogger(defaultLogger(logger), name, topic)
	return &Consumer{
		Name:     name,
		Topic:    topic,
		Reader:   kafkago.NewReader(cfg),
		Handlers: handlers,
		Logger:   defaultLogger(logger),
		DLQ:      newDLQWriter(brokers, topic),
	}
}

// Run consumes until ctx is cancelled (shutdown drain) or the reader fails.
// A message's offset is committed only after handling returned nil; a
// transient failure retries the SAME message with capped exponential
// backoff, forever (the effects are idempotent under the claimed event id);
// a poison message of a handled type is dead-lettered and committed past.
func (c *Consumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   c.Name,
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	if c.DLQ != nil {
		loop.deadLetter = func(ctx context.Context, msg kafkago.Message, cause error) error {
			return c.DLQ.WriteDeadLetter(ctx, c.Topic, msg, cause)
		}
	}
	return loop.run(ctx)
}

// Close releases the reader and the dead-letter writer.
func (c *Consumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}

// HandleMessage decodes one CloudEvents 1.0 message and dispatches it:
//   - not a valid CloudEvent: WARN, commit past (never dead-lettered — the
//     type is unknowable, so there is nothing to replay from the DLQ);
//   - a type this consumer does not act on: ignored;
//   - errSkip (no policy declared): WARN, commit past;
//   - errDeadLetter (poison of a handled type): WARN, the run loop
//     dead-letters it and commits past (nil here when no DLQ is wired);
//   - anything else: transient, returned for the capped retry.
func (c *Consumer) HandleMessage(ctx context.Context, value []byte) error {
	logger := defaultLogger(c.Logger)
	e, err := cloudevents.Decode(value)
	if err != nil {
		logger.WarnContext(ctx, "skipping a message that is not a valid CloudEvent", "consumer", c.Name, "topic", c.Topic, "error", err)
		return nil
	}
	handle, ok := c.Handlers[e.Type()]
	if !ok {
		return nil
	}
	err = handle(ctx, e)
	var poison *errDeadLetter
	if errors.As(err, &poison) {
		logger.WarnContext(ctx, "an event that can never be applied; dead-lettering it",
			"consumer", c.Name, "type", e.Type(), "event_id", e.ID(), "error", deadLetterCause(err))
		if c.DLQ == nil {
			return nil
		}
		return err
	}
	if errors.Is(err, errSkip) {
		logger.WarnContext(ctx, "skipping an event its source has no policy for",
			"consumer", c.Name, "type", e.Type(), "event_id", e.ID(), "error", err)
		return nil
	}
	return err
}

// decodeData decodes e's JSON `data` into a T; a failure is poison.
func decodeData[T any](e ce.Event) (T, error) {
	var v T
	if err := e.DataAs(&v); err != nil {
		return v, &errDeadLetter{cause: skipf("payload of %s does not decode: %v", e.Type(), err)}
	}
	return v, nil
}

// fact maps a decoded payload and its envelope to the use case's Fact. A
// natural id that cannot key a case (blank, or a blank-ish site) is poison.
func fact(e ce.Event, subject, site string) (usecases.Fact, error) {
	if _, err := shared.NewDedupKey(e.Type(), subject, site); err != nil {
		return usecases.Fact{}, &errDeadLetter{cause: skipf("payload of %s carries no mappable id: %v", e.Type(), err)}
	}
	return usecases.Fact{
		SourceType:    e.Type(),
		SourceSubject: subject,
		SiteID:        site,
		SourceEventID: e.ID(),
		Payload:       e.Data(),
	}, nil
}

// apply runs a consumer use case and logs its outcome; the use case's own
// deterministic refusals (no declared policy) become the commit-past skip.
func apply(ctx context.Context, logger *slog.Logger, e ce.Event, run func() (usecases.Outcome, error)) error {
	outcome, err := run()
	if err != nil {
		if errors.Is(err, usecases.ErrPolicyUndeclared) || errors.Is(err, usecases.ErrNoEventID) {
			return skipf("%s: %v", e.Type(), err)
		}
		return err
	}
	logger.InfoContext(ctx, "fact consumed", "type", e.Type(), "event_id", e.ID(), "subject", e.Subject(), "outcome", string(outcome))
	return nil
}
