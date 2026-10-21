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
)

// errSkip marks a DETERMINISTIC problem: a message of a handled type that
// can never be applied (a payload that does not decode, a blank natural id,
// an undeclared policy). The run loop logs it at WARN and commits past it —
// it can never succeed, so retrying would wedge the partition. Everything
// else (Postgres down, a failed commit) is transient and IS retried.
var errSkip = errors.New("kafka: event can never be applied; skipping")

// skipf builds a deterministic-skip error wrapping cause.
func skipf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errSkip, fmt.Sprintf(format, args...))
}

// HandlerFunc applies one decoded CloudEvent. nil means done (or
// deterministic and skippable via errSkip); any other non-nil error is
// transient and retried.
type HandlerFunc func(ctx context.Context, e ce.Event) error

// Consumer is one fact consumer: it reads Topic under a stable consumer
// group and dispatches each message on the FULL CloudEvents `type` to its
// handler. Types it has no handler for are ignored (WARN at debug level:
// another consumer may own them).
type Consumer struct {
	Name     string
	Topic    string
	Reader   Reader
	Handlers map[string]HandlerFunc
	Logger   *slog.Logger
	Retry    RetryPolicy

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// newConsumer builds a Consumer reading topic from brokers under groupID
// (from an environment variable at the composition root, never a literal).
// Constructing the reader does not dial.
func newConsumer(name string, brokers []string, topic, groupID string, handlers map[string]HandlerFunc, logger *slog.Logger) *Consumer {
	cfg := readerConfig(brokers, topic, groupID)
	cfg.ErrorLogger = readerErrorLogger(defaultLogger(logger), name, topic)
	return &Consumer{
		Name:     name,
		Topic:    topic,
		Reader:   kafkago.NewReader(cfg),
		Handlers: handlers,
		Logger:   defaultLogger(logger),
	}
}

// Run consumes until ctx is cancelled (shutdown drain) or the reader fails.
// A message's offset is committed only after handling returned nil; a
// transient failure retries the SAME message with capped exponential
// backoff, forever (see the package comment for why there is no DLQ).
func (c *Consumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   c.Name,
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// Close releases the reader.
func (c *Consumer) Close() error {
	return c.Reader.Close()
}

// HandleMessage decodes one CloudEvents 1.0 message and dispatches it. It
// returns nil for everything deterministic — a message that is not a valid
// CloudEvent or an unknown type silently (logged at WARN for the invalid
// one), a handled type's deterministic refusal at WARN via errSkip — and a
// non-nil error ONLY for a transient failure (or errSkip, which the run
// loop's caller treats as commit-past).
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
	if err := handle(ctx, e); err != nil {
		if errors.Is(err, errSkip) {
			logger.WarnContext(ctx, "skipping an event that can never be applied", "consumer", c.Name, "type", e.Type(), "event_id", e.ID(), "error", err)
			return nil
		}
		return err
	}
	return nil
}

// decodeData decodes e's JSON `data` into a T; a failure is deterministic.
func decodeData[T any](e ce.Event) (T, error) {
	var v T
	if err := e.DataAs(&v); err != nil {
		return v, skipf("payload of %s does not decode: %v", e.Type(), err)
	}
	return v, nil
}

// fact maps a decoded payload and its envelope to the use case's Fact. A
// blank natural id is deterministic (skipf).
func fact(e ce.Event, subject, site string) (usecases.Fact, error) {
	if subject == "" {
		return usecases.Fact{}, skipf("payload of %s carries no natural id", e.Type())
	}
	return usecases.Fact{
		SourceType:    e.Type(),
		SourceSubject: subject,
		SiteID:        site,
		SourceEventID: e.ID(),
		Payload:       e.Data(),
	}, nil
}

// apply runs a consumer use case and logs its outcome.
func apply(ctx context.Context, logger *slog.Logger, e ce.Event, run func() (usecases.Outcome, error)) error {
	outcome, err := run()
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "fact consumed", "type", e.Type(), "event_id", e.ID(), "subject", e.Subject(), "outcome", string(outcome))
	return nil
}
