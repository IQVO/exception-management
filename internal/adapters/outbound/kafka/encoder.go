// Package kafka is the outbound Kafka adapter. writer.go holds the one place
// a kafka-go Writer is constructed; this file holds the CloudEvents encoders:
// the mapping of a persisted outbox fact (ADR 0004's catalogue) to the exact
// wire message — full `type` string, `source` /warehouse/exception-management,
// `subject` = aggregate id (the Kafka key), `dataschema`
// urn:warehouse:exception-management:events:<Event>:v1, `time` = the fact's
// occurred-at in UTC, and the structured-mode JSON value plus the
// content-type header. Envelopes are built ONLY through
// internal/adapters/kafka/cloudevents.
package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/claudioed/exception-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/exception-management/internal/application/outbox"
)

// The topics this service publishes to. The outbox relay sets Message.Topic
// per message; the Kafka writer carries no Topic (the topic-not-set-twice
// rule).
const (
	// TopicEvents is the integration topic every published type goes to.
	TopicEvents = "warehouse.exception-management.events"
	// TopicAnalytics is the analytics twin of the integration topic: every
	// published type ALSO goes there, byte-identical except for the
	// dataschema stream segment (ADR 0006). Only cmd/exceptions-projector
	// consumes it; it is not an integration contract.
	TopicAnalytics = "warehouse.exception-management.analytics"
	// DLQSuffix extends a topic name into its dead-letter topic.
	DLQSuffix = ".dlq"
	// DeadLetterTopic is where the relay parks rows it can never publish
	// (the fleet pattern: the integration topic + DLQSuffix).
	DeadLetterTopic = TopicEvents + DLQSuffix
)

// schemaVersion is the dataschema version of every payload below
// (urn:warehouse:exception-management:events:<EventName>:v1).
const schemaVersion = 1

// published reports whether entity is known to raise event. The catalogue is
// ADR 0004's: six exceptioncase facts and the exceptionpolicy declaration.
// An unknown combination is NOT an error — it yields no message (the relay
// dead-letters such a row instead of retrying it forever).
func published(entity, event string) bool {
	switch entity {
	case "exceptioncase":
		switch event {
		case "ExceptionOpened", "ExceptionTriaged", "ExceptionAssigned",
			"ExceptionEscalated", "ExceptionResolved", "ExceptionAutoResolved":
			return true
		}
	case "exceptionpolicy":
		return event == "ExceptionPolicyDeclared"
	}
	return false
}

// decodeData parses raw JSON into a generic value the CloudEvents helper can
// re-marshal into the envelope's `data` object. json.Number keeps every
// number exactly as written (no float64 rounding of int64 fields).
func decodeData(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return struct{}{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("kafka: decode %s data: %w", "payload", err)
	}
	return v, nil
}

// Encode maps one persisted outbox fact to its CloudEvents messages: the
// integration twin on TopicEvents and the analytics twin on TopicAnalytics
// (ADR 0006) — one CloudEvents id, byte-identical payloads, dataschemas
// `urn:warehouse:exception-management:{events,analytics}:<Event>:v1`. A fact
// the service does not publish yields NO message and NO error — the caller
// (the relay) dead-letters the row. A known fact that cannot be built into a
// valid envelope (empty aggregate id, invalid data JSON, SDK validation) is
// an error: deterministic poison, never retried by re-encoding.
//
// The CloudEvents id is the fact's EventID — minted once at enqueue and
// persisted, so a relay retry republishes the same id and consumers dedupe
// (per topic: the projector's dedupe set lives in the analytical database).
func Encode(f outbox.Fact) ([]outbox.Message, error) {
	if !published(f.Entity, f.Event) {
		return nil, nil
	}
	data, err := decodeData(f.Data)
	if err != nil {
		return nil, fmt.Errorf("encode %s.%s: %w", f.Entity, f.Event, err)
	}
	out := make([]outbox.Message, 0, 2)
	for _, st := range []struct {
		stream string
		topic  string
	}{
		{cloudevents.StreamEvents, TopicEvents},
		{cloudevents.StreamAnalytics, TopicAnalytics},
	} {
		msg, err := message(f, data, st.stream, st.topic)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}

// message builds the CloudEvent of one stream: the shared envelope (id, type,
// subject, time, payload) with the stream's dataschema segment and topic.
func message(f outbox.Fact, data any, stream, topic string) (outbox.Message, error) {
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        f.EventID,
		Entity:    f.Entity,
		EventName: f.Event,
		Subject:   f.AggregateID,
		Time:      f.OccurredAt,
		Stream:    stream,
		Version:   schemaVersion,
		Data:      data,
	})
	if err != nil {
		return outbox.Message{}, fmt.Errorf("encode %s.%s: %w", f.Entity, f.Event, err)
	}
	ct := cloudevents.ContentTypeHeader()
	return outbox.Message{
		EventID:    f.EventID,
		Topic:      topic,
		EventType:  cloudevents.Type(f.Entity, f.Event),
		Subject:    f.AggregateID,
		DataSchema: cloudevents.DataSchema(stream, f.Event, schemaVersion),
		Key:        []byte(f.AggregateID),
		Value:      value,
		Headers:    []outbox.Header{{Key: ct.Key, Value: string(ct.Value)}},
	}, nil
}
