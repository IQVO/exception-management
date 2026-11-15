package kafka_test

// Shared support for the event consumer contract tests (E1-E4), which
// validate this repo's inbound Kafka edges against PINNED copies of the
// producing side's AsyncAPI spec (contracts/pinned.json; the
// pinned-contracts CI job fails when a copy drifts from the producer's
// develop). The pinned copies are byte-for-byte producer files: never
// edit them — refresh = copy the producer's file over the pin.
//
// exception-management is the fleet's widest EVENT consumer: ten producer
// topics feed its eleven fact consumers (the held-stock one reads the
// inventory topic a second time, ADR 0012) plus the order-correlation
// consumer, and its own projector (cmd/exceptions-projector) consumes the
// SELF edge warehouse.exception-management.analytics — pinned here as the
// repo's own apis/asyncapi.yaml.
//
// Spec shapes seen across the ten producers (all handled below):
//   - channels keyed by topic, messages under subscribe:/publish: ops
//     ($ref or oneOf) — the majority;
//   - yard-management: channels keyed by NAME with an `address` field,
//     messages a channel-level map;
//   - envelope `type`/`dataschema` pinned either as per-message const /
//     single-value enum overlays, or only typed in a shared envelope
//     schema with the concrete value in each message's examples
//     (inbound-receiving, returns-processing, the self pin).
import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	pinnedDockSchedulingSpec     = "testdata/dock-scheduling.asyncapi.yaml"
	pinnedFulfillmentSpec        = "testdata/fulfillment-execution.asyncapi.yaml"
	pinnedInboundReceivingSpec   = "testdata/inbound-receiving.asyncapi.yaml"
	pinnedInventoryStorageSpec   = "testdata/inventory-storage.asyncapi.yaml"
	pinnedNetworkFulfillmentSpec = "testdata/network-fulfillment.asyncapi.yaml"
	pinnedOrderManagementSpec    = "testdata/order-management.asyncapi.yaml"
	pinnedOutboundShippingSpec   = "testdata/outbound-shipping.asyncapi.yaml"
	pinnedReturnsSpec            = "testdata/returns-processing.asyncapi.yaml"
	pinnedWorkPlanningSpec       = "testdata/wes-work-planning.asyncapi.yaml"
	pinnedYardManagementSpec     = "testdata/yard-management.asyncapi.yaml"
	pinnedOwnAnalyticsSpec       = "testdata/exception-management.asyncapi.yaml"
)

// pinnedAsyncAPI is the minimal slice of AsyncAPI 2.6 the contract tests
// read: channels (by key AND by address), and the components/messages +
// components/schemas the channel message $refs point at.
type pinnedAsyncAPI struct {
	AsyncAPI           string                   `yaml:"asyncapi"`
	DefaultContentType string                   `yaml:"defaultContentType"`
	Channels           map[string]pinnedChannel `yaml:"channels"`
	Components         pinnedComponents         `yaml:"components"`
}

type pinnedChannel struct {
	Address   string           `yaml:"address"`
	Subscribe *pinnedOperation `yaml:"subscribe"`
	Publish   *pinnedOperation `yaml:"publish"`
	// Messages is yard-management's shape: a channel-level name -> $ref
	// map instead of operation-level message lists.
	Messages map[string]pinnedMessageRef `yaml:"messages"`
}

type pinnedOperation struct {
	OperationID string            `yaml:"operationId"`
	Message     *pinnedMessageRef `yaml:"message"`
}

// pinnedMessageRef is either a single $ref or a oneOf list of $refs.
type pinnedMessageRef struct {
	Ref   string             `yaml:"$ref"`
	OneOf []pinnedMessageRef `yaml:"oneOf"`
}

type pinnedComponents struct {
	Messages map[string]pinnedMessage `yaml:"messages"`
	Schemas  map[string]any           `yaml:"schemas"`
}

type pinnedMessage struct {
	Name        string          `yaml:"name"`
	ContentType string          `yaml:"contentType"`
	Payload     any             `yaml:"payload"`
	Examples    []pinnedExample `yaml:"examples"`
}

// pinnedExample is a message example: a full wire payload (a structured
// CloudEvents JSON document, as YAML) plus its Kafka headers.
type pinnedExample struct {
	Name    string            `yaml:"name"`
	Headers map[string]string `yaml:"headers"`
	Payload map[string]any    `yaml:"payload"`
}

// loadPinnedAsyncAPI reads and parses one pinned spec copy.
func loadPinnedAsyncAPI(t *testing.T, path string) *pinnedAsyncAPI {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pinned spec %s (copy it from the producer, never hand-write it): %v", path, err)
	}
	var doc pinnedAsyncAPI
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec %s: %v", path, err)
	}
	return &doc
}

// channelMessageRefs returns the components/messages names a topic's
// channel documents, wherever this producer's spec keeps them: a
// channel-level messages map (yard-management), or the subscribe/publish
// operations' message lists (the fleet's channel-perspective convention: a
// producer's OWN topics sit under 'subscribe:', consumed ones under
// 'publish:').
func (d *pinnedAsyncAPI) channelMessageRefs(t *testing.T, topic string) []string {
	t.Helper()
	var ch *pinnedChannel
	for key := range d.Channels {
		c := d.Channels[key]
		if key == topic || (c.Address != "" && c.Address == topic) {
			cc := c
			ch = &cc
			break
		}
	}
	if ch == nil {
		t.Fatalf("pinned spec has no channel for topic %q", topic)
	}
	names := make([]string, 0, len(ch.Messages))
	for _, ref := range ch.Messages {
		names = append(names, refName(t, ref.Ref))
	}
	for _, op := range []*pinnedOperation{ch.Subscribe, ch.Publish} {
		if op == nil || op.Message == nil {
			continue
		}
		if op.Message.Ref != "" {
			names = append(names, refName(t, op.Message.Ref))
		}
		for _, one := range op.Message.OneOf {
			names = append(names, refName(t, one.Ref))
		}
	}
	if len(names) == 0 {
		t.Fatalf("pinned spec documents topic %q without any message", topic)
	}
	return names
}

// message resolves one components/messages entry, failing on a dangling
// $ref (a spec whose channel points at a message it does not define).
func (d *pinnedAsyncAPI) message(t *testing.T, name string) pinnedMessage {
	t.Helper()
	msg, ok := d.Components.Messages[name]
	if !ok {
		t.Fatalf("pinned spec references undefined components/messages/%s", name)
	}
	return msg
}

// messageForType returns the message documenting the CloudEvents type this
// repo routes on, or ok=false when the topic does not document it.
func (d *pinnedAsyncAPI) messageForType(t *testing.T, topic, typ string) (pinnedMessage, bool) {
	t.Helper()
	for _, name := range d.channelMessageRefs(t, topic) {
		msg := d.message(t, name) // fails the test on a dangling $ref
		if c := d.payloadConsts(t, msg.Payload); c["type"] == typ {
			return msg, true
		}
		for _, ex := range msg.Examples {
			if s, _ := ex.Payload["type"].(string); s == typ {
				return msg, true
			}
		}
	}
	return pinnedMessage{}, false
}

// documentedTypes maps the CloudEvents `type` each message on a topic
// documents to its message name: from a payload const / single-value enum
// overlay when the spec pins one, else from the message's own example
// payload (the three specs whose shared envelope only types `type` as
// string document the concrete value exactly there).
func (d *pinnedAsyncAPI) documentedTypes(t *testing.T, topic string) map[string]string {
	t.Helper()
	documented := map[string]string{}
	for _, name := range d.channelMessageRefs(t, topic) {
		msg := d.message(t, name)
		if c := d.payloadConsts(t, msg.Payload); c["type"] != "" {
			documented[c["type"]] = name
			continue
		}
		for _, ex := range msg.Examples {
			if s, _ := ex.Payload["type"].(string); s != "" {
				documented[s] = name
			}
		}
	}
	if len(documented) == 0 {
		t.Fatalf("pinned spec documents no message types on %q", topic)
	}
	return documented
}

// payloadConsts collects the const (or single-value enum) a message
// payload declares for CloudEvents envelope attributes, wherever the spec
// keeps them — directly under properties, in an allOf overlay on the
// shared envelope schema, or behind a payload-level $ref into
// components/schemas.

// refName extracts the last path segment of a local $ref
// ("#/components/messages/TaskCompleted" -> "TaskCompleted").
func refName(t *testing.T, ref string) string {
	t.Helper()
	i := strings.LastIndex(ref, "/")
	if i < 0 || i == len(ref)-1 {
		t.Fatalf("malformed local $ref %q", ref)
	}
	return ref[i+1:]
}

// goldenJSON marshals a pinned example's YAML-decoded payload into the
// JSON bytes a Kafka consumer actually receives (structured-mode
