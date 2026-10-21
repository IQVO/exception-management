package kafka

import (
	"context"
	"errors"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

type fakeMetadata struct {
	resp *kafkago.MetadataResponse
	err  error
	req  *kafkago.MetadataRequest
}

func (f *fakeMetadata) Metadata(_ context.Context, req *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error) {
	f.req = req
	return f.resp, f.err
}

func TestBrokerCheckAsksAnEmptyNonNilTopicList(t *testing.T) {
	fake := &fakeMetadata{resp: &kafkago.MetadataResponse{Brokers: []kafkago.Broker{{Host: "b1", Port: 9092}}}}
	if err := NewBrokerCheckWithClient(fake).Check(context.Background()); err != nil {
		t.Fatalf("check = %v", err)
	}
	if fake.req == nil || fake.req.Topics == nil || len(fake.req.Topics) != 0 {
		t.Fatalf("metadata request topics = %#v, want an EMPTY non-nil list (never the null list, never topics)", fake.req)
	}
}

func TestBrokerCheckFailsWhenUnreachable(t *testing.T) {
	if err := NewBrokerCheckWithClient(&fakeMetadata{err: errors.New("dial tcp: refused")}).Check(context.Background()); err == nil {
		t.Fatal("an unreachable broker must fail the check")
	}
	if err := NewBrokerCheckWithClient(&fakeMetadata{resp: &kafkago.MetadataResponse{}}).Check(context.Background()); err == nil {
		t.Fatal("a metadata answer with no broker must fail the check")
	}
}
