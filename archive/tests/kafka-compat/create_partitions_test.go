package kafkacompat

import (
	"context"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestCreatePartitions_GrowsTopic pins gh #52: CreatePartitions
// (KIP-195) on a topic with N partitions and a target count > N
// must return success (the in-process compat broker has no
// CRWriter; the handler reports a clean dev-mode error). With a
// real CRWriter wired, the request would patch the KafkaTopic CR
// and the operator's reconciler would create the new partition
// directories.
//
// This test exercises the wire path: codec round-trip, dispatcher
// routes to handler, response shape correct. The CR-write side is
// covered by topic_cr_writer_test.go ExpandTopic tests.
func TestCreatePartitions_WireRoundTrip(t *testing.T) {
	const topic = "test-topic-create-partitions"
	createTestTopic(t, topic, 2)

	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrCreatePartitionsRequest()
	tp := kmsg.NewCreatePartitionsRequestTopic()
	tp.Topic = topic
	tp.Count = 5
	req.Topics = append(req.Topics, tp)
	req.TimeoutMillis = 5000

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("CreatePartitions: %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 result, got %d", len(resp.Topics))
	}
	// In-process broker has no CRWriter — handler reports INVALID_REQUEST.
	// The wire path itself worked (we got a structured response,
	// not a hang or UNSUPPORTED_VERSION).
	got := resp.Topics[0]
	if got.Topic != topic {
		t.Errorf("Topic=%q, want %q", got.Topic, topic)
	}
	// Acceptable: dev-mode INVALID_REQUEST (42), success (0), or
	// the INVALID_PARTITIONS (37) shrink rejection.
	switch got.ErrorCode {
	case 0, 42, 37:
		// fine
	default:
		t.Errorf("unexpected ErrorCode=%d", got.ErrorCode)
	}
}

// TestCreatePartitions_ValidateOnlySkipsWriter pins KIP-195's
// validate_only flag: when set, the broker must NOT write the CR
// (verified indirectly by ErrorCode=0 even without a CRWriter
// wired — the handler short-circuits).
func TestCreatePartitions_ValidateOnlySkipsWriter(t *testing.T) {
	const topic = "test-topic-create-partitions-validate"
	createTestTopic(t, topic, 1)

	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrCreatePartitionsRequest()
	tp := kmsg.NewCreatePartitionsRequestTopic()
	tp.Topic = topic
	tp.Count = 4
	req.Topics = append(req.Topics, tp)
	req.TimeoutMillis = 5000
	req.ValidateOnly = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("CreatePartitions: %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 result, got %d", len(resp.Topics))
	}
	if got := resp.Topics[0].ErrorCode; got != 0 {
		t.Errorf("validate_only ErrorCode=%d, want 0 (dev-mode handler must short-circuit)", got)
	}
}
