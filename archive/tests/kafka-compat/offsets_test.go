package kafkacompat

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestOffsetReset_EarliestLatest pins gh #20: a fresh consumer
// group's first OffsetFetch returns "no committed offset", and the
// consumer's local auto.offset.reset setting then decides where to
// start. Skafka's role is to honestly report the "no offset" state;
// the position selection happens client-side. We assert that by
// exercising both ResetOffset.AtStart and ResetOffset.AtEnd against
// the same pre-populated topic and confirming the consumed-record
// count matches the policy.
func TestOffsetReset_EarliestLatest(t *testing.T) {
	const (
		topic = "test-topic-offset-reset"
		N     = 20
	)
	// Pre-create and pre-populate.
	createTestTopic(t, topic, 1)
	populate(t, topic, N)

	t.Run("earliest_reads_all", func(t *testing.T) {
		count := consumeWithReset(t, topic, "grp-earliest-"+t.Name(), kgo.NewOffset().AtStart(), 5*time.Second)
		if count != N {
			t.Errorf("earliest got %d records, want %d", count, N)
		}
	})
	t.Run("latest_reads_zero", func(t *testing.T) {
		// "latest" against a new group on a topic with no in-flight
		// produces means: position at HWM, nothing to consume.
		count := consumeWithReset(t, topic, "grp-latest-"+t.Name(), kgo.NewOffset().AtEnd(), 2*time.Second)
		if count != 0 {
			t.Errorf("latest got %d records, want 0", count)
		}
	})
}

// TestOffsetCommit_MetadataRoundTrip pins gh #21: when an
// OffsetCommit carries a metadata string (Apache's per-offset
// arbitrary string slot — used by Connect, Streams, app-level
// state markers), the broker MUST persist and return it byte-for-
// byte through OffsetFetch. A regression that dropped the field
// would silently break every Connect connector.
func TestOffsetCommit_MetadataRoundTrip(t *testing.T) {
	const (
		topic = "test-topic-offset-metadata"
		group = "grp-metadata-test"
	)
	createTestTopic(t, topic, 1)

	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	// Commit offset 7 with a metadata string under group/topic/partition 0.
	const wantMeta = "checkpoint:v=42:state=processed"
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = group
	commit.Generation = -1
	commit.MemberID = ""
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = topic
	cp := kmsg.NewOffsetCommitRequestTopicPartition()
	cp.Partition = 0
	cp.Offset = 7
	metaCopy := wantMeta
	cp.Metadata = &metaCopy
	ct.Partitions = append(ct.Partitions, cp)
	commit.Topics = append(commit.Topics, ct)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cresp, err := commit.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("OffsetCommit: %v", err)
	}
	if len(cresp.Topics) != 1 || len(cresp.Topics[0].Partitions) != 1 {
		t.Fatalf("OffsetCommit response shape: %+v", cresp)
	}
	if ec := cresp.Topics[0].Partitions[0].ErrorCode; ec != 0 {
		t.Fatalf("OffsetCommit ErrorCode=%d", ec)
	}

	// OffsetFetch back and assert metadata round-tripped.
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Group = group
	ft := kmsg.NewOffsetFetchRequestTopic()
	ft.Topic = topic
	ft.Partitions = []int32{0}
	fetch.Topics = []kmsg.OffsetFetchRequestTopic{ft}
	fresp, err := fetch.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("OffsetFetch: %v", err)
	}
	// v1+ uses the flat Topics path; v8+ moves it under Groups.
	// franz-go pivots automatically based on what the broker supports.
	var gotMeta string
	var gotOff int64 = -1
	switch {
	case len(fresp.Groups) > 0:
		if len(fresp.Groups[0].Topics) == 0 || len(fresp.Groups[0].Topics[0].Partitions) == 0 {
			t.Fatalf("OffsetFetch v8 response missing topic/partition: %+v", fresp)
		}
		p := fresp.Groups[0].Topics[0].Partitions[0]
		gotOff = p.Offset
		if p.Metadata != nil {
			gotMeta = *p.Metadata
		}
	default:
		if len(fresp.Topics) == 0 || len(fresp.Topics[0].Partitions) == 0 {
			t.Fatalf("OffsetFetch response missing topic/partition: %+v", fresp)
		}
		p := fresp.Topics[0].Partitions[0]
		gotOff = p.Offset
		if p.Metadata != nil {
			gotMeta = *p.Metadata
		}
	}

	if gotOff != 7 {
		t.Errorf("offset=%d, want 7", gotOff)
	}
	if gotMeta != wantMeta {
		t.Errorf("metadata=%q, want %q (regression in OffsetCommit/Fetch round-trip)", gotMeta, wantMeta)
	}
}

// --- helpers ---

// createTestTopic creates the named topic via CreateTopics; ignored
// if it already exists.
func createTestTopic(t *testing.T, topic string, partitions int32) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = topic
	ct.NumPartitions = partitions
	ct.ReplicationFactor = 1
	req.Topics = append(req.Topics, ct)
	req.TimeoutMillis = 5000

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Logf("CreateTopics: %v (continuing)", err)
		return
	}
	if len(resp.Topics) > 0 && resp.Topics[0].ErrorCode != 0 && resp.Topics[0].ErrorCode != 36 {
		t.Logf("CreateTopics errorCode=%d", resp.Topics[0].ErrorCode)
	}
}

// populate produces N small records to (topic, partition 0).
func populate(t *testing.T, topic string, n int) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer cl.Close()

	recs := make([]*kgo.Record, n)
	for i := 0; i < n; i++ {
		recs[i] = &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("v-%d-%s", i, strings.Repeat(".", 8))),
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, recs...)
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("populate[%d]: %v", i, r.Err)
		}
	}
}

// consumeWithReset starts a fresh consumer group with the given
// auto.offset.reset policy (encoded as a franz-go kgo.Offset) and
// reports how many records arrived within deadline.
func consumeWithReset(t *testing.T, topic, group string, reset kgo.Offset, deadline time.Duration) int {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(reset),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	count := 0
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// Context-deadline errors are expected when reading from
			// "latest" with nothing to consume; surface only the real
			// failures.
			for _, e := range errs {
				if e.Err == context.DeadlineExceeded {
					return count
				}
				t.Fatalf("PollFetches err: %v", e.Err)
			}
		}
		fetches.EachRecord(func(_ *kgo.Record) { count++ })
		if ctx.Err() != nil {
			return count
		}
	}
}
