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

// TestCompressionCodecsRoundTrip pins gh #13: every Apache-defined
// codec (gzip, snappy, lz4, zstd) round-trips through the broker
// byte-for-byte. Skafka stores the compressed RecordBatch verbatim
// — it never decompresses — so this test is really verifying that
// the wire-protocol bytes-are-opaque guarantee from gh #79 holds
// for every CompressionType the Java/franz-go client can request.
//
// One subtest per codec so a regression points at the specific
// codec (e.g. a zstd-only failure tells you the issue is in the
// length-prefix handling of the compressed payload, not the
// compression-flag bits).
func TestCompressionCodecsRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		codec kgo.CompressionCodec
	}{
		{"gzip", "test-topic-compression-gzip", kgo.GzipCompression()},
		{"snappy", "test-topic-compression-snappy-roundtrip", kgo.SnappyCompression()},
		{"lz4", "test-topic-compression-lz4", kgo.Lz4Compression()},
		{"zstd", "test-topic-compression-zstd", kgo.ZstdCompression()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			roundTripOneCodec(t, tc.topic, tc.codec)
		})
	}
}

func roundTripOneCodec(t *testing.T, topic string, codec kgo.CompressionCodec) {
	t.Helper()
	const N = 50
	// Pre-create the topic on the shared broker so the producer
	// doesn't have to wait for auto-create (default tests don't
	// enable auto-create).
	addCompressionTopic(t, topic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ProducerBatchCompression(codec),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer producer.Close()

	records := make([]*kgo.Record, N)
	for i := 0; i < N; i++ {
		// Wide repetitive payload so every codec compresses
		// meaningfully (otherwise lz4/zstd may decline to compress
		// short uncompressible payloads).
		records[i] = &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("%s-key-%04d", topic, i)),
			Value:     []byte(fmt.Sprintf("%s-value-%04d-%s", topic, i, strings.Repeat("x", 96))),
		}
	}
	results := producer.ProduceSync(ctx, records...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("ProduceSync[%d]: %v", i, res.Err)
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("NewClient consumer: %v", err)
	}
	defer consumer.Close()

	received := 0
	deadline := time.Now().Add(8 * time.Second)
	for received < N && time.Now().Before(deadline) {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("PollFetches errors: %v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			wantValue := fmt.Sprintf("%s-value-%04d-%s", topic, received, strings.Repeat("x", 96))
			if string(r.Value) != wantValue {
				t.Errorf("record %d: value mismatch", received)
			}
			received++
		})
	}
	if received != N {
		t.Fatalf("consumed %d records, want %d (broker failed to round-trip %s-compressed bytes)",
			received, N, topic)
	}
}

// addCompressionTopic creates the per-subtest topic via the
// CreateTopics admin RPC; ignoring "already exists" makes the test
// rerunnable without flushing state. Mirrors the pattern in
// admin_test.go rather than reaching into broker.AddTopic so the
// test stays independent of internal symbols.
func addCompressionTopic(t *testing.T, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatalf("admin NewClient: %v", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = topic
	ct.NumPartitions = 1
	ct.ReplicationFactor = 1
	req.Topics = append(req.Topics, ct)
	req.TimeoutMillis = 5000

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Logf("CreateTopics %s: %v (continuing — may already exist)", topic, err)
		return
	}
	if len(resp.Topics) > 0 && resp.Topics[0].ErrorCode != 0 && resp.Topics[0].ErrorCode != 36 {
		// 36 = TOPIC_ALREADY_EXISTS — fine, the topic survives from a
		// prior subtest run.
		t.Logf("CreateTopics %s errorCode=%d (continuing)", topic, resp.Topics[0].ErrorCode)
	}
}
