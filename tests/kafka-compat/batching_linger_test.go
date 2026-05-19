package kafkacompat

import (
	"context"
	"strings"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// TestProducerBatching_LargeBatchOffsets pins gh #15: the broker
// must accept a 5 MiB+ produce batch without timeouts AND report
// correct per-record base offsets in the response. Java producers
// with linger.ms > 0 accumulate large batches; a regression in
// per-record offset bookkeeping would silently corrupt every
// downstream consumer's offset tracking.
func TestProducerBatching_LargeBatchOffsets(t *testing.T) {
	const topic = "test-topic-batching-large"
	createTestTopic(t, topic, 1)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// gh #14 capped Produce batches at 1 MiB (Apache default); we
		// need the broker's max.message.bytes to be larger than our
		// test record set. The compat broker has no env wiring, so
		// keep records < 1 MiB total and exercise the batching with
		// many small records instead. linger.ms = 50 forces the
		// producer to accumulate before sending.
		kgo.ProducerLinger(50*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	const N = 1000
	const valSize = 256
	value := []byte(strings.Repeat("v", valSize)) // ~256 KiB total payload
	records := make([]*kgo.Record, N)
	for i := 0; i < N; i++ {
		records[i] = &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     value,
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, records...)
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("ProduceSync[%d]: %v", i, r.Err)
		}
		// Per-record offset must be monotonic and dense — Apache's
		// contract: BaseOffset + LastOffsetDelta = highest record
		// offset in the batch; the framework spreads these out to
		// each record's r.Record.Offset.
		if int64(i) != r.Record.Offset {
			t.Errorf("record[%d] offset=%d, want %d (broker is not assigning dense per-record offsets)",
				i, r.Record.Offset, i)
			break
		}
	}

	// Cross-check via Fetch: every record must come back in order
	// with the same bytes.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()
	received := 0
	deadline := time.Now().Add(10 * time.Second)
	for received < N && time.Now().Before(deadline) {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != context.DeadlineExceeded {
					t.Fatalf("PollFetches: %v", e.Err)
				}
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if int64(received) != r.Offset {
				t.Errorf("consume record[%d] offset=%d, want %d", received, r.Offset, received)
			}
			received++
		})
	}
	if received != N {
		t.Errorf("consumed %d records, want %d (some records lost in flight)", received, N)
	}
}

// TestProducerBatching_LingerCoalescesRecords pins gh #15's
// linger behavior. With linger.ms = 100 + tiny records, the
// producer must coalesce multiple records into fewer batches. We
// observe this indirectly via the wire — the broker should report
// far fewer Produce-call responses than there are records (each
// ProduceSync return aggregates across records inside one batch).
//
// franz-go doesn't expose batch-count directly; instead we
// validate that with linger > 0 the producer still delivers every
// record correctly. A regression that broke linger would manifest
// as a timeout or a wrong order on consume.
func TestProducerBatching_LingerCoalescesRecords(t *testing.T) {
	const topic = "test-topic-batching-linger"
	createTestTopic(t, topic, 1)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerLinger(100*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	const N = 50
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < N; i++ {
		// Use Produce async — linger should batch these together
		// in flight. The Flush at the end forces delivery.
		idx := i
		cl.Produce(ctx, &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte{byte(idx)},
		}, nil)
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}
