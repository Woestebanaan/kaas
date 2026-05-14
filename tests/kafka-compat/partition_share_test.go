package kafkacompat

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// TestFranzGoConsumerGroupPartitionShare is the direct E2E probe for gh #134:
// produce N records across P partitions to a single group of C consumers, and
// observe which partitions each consumer's records actually came from.
//
// Correct behavior: each consumer's fetched records carry partitions disjoint
// from every other consumer's; the union covers all P partitions.
// gh #134 symptom: every consumer's fetched records cover all P partitions
// (the bench reports every consumer reading ~the full topic).
//
// Configured here: P=16 partitions, C=4 consumers, N=160 records — same
// shape as the live kafka-consumer-perf-test bench that surfaced gh #134.
// Each consumer should land on 4 partitions and read ~40 records.
func TestFranzGoConsumerGroupPartitionShare(t *testing.T) {
	const (
		N         = 160
		consumers = 4
		groupID   = "compat-partition-share"
	)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	producer := franzClient(t)
	records := make([]*kgo.Record, N)
	for i := range N {
		records[i] = &kgo.Record{
			Topic: topicConsumerGrpShare,
			Key:   fmt.Appendf(nil, "k-%03d", i),
			Value: fmt.Appendf(nil, "v-%03d", i),
		}
	}
	res := producer.ProduceSync(ctx, records...)
	for i, r := range res {
		if r.Err != nil {
			t.Fatalf("ProduceSync[%d]: %v", i, r.Err)
		}
	}

	type seenSet struct {
		mu         sync.Mutex
		partitions map[int32]int // partition -> record count
		total      int
	}
	perConsumer := make([]*seenSet, consumers)
	for i := range perConsumer {
		perConsumer[i] = &seenSet{partitions: map[int32]int{}}
	}

	var stop atomic.Bool
	var wg sync.WaitGroup

	mkConsumer := func(idx int) {
		defer wg.Done()
		c := franzClient(t,
			kgo.ConsumeTopics(topicConsumerGrpShare),
			kgo.ConsumerGroup(groupID),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.SessionTimeout(10*time.Second),
			kgo.HeartbeatInterval(1*time.Second),
		)
		for !stop.Load() {
			pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
			fetches := c.PollFetches(pollCtx)
			pollCancel()
			if errs := fetches.Errors(); len(errs) > 0 {
				// transient during initial rebalance — keep polling
				continue
			}
			fetches.EachRecord(func(r *kgo.Record) {
				perConsumer[idx].mu.Lock()
				perConsumer[idx].partitions[r.Partition]++
				perConsumer[idx].total++
				perConsumer[idx].mu.Unlock()
			})
		}
	}

	wg.Add(consumers)
	for i := range consumers {
		go mkConsumer(i)
	}

	// Wait until the cumulative seen-count reaches N (correct partition share)
	// OR exceeds 1.5×N (clear "every consumer reads every record" symptom).
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		minTotal := -1
		for _, s := range perConsumer {
			s.mu.Lock()
			total += s.total
			if minTotal == -1 || s.total < minTotal {
				minTotal = s.total
			}
			s.mu.Unlock()
		}
		if total >= int(1.5*float64(N)) {
			break // clear oversampling — bail early
		}
		if total >= N && minTotal > 0 {
			// All consumers got something and the union reached N — done.
			time.Sleep(500 * time.Millisecond)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	stop.Store(true)
	wg.Wait()

	// Report and verify.
	allParts := make([][]int32, consumers)
	totals := make([]int, consumers)
	cumulative := 0
	for i, s := range perConsumer {
		parts := make([]int32, 0, len(s.partitions))
		for p := range s.partitions {
			parts = append(parts, p)
		}
		allParts[i] = parts
		totals[i] = s.total
		cumulative += s.total
		t.Logf("consumer-%d: %d records from %d partitions: %v", i, s.total, len(parts), parts)
	}
	t.Logf("union: %d records (topic has %d, expected ~%d per consumer)", cumulative, N, N/consumers)

	// Every consumer must have seen records.
	for i, total := range totals {
		if total == 0 {
			t.Fatalf("consumer-%d saw zero records — rebalance never gave it any partitions", i)
		}
	}

	// Partition sets MUST be pairwise disjoint. Any overlap = gh #134.
	seenBy := map[int32]int{} // partition -> first consumer index that owned it
	for i, parts := range allParts {
		for _, p := range parts {
			if prev, ok := seenBy[p]; ok {
				t.Errorf("gh #134: consumer-%d and consumer-%d both saw partition %d — partition share is broken", prev, i, p)
			}
			seenBy[p] = i
		}
	}

	if len(seenBy) < 16 {
		t.Logf("WARN: only %d of 16 partitions saw any consumer (some may have gotten 0 records from the partitioner)", len(seenBy))
	}

	// Per-consumer cumulative should be ~N/consumers ± slack. If any consumer's
	// total > 0.7×N, it read most of the topic — that's the gh #134 symptom.
	threshold := int(0.7 * float64(N))
	for i, total := range totals {
		if total > threshold {
			t.Errorf("gh #134: consumer-%d read %d records (>0.7×N=%d) — looks like the full-topic-per-consumer pattern",
				i, total, threshold)
		}
	}
}
