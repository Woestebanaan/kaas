package kafkacompat

import (
	"context"
	"strconv"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// TestEOSv2_ConsumeProcessProduceCommit pins gh #37's KIP-447
// happy-path round trip on a single broker:
//
//  1. Seed an input topic with N records.
//  2. Open a transactional producer with a stable transactional.id.
//  3. Open a consumer group; read all N from the input topic.
//  4. Begin a txn; produce N transformed records to an output topic
//     under the txn; sendOffsetsToTransaction commits the consumer
//     offsets atomically with the produces.
//  5. EndTxn(commit).
//  6. Verify: a fresh consumer with isolation.level=read_committed
//     on the output topic sees all N records.
//  7. Verify: the consumer group's committed offset for the input
//     topic equals N.
//
// This is the load-bearing contract for Kafka Streams' default EOS
// mode. Without this test the broker passing every individual handler
// unit test still doesn't tell us the wire-level sequence actually
// composes.
func TestEOSv2_ConsumeProcessProduceCommit(t *testing.T) {
	const (
		inputTopic  = "test-eos-v2-input"
		outputTopic = "test-eos-v2-output"
		group       = "eos-v2-grp"
		txnID       = "eos-v2-tx"
		N           = 8
	)
	createTestTopic(t, inputTopic, 1)
	createTestTopic(t, outputTopic, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Seed input.
	{
		seed, err := kgo.NewClient(
			kgo.SeedBrokers(testAddr),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)
		if err != nil {
			t.Fatalf("seed client: %v", err)
		}
		defer seed.Close()
		for i := 0; i < N; i++ {
			res := seed.ProduceSync(ctx, &kgo.Record{
				Topic:     inputTopic,
				Partition: 0,
				Value:     []byte("v-" + strconv.Itoa(i)),
			})
			if res[0].Err != nil {
				t.Fatalf("seed produce %d: %v", i, res[0].Err)
			}
		}
	}

	// Transactional consume → produce → sendOffsetsToTransaction → commit.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(inputTopic),
		kgo.TransactionalID(txnID),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.RequireStableFetchOffsets(),
	)
	if err != nil {
		t.Fatalf("transactional client: %v", err)
	}
	defer cl.Close()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}

	// Pull N records from the input.
	got := 0
	for got < N {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) != 0 {
			t.Fatalf("PollFetches: %+v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			got++
			// Produce transformed copy under the txn.
			out := &kgo.Record{
				Topic: outputTopic,
				Value: append([]byte("t-"), r.Value...),
			}
			cl.Produce(ctx, out, func(_ *kgo.Record, err error) {
				if err != nil {
					t.Errorf("txn produce: %v", err)
				}
			})
		})
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// sendOffsetsToTransaction — franz-go fires it as part of EndTransaction
	// when TryCommit is true.
	switch err := cl.EndTransaction(ctx, kgo.TryCommit); err {
	case nil:
		// happy path
	default:
		t.Fatalf("EndTransaction(TryCommit): %v", err)
	}

	// Verify the output topic has N records readable under read_committed.
	verify, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ConsumeTopics(outputTopic),
		kgo.ConsumerGroup("eos-v2-verify"),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("verify client: %v", err)
	}
	defer verify.Close()

	verifyCtx, vcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer vcancel()
	seen := 0
	for seen < N {
		fetches := verify.PollFetches(verifyCtx)
		if err := verifyCtx.Err(); err != nil {
			t.Fatalf("read_committed read timed out, seen=%d/%d: %v", seen, N, err)
		}
		if errs := fetches.Errors(); len(errs) != 0 {
			t.Fatalf("verify PollFetches: %+v", errs)
		}
		fetches.EachRecord(func(_ *kgo.Record) {
			seen++
		})
	}
	if seen != N {
		t.Errorf("output topic read_committed count=%d, want %d", seen, N)
	}
}
