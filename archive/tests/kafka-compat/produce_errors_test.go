package kafkacompat

import (
	"context"
	"strings"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// TestProduceErrorCodes pins gh #3: Produce returns the Apache-
// canonical error codes for the conditions skafka can detect. Today's
// matrix:
//
//   - UNKNOWN_TOPIC_OR_PARTITION (3): producing to a partition that
//     does not exist on a topic skafka knows about.
//   - MESSAGE_TOO_LARGE / RECORD_LIST_TOO_LARGE (10/18): see gh #14
//     (covered separately in max_message_bytes_test.go).
//
// CORRUPT_MESSAGE, INVALID_REQUIRED_ACKS, etc. require crafting
// malformed wire bytes — too invasive to test through franz-go.
// Those paths are covered by the produce_test.go unit tests under
// internal/protocol/handlers.
// TestProduceErrorCodes_NotLeaderForUnownedPartition shows the
// expected wire error when produce hits a partition this broker
// doesn't lead — the production-mode response is
// NOT_LEADER_OR_FOLLOWER (6) once the v3 coordinator is wired.
//
// The in-process kafka-compat broker uses LocalLeaseManager which
// always answers "yes I lead" for any partition; that makes a
// realistic UNKNOWN_TOPIC_OR_PARTITION test infeasible without
// spinning up the full assignment-loop infrastructure. The
// produce_test.go unit tests cover the coord.Owns()==false branch
// of checkOwnership directly. Skipping this here rather than
// pretending to test it.
func TestProduceErrorCodes_NotLeaderForUnownedPartition(t *testing.T) {
	t.Skip("compat broker uses LocalLeaseManager which always reports leader; the NOT_LEADER_OR_FOLLOWER path is covered by produce_test.go unit tests (TestCheckOwnership_CoordinatorDoesNotOwn).")
}

// TestProduceErrorCodes_UnknownTopic covers the topic-doesn't-exist
// case. Auto-create may turn this into UNKNOWN_TOPIC vs an actual
// create depending on the broker config; the in-process compat
// broker doesn't enable auto-create.
func TestProduceErrorCodes_UnknownTopic(t *testing.T) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DisableIdempotentWrite(),
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
		kgo.RequestRetries(1),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res := cl.ProduceSync(ctx, &kgo.Record{
		Topic:     "topic-that-does-not-exist-" + t.Name(),
		Partition: 0,
		Value:     []byte("x"),
	})
	if res[0].Err == nil {
		// Auto-create may have created it. Not a failure — the contract
		// is "either fail with UNKNOWN_TOPIC, or succeed cleanly".
		t.Logf("produce to unknown topic succeeded — auto-create may be on")
		return
	}
	// Must be a topic/partition-related error, not e.g. CORRUPT_MESSAGE
	// or NETWORK_EXCEPTION.
	msg := res[0].Err.Error()
	known := []string{
		"UNKNOWN_TOPIC_OR_PARTITION",
		"UNKNOWN_TOPIC_ID",
		"UnknownTopic",
		// franz-go may also wrap with "no such topic" when the
		// Metadata response shows no partitions.
		"no such topic",
	}
	matched := false
	for _, s := range known {
		if strings.Contains(msg, s) {
			matched = true
			break
		}
	}
	if !matched {
		t.Errorf("err=%v, want UNKNOWN_TOPIC_* wrapping", res[0].Err)
	}
}
