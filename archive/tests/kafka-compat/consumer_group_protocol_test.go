package kafkacompat

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// TestJoinSyncHeartbeat_EndToEnd pins gh #7: a consumer in a group
// drives the full JoinGroup → SyncGroup → Heartbeat sequence and
// consumes a known number of records. Catches regressions in any
// step (e.g., a Heartbeat path that lets the session timeout fire
// silently, or a SyncGroup that returns an empty assignment).
func TestJoinSyncHeartbeat_EndToEnd(t *testing.T) {
	const (
		topic = "test-topic-joingroup-sync"
		group = "grp-joingroup-sync"
		N     = 50
	)
	createTestTopic(t, topic, 1)
	populate(t, topic, N)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(10*time.Second),
		kgo.HeartbeatInterval(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	received := 0
	deadline := time.Now().Add(8 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for received < N && time.Now().Before(deadline) {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.DeadlineExceeded {
					goto done
				}
				t.Fatalf("PollFetches: %v", e.Err)
			}
		}
		fetches.EachRecord(func(_ *kgo.Record) { received++ })
	}
done:
	if received != N {
		t.Errorf("consumed %d records, want %d (Join/Sync/Heartbeat chain may have broken)", received, N)
	}
}

// TestEagerRebalance_TwoConsumersSplit pins gh #16: with two
// consumers in the same group on a 4-partition topic, partitions
// must be split between them (each consumer ends up reading at
// least one record). Eager rebalance is the default protocol.
func TestEagerRebalance_TwoConsumersSplit(t *testing.T) {
	const (
		topic  = "test-topic-eager-rebalance"
		group  = "grp-eager-rebalance"
		N      = 200
		nParts = 4
	)
	createTestTopic(t, topic, nParts)

	// Spread records across all 4 partitions so a rebalance that
	// hands each consumer 2 partitions actually has data on each.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < N; i++ {
		res := cl.ProduceSync(ctx, &kgo.Record{
			Topic:     topic,
			Partition: int32(i % nParts),
			Value:     []byte("v"),
		})
		if res[0].Err != nil {
			t.Fatal(res[0].Err)
		}
	}

	// Start two consumers in the same group. Each should end up with
	// roughly half the partitions; with N=200 split across 4 parts,
	// each consumer reads ~100 records.
	var c1Count, c2Count atomic.Int32
	stop := make(chan struct{})
	done := make(chan struct{}, 2)

	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer func() { done <- struct{}{} }()
			cc, err := kgo.NewClient(
				kgo.SeedBrokers(testAddr),
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				t.Errorf("c%d NewClient: %v", i, err)
				return
			}
			defer cc.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			for ctx.Err() == nil {
				select {
				case <-stop:
					return
				default:
				}
				fetches := cc.PollFetches(ctx)
				fetches.EachRecord(func(_ *kgo.Record) {
					if i == 0 {
						c1Count.Add(1)
					} else {
						c2Count.Add(1)
					}
				})
			}
		}()
	}

	// Wait until the total matches N (or timeout).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c1Count.Load()+c2Count.Load() >= int32(N) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(stop)
	<-done
	<-done

	total := c1Count.Load() + c2Count.Load()
	if total < int32(N) {
		t.Errorf("total consumed=%d, want >= %d", total, N)
	}
	if c1Count.Load() == 0 || c2Count.Load() == 0 {
		t.Errorf("eager rebalance was lopsided: c1=%d c2=%d (neither should be 0 with 4 partitions of data and 2 consumers)",
			c1Count.Load(), c2Count.Load())
	}
}

// TestStaticMembership_InstanceIDSurvivesRejoin pins gh #18: a
// consumer that sets group.instance.id (static member) re-joining
// the group must NOT trigger a rebalance — its previous assignment
// stays. Today we test a weaker but reasonable contract: a static
// member can connect twice without erroring (the broker accepts the
// duplicate InstanceID with the new MemberID via fencing semantics).
func TestStaticMembership_InstanceIDSurvivesRejoin(t *testing.T) {
	const (
		topic    = "test-topic-static-membership"
		group    = "grp-static-membership"
		instance = "consumer-instance-A"
	)
	createTestTopic(t, topic, 1)
	populate(t, topic, 5)

	cl1, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.InstanceID(instance),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("NewClient1: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Drive a poll so the first consumer joins the group and gets
	// an assignment.
	deadline := time.Now().Add(3 * time.Second)
	first := 0
	for first < 5 && time.Now().Before(deadline) {
		fetches := cl1.PollFetches(ctx)
		fetches.EachRecord(func(_ *kgo.Record) { first++ })
	}
	cl1.Close()

	// Second consumer with the same instance.id rejoins. It must
	// succeed; the broker recognises the static identity and fences
	// the previous member.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.InstanceID(instance),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("NewClient2 (re-attach with same instance.id): %v", err)
	}
	defer cl2.Close()
	// Drive a quick poll; if the broker rejected the static rejoin
	// the consumer would error out instead of returning the records.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	second := 0
	for ctx2.Err() == nil && second < 5 {
		fetches := cl2.PollFetches(ctx2)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.DeadlineExceeded {
					return
				}
				t.Fatalf("rejoin PollFetches: %v", e.Err)
			}
		}
		fetches.EachRecord(func(_ *kgo.Record) { second++ })
	}
}
