package kafkacompat

import (
	"context"
	"sync"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// TestCooperativeSticky_TwoConsumersShareWithoutFullRevoke pins gh #17:
// when a second consumer joins a group whose first member uses the
// cooperative-sticky balancer, the broker's JoinGroup/SyncGroup path
// must round-trip the protocol intact so franz-go's
// CooperativeStickyBalancer can do its incremental dance (revoke
// some partitions, keep the rest).
//
// Skafka's broker side is a pass-through for the assignment bytes:
// the LEADER (one of the consumers) computes the assignment locally
// and the broker distributes it. The test catches the regression
// where the protocol name / member metadata / assignment bytes get
// corrupted in transit — every member would then crash on
// "InconsistentGroupProtocol" or end up with empty assignments.
func TestCooperativeSticky_TwoConsumersShareWithoutFullRevoke(t *testing.T) {
	const (
		topic   = "test-topic-cooperative-sticky"
		group   = "grp-cooperative-sticky"
		nParts  = 6
		N       = 240
	)
	createTestTopic(t, topic, nParts)

	// Pre-populate every partition with N/nParts records so both
	// consumers have something to count once they own partitions.
	prod, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < N; i++ {
		res := prod.ProduceSync(ctx, &kgo.Record{
			Topic:     topic,
			Partition: int32(i % nParts),
			Value:     []byte("v"),
		})
		if res[0].Err != nil {
			t.Fatal(res[0].Err)
		}
	}
	prod.Close()

	// Two consumers in the same group using the cooperative-sticky
	// balancer. Both should end up with non-zero partition counts
	// and the union of their reads should equal N.
	var (
		mu      sync.Mutex
		counts  = map[int]int{}
		stop    = make(chan struct{})
		done    = make(chan struct{}, 2)
	)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer func() { done <- struct{}{} }()
			cl, err := kgo.NewClient(
				kgo.SeedBrokers(testAddr),
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.Balancers(kgo.CooperativeStickyBalancer()),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.SessionTimeout(15*time.Second),
				kgo.HeartbeatInterval(3*time.Second),
			)
			if err != nil {
				t.Errorf("c%d NewClient: %v", i, err)
				return
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			for {
				select {
				case <-stop:
					return
				default:
				}
				fetches := cl.PollFetches(ctx)
				fetches.EachRecord(func(_ *kgo.Record) {
					mu.Lock()
					counts[i]++
					mu.Unlock()
				})
				if ctx.Err() != nil {
					return
				}
			}
		}()
		// Stagger the second consumer so the first one fully owns
		// the topic before the rebalance kicks in. This is the
		// scenario where cooperative-sticky should partially revoke
		// rather than do a full stop-the-world reassignment.
		time.Sleep(2 * time.Second)
	}

	// Watch for both consumers to register progress, then stop.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		total := counts[0] + counts[1]
		mu.Unlock()
		if total >= N {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	close(stop)
	<-done
	<-done

	mu.Lock()
	total := counts[0] + counts[1]
	c1, c2 := counts[0], counts[1]
	mu.Unlock()
	if total < N {
		t.Errorf("total consumed=%d, want >= %d", total, N)
	}
	if c1 == 0 || c2 == 0 {
		t.Errorf("one consumer ended up with 0 partitions (c1=%d c2=%d) — cooperative-sticky negotiation failed",
			c1, c2)
	}
}
