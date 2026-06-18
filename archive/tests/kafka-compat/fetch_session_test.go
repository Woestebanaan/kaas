package kafkacompat

import (
	"context"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestFetchSession_AlwaysReturnsZeroSessionID pins gh #4's stateless-
// mode contract: skafka doesn't maintain per-session state, so every
// Fetch response carries SessionID=0 — Apache's documented signal
// for "the broker doesn't support sessions, send full data next
// time."
//
// Echoing the client's SessionID would be incorrect: the client
// would then send incremental fetch deltas on the next request and
// skafka would have no state to apply them against, losing topics
// from the consumer's view.
func TestFetchSession_AlwaysReturnsZeroSessionID(t *testing.T) {
	const topic = "test-topic-fetch-session"
	createTestTopic(t, topic, 1)

	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cases := []struct {
		name      string
		sessionID int32
	}{
		{"initial_zero_session", 0},
		{"claimed_session_id_42", 42},
		{"high_session_id", 1 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := kmsg.NewPtrFetchRequest()
			req.SessionID = tc.sessionID
			req.SessionEpoch = 0
			req.MaxWaitMillis = 200
			req.MinBytes = 1
			ft := kmsg.NewFetchRequestTopic()
			ft.Topic = topic
			fp := kmsg.NewFetchRequestTopicPartition()
			fp.Partition = 0
			fp.PartitionMaxBytes = 1024 * 1024
			ft.Partitions = append(ft.Partitions, fp)
			req.Topics = append(req.Topics, ft)
			resp, err := req.RequestWith(ctx, cl)
			if err != nil {
				t.Fatalf("Fetch with session=%d: %v", tc.sessionID, err)
			}
			if resp.SessionID != 0 {
				t.Errorf("response SessionID=%d, want 0 (stateless mode signal)", resp.SessionID)
			}
		})
	}
}
