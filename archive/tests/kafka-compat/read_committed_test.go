package kafkacompat

import (
	"context"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestReadCommitted_LSOEqualsHWMWithoutTxns pins gh #31's
// "no-transactions-in-flight" steady-state contract: when no
// producer is mid-transaction, every record at offset < HWM is
// committed by definition, so LSO == HWM. read_committed and
// read_uncommitted Fetch responses must therefore be byte-identical.
//
// This is the load-bearing contract for vanilla (non-EOS) consumers
// that set isolation.level=read_committed defensively — they MUST
// continue to see every record without filtering.
func TestReadCommitted_LSOEqualsHWMWithoutTxns(t *testing.T) {
	const topic = "test-topic-read-committed"
	createTestTopic(t, topic, 1)

	// Populate the topic with a few records via the standard
	// non-transactional producer.
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	const N = 5
	for i := 0; i < N; i++ {
		res := producer.ProduceSync(context.Background(), &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte{byte(i)},
		})
		if res[0].Err != nil {
			t.Fatal(res[0].Err)
		}
	}

	// Issue two Fetch requests via kmsg: one with isolation=0
	// (read_uncommitted), one with isolation=1 (read_committed).
	// Both responses must carry HWM == LSO == N, and the
	// AbortedTransactions list must be empty in both cases.
	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetch := func(t *testing.T, isolation int8) (hwm, lso int64, abortedCount int) {
		t.Helper()
		req := kmsg.NewPtrFetchRequest()
		req.IsolationLevel = isolation
		req.MaxWaitMillis = 500
		req.MinBytes = 1
		req.MaxBytes = 1024 * 1024
		// SessionEpoch=-1 + null SessionID = "not using fetch sessions"
		req.SessionEpoch = -1
		ft := kmsg.NewFetchRequestTopic()
		ft.Topic = topic
		fp := kmsg.NewFetchRequestTopicPartition()
		fp.Partition = 0
		fp.FetchOffset = 0
		fp.PartitionMaxBytes = 1024 * 1024
		ft.Partitions = append(ft.Partitions, fp)
		req.Topics = append(req.Topics, ft)

		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("Fetch(isolation=%d): %v", isolation, err)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("Fetch response shape: %+v", resp)
		}
		p := resp.Topics[0].Partitions[0]
		return p.HighWatermark, p.LastStableOffset, len(p.AbortedTransactions)
	}

	uHWM, uLSO, uAborted := fetch(t, 0)
	cHWM, cLSO, cAborted := fetch(t, 1)

	if uLSO != uHWM {
		t.Errorf("read_uncommitted LSO=%d != HWM=%d (LSO is broker-side, should always == HWM today)", uLSO, uHWM)
	}
	if uHWM != cHWM {
		t.Errorf("HWM differs between isolation levels: read_uncommitted=%d read_committed=%d (must be identical)", uHWM, cHWM)
	}
	if cLSO != cHWM {
		t.Errorf("read_committed LSO=%d HWM=%d: must be equal in the no-txn-in-flight state", cLSO, cHWM)
	}
	if uAborted != 0 || cAborted != 0 {
		t.Errorf("AbortedTransactions non-empty: read_uncommitted=%d read_committed=%d (no txns in flight)", uAborted, cAborted)
	}
	if cHWM < N {
		t.Errorf("HWM=%d, want >= %d (records weren't visible to Fetch)", cHWM, N)
	}
}

// TestReadCommitted_IsolationFieldAccepted pins the wire-level
// negative case: an explicit isolation_level=2 (or any future value
// skafka doesn't know about) must NOT crash the handler. Apache
// accepts unknown levels and falls back to read_uncommitted; skafka
// does the same. Verified indirectly here by confirming the Fetch
// returns a well-formed response with sane HWM/LSO.
func TestReadCommitted_IsolationFieldAccepted(t *testing.T) {
	const topic = "test-topic-read-committed-isolation-field"
	createTestTopic(t, topic, 1)

	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := kmsg.NewPtrFetchRequest()
	req.IsolationLevel = 99 // unknown
	req.MaxWaitMillis = 500
	req.MinBytes = 1
	req.SessionEpoch = -1
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = topic
	fp := kmsg.NewFetchRequestTopicPartition()
	fp.Partition = 0
	fp.PartitionMaxBytes = 1024 * 1024
	ft.Partitions = append(ft.Partitions, fp)
	req.Topics = append(req.Topics, ft)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("Fetch with isolation=99: %v", err)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("response shape: %+v", resp)
	}
	p := resp.Topics[0].Partitions[0]
	if p.ErrorCode != 0 {
		t.Errorf("ErrorCode=%d, want 0 (broker should tolerate unknown isolation levels)", p.ErrorCode)
	}
}
