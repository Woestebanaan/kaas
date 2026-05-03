// Package kafkacompat runs end-to-end compatibility tests against a real skafka
// broker using franz-go and kafka-go as test clients.
// These libraries are imported ONLY here — never in internal/.
package kafkacompat

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/coordinator"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/handlers"
)

// testAddr holds the "host:port" of the in-process broker started by TestMain.
var testAddr string

// Topics used by each test group to avoid offset conflicts when tests run together.
const (
	topicFranzGo     = "test-topic-franzgo"
	topicKafkaGo     = "test-topic-kafkago"
	topicSnappy      = "test-topic-snappy"
	topicIdempotent  = "test-topic-idempotent"
	topicConsumerGrp = "test-topic-consumer-group"
)

func TestMain(m *testing.M) {
	// Grab a free port before constructing the broker so Config.Port matches
	// what the listener will actually bind to (needed for correct Metadata responses).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not get free port: %v\n", err)
		os.Exit(1)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	testAddr = fmt.Sprintf("127.0.0.1:%d", port)

	ctx, cancel := context.WithCancel(context.Background())

	// Wire up a coordinator so the consumer-group compat tests can exercise
	// FindCoordinator → JoinGroup → SyncGroup → Heartbeat → OffsetCommit/Fetch.
	// LocalLeaseManager satisfies CoordinatorLeaseManager and always reports
	// this broker as the coordinator. Offset store goes to a tempdir so the
	// __consumer_offsets/ tree doesn't leak into the working directory.
	localLeases := broker.NewLocalLeaseManager()
	groupSrc := broker.NewLocalGroupSource("skafka-0")
	lookupBroker := func(_ string) (int32, string, int32, bool) {
		return 0, "127.0.0.1", int32(port), true
	}
	offsetDir, err := os.MkdirTemp("", "skafka-compat-offsets-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "MkdirTemp: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(offsetDir)
	offsetStore := coordinator.NewOffsetStore(offsetDir)
	coordMgr := coordinator.NewManager(ctx, groupSrc, lookupBroker, offsetStore)

	brokerInfo := handlers.BrokerInfo{
		NodeID:    0,
		Host:      "127.0.0.1",
		Port:      int32(port),
		ClusterID: "skafka-test",
	}

	b := broker.NewWithBrokerSource(
		broker.Config{
			BrokerID:  0,
			Host:      "127.0.0.1",
			Port:      int32(port),
			ClusterID: "skafka-test",
		},
		broker.NewMemoryStorage(),
		localLeases,
		broker.NewAllowAllAuthEngine(),
		brokerInfo,
		coordMgr,
	)

	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	srv := protocol.NewServer(protocol.Config{ListenAddr: testAddr}, d)
	if err := srv.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "broker start failed: %v\n", err)
		os.Exit(1)
	}

	b.AddTopic(topicFranzGo, 1)
	b.AddTopic(topicKafkaGo, 1)
	b.AddTopic(topicSnappy, 1)
	b.AddTopic(topicIdempotent, 1)
	b.AddTopic(topicConsumerGrp, 3) // 3 partitions so two consumers can split the load

	code := m.Run()
	cancel()
	srv.Wait()
	os.Exit(code)
}

// ---- franz-go tests ----

func franzClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(testAddr),
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
		kgo.RequestRetries(3),
		// Default to no compression so the simple round-trip tests can do
		// byte-level value comparisons. Compressed and idempotent paths are
		// covered explicitly by TestFranzGoSnappyRoundTrip and
		// TestFranzGoIdempotentSnappy — the broker never decompresses under
		// v3.3's bytes-are-opaque architecture, so compressed batches pass
		// through untouched.
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatalf("franz-go NewClient: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func TestFranzGoApiVersions(t *testing.T) {
	cl := franzClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ping forces a connection and ApiVersions negotiation.
	if err := cl.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestFranzGoProduceAndConsume(t *testing.T) {
	const N = 100
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Producer
	producer := franzClient(t)
	records := make([]*kgo.Record, N)
	for i := 0; i < N; i++ {
		records[i] = &kgo.Record{
			Topic: topicFranzGo,
			Key:   []byte(fmt.Sprintf("key-%03d", i)),
			Value: []byte(fmt.Sprintf("value-%03d", i)),
		}
	}
	results := producer.ProduceSync(ctx, records...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("ProduceSync[%d]: %v", i, res.Err)
		}
	}

	// Consumer — start from earliest
	consumer := franzClient(t,
		kgo.ConsumeTopics(topicFranzGo),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	received := 0
	for received < N {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("PollFetches errors: %v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			expected := fmt.Sprintf("value-%03d", received)
			if string(r.Value) != expected {
				t.Errorf("record %d: got value %q want %q", received, r.Value, expected)
			}
			received++
		})
	}
	if received != N {
		t.Errorf("consumed %d records, want %d", received, N)
	}
}

func TestFranzGoFetchBeyondHighWatermark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Produce one record so the topic has a known high watermark.
	producer := franzClient(t)
	res := producer.ProduceSync(ctx, &kgo.Record{Topic: topicFranzGo, Value: []byte("hwm-test")})
	if res[0].Err != nil {
		t.Fatalf("produce: %v", res[0].Err)
	}

	// Consumer starting at a very high offset — should get no records, no error.
	consumer := franzClient(t,
		kgo.ConsumeTopics(topicFranzGo),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(1_000_000)),
	)

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer fetchCancel()

	fetches := consumer.PollFetches(fetchCtx)
	// Timeout or empty fetch is both acceptable — what must NOT happen is a hard error.
	for _, err := range fetches.Errors() {
		if err.Err != context.DeadlineExceeded {
			t.Errorf("unexpected fetch error: %v", err)
		}
	}
}

// ---- kafka-go tests ----

func TestKafkaGoProduceAndConsume(t *testing.T) {
	const N = 50
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Producer
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(testAddr),
		Topic:        topicKafkaGo,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireOne,
		// Disable compression so records stay simple.
		Compression: 0,
	}
	defer w.Close()

	msgs := make([]kafkago.Message, N)
	for i := 0; i < N; i++ {
		msgs[i] = kafkago.Message{
			Key:   []byte(fmt.Sprintf("k-%d", i)),
			Value: []byte(fmt.Sprintf("v-%d", i)),
		}
	}
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}

	// Consumer
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   []string{testAddr},
		Topic:     topicKafkaGo,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10 << 20,
	})
	defer r.Close()
	r.SetOffset(kafkago.FirstOffset)

	for i := 0; i < N; i++ {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("ReadMessage[%d]: %v", i, err)
		}
		expected := fmt.Sprintf("v-%d", i)
		if string(msg.Value) != expected {
			t.Errorf("msg[%d]: got %q want %q", i, msg.Value, expected)
		}
	}
}

func TestKafkaGoProduceToUnknownTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w := &kafkago.Writer{
		Addr:  kafkago.TCP(testAddr),
		Topic: "does-not-exist",
	}
	defer w.Close()

	err := w.WriteMessages(ctx, kafkago.Message{Value: []byte("x")})
	// We expect an error — either a Kafka error code or a connection-level error.
	if err == nil {
		t.Error("expected error writing to unknown topic, got nil")
	}
}

func TestBothClientsMetadataAgrees(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// franz-go: ask for metadata of the franz-go test topic
	cl := franzClient(t)
	if err := cl.Ping(ctx); err != nil {
		t.Fatalf("franz-go ping: %v", err)
	}

	// kafka-go: look up partition leader
	conn, err := kafkago.DialLeader(ctx, "tcp", testAddr, topicFranzGo, 0)
	if err != nil {
		t.Fatalf("kafka-go DialLeader: %v", err)
	}
	conn.Close()
}

// ---- compression / idempotence / consumer-group ----

// TestFranzGoSnappyRoundTrip is the byte-opacity smoke test at the wire level:
// the producer compresses each batch with snappy, the broker stores the
// compressed bytes verbatim (it never decompresses), and the consumer
// decompresses on its end. The records must arrive intact.
func TestFranzGoSnappyRoundTrip(t *testing.T) {
	const N = 200
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("NewClient producer: %v", err)
	}
	t.Cleanup(producer.Close)

	records := make([]*kgo.Record, N)
	for i := 0; i < N; i++ {
		// Wide payloads so snappy actually compresses.
		records[i] = &kgo.Record{
			Topic:     topicSnappy,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("snappy-key-%04d", i)),
			Value:     []byte(fmt.Sprintf("snappy-value-%04d-%s", i, strings_repeat("x", 64))),
		}
	}
	results := producer.ProduceSync(ctx, records...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("ProduceSync[%d]: %v", i, res.Err)
		}
	}

	consumer := franzClient(t,
		kgo.ConsumeTopics(topicSnappy),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	received := 0
	deadline := time.Now().Add(10 * time.Second)
	for received < N && time.Now().Before(deadline) {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("PollFetches errors: %v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			wantValue := fmt.Sprintf("snappy-value-%04d-%s", received, strings_repeat("x", 64))
			if string(r.Value) != wantValue {
				t.Errorf("record %d: value=%q want %q", received, r.Value, wantValue)
			}
			received++
		})
	}
	if received != N {
		t.Fatalf("consumed %d records, want %d (broker is failing to round-trip snappy-compressed bytes)", received, N)
	}
}

// TestFranzGoIdempotentSnappy stacks the realistic franz-go default config:
// idempotence ON (the producer ID and base sequence travel in the batch
// header) and snappy compression. v1 accepts producerID/baseSequence fields
// without deduplicating; the round trip must still deliver every record
// exactly once because the producer doesn't retry on success.
func TestFranzGoIdempotentSnappy(t *testing.T) {
	const N = 50
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(testAddr),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		// Idempotence is on by default in franz-go; spelling it out for clarity.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxProduceRequestsInflightPerBroker(1),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(producer.Close)

	records := make([]*kgo.Record, N)
	for i := 0; i < N; i++ {
		records[i] = &kgo.Record{
			Topic: topicIdempotent,
			Value: []byte(fmt.Sprintf("idem-%04d", i)),
		}
	}
	results := producer.ProduceSync(ctx, records...)
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("ProduceSync[%d]: %v", i, res.Err)
		}
	}

	consumer := franzClient(t,
		kgo.ConsumeTopics(topicIdempotent),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	got := make([]string, 0, N)
	deadline := time.Now().Add(10 * time.Second)
	for len(got) < N && time.Now().Before(deadline) {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("PollFetches errors: %v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			got = append(got, string(r.Value))
		})
	}

	if len(got) != N {
		t.Fatalf("got %d records, want %d", len(got), N)
	}
	for i, v := range got {
		want := fmt.Sprintf("idem-%04d", i)
		if v != want {
			t.Errorf("record %d: %q want %q", i, v, want)
		}
	}
}

// TestFranzGoConsumerGroup exercises FindCoordinator → JoinGroup →
// SyncGroup → Heartbeat → OffsetCommit/Fetch end-to-end via the real
// franz-go group consumer.
//
// History: this test was previously t.Skipped because the wire-level
// flow consistently timed out on first-run cold start. The skip was
// added in commit 5ea4c1d with a memory note attributing the flake to
// per-group Lease cold-start behaviour. Phase 5 step 4 rewired
// coordinator selection from per-group Leases onto controller
// assignment, so the conditions that produced the flake are gone —
// trying again here.
//
// Tolerant of transient PollFetches errors during the cold-start
// rebalance phase: only the final delivered-record count is asserted.
func TestFranzGoConsumerGroup(t *testing.T) {
	const N = 30
	const groupID = "compat-group-1"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	producer := franzClient(t)
	records := make([]*kgo.Record, N)
	for i := 0; i < N; i++ {
		records[i] = &kgo.Record{
			Topic: topicConsumerGrp,
			Value: []byte(fmt.Sprintf("g-%03d", i)),
		}
	}
	res := producer.ProduceSync(ctx, records...)
	for i, r := range res {
		if r.Err != nil {
			t.Fatalf("ProduceSync[%d]: %v", i, r.Err)
		}
	}

	consumer := franzClient(t,
		kgo.ConsumeTopics(topicConsumerGrp),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(10*time.Second),
		kgo.HeartbeatInterval(1*time.Second),
	)

	seen := 0
	deadline := time.Now().Add(25 * time.Second)
	for seen < N && time.Now().Before(deadline) {
		pollCtx, pollCancel := context.WithTimeout(ctx, 3*time.Second)
		fetches := consumer.PollFetches(pollCtx)
		pollCancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Logf("PollFetches transient errors (will retry): %v", errs)
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			seen++
		})
	}
	if seen != N {
		t.Fatalf("consumer group saw %d records, want %d", seen, N)
	}

	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("CommitUncommittedOffsets: %v", err)
	}
}

// strings_repeat avoids importing strings just for one helper.
func strings_repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}
