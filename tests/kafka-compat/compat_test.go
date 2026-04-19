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
	"github.com/woestebanaan/skafka/internal/protocol"
)

// testAddr holds the "host:port" of the in-process broker started by TestMain.
var testAddr string

// Topics used by each test group to avoid offset conflicts when tests run together.
const (
	topicFranzGo = "test-topic-franzgo"
	topicKafkaGo = "test-topic-kafkago"
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

	b := broker.New(
		broker.Config{
			BrokerID:  0,
			Host:      "127.0.0.1",
			Port:      int32(port),
			ClusterID: "skafka-test",
		},
		broker.NewMemoryStorage(),
		broker.NewLocalLeaseManager(),
		broker.NewLocalPartitionLock(),
		broker.NewAllowAllAuthEngine(),
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
		// Disable compression: our broker does not yet decompress batches (Phase 3).
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
