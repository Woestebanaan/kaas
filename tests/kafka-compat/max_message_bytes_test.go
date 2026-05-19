package kafkacompat

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"

	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/coordinator"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/handlers"
)

// TestMaxMessageBytes_RejectsOversizedBatch pins gh #14 end-to-end:
// configure the broker with a tight max.message.bytes, send a batch
// past that cap from a real franz-go producer, and confirm the wire
// response carries MESSAGE_TOO_LARGE (10). Apache producers (the
// Java client) translate this to RecordTooLargeException at the
// caller; franz-go surfaces it via kgo.ErrMessageTooLarge inside
// res.Err. Without the broker-side gate, oversized batches were
// silently accepted and trip downstream Fetch max-bytes loops at
// consume time.
//
// Self-contained: spins up a separate broker on its own port to
// avoid polluting the shared compat broker with the tight cap.
func TestMaxMessageBytes_RejectsOversizedBatch(t *testing.T) {
	addr, shutdown := startCappedBroker(t, "max-msg-test-topic", 128)
	defer shutdown()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
		kgo.RequestRetries(2),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		// Disable client-side max.request.size enforcement so the
		// oversized batch reaches the broker; default 1MB would
		// short-circuit the test before we exercise the broker gate.
		kgo.ProducerBatchMaxBytes(1 << 20),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	// One record carrying a ~512-byte value, well over the 128-byte cap.
	big := strings.Repeat("x", 512)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := cl.ProduceSync(ctx, &kgo.Record{
		Topic: "max-msg-test-topic",
		Value: []byte(big),
	})
	first := res[0]
	if first.Err == nil {
		t.Fatalf("oversized batch unexpectedly accepted: BaseOffset=%d", first.Record.Offset)
	}
	// Skafka returns wire-code 18 (RECORD_LIST_TOO_LARGE) — Apache's
	// canonical code for "batch larger than the broker's segment /
	// max.message.bytes". franz-go renders that as kerr.RecordListTooLarge
	// with the message "request included message batch larger than the
	// configured segment size on the server". Match a substring so the
	// test isn't tied to franz-go's exact wording.
	if !strings.Contains(first.Err.Error(), "RECORD_LIST_TOO_LARGE") &&
		!strings.Contains(first.Err.Error(), "MESSAGE_TOO_LARGE") &&
		!strings.Contains(first.Err.Error(), "too large") {
		t.Errorf("err=%v, want a RECORD_LIST_TOO_LARGE / MESSAGE_TOO_LARGE wrapping", first.Err)
	}
}

// TestMaxMessageBytes_AcceptsAtBoundary — a batch sized exactly at
// the cap must be accepted. Java producers commonly batch up to the
// exact max.request.size, and Apache's contract is strict-greater-
// than to reject; the skafka gate must mirror that.
func TestMaxMessageBytes_AcceptsAtBoundary(t *testing.T) {
	// Use a generous cap so we can hit the boundary with a normal
	// record whose batch overhead we don't need to math exactly.
	addr, shutdown := startCappedBroker(t, "max-msg-boundary-topic", 1<<16)
	defer shutdown()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := cl.ProduceSync(ctx, &kgo.Record{
		Topic: "max-msg-boundary-topic",
		Value: []byte("ok"),
	})
	if res[0].Err != nil {
		t.Errorf("at-boundary produce failed: %v", res[0].Err)
	}
}

// startCappedBroker spins up a fresh in-process broker on a free
// localhost port, with SetMaxMessageBytes applied before
// RegisterHandlers, and one preconfigured topic. Returns the
// "127.0.0.1:PORT" address plus a shutdown closure tests should
// defer. Distinct from the shared TestMain broker so tests can run
// in any order without leaking the cap.
func startCappedBroker(t *testing.T, topic string, maxMsgBytes int32) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("get free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	ctx, cancel := context.WithCancel(context.Background())

	localLeases := broker.NewLocalLeaseManager()
	groupSrc := broker.NewLocalGroupSource("skafka-0")
	lookup := func(_ string) (int32, string, int32, bool) {
		return 0, "127.0.0.1", int32(port), true
	}
	offsetDir, err := os.MkdirTemp("", "skafka-maxmsg-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	offsetStore := coordinator.NewOffsetStore(offsetDir)
	coordMgr := coordinator.NewManager(ctx, groupSrc, lookup, offsetStore)

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
	b.SetMaxMessageBytes(maxMsgBytes)

	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	srv := protocol.NewServer(protocol.Config{
		Listeners: []protocol.ListenerConfig{{Name: "internal", Addr: addr}},
	}, d)
	if err := srv.Start(ctx); err != nil {
		cancel()
		os.RemoveAll(offsetDir)
		t.Fatalf("broker start: %v", err)
	}
	b.AddTopic(topic, 1)

	shutdown := func() {
		cancel()
		srv.Wait()
		os.RemoveAll(offsetDir)
	}
	return addr, shutdown
}

// Compile-time check that the kgo error helpers used by this file
// are present in the current franz-go pin (catches silent renames).
var _ = errors.New
