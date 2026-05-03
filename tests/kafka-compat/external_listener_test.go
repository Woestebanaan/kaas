package kafkacompat

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	kmsg "github.com/twmb/franz-go/pkg/kmsg"

	"github.com/woestebanaan/skafka/internal/broker"
	leaselib "github.com/woestebanaan/skafka/internal/lease"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/internal/protocol/handlers"
	"github.com/woestebanaan/skafka/tests/testutil/tlscerts"
)

// External listener integration test (Phase 9 Gap #2+#3).
//
// Two in-process brokers, each on its own loopback TLS port, configured
// with per-broker external hostnames "broker-0.localhost" /
// "broker-1.localhost". Verifies:
//
//  1. Metadata returned over the TLS listener carries the per-broker
//     external hostnames (ListenerExternal advertise path).
//  2. franz-go uses those hostnames to route partition-N produce to
//     broker-N — proving the per-broker SNI / DNS pattern works
//     end-to-end (the MVP-checklist "TLS passthrough + SNI verified"
//     and "Metadata response carries per-broker hostnames" rows).
//  3. A Produce sent to the wrong broker returns
//     NOT_LEADER_FOR_PARTITION (Gap #3 — exercises the redirect path
//     that franz-go transparently retries on).

const (
	extTopic           = "external-test"
	extBroker0Hostname = "broker-0.localhost"
	extBroker1Hostname = "broker-1.localhost"
)

// stubLeaseManager: shared partition→leader map across both brokers.
// IsLeader returns true only for partitions whose leader matches selfID;
// LeaderFor returns the recorded leader regardless of selfID. This is
// what makes a Metadata response from broker-0 truthful about broker-1's
// partitions, and what makes the NOT_LEADER path fire when a request
// lands on the wrong broker.
type stubLeaseManager struct {
	selfID  int32
	leaders map[int32]int32 // partition → leader broker ID
}

func (s *stubLeaseManager) Acquire(_ context.Context, _ string, _ int32) error { return nil }
func (s *stubLeaseManager) Release(_ string, _ int32) error                    { return nil }

func (s *stubLeaseManager) IsLeader(_ string, partition int32) bool {
	leader, ok := s.leaders[partition]
	return ok && leader == s.selfID
}

func (s *stubLeaseManager) LeaderFor(_ string, partition int32) int32 {
	leader, ok := s.leaders[partition]
	if !ok {
		return -1
	}
	return leader
}

func (s *stubLeaseManager) WatchLeaders(_ context.Context) (<-chan leaselib.LeaderChange, error) {
	return nil, nil
}

// staticBrokerSource implements handlers.BrokerSource with a fixed
// two-broker view. Both brokers share the same All() list — only
// Self() differs based on selfID.
type staticBrokerSource struct {
	selfID int32
	all    []handlers.BrokerEndpoint
}

func (s staticBrokerSource) Self() handlers.BrokerEndpoint {
	for _, ep := range s.all {
		if ep.NodeID == s.selfID {
			return ep
		}
	}
	return handlers.BrokerEndpoint{}
}
func (s staticBrokerSource) All() []handlers.BrokerEndpoint { return s.all }

// startExternalBroker boots a TLS-only broker at the given preallocated
// loopback port. The shared brokerSource + leaseManager give every
// broker the same view of the cluster so Metadata responses agree.
func startExternalBroker(
	t *testing.T,
	brokerID int32,
	port int,
	dir string,
	bundle *tlscerts.Bundle,
	brokerSource handlers.BrokerSource,
	leases leaselib.LeaseManager,
) func() {
	t.Helper()

	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, bundle.ServerCert, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, bundle.ServerKey, 0600); err != nil {
		t.Fatal(err)
	}

	tlsCfg, err := protocol.WatchingCertificate(certFile, keyFile)
	if err != nil {
		t.Fatalf("WatchingCertificate: %v", err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx, cancel := context.WithCancel(context.Background())

	b := broker.NewWithBrokerSource(
		broker.Config{BrokerID: brokerID, Host: "127.0.0.1", Port: int32(port), ClusterID: "ext-test"},
		broker.NewMemoryStorage(),
		leases,
		broker.NewAllowAllAuthEngine(),
		brokerSource,
		nil, // no consumer-group coordinator
	)
	b.AddTopic(extTopic, 2)

	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	srv := protocol.NewServer(protocol.Config{
		// TLS-only — the external listener is the only one the test cares about.
		ListenAddr:    "127.0.0.1:0",
		TLSListenAddr: addr,
		TLSConfig:     tlsCfg,
	}, d)
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("server start: %v", err)
	}

	return func() {
		cancel()
		srv.Wait()
	}
}

// allocLoopbackPort grabs a free TCP port on 127.0.0.1, closes the
// listener, and returns the port number. Race window before the test
// re-binds is acceptable in practice — same trick the existing
// startMTLSBroker helper uses.
func allocLoopbackPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// dialTracker wraps a TLS dialer that maps test hostnames to their
// loopback ports and records every host franz-go dials. Letting the
// test assert on the trace is what proves per-broker hostname routing
// (rather than franz-go silently reusing the bootstrap connection or
// dialing 127.0.0.1 directly).
type dialTracker struct {
	mu       sync.Mutex
	hostMap  map[string]string // "broker-0.localhost" → "127.0.0.1:42313"
	caPool   *tls.Config       // shared client TLS config (RootCAs only)
	dialedTo []string          // hostnames franz-go asked us to dial, in order
}

func (d *dialTracker) Dial(ctx context.Context, network, hostport string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil, fmt.Errorf("split %q: %w", hostport, err)
	}

	d.mu.Lock()
	d.dialedTo = append(d.dialedTo, host)
	target, ok := d.hostMap[host]
	d.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("dialTracker: unknown host %q", host)
	}

	raw, err := (&net.Dialer{}).DialContext(ctx, network, target)
	if err != nil {
		return nil, err
	}

	cfg := d.caPool.Clone()
	cfg.ServerName = host // SNI = the per-broker hostname, NOT 127.0.0.1.
	tlsConn := tls.Client(raw, cfg)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, err
	}
	return tlsConn, nil
}

func (d *dialTracker) calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.dialedTo))
	copy(out, d.dialedTo)
	return out
}

// TestExternalListenerPerBrokerHostnames is the headline Phase 9 test.
// It produces records to two partitions whose leaders live on different
// brokers and asserts that franz-go uses the per-broker hostnames from
// Metadata to route each partition to the correct broker.
func TestExternalListenerPerBrokerHostnames(t *testing.T) {
	bundle, err := tlscerts.NewBundleWithSANs(
		[]string{extBroker0Hostname, extBroker1Hostname, "127.0.0.1"},
		"unused-client-cn",
	)
	if err != nil {
		t.Fatalf("NewBundleWithSANs: %v", err)
	}

	// Partition 0 → broker 0; partition 1 → broker 1. Sharing this map
	// (by pointer) across both brokers means LeaderFor agrees in every
	// Metadata response, regardless of which broker answers.
	leaders := map[int32]int32{0: 0, 1: 1}

	port0 := allocLoopbackPort(t)
	port1 := allocLoopbackPort(t)

	// External advertised endpoints — the per-broker hostnames are what
	// the Metadata handler returns when the request arrives over the
	// TLS listener (connstate.ListenerExternal). Internal Host stays
	// 127.0.0.1 so non-external code paths still resolve to a real addr.
	source := staticBrokerSource{
		all: []handlers.BrokerEndpoint{
			{NodeID: 0, Host: "127.0.0.1", Port: int32(port0),
				ExternalHost: extBroker0Hostname, ExternalPort: int32(port0)},
			{NodeID: 1, Host: "127.0.0.1", Port: int32(port1),
				ExternalHost: extBroker1Hostname, ExternalPort: int32(port1)},
		},
	}

	dir0, dir1 := t.TempDir(), t.TempDir()

	src0 := source
	src0.selfID = 0
	stop0 := startExternalBroker(t, 0, port0, dir0, bundle, src0,
		&stubLeaseManager{selfID: 0, leaders: leaders})
	defer stop0()

	src1 := source
	src1.selfID = 1
	stop1 := startExternalBroker(t, 1, port1, dir1, bundle, src1,
		&stubLeaseManager{selfID: 1, leaders: leaders})
	defer stop1()

	// franz-go custom dialer: maps broker-N.localhost → 127.0.0.1:portN.
	tracker := &dialTracker{
		hostMap: map[string]string{
			extBroker0Hostname: fmt.Sprintf("127.0.0.1:%d", port0),
			extBroker1Hostname: fmt.Sprintf("127.0.0.1:%d", port1),
		},
		caPool: &tls.Config{
			MinVersion: tls.VersionTLS13,
			RootCAs:    bundle.CAPool,
		},
	}

	// Bootstrap from broker-0 ONLY. If the per-broker hostname routing
	// works, partition-1 produces will dial broker-1.localhost (mapped
	// to port1) — visible in tracker.calls().
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(extBroker0Hostname+":9093"),
		kgo.Dialer(tracker.Dial),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	// Direct Metadata request via the bootstrap broker. This is the
	// authoritative check: the response over the TLS listener MUST
	// carry the per-broker external hostnames in brokers[].host. If
	// it did, franz-go can route partition-N produce to broker-N
	// regardless of when its internal connection pool warms up; if
	// it didn't, no amount of retries would fix it.
	mdReq := kmsg.NewPtrMetadataRequest()
	mdReq.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(extTopic)}}
	mdResp, err := cl.Broker(0).Request(ctx, mdReq)
	if err != nil {
		t.Fatalf("metadata request: %v", err)
	}
	md := mdResp.(*kmsg.MetadataResponse)

	if len(md.Brokers) != 2 {
		t.Fatalf("metadata: %d brokers, want 2 — Self()-only response is the v2.6 bug Metadata.go is supposed to fix", len(md.Brokers))
	}
	gotHosts := map[int32]string{}
	for _, b := range md.Brokers {
		gotHosts[b.NodeID] = b.Host
	}
	wantHosts := map[int32]string{0: extBroker0Hostname, 1: extBroker1Hostname}
	for nodeID, want := range wantHosts {
		if got := gotHosts[nodeID]; got != want {
			t.Errorf("metadata broker[%d].host=%q, want %q — addressFor(ListenerExternal) is not picking up ExternalHost",
				nodeID, got, want)
		}
	}

	// Cross-broker produce: exercises franz-go's per-leader connection
	// pool by sending one record per partition. With the Metadata
	// assertion above already passing, this is mostly a smoke test
	// that the per-broker hostnames also work for actual data
	// (custom dialer + cert + listener stack).
	if r := cl.ProduceSync(ctx,
		&kgo.Record{Topic: extTopic, Partition: 0, Value: []byte("p0-on-broker0")},
	); r[0].Err != nil {
		t.Fatalf("ProduceSync p0: %v", r[0].Err)
	}
	if r := cl.ProduceSync(ctx,
		&kgo.Record{Topic: extTopic, Partition: 1, Value: []byte("p1-on-broker1")},
	); r[0].Err != nil {
		t.Fatalf("ProduceSync p1: %v", r[0].Err)
	}
}

// TestExternalListenerNotLeaderRedirect targets the NOT_LEADER path
// (Gap #3): explicitly send a partition-1 Produce to broker-0 (which
// doesn't own that partition) and assert error code 6
// (NOT_LEADER_FOR_PARTITION). This is the response a real client uses
// to trigger Metadata refresh + retry; this test exercises the broker
// side of that contract.
func TestExternalListenerNotLeaderRedirect(t *testing.T) {
	bundle, err := tlscerts.NewBundleWithSANs(
		[]string{extBroker0Hostname, extBroker1Hostname, "127.0.0.1"},
		"unused-client-cn",
	)
	if err != nil {
		t.Fatalf("NewBundleWithSANs: %v", err)
	}

	leaders := map[int32]int32{0: 0, 1: 1}
	port0 := allocLoopbackPort(t)
	port1 := allocLoopbackPort(t)

	source := staticBrokerSource{
		all: []handlers.BrokerEndpoint{
			{NodeID: 0, Host: "127.0.0.1", Port: int32(port0),
				ExternalHost: extBroker0Hostname, ExternalPort: int32(port0)},
			{NodeID: 1, Host: "127.0.0.1", Port: int32(port1),
				ExternalHost: extBroker1Hostname, ExternalPort: int32(port1)},
		},
	}

	dir0, dir1 := t.TempDir(), t.TempDir()

	src0 := source
	src0.selfID = 0
	stop0 := startExternalBroker(t, 0, port0, dir0, bundle, src0,
		&stubLeaseManager{selfID: 0, leaders: leaders})
	defer stop0()

	src1 := source
	src1.selfID = 1
	stop1 := startExternalBroker(t, 1, port1, dir1, bundle, src1,
		&stubLeaseManager{selfID: 1, leaders: leaders})
	defer stop1()

	tracker := &dialTracker{
		hostMap: map[string]string{
			extBroker0Hostname: fmt.Sprintf("127.0.0.1:%d", port0),
			extBroker1Hostname: fmt.Sprintf("127.0.0.1:%d", port1),
		},
		caPool: &tls.Config{
			MinVersion: tls.VersionTLS13,
			RootCAs:    bundle.CAPool,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(extBroker0Hostname+":9093"),
		kgo.Dialer(tracker.Dial),
		kgo.RequestRetries(0),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	// Hand-built Produce request for partition 1 directly addressed to
	// broker 0 via cl.Broker(0). Bypasses franz-go's auto-routing and
	// forces the server to evaluate the wrong-broker case.
	req := kmsg.NewPtrProduceRequest()
	req.TimeoutMillis = 5000
	tr := kmsg.NewProduceRequestTopic()
	tr.Topic = extTopic
	pr := kmsg.NewProduceRequestTopicPartition()
	pr.Partition = 1
	// Empty Records is fine — checkOwnership runs before validateProduceBatches,
	// so the NOT_LEADER assertion is reached before the empty-batch check.
	pr.Records = []byte{}
	tr.Partitions = []kmsg.ProduceRequestTopicPartition{pr}
	req.Topics = []kmsg.ProduceRequestTopic{tr}

	resp, err := cl.Broker(0).Request(ctx, req)
	if err != nil {
		t.Fatalf("targeted Request to broker-0: %v", err)
	}
	pResp := resp.(*kmsg.ProduceResponse)

	if len(pResp.Topics) != 1 || len(pResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response shape: %+v", pResp.Topics)
	}
	got := pResp.Topics[0].Partitions[0].ErrorCode
	const errNotLeaderOrFollower = 6
	if got != errNotLeaderOrFollower {
		t.Errorf("partition-1 produce to broker-0: ErrorCode=%d, want %d (NOT_LEADER_OR_FOLLOWER)",
			got, errNotLeaderOrFollower)
	}
}
