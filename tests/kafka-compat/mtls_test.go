package kafkacompat

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/tests/testutil/tlscerts"
)

const topicMTLS = "test-topic-mtls"

// writeMTLSAuthFiles drops a credentials.json that maps the test
// client's CN → username, plus an acls.json that grants that user
// access to the test topic. RealAuthEngine reads both at startup.
func writeMTLSAuthFiles(t *testing.T, dataDir, username, clientCN, topic string) {
	t.Helper()
	clusterDir := filepath.Join(dataDir, "__cluster")
	if err := os.MkdirAll(clusterDir, 0755); err != nil {
		t.Fatal(err)
	}

	credsJSON := fmt.Sprintf(`{
  "version": 1,
  "users": [{
    "username": %q,
    "authType": "tls",
    "tlsCN": %q
  }]
}`, username, clientCN)
	if err := os.WriteFile(filepath.Join(clusterDir, "credentials.json"), []byte(credsJSON), 0644); err != nil {
		t.Fatal(err)
	}

	principal := fmt.Sprintf("User:%s", username)
	aclsJSON := fmt.Sprintf(`{
  "version": 1,
  "acls": [
    {
      "principal": %q,
      "resource": {"type":"topic","name":%q,"patternType":"literal"},
      "operations":["Read","Write","Describe"],
      "permission":"Allow"
    },
    {
      "principal": %q,
      "resource": {"type":"cluster","name":"kafka-cluster","patternType":"literal"},
      "operations":["Describe"],
      "permission":"Allow"
    }
  ]
}`, principal, topic, principal)
	if err := os.WriteFile(filepath.Join(clusterDir, "acls.json"), []byte(aclsJSON), 0644); err != nil {
		t.Fatal(err)
	}
}

// startMTLSBroker spins a self-contained TLS-enabled broker that
// requires every client to present a cert signed by bundle.CAPool.
// Returns the TLS listen address + a stop function.
func startMTLSBroker(t *testing.T, dataDir string, bundle *tlscerts.Bundle) (string, func()) {
	t.Helper()

	// Persist the server cert/key to disk so WatchingCertificate's
	// fsnotify-on-parent-dir setup has actual files to watch.
	certFile := filepath.Join(dataDir, "server.crt")
	keyFile := filepath.Join(dataDir, "server.key")
	if err := os.WriteFile(certFile, bundle.ServerCert, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, bundle.ServerKey, 0600); err != nil {
		t.Fatal(err)
	}

	tlsCfg, err := protocol.WatchingCertificate(certFile, keyFile,
		protocol.WithClientCAPool(bundle.CAPool))
	if err != nil {
		t.Fatalf("WatchingCertificate: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	ctx, cancel := context.WithCancel(context.Background())

	authEng, err := auth.NewRealAuthEngine(dataDir, nil)
	if err != nil {
		cancel()
		t.Fatalf("NewRealAuthEngine: %v", err)
	}

	b := broker.New(
		broker.Config{BrokerID: 0, Host: "127.0.0.1", Port: int32(port), ClusterID: "mtls-test"},
		broker.NewMemoryStorage(),
		broker.NewLocalLeaseManager(),
		authEng,
	)
	b.AddTopic(topicMTLS, 1)

	d := protocol.NewDispatcher()
	b.RegisterHandlers(d)

	srv := protocol.NewServer(protocol.Config{
		Listeners: []protocol.ListenerConfig{
			// No anonymous plaintext listener — TLS-only on the
			// external listener tag.
			{Name: "external", Addr: addr, TLSConfig: tlsCfg},
		},
	}, d)
	srv.SetAuthEngine(authEng)
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("server start: %v", err)
	}

	return addr, func() {
		cancel()
		srv.Wait()
	}
}

// TestFranzGoMTLSPrincipalExtraction verifies the full mTLS flow:
// the broker requires a client cert, franz-go presents one, the
// server extracts the CN, looks it up in credentials.json's tlsCN
// map, derives the User:<username> principal, and authorizes the
// produce based on ACLs. The unit-level mTLS test in
// internal/protocol/server.go exercises CN extraction; this test
// closes the loop on certificate-bundle wiring + ACL evaluation.
func TestFranzGoMTLSPrincipalExtraction(t *testing.T) {
	const username, clientCN = "alice", "alice-client.skafka.local"

	bundle, err := tlscerts.NewBundle("127.0.0.1", clientCN)
	if err != nil {
		t.Fatalf("tlscerts.NewBundle: %v", err)
	}

	dir := t.TempDir()
	writeMTLSAuthFiles(t, dir, username, clientCN, topicMTLS)
	addr, stop := startMTLSBroker(t, dir, bundle)
	defer stop()

	// Build franz-go's TLS config: trust our test CA + present the
	// test client cert. franz-go opens TLS connections with this
	// config; the broker's RequireAndVerifyClientCert hook validates
	// against the same CA.
	clientCert, err := tls.X509KeyPair(bundle.ClientCert, bundle.ClientKey)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	clientTLS := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      bundle.CAPool,
		Certificates: []tls.Certificate{clientCert},
		ServerName:   "127.0.0.1",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DialTLSConfig(clientTLS),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	const N = 10
	records := make([]*kgo.Record, N)
	for i := 0; i < N; i++ {
		records[i] = &kgo.Record{
			Topic: topicMTLS,
			Value: []byte(fmt.Sprintf("mtls-%02d", i)),
		}
	}
	results := cl.ProduceSync(ctx, records...)
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("ProduceSync[%d]: %v", i, r.Err)
		}
	}
}

// TestFranzGoMTLSWithoutClientCertRejected verifies the negative path:
// a client that connects to a RequireAndVerifyClientCert listener
// without a cert MUST fail the TLS handshake. Guards against
// "RequireAndVerifyClientCert silently degrades to opportunistic"
// regressions.
func TestFranzGoMTLSWithoutClientCertRejected(t *testing.T) {
	bundle, err := tlscerts.NewBundle("127.0.0.1", "rejected-client.skafka.local")
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	writeMTLSAuthFiles(t, dir, "alice", "alice-client.skafka.local", topicMTLS)
	addr, stop := startMTLSBroker(t, dir, bundle)
	defer stop()

	// Trust the server's CA, but DON'T present a client cert.
	clientTLS := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    bundle.CAPool,
		ServerName: "127.0.0.1",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DialTLSConfig(clientTLS),
		kgo.RequestRetries(0),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	res := cl.ProduceSync(ctx, &kgo.Record{Topic: topicMTLS, Value: []byte("should-fail")})
	for _, r := range res {
		if r.Err == nil {
			t.Error("ProduceSync without client cert unexpectedly succeeded")
		}
	}
}
