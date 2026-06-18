package kafkacompat

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/pbkdf2"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	scram "github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/internal/protocol"
)

const scramIterations = 8192

// scramCreds derives the SCRAM-SHA-512 server-side keys for the given
// password. Mirrors operator/controllers.computeScram so the test
// produces credentials.json content the loader will accept verbatim.
func scramCreds(t *testing.T, password string) (saltB64, storedKeyB64, serverKeyB64 string, iters int) {
	t.Helper()
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	saltedPw := pbkdf2.Key([]byte(password), salt, scramIterations, 64, sha512.New)
	clientKey := hmacSHA512(saltedPw, []byte("Client Key"))
	storedKey := sha512Sum(clientKey)
	serverKey := hmacSHA512(saltedPw, []byte("Server Key"))
	return base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(storedKey),
		base64.StdEncoding.EncodeToString(serverKey),
		scramIterations
}

func hmacSHA512(key, msg []byte) []byte {
	mac := hmac.New(sha512.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

func sha512Sum(b []byte) []byte {
	s := sha512.Sum512(b)
	return s[:]
}

// startAuthBroker spins a self-contained broker on a free port with
// RealAuthEngine wired against the given credentials/ACLs in dataDir.
// Returns the address + a stop function.
func startAuthBroker(t *testing.T, dataDir string, requireSASL bool) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
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
		broker.Config{BrokerID: 0, Host: "127.0.0.1", Port: int32(port), ClusterID: "scram-test"},
		broker.NewMemoryStorage(),
		broker.NewLocalLeaseManager(),
		authEng,
	)
	b.AddTopic(topicSCRAM, 1)

	d := protocol.NewDispatcher()
	// gh #124: Dispatcher.RequireSASL is gone; pre-SASL gating is per
	// engine via AuthEngineSelector.RequiresPreAuth. When `requireSASL`
	// is true the test wires the RealAuthEngine through the dispatcher,
	// which returns RequiresPreAuth=true; when false, no selector is
	// wired and the gate is open (preserves the old `false` branch).
	if requireSASL {
		d.SetAuthEngines(auth.NewSingleAuthEngine(authEng))
	}
	b.RegisterHandlers(d)

	srv := protocol.NewServer(protocol.Config{
		Listeners: []protocol.ListenerConfig{{Name: "internal", Addr: addr}},
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

const topicSCRAM = "test-topic-scram"

// writeAuthFiles drops minimal credentials.json + acls.json files into
// dataDir/__cluster/ so RealAuthEngine has something to load. Returns
// the password chosen for `username`.
func writeAuthFiles(t *testing.T, dataDir, username, password, topic string) {
	t.Helper()
	clusterDir := filepath.Join(dataDir, "__cluster")
	if err := os.MkdirAll(clusterDir, 0755); err != nil {
		t.Fatal(err)
	}

	saltB64, storedKeyB64, serverKeyB64, iters := scramCreds(t, password)

	credentialsJSON := fmt.Sprintf(`{
  "version": 1,
  "users": [{
    "username": %q,
    "authType": "scram-sha-512",
    "scram": {
      "salt": %q,
      "storedKey": %q,
      "serverKey": %q,
      "iterations": %d
    }
  }]
}`, username, saltB64, storedKeyB64, serverKeyB64, iters)
	if err := os.WriteFile(filepath.Join(clusterDir, "credentials.json"), []byte(credentialsJSON), 0644); err != nil {
		t.Fatal(err)
	}

	// Wide-open ACLs for User:<username> on this topic + any consumer
	// group. Each entry in acls[] is one (principal, resource, operations,
	// permission) tuple — mirrors internal/auth/loader.go's expected shape,
	// which is flatter than the KafkaAcl CRD's nested rules[].
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
      "resource": {"type":"group","name":"","patternType":"prefix"},
      "operations":["Read","Describe"],
      "permission":"Allow"
    },
    {
      "principal": %q,
      "resource": {"type":"cluster","name":"kafka-cluster","patternType":"literal"},
      "operations":["Describe","DescribeConfigs"],
      "permission":"Allow"
    }
  ]
}`, principal, topic, principal, principal)
	if err := os.WriteFile(filepath.Join(clusterDir, "acls.json"), []byte(aclsJSON), 0644); err != nil {
		t.Fatal(err)
	}
}

// TestFranzGoSCRAMRoundTrip authenticates franz-go via SASL/SCRAM-SHA-512
// against a broker running RealAuthEngine, then produces and consumes
// records. Closes the loop on the auth state machine — the unit test
// in internal/auth/scram_test.go covers the message exchange in
// isolation; this test proves real client wire-framing works.
func TestFranzGoSCRAMRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const username, password = "alice", "secret-pw-12345"
	writeAuthFiles(t, dir, username, password, topicSCRAM)

	addr, stop := startAuthBroker(t, dir, true /* requireSASL */)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: username, Pass: password}, nil
		})),
		// No compression so the produce/consume comparison stays simple.
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	const N = 20
	records := make([]*kgo.Record, N)
	for i := 0; i < N; i++ {
		records[i] = &kgo.Record{
			Topic: topicSCRAM,
			Value: []byte(fmt.Sprintf("scram-%02d", i)),
		}
	}
	results := cl.ProduceSync(ctx, records...)
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("ProduceSync[%d]: %v", i, r.Err)
		}
	}

	// Consume on a separate client so the ProducerID isn't conflated.
	cons, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: username, Pass: password}, nil
		})),
		kgo.ConsumeTopics(topicSCRAM),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("NewClient consumer: %v", err)
	}
	defer cons.Close()

	got := 0
	deadline := time.Now().Add(10 * time.Second)
	for got < N && time.Now().Before(deadline) {
		fetches := cons.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("PollFetches: %v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			want := fmt.Sprintf("scram-%02d", got)
			if string(r.Value) != want {
				t.Errorf("record %d: %q want %q", got, r.Value, want)
			}
			got++
		})
	}
	if got != N {
		t.Fatalf("got %d records, want %d", got, N)
	}
}

// TestFranzGoSCRAMWrongPasswordRejected proves the negative path: a
// wrong-password connection cannot produce records. This is what
// guards against silent auth-bypass regressions where the SCRAM
// handshake might erroneously accept any password.
func TestFranzGoSCRAMWrongPasswordRejected(t *testing.T) {
	dir := t.TempDir()
	writeAuthFiles(t, dir, "alice", "correct-password", topicSCRAM)

	addr, stop := startAuthBroker(t, dir, true /* requireSASL */)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: "alice", Pass: "wrong-password"}, nil
		})),
		kgo.RequestRetries(0), // fail fast — don't retry the auth error
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: topicSCRAM,
		Value: []byte("should-fail"),
	})
	for _, r := range results {
		if r.Err == nil {
			t.Error("ProduceSync with wrong password unexpectedly succeeded")
		}
	}
}
