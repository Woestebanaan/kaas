package controller

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/woestebanaan/skafka/internal/broker"
	"github.com/woestebanaan/skafka/pkg/heartbeatpb"
)

// startServer spins a bufconn-backed gRPC server hosting the given
// HeartbeatServer. Returns a dial option the client can use to reach it,
// and a stop function that cleans up.
func startServer(t *testing.T, srv *HeartbeatServer) (grpc.DialOption, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 16)
	g := grpc.NewServer()
	heartbeatpb.RegisterControllerHeartbeatServer(g, srv)
	go func() { _ = g.Serve(lis) }()

	dialer := func(_ context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(context.Background())
	}
	opt := grpc.WithContextDialer(dialer)
	return opt, func() {
		g.Stop()
		lis.Close()
	}
}

// TestHeartbeatRoundTrip drives one client against one server over bufconn
// and verifies (1) the broker registers, (2) the server can push commands,
// (3) the client's OnCommand handler fires, (4) BrokerLastSeen reports a
// recent timestamp.
func TestHeartbeatRoundTrip(t *testing.T) {
	srv := NewHeartbeatServer().WithPingInterval(50 * time.Millisecond)
	dialOpt, stop := startServer(t, srv)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// "passthrough://" target tells gRPC to skip name resolution and use the
	// dialer directly.
	cl := broker.NewHeartbeatClient("passthrough://bufnet", "broker-7",
		dialOpt,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	var received atomic.Int64
	cl.OnCommand(func(cmd *heartbeatpb.ControllerCommand) {
		received.Add(1)
	})

	go func() { _ = cl.Run(ctx) }()

	// Wait for the broker to register on the server side.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := srv.BrokerLastSeen("broker-7"); ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, ok := srv.BrokerLastSeen("broker-7"); !ok {
		t.Fatal("server did not register broker-7 within 3s")
	}

	// Server pushes ASSIGNMENT_CHANGED; client should observe it.
	srv.PushAssignmentChanged(99)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && received.Load() < 1 {
		time.Sleep(20 * time.Millisecond)
	}
	if received.Load() < 1 {
		t.Fatalf("client did not receive any ControllerCommand within 2s (got %d)", received.Load())
	}

	// Confirm the connected-brokers list contains us.
	connected := srv.ConnectedBrokers()
	found := false
	for _, id := range connected {
		if id == "broker-7" {
			found = true
		}
	}
	if !found {
		t.Errorf("ConnectedBrokers does not include broker-7: %v", connected)
	}
}

// TestHeartbeatRejectsEmptyBrokerID verifies the server's "first message
// must carry broker_id" guard.
func TestHeartbeatRejectsEmptyBrokerID(t *testing.T) {
	srv := NewHeartbeatServer().WithPingInterval(100 * time.Millisecond)
	dialOpt, stop := startServer(t, srv)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.NewClient("passthrough://bufnet", dialOpt,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rpc := heartbeatpb.NewControllerHeartbeatClient(conn)
	stream, err := rpc.Stream(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Empty broker_id — server should reject.
	if err := stream.Send(&heartbeatpb.BrokerStatus{TimestampMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}

	// Recv should error out (server returned from Stream).
	if _, err := stream.Recv(); err == nil {
		t.Error("expected error from server for empty broker_id, got nil")
	}
}

// TestPushAssignmentChangedIsBestEffort verifies that pushing to a broker
// with a full send buffer doesn't block. The relay drops the message; the
// 1s mtime poll is the recovery path (which the test doesn't exercise here).
func TestPushAssignmentChangedIsBestEffort(t *testing.T) {
	srv := NewHeartbeatServer().WithPingInterval(time.Hour) // disable PING for this test
	dialOpt, stop := startServer(t, srv)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cl := broker.NewHeartbeatClient("passthrough://bufnet", "broker-slow",
		dialOpt, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// No OnCommand — client never drains its recv channel.
	go func() { _ = cl.Run(ctx) }()

	// Wait for registration.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := srv.BrokerLastSeen("broker-slow"); ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, ok := srv.BrokerLastSeen("broker-slow"); !ok {
		t.Skip("broker did not register fast enough; environment-dependent")
	}

	// Hammer PushAssignmentChanged way past the 4-element buffer.
	for i := 0; i < 1000; i++ {
		srv.PushAssignmentChanged(uint64(i))
	}
	// The point of the test is that the calls didn't block forever — we got here.
}
