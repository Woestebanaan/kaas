package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/pkg/heartbeatpb"
)

// CommandHandler is invoked for every ControllerCommand received from the
// controller. It runs synchronously on the heartbeat client's recv goroutine,
// so it must not block on heavy work.
type CommandHandler func(*heartbeatpb.ControllerCommand)

// HeartbeatClient is the broker-side endpoint of the bidi heartbeat protocol.
// One client per broker process; long-lived; reconnects on disconnect with
// exponential backoff.
//
// Send() is the upstream half — typically called every heartbeatInterval
// (default 1s) by the BrokerCoordinator with a fresh BrokerStatus snapshot.
// The downstream half is consumed via OnCommand callback.
//
// LastReceived() returns the wall-clock time of the most recent message
// received from the controller. Self-fencing reads this to decide whether
// the controller is still alive.
type HeartbeatClient struct {
	target     string             // "host:port", used when targetFunc is nil
	targetFunc func() string      // re-resolved each runOnce to follow controller changes
	brokerID   string

	dialOpts []grpc.DialOption

	mu       sync.Mutex
	stream   heartbeatpb.ControllerHeartbeat_StreamClient
	cancelFn context.CancelFunc

	onCommand    CommandHandler
	lastReceived atomic.Int64 // unix nanoseconds

	connectInterval time.Duration // base reconnect backoff
}

// NewHeartbeatClient builds a client. target is a gRPC address ("host:port").
// brokerID is sent in the first BrokerStatus to identify this broker.
//
// dialOpts override the default insecure transport — for production you'll
// want gRPC credentials configured for mTLS to the controller.
func NewHeartbeatClient(target, brokerID string, dialOpts ...grpc.DialOption) *HeartbeatClient {
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	return &HeartbeatClient{
		target:          target,
		brokerID:        brokerID,
		dialOpts:        dialOpts,
		connectInterval: 500 * time.Millisecond,
	}
}

// OnCommand registers the handler invoked for each ControllerCommand. Replaces
// any previously registered handler.
func (c *HeartbeatClient) OnCommand(h CommandHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onCommand = h
}

// WithTargetFunc replaces the static target with a resolver that's called
// at the start of every reconnect cycle. Used to follow the controller:
// the resolver reads ControllerWatch.CurrentHolder + BrokerRegistry to
// produce "host:peerHeartbeatPort" for whoever currently holds the
// cluster Lease. An empty return triggers a backoff retry rather than a
// dial — useful when no holder is known yet (cluster startup).
func (c *HeartbeatClient) WithTargetFunc(fn func() string) *HeartbeatClient {
	c.targetFunc = fn
	return c
}

// resolveTarget returns the dial target for the next reconnect cycle.
// Honours the dynamic resolver when set; otherwise falls back to the
// static target field.
func (c *HeartbeatClient) resolveTarget() string {
	if c.targetFunc != nil {
		return c.targetFunc()
	}
	return c.target
}

// Run blocks until ctx is cancelled, maintaining a long-lived bidi stream
// to the controller. On disconnect, reconnects with exponential backoff
// (capped at 5s). Returns nil on clean shutdown.
func (c *HeartbeatClient) Run(ctx context.Context) error {
	backoff := c.connectInterval
	const maxBackoff = 5 * time.Second

	for {
		if ctx.Err() != nil {
			return nil
		}
		err := c.runOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		// Exponential backoff capped at 5s.
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		_ = err // surfaced via observability when wired in step 5
	}
}

func (c *HeartbeatClient) runOnce(ctx context.Context) error {
	target := c.resolveTarget()
	if target == "" {
		// No controller known yet — return so the outer Run loop's
		// backoff timer fires; on the next attempt the resolver may
		// have been populated by ControllerWatch.
		return errors.New("heartbeat: no controller target yet")
	}
	conn, err := grpc.NewClient(target, c.dialOpts...)
	if err != nil {
		return fmt.Errorf("heartbeat: dial: %w", err)
	}
	defer conn.Close()

	rpc := heartbeatpb.NewControllerHeartbeatClient(conn)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := rpc.Stream(streamCtx)
	if err != nil {
		return fmt.Errorf("heartbeat: open stream: %w", err)
	}

	c.mu.Lock()
	c.stream = stream
	c.cancelFn = cancel
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.stream = nil
		c.cancelFn = nil
		c.mu.Unlock()
	}()

	// The first upstream message MUST carry broker_id (the server uses it
	// to register the connection in its client map). Send a minimal status
	// immediately so the controller's send loop has someone to write to.
	if err := stream.Send(&heartbeatpb.BrokerStatus{
		BrokerId:    c.brokerID,
		TimestampMs: time.Now().UnixMilli(),
	}); err != nil {
		return fmt.Errorf("heartbeat: initial send: %w", err)
	}

	// recvLoop consumes downstream commands until the stream closes.
	for {
		cmd, err := stream.Recv()
		if err != nil {
			return err
		}
		now := time.Now()
		c.lastReceived.Store(now.UnixNano())

		// Heartbeat RTT: when the controller's PING echoes back the
		// broker_status_timestamp_ms we sent in our last BrokerStatus,
		// (now - that) is the round-trip. Skip when zero — the very
		// first PING after stream open precedes any BrokerStatus the
		// controller has time to record.
		if echo := cmd.GetBrokerStatusTimestampMs(); echo > 0 {
			rtt := now.Sub(time.UnixMilli(echo)).Seconds()
			if rtt >= 0 {
				observability.Global().HeartbeatRTT.Record(streamCtx, rtt)
			}
		}

		c.mu.Lock()
		h := c.onCommand
		c.mu.Unlock()
		if h != nil {
			h(cmd)
		}
	}
}

// Send pushes a BrokerStatus upstream. Returns an error if the stream isn't
// connected yet; the caller's heartbeat tick should swallow it (a reconnect
// is already in progress).
func (c *HeartbeatClient) Send(status *heartbeatpb.BrokerStatus) error {
	c.mu.Lock()
	stream := c.stream
	c.mu.Unlock()
	if stream == nil {
		return errors.New("heartbeat: not connected")
	}
	if status.GetBrokerId() == "" {
		status.BrokerId = c.brokerID
	}
	return stream.Send(status)
}

// LastReceived returns the wall-clock time of the most recent message
// received from the controller. Zero Time when nothing has been received yet.
// Self-fence reads this to evaluate IsHeartbeatFresh.
func (c *HeartbeatClient) LastReceived() time.Time {
	ns := c.lastReceived.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// IsConnected reports whether a stream is currently open. Useful in tests
// and observability metrics.
func (c *HeartbeatClient) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stream != nil
}
