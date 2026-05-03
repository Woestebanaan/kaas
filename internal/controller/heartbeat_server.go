package controller

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/woestebanaan/skafka/pkg/heartbeatpb"
)

// HeartbeatServer is the controller-side endpoint of the bidi heartbeat
// protocol defined in proto/heartbeat.proto. It implements
// heartbeatpb.ControllerHeartbeatServer (registered into a gRPC server by
// heartbeatpb.RegisterControllerHeartbeatServer).
//
// Responsibilities:
//   - Receive BrokerStatus on the upstream half of the bidi stream and
//     update per-broker liveness (lastSeen, lastSeenAssignmentVersion).
//   - Push ControllerCommand on the downstream half: PING for keep-alive,
//     ASSIGNMENT_CHANGED to trigger an immediate file re-read on brokers,
//     LEAVING when the controller is shutting down gracefully.
//
// HeartbeatServer does NOT decide *what* to assign — that's the assignment
// loop's job. It is the messenger.
type HeartbeatServer struct {
	heartbeatpb.UnimplementedControllerHeartbeatServer

	pingInterval time.Duration

	mu      sync.Mutex
	clients map[string]*clientState // brokerID → state
	// pendingVersion is the latest assignmentVersion the controller has
	// written. PushAssignmentChanged(v) updates this and signals every
	// connected client to re-read.
	pendingVersion uint64
}

// clientState tracks one connected broker's stream + last-seen status.
type clientState struct {
	send         chan *heartbeatpb.ControllerCommand
	lastSeen     time.Time
	lastVersion  uint64
	activeGroups []string // most recently reported in BrokerStatus.active_groups
}

// NewHeartbeatServer constructs a server. pingInterval (default 1s) is how
// often the server pushes a PING when no ASSIGNMENT_CHANGED is queued.
func NewHeartbeatServer() *HeartbeatServer {
	return &HeartbeatServer{
		pingInterval: 1 * time.Second,
		clients:      make(map[string]*clientState),
	}
}

// WithPingInterval overrides the keep-alive cadence (test hook).
func (s *HeartbeatServer) WithPingInterval(d time.Duration) *HeartbeatServer {
	s.pingInterval = d
	return s
}

// Stream is the bidi RPC handler. The first BrokerStatus carries the
// broker's identity (broker_id); subsequent messages refresh liveness.
// One goroutine handles upstream Recv; another pushes ControllerCommand
// downstream from the per-client send channel.
func (s *HeartbeatServer) Stream(stream heartbeatpb.ControllerHeartbeat_StreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	brokerID := first.GetBrokerId()
	if brokerID == "" {
		return errors.New("heartbeat: first BrokerStatus must carry broker_id")
	}

	state := &clientState{
		send:         make(chan *heartbeatpb.ControllerCommand, 4),
		lastSeen:     time.Now(),
		lastVersion:  first.GetLastSeenAssignmentVersion(),
		activeGroups: append([]string(nil), first.GetActiveGroups()...),
	}
	s.mu.Lock()
	// If a prior stream from the same broker is still registered, replace it
	// — the new connection is authoritative, and the old goroutine will exit
	// when its stream closes.
	s.clients[brokerID] = state
	s.mu.Unlock()

	// Cleanup on stream end.
	defer func() {
		s.mu.Lock()
		// Only remove the entry if it's still ours; a reconnect during this
		// goroutine's lifetime may have replaced it.
		if cur, ok := s.clients[brokerID]; ok && cur == state {
			delete(s.clients, brokerID)
		}
		s.mu.Unlock()
	}()

	ctx := stream.Context()
	go s.recvLoop(ctx, stream, brokerID, state)
	return s.sendLoop(ctx, stream, state)
}

// recvLoop drains BrokerStatus messages from the broker.
func (s *HeartbeatServer) recvLoop(
	ctx context.Context,
	stream heartbeatpb.ControllerHeartbeat_StreamServer,
	brokerID string,
	state *clientState,
) {
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := stream.Recv()
		if err != nil {
			// EOF or stream error — sendLoop will see ctx done shortly.
			return
		}
		s.mu.Lock()
		// Defensive: only update if this state is still the current one.
		if cur, ok := s.clients[brokerID]; ok && cur == state {
			cur.lastSeen = time.Now()
			cur.lastVersion = msg.GetLastSeenAssignmentVersion()
			cur.activeGroups = append(cur.activeGroups[:0], msg.GetActiveGroups()...)
		}
		s.mu.Unlock()
	}
}

// sendLoop pushes ControllerCommand messages downstream. Messages come from
// the client's send channel (queued by PushAssignmentChanged) or, when idle,
// from a periodic PING ticker.
func (s *HeartbeatServer) sendLoop(
	ctx context.Context,
	stream heartbeatpb.ControllerHeartbeat_StreamServer,
	state *clientState,
) error {
	tick := time.NewTicker(s.pingInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case cmd := <-state.send:
			if err := stream.Send(cmd); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		case t := <-tick.C:
			if err := stream.Send(&heartbeatpb.ControllerCommand{
				TimestampMs: t.UnixMilli(),
				Type:        heartbeatpb.ControllerCommand_PING,
			}); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	}
}

// PushAssignmentChanged signals every connected broker to re-read
// /data/__cluster/assignment.json. Best-effort: if a broker's send channel
// is full, the message is dropped (the broker will pick up the change via
// its 1s mtime poll within ~1s). Called by the assignment loop after a
// successful Write.
func (s *HeartbeatServer) PushAssignmentChanged(version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingVersion = version
	for _, c := range s.clients {
		select {
		case c.send <- &heartbeatpb.ControllerCommand{
			TimestampMs:        time.Now().UnixMilli(),
			Type:               heartbeatpb.ControllerCommand_ASSIGNMENT_CHANGED,
			AssignmentVersion:  version,
		}:
		default:
			// Send buffer full — broker is slow consuming. The 1s mtime poll
			// will still pick up the change.
		}
	}
}

// PushLeaving notifies every broker that the controller is shutting down
// gracefully. Brokers should switch to looking for a new controller via
// the Lease informer rather than waiting for heartbeat timeout.
func (s *HeartbeatServer) PushLeaving() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clients {
		select {
		case c.send <- &heartbeatpb.ControllerCommand{
			TimestampMs: time.Now().UnixMilli(),
			Type:        heartbeatpb.ControllerCommand_LEAVING,
		}:
		default:
		}
	}
}

// BrokerLastSeen reports the wall-clock time of the most recent BrokerStatus
// from the given broker. Returns the zero Time and false when the broker is
// not currently connected.
func (s *HeartbeatServer) BrokerLastSeen(brokerID string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clients[brokerID]
	if !ok {
		return time.Time{}, false
	}
	return c.lastSeen, true
}

// ConnectedBrokers returns a snapshot of every broker currently streaming
// to this controller.
func (s *HeartbeatServer) ConnectedBrokers() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.clients))
	for id := range s.clients {
		out = append(out, id)
	}
	return out
}

// ActiveGroups returns the union of consumer group IDs every connected
// broker reports in its BrokerStatus.active_groups. The Phase 5
// AssignmentLoop consumes this as its GroupSource: any group at least
// one broker is currently coordinating shows up in the next assignment.
//
// The result is deduplicated; order is unspecified. Returns an empty
// slice when no broker reports any active group.
func (s *HeartbeatServer) ActiveGroups() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]struct{})
	for _, c := range s.clients {
		for _, g := range c.activeGroups {
			seen[g] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for g := range seen {
		out = append(out, g)
	}
	return out
}
