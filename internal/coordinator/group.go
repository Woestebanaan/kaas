package coordinator

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

type groupState int

const (
	stateEmpty               groupState = iota
	statePreparingRebalance
	stateCompletingRebalance
	stateStable
	stateDead
)

func (s groupState) String() string {
	switch s {
	case stateEmpty:
		return "Empty"
	case statePreparingRebalance:
		return "PreparingRebalance"
	case stateCompletingRebalance:
		return "CompletingRebalance"
	case stateStable:
		return "Stable"
	case stateDead:
		return "Dead"
	}
	return "Unknown"
}

type groupMember struct {
	id                 string
	clientID           string
	clientHost         string
	groupInstanceID    string
	sessionTimeoutMs   int32
	rebalanceTimeoutMs int32
	protocols          []api.JoinGroupProtocol
	assignment         []byte
	heartbeatTimer     *time.Timer
}

type joinWaiter struct {
	memberID string
	ch       chan joinResult
}

type joinResult struct {
	resp *api.JoinGroupResponse
}

type syncState struct {
	mu          sync.Mutex
	assignments map[string][]byte
	done        chan struct{}
}

// group holds the in-memory state for one consumer group.
type group struct {
	id           string
	protocolType string

	mu             sync.Mutex
	state          groupState
	generationID   int32
	protocolName   string
	leaderID       string
	members        map[string]*groupMember
	joinWaiters    []joinWaiter
	currentSync    *syncState
	rebalanceTimer *time.Timer

	// pendingMembers holds memberIDs assigned in response to a first
	// JoinGroup with empty memberID under KIP-394 (gh #98 #2). Each
	// pending entry has a short cleanup timer; if the client doesn't
	// follow up with a JoinGroup using the assigned ID within
	// initialRebalanceDelayMs the entry is dropped. Lets the
	// coordinator fence "zombie" reconnects from clients that lost
	// their memberID across a network blip without polluting the
	// rebalance — Apache's `pendingMembers` set in GroupMetadata.scala.
	pendingMembers map[string]*time.Timer
}

func newGroup(id string) *group {
	return &group{
		id:             id,
		members:        make(map[string]*groupMember),
		pendingMembers: make(map[string]*time.Timer),
	}
}

// join handles a JoinGroup request. Blocks until the rebalance completes.
// clientID comes from the connection state (not the request body).
// version is the JoinGroupRequest API version, used to gate KIP-394
// (MEMBER_ID_REQUIRED) on v4+.
func (g *group) join(req *api.JoinGroupRequest, version int16, clientID string) *api.JoinGroupResponse {
	g.mu.Lock()

	memberID := req.MemberID

	// KIP-394 (gh #98 #2): a dynamic member (no GroupInstanceID)
	// joining with empty memberID at v4+ gets a freshly-assigned ID
	// back with ErrMemberIDRequired. The client must retry with the
	// assigned ID; only THEN does the join trigger a rebalance.
	// Without this, a network-blipped client that retries a
	// JoinGroup with empty memberID ends up registered TWICE in
	// g.members on consecutive attempts — duplicate-member problem
	// that amplifies the gh #98 #1 leader-session-expiry race.
	dynamic := req.GroupInstanceID == ""
	if memberID == "" && version >= 4 && dynamic {
		assigned := generateMemberID(clientID)
		g.registerPendingMember(assigned)
		g.mu.Unlock()
		return &api.JoinGroupResponse{
			ErrorCode: int16(codec.ErrMemberIDRequired),
			MemberID:  assigned,
		}
	}

	// Static member or v0-v3 client: legacy "assign memberID inline"
	// path. Static members re-identify themselves via GroupInstanceID
	// across reconnects, so we don't need the KIP-394 fence here.
	if memberID == "" {
		memberID = generateMemberID(clientID)
	}

	// Promote from pendingMembers if this is the follow-up to a
	// MEMBER_ID_REQUIRED response: stop the cleanup timer so the
	// member doesn't get culled mid-rebalance.
	if t, ok := g.pendingMembers[memberID]; ok {
		t.Stop()
		delete(g.pendingMembers, memberID)
	}

	// Upsert member.
	m, exists := g.members[memberID]
	if !exists {
		m = &groupMember{id: memberID, clientID: clientID}
		g.members[memberID] = m
	}
	m.groupInstanceID = req.GroupInstanceID
	m.sessionTimeoutMs = req.SessionTimeoutMs
	m.rebalanceTimeoutMs = req.RebalanceTimeoutMs
	m.protocols = req.Protocols
	if req.ProtocolType != "" {
		g.protocolType = req.ProtocolType
	}

	g.resetHeartbeatTimer(m)

	// isNewGroup tracks whether this is the first member of a brand-new group.
	// For new groups we never complete early — we always wait for the rebalance timer
	// so that concurrent joiners have a chance to register before the rebalance fires.
	isNewGroup := g.state == stateEmpty

	switch g.state {
	case stateEmpty:
		g.state = statePreparingRebalance
		g.startRebalanceTimer(true)
	case stateStable, stateCompletingRebalance:
		g.state = statePreparingRebalance
		g.startRebalanceTimer(false)
		// Cancel pending sync so blocked SyncGroup calls unblock with REBALANCE_IN_PROGRESS.
		if g.currentSync != nil {
			select {
			case <-g.currentSync.done:
			default:
				close(g.currentSync.done)
			}
			g.currentSync = nil
		}
	case statePreparingRebalance:
		// Already rebalancing — just add/update the member.
	}

	ch := make(chan joinResult, 1)
	g.joinWaiters = append(g.joinWaiters, joinWaiter{memberID: memberID, ch: ch})
	if !isNewGroup {
		g.maybeCompleteRebalance()
	}

	g.mu.Unlock()

	result := <-ch
	return result.resp
}

func (g *group) maybeCompleteRebalance() {
	if g.state == statePreparingRebalance && len(g.joinWaiters) >= len(g.members) {
		g.completeRebalance()
	}
}

func (g *group) completeRebalance() {
	// completeRebalance fires from two paths: the rebalance timer's
	// time.AfterFunc (no inbound ctx) and maybeCompleteRebalance from
	// inside join() (where the request ctx exists but isn't threaded
	// through). Start a fresh root span — operators look these up by
	// group_id + generation, not by parent trace.
	_, span := observability.Tracer().Start(context.Background(),
		"coordinator.complete_rebalance",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("group.id", g.id),
			attribute.Int("members", len(g.members)),
			attribute.Int("waiters", len(g.joinWaiters)),
		),
	)
	defer span.End()

	if g.rebalanceTimer != nil {
		g.rebalanceTimer.Stop()
		g.rebalanceTimer = nil
	}
	g.state = stateCompletingRebalance
	g.generationID++
	g.protocolName = selectProtocol(g.members, g.joinWaiters)
	span.SetAttributes(
		attribute.Int("generation", int(g.generationID)),
		attribute.String("protocol.name", g.protocolName),
		attribute.String("protocol.type", g.protocolType),
	)

	observability.Global().GroupRebalances.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("consumer_group", g.id)))

	if len(g.joinWaiters) > 0 {
		g.leaderID = g.joinWaiters[0].memberID
	}

	g.currentSync = &syncState{
		assignments: make(map[string][]byte),
		done:        make(chan struct{}),
	}

	for _, w := range g.joinWaiters {
		resp := &api.JoinGroupResponse{
			GenerationID: g.generationID,
			ProtocolName: g.protocolName,
			ProtocolType: g.protocolType,
			Leader:       g.leaderID,
			MemberID:     w.memberID,
		}
		if w.memberID == g.leaderID {
			for _, ww := range g.joinWaiters {
				m := g.members[ww.memberID]
				jm := api.JoinGroupMember{MemberID: ww.memberID}
				if m != nil {
					jm.GroupInstanceID = m.groupInstanceID
					for _, p := range m.protocols {
						if p.Name == g.protocolName {
							jm.Metadata = p.Metadata
							break
						}
					}
				}
				resp.Members = append(resp.Members, jm)
			}
		}
		w.ch <- joinResult{resp: resp}
	}
	g.joinWaiters = nil
}

// sync handles a SyncGroup request. Blocks until the leader delivers assignments.
func (g *group) sync(req *api.SyncGroupRequest) *api.SyncGroupResponse {
	g.mu.Lock()

	if _, ok := g.members[req.MemberID]; !ok {
		g.mu.Unlock()
		return &api.SyncGroupResponse{ErrorCode: int16(codec.ErrUnknownMemberId)}
	}
	if req.GenerationID != g.generationID {
		g.mu.Unlock()
		return &api.SyncGroupResponse{ErrorCode: int16(codec.ErrIllegalGeneration)}
	}
	// Apache's GroupCoordinator (gh #111): follower SyncGroup is valid in
	// CompletingRebalance (wait for leader) AND Stable (assignments
	// already cached — the leader finished first). Skafka previously
	// rejected Stable with ErrRebalanceInProgress, which made any
	// follower whose SyncGroupRequest arrived after the leader's
	// re-join unnecessarily; with concurrent multi-pod consumers
	// (kafka-consumer-perf-test parallelism=4) that race fired often
	// enough to wedge the rebalance.
	if g.state != stateCompletingRebalance && g.state != stateStable {
		g.mu.Unlock()
		return &api.SyncGroupResponse{ErrorCode: int16(codec.ErrRebalanceInProgress)}
	}

	ss := g.currentSync
	isLeader := req.MemberID == g.leaderID
	// Snapshot the group's selected protocol so the response can echo
	// it back to the client. Apache populates these on success-path
	// SyncGroupResponses and the Java client validates the response's
	// ProtocolType against what it sent in JoinGroup — an empty
	// ProtocolType raises InconsistentGroupProtocolException. Pre-fix
	// skafka always returned "" here, but the gh #96 / gh #98 #6
	// nullable→non-nullable encoder change forced the field to land on
	// the wire as an empty string instead of null, which the Java
	// client surfaces as a real validation error. Echoing the actual
	// values is what the protocol always asked us to do.
	protocolType := g.protocolType
	protocolName := g.protocolName

	if isLeader {
		ss.mu.Lock()
		for _, a := range req.Assignments {
			ss.assignments[a.MemberID] = a.Assignment
		}
		// gh #111: Apache fills any member missing from the leader's
		// payload with explicit empty bytes + warn log
		// (GroupCoordinator.scala:605-609). This makes the broker's
		// view explicit instead of inferring from a nil map lookup,
		// and any future regression that drops a member shows up in
		// the broker logs rather than as an opaque
		// IllegalStateException on the client.
		for memberID := range g.members {
			if _, ok := ss.assignments[memberID]; !ok {
				ss.assignments[memberID] = []byte{}
				slog.Warn("syncgroup: leader omitted member from assignment, sending empty bytes",
					"group", g.id,
					"member", memberID,
					"generation", g.generationID,
					"leader", g.leaderID)
			}
		}
		ss.mu.Unlock()
		g.state = stateStable
		close(ss.done)
	}

	memberID := req.MemberID
	g.mu.Unlock()

	// Block until leader closes ss.done (or it was already closed).
	<-ss.done

	ss.mu.Lock()
	assignment := ss.assignments[memberID]
	ss.mu.Unlock()

	return &api.SyncGroupResponse{
		Assignment:   assignment,
		ProtocolType: protocolType,
		ProtocolName: protocolName,
	}
}

// heartbeat handles a Heartbeat request.
func (g *group) heartbeat(req *api.HeartbeatRequest) int16 {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Apache GroupMetadata.scala docs the Empty-state contract: any
	// Heartbeat against a group with no members must surface
	// UNKNOWN_MEMBER_ID so disconnected clients re-bootstrap rather
	// than silently keep heartbeating to a vanished group (gh #98 #7).
	// In skafka, stateEmpty is reached when the last member leaves
	// (LeaveGroup) or is evicted (session timeout); a stray heartbeat
	// from a client whose memberID was just dropped should not see
	// ErrNone just because g.members is empty.
	if g.state == stateEmpty {
		return int16(codec.ErrUnknownMemberId)
	}

	m, ok := g.members[req.MemberID]
	if !ok {
		return int16(codec.ErrUnknownMemberId)
	}
	if req.GenerationID != g.generationID {
		return int16(codec.ErrIllegalGeneration)
	}

	g.resetHeartbeatTimer(m)

	switch g.state {
	case statePreparingRebalance, stateCompletingRebalance:
		return int16(codec.ErrRebalanceInProgress)
	case stateDead:
		return int16(codec.ErrUnknownMemberId)
	}
	return int16(codec.ErrNone)
}

// leave handles a LeaveGroup request. Returns per-member error codes.
func (g *group) leave(memberIDs []string) []api.LeaveMemberResponse {
	g.mu.Lock()
	defer g.mu.Unlock()

	var responses []api.LeaveMemberResponse
	for _, mid := range memberIDs {
		if _, ok := g.members[mid]; !ok {
			responses = append(responses, api.LeaveMemberResponse{MemberID: mid, ErrorCode: int16(codec.ErrUnknownMemberId)})
			continue
		}
		g.removeMember(mid)
		responses = append(responses, api.LeaveMemberResponse{MemberID: mid})
	}

	if len(g.members) == 0 {
		g.state = stateEmpty
		if g.rebalanceTimer != nil {
			g.rebalanceTimer.Stop()
			g.rebalanceTimer = nil
		}
	} else if g.state == stateStable {
		g.state = statePreparingRebalance
		g.startRebalanceTimer(false)
	}

	return responses
}

// registerPendingMember adds memberID to g.pendingMembers and starts
// a short cleanup timer. The timer fires if the client never follows
// up with the second JoinGroup, dropping the assigned memberID so it
// can be reused. Caller must hold g.mu.
//
// Cleanup-timer duration mirrors initialRebalanceDelayMs (3s) — the
// client is expected to retry immediately, so anything longer is
// just leaked memberID space.
func (g *group) registerPendingMember(memberID string) {
	if old, ok := g.pendingMembers[memberID]; ok {
		old.Stop()
	}
	g.pendingMembers[memberID] = time.AfterFunc(
		time.Duration(initialRebalanceDelayMs)*time.Millisecond,
		func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			delete(g.pendingMembers, memberID)
		},
	)
}

// removeMember evicts a member from the group state: stops its
// heartbeat timer, deletes from g.members, and drains the matching
// join-waiter (notifying the blocked join() goroutine with
// UNKNOWN_MEMBER_ID so it doesn't deadlock). Does NOT touch group
// state — caller decides whether to transition to Empty or restart
// a rebalance timer based on what's left. Idempotent: calling
// twice is a no-op the second time. Caller must hold g.mu.
//
// Pre-gh #98 #1 the heartbeat-timer AfterFunc deleted from
// g.members but did NOT drain g.joinWaiters; if the evicted member
// was joinWaiters[0] (the pending leader), the next
// completeRebalance returned protocolName="" because
// selectProtocol's `members[waiters[0].memberID]` was nil. The
// blocked join() goroutine for that waiter sat on <-ch forever —
// memory leak + potential deadlock at scale.
//
// With the waiter drained here, completeRebalance sees a
// joinWaiters slice containing only live members; the next live
// waiter becomes leader on the next completeRebalance. That's
// Apache's `maybeElectNewJoinedLeader` parity (#98 divergence #3).
func (g *group) removeMember(mid string) {
	m, ok := g.members[mid]
	if !ok {
		return
	}
	if m.heartbeatTimer != nil {
		m.heartbeatTimer.Stop()
		m.heartbeatTimer = nil
	}
	delete(g.members, mid)

	for i, w := range g.joinWaiters {
		if w.memberID == mid {
			g.joinWaiters = append(g.joinWaiters[:i], g.joinWaiters[i+1:]...)
			// ch is buffered (cap 1) and only ever populated by
			// completeRebalance, which removes the waiter from the
			// slice in the same critical section. So if we're here
			// and the waiter is still in the slice, ch is empty and
			// the send is non-blocking.
			w.ch <- joinResult{resp: &api.JoinGroupResponse{
				ErrorCode: int16(codec.ErrUnknownMemberId),
				MemberID:  mid,
			}}
			break
		}
	}
}

// describe returns a snapshot of the group for DescribeGroups.
func (g *group) describe() groupSnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	snap := groupSnapshot{
		id:           g.id,
		state:        g.state.String(),
		protocolType: g.protocolType,
		protocolName: g.protocolName,
		generationID: g.generationID,
	}
	for _, m := range g.members {
		snap.members = append(snap.members, memberSnapshot{
			memberID:        m.id,
			clientID:        m.clientID,
			groupInstanceID: m.groupInstanceID,
			assignment:      m.assignment,
		})
	}
	return snap
}

type groupSnapshot struct {
	id           string
	state        string
	protocolType string
	protocolName string
	generationID int32
	members      []memberSnapshot
}

type memberSnapshot struct {
	memberID        string
	clientID        string
	groupInstanceID string
	assignment      []byte
}

// shutdown stops every outstanding timer and unblocks any pending
// SyncGroup waiters. Called by Manager.RelinquishGroup when the
// controller reassigns the group to another broker.
func (g *group) shutdown() {
	g.mu.Lock()
	defer g.mu.Unlock()
	// Snapshot member IDs so removeMember can mutate g.members under
	// us without invalidating the iteration. removeMember drains
	// each member's joinWaiter and notifies the blocked goroutine —
	// without this the waiter would be parked on <-ch indefinitely
	// after the manager dropped the group.
	mids := make([]string, 0, len(g.members))
	for mid := range g.members {
		mids = append(mids, mid)
	}
	for _, mid := range mids {
		g.removeMember(mid)
	}
	if g.rebalanceTimer != nil {
		g.rebalanceTimer.Stop()
		g.rebalanceTimer = nil
	}
	if g.currentSync != nil {
		select {
		case <-g.currentSync.done:
		default:
			close(g.currentSync.done)
		}
		g.currentSync = nil
	}
	g.state = stateDead
	g.members = nil
}

// resetHeartbeatTimer resets (or starts) the session timeout timer for a member.
// Must be called with g.mu held.
func (g *group) resetHeartbeatTimer(m *groupMember) {
	if m.heartbeatTimer != nil {
		m.heartbeatTimer.Stop()
	}
	timeout := time.Duration(m.sessionTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	mid := m.id
	m.heartbeatTimer = time.AfterFunc(timeout, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if _, ok := g.members[mid]; !ok {
			return
		}
		// removeMember handles the waiter-drain (gh #98 #1+#3) so a
		// session-timed-out leader doesn't strand its join()
		// goroutine on <-ch and doesn't leave selectProtocol with a
		// nil leader on the next rebalance.
		g.removeMember(mid)
		if len(g.members) == 0 {
			g.state = stateEmpty
			if g.rebalanceTimer != nil {
				g.rebalanceTimer.Stop()
				g.rebalanceTimer = nil
			}
		} else if g.state == stateStable {
			g.state = statePreparingRebalance
			g.startRebalanceTimer(false)
		}
	})
}

// initialRebalanceDelayMs caps the wait for the first rebalance of a new (empty) group.
// Mirrors Kafka's group.initial.rebalance.delay.ms — short enough that a single consumer
// joining a brand-new group doesn't have to wait out the client's max.poll.interval.ms
// (which is sent as RebalanceTimeoutMs and defaults to 5 minutes).
const initialRebalanceDelayMs int32 = 3000

// startRebalanceTimer starts the rebalance completion timer.
// initial=true caps the wait at initialRebalanceDelayMs for the first rebalance of a new group.
// Must be called with g.mu held.
func (g *group) startRebalanceTimer(initial bool) {
	var maxMs int32
	for _, m := range g.members {
		if m.rebalanceTimeoutMs > maxMs {
			maxMs = m.rebalanceTimeoutMs
		}
	}
	if maxMs <= 0 {
		maxMs = 30_000 // default 30s when no member has set a timeout
	}
	if initial && maxMs > initialRebalanceDelayMs {
		maxMs = initialRebalanceDelayMs
	}
	if g.rebalanceTimer != nil {
		g.rebalanceTimer.Stop()
	}
	g.rebalanceTimer = time.AfterFunc(time.Duration(maxMs)*time.Millisecond, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.state == statePreparingRebalance && len(g.joinWaiters) > 0 {
			g.completeRebalance()
		}
	})
}

// selectProtocol finds the first protocol in the leader's preference
// list that ALL other waiters declare support for. Mirrors Apache
// Kafka's "leader-first-mutual" algorithm at the homogeneous-Java-
// consumer case.
//
// Pre-gh #98 #5 a non-leader waiter whose member entry was nil
// (e.g., the member had been evicted between joinWaiters being
// populated and selectProtocol being called) was treated as
// "supports every protocol" — silently voted yes. With the gh #98
// #1 fix, removeMember drains joinWaiters in lock-step with
// g.members so this case shouldn't fire in practice; this function
// now treats a nil member as "does not support" (defensive — fail
// closed if the invariant slips).
func selectProtocol(members map[string]*groupMember, waiters []joinWaiter) string {
	if len(waiters) == 0 {
		return ""
	}
	leader := members[waiters[0].memberID]
	if leader == nil {
		return ""
	}
	for _, p := range leader.protocols {
		allSupport := true
		for _, w := range waiters[1:] {
			m := members[w.memberID]
			if m == nil {
				// gh #98 #5: nil member can't support anything.
				// Treat as a NO vote so an inconsistent
				// (waiters, members) pair fails closed instead of
				// silently selecting a protocol non-aligned waiters
				// won't honour.
				allSupport = false
				break
			}
			found := false
			for _, mp := range m.protocols {
				if mp.Name == p.Name {
					found = true
					break
				}
			}
			if !found {
				allSupport = false
				break
			}
		}
		if allSupport {
			return p.Name
		}
	}
	return ""
}

func generateMemberID(clientID string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%x", clientID, b)
}
