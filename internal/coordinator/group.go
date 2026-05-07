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
}

func newGroup(id string) *group {
	return &group{
		id:      id,
		members: make(map[string]*groupMember),
	}
}

// join handles a JoinGroup request. Blocks until the rebalance completes.
// clientID comes from the connection state (not the request body).
func (g *group) join(req *api.JoinGroupRequest, clientID string) *api.JoinGroupResponse {
	g.mu.Lock()

	memberID := req.MemberID
	if memberID == "" {
		memberID = generateMemberID(clientID)
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
	if g.rebalanceTimer != nil {
		g.rebalanceTimer.Stop()
		g.rebalanceTimer = nil
	}
	g.state = stateCompletingRebalance
	g.generationID++
	g.protocolName = selectProtocol(g.members, g.joinWaiters)

	// gh #98 diagnostic: every completeRebalance logs the final
	// protocolName, member count, and waiter count so we can trace
	// "Coordinator selected invalid assignment protocol: null" back
	// to its origin. To be reverted once the bug is identified.
	{
		var leaderID string
		var leaderProtos int
		if len(g.joinWaiters) > 0 {
			leaderID = g.joinWaiters[0].memberID
			if m := g.members[leaderID]; m != nil {
				leaderProtos = len(m.protocols)
			}
		}
		slog.Info("[gh-#98 debug] completeRebalance",
			"group", g.id,
			"generation", g.generationID,
			"protocolName", g.protocolName,
			"protocolType", g.protocolType,
			"members", len(g.members),
			"waiters", len(g.joinWaiters),
			"leader", leaderID,
			"leaderProtocols", leaderProtos)
	}

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
	if g.state != stateCompletingRebalance {
		g.mu.Unlock()
		return &api.SyncGroupResponse{ErrorCode: int16(codec.ErrRebalanceInProgress)}
	}

	ss := g.currentSync
	isLeader := req.MemberID == g.leaderID

	if isLeader {
		ss.mu.Lock()
		for _, a := range req.Assignments {
			ss.assignments[a.MemberID] = a.Assignment
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

	return &api.SyncGroupResponse{Assignment: assignment}
}

// heartbeat handles a Heartbeat request.
func (g *group) heartbeat(req *api.HeartbeatRequest) int16 {
	g.mu.Lock()
	defer g.mu.Unlock()

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
		m, ok := g.members[mid]
		if !ok {
			responses = append(responses, api.LeaveMemberResponse{MemberID: mid, ErrorCode: int16(codec.ErrUnknownMemberId)})
			continue
		}
		if m.heartbeatTimer != nil {
			m.heartbeatTimer.Stop()
			m.heartbeatTimer = nil
		}
		delete(g.members, mid)
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
	for _, m := range g.members {
		if m.heartbeatTimer != nil {
			m.heartbeatTimer.Stop()
			m.heartbeatTimer = nil
		}
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
		delete(g.members, mid)
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

// selectProtocol finds the first protocol in the leader's preference list that all
// joining members support.
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
				continue
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
