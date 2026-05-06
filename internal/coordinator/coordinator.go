package coordinator

import (
	"context"
	"sort"
	"sync"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// GroupAssignmentSource is the v3 contract Manager uses to decide who
// coordinates each consumer group. It replaces the v2.6
// per-group-Lease informer pattern: ownership is now a row in the
// cluster controller's assignment.json, validated by the broker's
// BrokerCoordinator, and exposed through this two-method interface.
//
// *internal/broker.Coordinator satisfies it structurally — no import
// in this direction (broker already imports coordinator).
type GroupAssignmentSource interface {
	// OwnsGroup reports whether this broker is the assigned coordinator
	// for groupID under the most recently applied assignment.
	OwnsGroup(groupID string) bool
	// GroupCoordinator returns the broker ID assigned to coordinate
	// groupID. Second return is false when the group has no row in the
	// current assignment (typically the first JoinGroup arrives before
	// the controller has registered the group).
	GroupCoordinator(groupID string) (brokerID string, ok bool)
}

// BrokerLookup maps a broker ID string to the (NodeID, host, port)
// triple Kafka clients need to address it. v2.6 used ordinal-based
// lookups; v3's controller-driven assignment uses string IDs (matching
// the StatefulSet pod name), but Kafka's wire format keeps the int32
// NodeID, so the lookup returns both.
type BrokerLookup func(brokerID string) (nodeID int32, host string, port int32, ok bool)

// Manager handles consumer group state and offset storage for groups
// this broker is the assigned coordinator for. Coordinator selection
// is the GroupAssignmentSource's job (Phase 5: the cluster controller
// assigns groups via assignment.json, validated by BrokerCoordinator).
type Manager struct {
	ctx          context.Context
	groupSrc     GroupAssignmentSource
	lookupBroker BrokerLookup
	offsets      *OffsetStore

	mu     sync.Mutex
	groups map[string]*group
}

// NewManager creates a Manager. groupSrc is the source of truth for
// "is this broker the coordinator for group G?" — typically a
// *internal/broker.Coordinator. lookupBroker maps broker ID strings
// to their (NodeID, host, port) for FindCoordinator responses.
func NewManager(
	ctx context.Context,
	groupSrc GroupAssignmentSource,
	lookupBroker BrokerLookup,
	offsets *OffsetStore,
) *Manager {
	return &Manager{
		ctx:          ctx,
		groupSrc:     groupSrc,
		lookupBroker: lookupBroker,
		offsets:      offsets,
		groups:       make(map[string]*group),
	}
}

func (m *Manager) getOrCreate(groupID string) *group {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[groupID]
	if !ok {
		g = newGroup(groupID)
		m.groups[groupID] = g
		// Load persisted offsets for this group.
		_ = m.offsets.Load(groupID)
	}
	return g
}

// LocalGroups returns the IDs of every consumer group this broker is
// currently coordinating. The cluster controller aggregates this across
// brokers (via BrokerStatus.active_groups in the heartbeat) into the
// GroupSource it uses to compute assignments. Order is unspecified.
func (m *Manager) LocalGroups() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.groups))
	for id := range m.groups {
		out = append(out, id)
	}
	return out
}

// RelinquishGroup drops in-memory state for groupID. Called by
// GroupTakeoverDriver when the cluster controller reassigns the group
// to another broker. Pending offset commits remain on disk for the new
// coordinator to load lazily on its first JoinGroup. Idempotent: a
// second call after a group is already gone is a no-op.
//
// v1 deliberately does NOT migrate group state across brokers — the
// new coordinator starts the state machine at Empty, and Kafka
// clients re-join organically on the next heartbeat. Acceptable cost:
// one rebalance round trip per coordinator transition. v2 will add
// state-transfer if the latency hurts.
func (m *Manager) RelinquishGroup(groupID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g, ok := m.groups[groupID]; ok {
		g.shutdown()
		delete(m.groups, groupID)
	}
}

// isCoordinator returns true if this broker is the assigned coordinator
// for groupID under the cluster's current assignment.
func (m *Manager) isCoordinator(groupID string) bool {
	if m.groupSrc == nil {
		return false
	}
	return m.groupSrc.OwnsGroup(groupID)
}

// ---- API handler entry points ----

func (m *Manager) FindCoordinator(req *api.FindCoordinatorRequest) *api.FindCoordinatorResponse {
	resp := &api.FindCoordinatorResponse{}

	lookupOne := func(groupID string) (int32, string, int32, int16) {
		if m.groupSrc == nil {
			return 0, "", 0, int16(codec.ErrCoordinatorNotAvailable)
		}
		brokerID, ok := m.groupSrc.GroupCoordinator(groupID)
		if !ok {
			// Group not yet in the cluster assignment. The client should
			// retry — the controller will register the group on the next
			// recompute (driven by ActiveGroups in BrokerStatus).
			return 0, "", 0, int16(codec.ErrCoordinatorNotAvailable)
		}
		nodeID, host, port, ok := m.lookupBroker(brokerID)
		if !ok {
			return 0, "", 0, int16(codec.ErrCoordinatorNotAvailable)
		}
		return nodeID, host, port, 0
	}

	if len(req.CoordinatorKeys) > 0 {
		// v3+: multiple groups in one request.
		for _, key := range req.CoordinatorKeys {
			nodeID, host, port, errCode := lookupOne(key)
			resp.Coordinators = append(resp.Coordinators, api.CoordinatorResult{
				Key:       key,
				NodeID:    nodeID,
				Host:      host,
				Port:      port,
				ErrorCode: errCode,
			})
		}
	} else {
		// v0–v2: single group.
		nodeID, host, port, errCode := lookupOne(req.Key)
		resp.NodeID = nodeID
		resp.Host = host
		resp.Port = port
		resp.ErrorCode = errCode
	}
	return resp
}

func (m *Manager) JoinGroup(req *api.JoinGroupRequest, clientID string) *api.JoinGroupResponse {
	if !m.isCoordinator(req.GroupID) {
		return &api.JoinGroupResponse{ErrorCode: int16(codec.ErrNotCoordinator), GenerationID: -1}
	}
	return m.getOrCreate(req.GroupID).join(req, clientID)
}

func (m *Manager) SyncGroup(req *api.SyncGroupRequest) *api.SyncGroupResponse {
	if !m.isCoordinator(req.GroupID) {
		return &api.SyncGroupResponse{ErrorCode: int16(codec.ErrNotCoordinator)}
	}
	m.mu.Lock()
	g, ok := m.groups[req.GroupID]
	m.mu.Unlock()
	if !ok {
		return &api.SyncGroupResponse{ErrorCode: int16(codec.ErrUnknownMemberId)}
	}
	return g.sync(req)
}

func (m *Manager) Heartbeat(req *api.HeartbeatRequest) *api.HeartbeatResponse {
	if !m.isCoordinator(req.GroupID) {
		return &api.HeartbeatResponse{ErrorCode: int16(codec.ErrNotCoordinator)}
	}
	m.mu.Lock()
	g, ok := m.groups[req.GroupID]
	m.mu.Unlock()
	if !ok {
		return &api.HeartbeatResponse{ErrorCode: int16(codec.ErrUnknownMemberId)}
	}
	return &api.HeartbeatResponse{ErrorCode: g.heartbeat(req)}
}

func (m *Manager) LeaveGroup(req *api.LeaveGroupRequest) *api.LeaveGroupResponse {
	if !m.isCoordinator(req.GroupID) {
		return &api.LeaveGroupResponse{ErrorCode: int16(codec.ErrNotCoordinator)}
	}
	m.mu.Lock()
	g, ok := m.groups[req.GroupID]
	m.mu.Unlock()
	if !ok {
		return &api.LeaveGroupResponse{}
	}

	// Collect member IDs from both v0–v2 and v3+ request shapes.
	var memberIDs []string
	if req.MemberID != "" {
		memberIDs = append(memberIDs, req.MemberID)
	}
	for _, lm := range req.Members {
		memberIDs = append(memberIDs, lm.MemberID)
	}

	memberResponses := g.leave(memberIDs)
	resp := &api.LeaveGroupResponse{}
	for _, mr := range memberResponses {
		resp.Members = append(resp.Members, mr)
	}
	return resp
}

func (m *Manager) OffsetCommit(req *api.OffsetCommitRequest) *api.OffsetCommitResponse {
	if !m.isCoordinator(req.GroupID) {
		resp := &api.OffsetCommitResponse{}
		for _, t := range req.Topics {
			tr := api.OffsetCommitTopicResponse{Name: t.Name}
			for _, p := range t.Partitions {
				tr.Partitions = append(tr.Partitions, api.OffsetCommitPartitionResponse{
					PartitionIndex: p.PartitionIndex,
					ErrorCode:      int16(codec.ErrNotCoordinator),
				})
			}
			resp.Topics = append(resp.Topics, tr)
		}
		return resp
	}

	offsets := make(map[string]int64)
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			offsets[offsetKey(t.Name, p.PartitionIndex)] = p.CommittedOffset
		}
	}
	_ = m.offsets.Commit(req.GroupID, offsets)

	resp := &api.OffsetCommitResponse{}
	for _, t := range req.Topics {
		tr := api.OffsetCommitTopicResponse{Name: t.Name}
		for _, p := range t.Partitions {
			tr.Partitions = append(tr.Partitions, api.OffsetCommitPartitionResponse{
				PartitionIndex: p.PartitionIndex,
			})
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}

func (m *Manager) OffsetFetch(req *api.OffsetFetchRequest) *api.OffsetFetchResponse {
	resp := &api.OffsetFetchResponse{}

	fetchForGroup := func(groupID string, topics []api.OffsetFetchTopic) []api.OffsetFetchTopicResponse {
		specs := make([]FetchSpec, 0, len(topics))
		for _, t := range topics {
			specs = append(specs, FetchSpec{Topic: t.Name, Partitions: t.PartitionIndexes})
		}
		committed := m.offsets.Fetch(groupID, specs)

		var trs []api.OffsetFetchTopicResponse
		for _, t := range topics {
			tr := api.OffsetFetchTopicResponse{Name: t.Name}
			for _, p := range t.PartitionIndexes {
				k := offsetKey(t.Name, p)
				tr.Partitions = append(tr.Partitions, api.OffsetFetchPartitionResponse{
					PartitionIndex:  p,
					CommittedOffset: committed[k],
				})
			}
			trs = append(trs, tr)
		}
		return trs
	}

	if len(req.Groups) > 0 {
		// v8+: multi-group.
		for _, g := range req.Groups {
			gr := api.OffsetFetchGroupResponse{GroupID: g.GroupID}
			gr.Topics = fetchForGroup(g.GroupID, g.Topics)
			resp.Groups = append(resp.Groups, gr)
		}
	} else {
		resp.Topics = fetchForGroup(req.GroupID, req.Topics)
	}
	return resp
}

// DeleteGroups removes the listed groups' coordinator-side state
// and committed offsets. Per the Kafka protocol contract used by
// AdminClient.deleteConsumerGroups() and kafka-consumer-groups.sh
// --delete (gh #89):
//
//   - NOT_COORDINATOR (16): this broker is not the assigned
//     coordinator for the group. Client uses FindCoordinator and
//     retries against the right broker.
//   - GROUP_ID_NOT_FOUND (69): group is unknown. Returned for a
//     groupID this broker is responsible for but has no record of
//     (no in-memory state, no offsets file on disk).
//   - NON_EMPTY_GROUP (67): group has active members or pending
//     state. Per Kafka semantics only Empty / Dead groups can be
//     deleted; Stable / PreparingRebalance / CompletingRebalance
//     must rebalance away first.
//   - 0 (success): in-memory state dropped, offsets file removed.
func (m *Manager) DeleteGroups(req *api.DeleteGroupsRequest) *api.DeleteGroupsResponse {
	resp := &api.DeleteGroupsResponse{}
	for _, id := range req.GroupNames {
		errCode := m.deleteGroup(id)
		resp.Results = append(resp.Results, api.DeleteGroupsResult{
			GroupID:   id,
			ErrorCode: errCode,
		})
	}
	return resp
}

// deleteGroup encapsulates the per-group delete decision so the
// Manager-level loop stays a thin per-result aggregator. Returns
// the wire error code (0 = success).
func (m *Manager) deleteGroup(groupID string) int16 {
	if !m.isCoordinator(groupID) {
		return int16(codec.ErrNotCoordinator)
	}

	m.mu.Lock()
	g, inMemory := m.groups[groupID]
	m.mu.Unlock()

	hasOffsets := func() bool {
		// Load reads from disk into the cache; if the file is
		// missing it's a no-op and the cache stays empty. We
		// inspect the cache after Load.
		_ = m.offsets.Load(groupID)
		return m.offsets.HasGroup(groupID)
	}

	if !inMemory && !hasOffsets() {
		return int16(codec.ErrGroupIDNotFound)
	}

	if inMemory {
		snap := g.describe()
		// Per Kafka semantics: only Empty / Dead groups are
		// deletable. Anything mid-flight (Stable, *Rebalance) must
		// drain first.
		if snap.state != "Empty" && snap.state != "Dead" {
			return int16(codec.ErrNonEmptyGroup)
		}
		// shutdown closes any pending join/sync waiters before we
		// drop the map entry, so a concurrent Heartbeat sees
		// stateDead instead of hanging.
		m.mu.Lock()
		g.shutdown()
		delete(m.groups, groupID)
		m.mu.Unlock()
	}

	if err := m.offsets.Delete(groupID); err != nil {
		// File-system error: the broker's view of the group is
		// already gone (in-memory dropped above), but offsets
		// stayed. Surface UNKNOWN_SERVER_ERROR rather than 0 —
		// the operator wants to see this rather than the next
		// AdminClient call rediscovering "stale" offsets.
		return int16(codec.ErrUnknownServerError)
	}
	return 0
}

func (m *Manager) DescribeGroups(req *api.DescribeGroupsRequest) *api.DescribeGroupsResponse {
	resp := &api.DescribeGroupsResponse{}
	for _, id := range req.Groups {
		m.mu.Lock()
		g, ok := m.groups[id]
		m.mu.Unlock()

		dg := api.DescribedGroup{GroupID: id, AuthorizedOperations: -2147483648}
		if !ok {
			dg.GroupState = "Dead"
		} else {
			snap := g.describe()
			dg.GroupState = snap.state
			dg.ProtocolType = snap.protocolType
			dg.ProtocolData = snap.protocolName
			for _, ms := range snap.members {
				dg.Members = append(dg.Members, api.DescribedGroupMember{
					MemberID:        ms.memberID,
					ClientID:        ms.clientID,
					GroupInstanceID: ms.groupInstanceID,
					MemberAssignment: ms.assignment,
				})
			}
		}
		resp.Groups = append(resp.Groups, dg)
	}
	return resp
}

func (m *Manager) ListGroups(req *api.ListGroupsRequest) *api.ListGroupsResponse {
	m.mu.Lock()
	defer m.mu.Unlock()

	resp := &api.ListGroupsResponse{}
	for id, g := range m.groups {
		snap := g.describe()
		if len(req.StatesFilter) > 0 && !containsString(req.StatesFilter, snap.state) {
			continue
		}
		resp.Groups = append(resp.Groups, api.ListedGroup{
			GroupID:      id,
			ProtocolType: snap.protocolType,
			GroupState:   snap.state,
		})
	}
	sort.Slice(resp.Groups, func(i, j int) bool {
		return resp.Groups[i].GroupID < resp.Groups[j].GroupID
	})
	return resp
}

func containsString(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
