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

// TxnAssignmentSource is the gh #91 sibling of GroupAssignmentSource:
// the contract Manager uses to answer FindCoordinator(KeyType=
// transaction). Same structural shape — *internal/broker.Coordinator
// satisfies it via OwnsTxn / TxnCoordinator (PR 1).
//
// Two interfaces instead of one because (a) the gh #92 explicit-
// override tier in assignment.json is group-only and may stay that
// way, (b) tests / boot-time stubs already substitute group-only
// sources and we don't want to force a no-op TxnCoordinator on them.
type TxnAssignmentSource interface {
	// OwnsTxn reports whether this broker is the assigned txn
	// coordinator for transactionalID under the most recently
	// applied assignment.
	OwnsTxn(transactionalID string) bool
	// TxnCoordinator returns the broker ID assigned to coordinate
	// transactionalID. Second return is false when no broker is
	// alive in the current assignment (or no assignment is loaded
	// yet — bootstrap).
	TxnCoordinator(transactionalID string) (brokerID string, ok bool)
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
	lookupBroker BrokerLookup
	offsets      *OffsetStore

	// groupSrcMu protects atomic swap of groupSrc — cluster_runtime
	// hot-replaces the bootstrap LocalGroupSource with the real
	// broker.Coordinator once the cluster runtime is up. Read on
	// every isCoordinator call (hot path: JoinGroup, OffsetCommit,
	// ListGroups, ...).
	groupSrcMu sync.RWMutex
	groupSrc   GroupAssignmentSource

	// txnSrcMu / txnSrc are the gh #91 sibling lookup used by
	// FindCoordinator(KeyType=transaction). Same hot-swap shape as
	// groupSrc; nil ⇒ KeyType=transaction returns
	// COORDINATOR_NOT_AVAILABLE (boot-time before cluster_runtime
	// wires the broker.Coordinator).
	txnSrcMu sync.RWMutex
	txnSrc   TxnAssignmentSource

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
	m.groupSrcMu.RLock()
	src := m.groupSrc
	m.groupSrcMu.RUnlock()
	if src == nil {
		return false
	}
	return src.OwnsGroup(groupID)
}

// SetGroupAssignmentSource atomically replaces the source the
// Manager consults to decide "do I own this group?". cmd/skafka
// uses it to swap the bootstrap LocalGroupSource (always-true,
// good enough until the cluster runtime is up) for the real
// broker.Coordinator that consults assignment.json.
//
// gh #89/v0.1.51 follow-up: with LocalGroupSource still wired in
// production, the read-side filter on ListGroups and DescribeGroups
// has nothing to filter against — every broker reports every group
// it has ever seen. Swapping in broker.Coordinator makes the filter
// load-bearing.
func (m *Manager) SetGroupAssignmentSource(src GroupAssignmentSource) {
	m.groupSrcMu.Lock()
	m.groupSrc = src
	m.groupSrcMu.Unlock()
}

// SetTxnAssignmentSource is the gh #91 sibling of
// SetGroupAssignmentSource. cluster_runtime hot-swaps in the real
// *broker.Coordinator once the cluster runtime is up so
// FindCoordinator(KeyType=transaction) can answer with a real
// broker. Before this is called the txn path returns
// COORDINATOR_NOT_AVAILABLE — the same retry-friendly shape group
// requests get during boot.
func (m *Manager) SetTxnAssignmentSource(src TxnAssignmentSource) {
	m.txnSrcMu.Lock()
	m.txnSrc = src
	m.txnSrcMu.Unlock()
}

// ---- API handler entry points ----

func (m *Manager) FindCoordinator(req *api.FindCoordinatorRequest) *api.FindCoordinatorResponse {
	resp := &api.FindCoordinatorResponse{}

	// Resolve the lookup function once per request — KeyType applies
	// at the request level (not per-key in the v4 array). Apache 3.7
	// defines two key types: 0=group, 1=transaction (gh #91 PR 3).
	// Anything else is INVALID_REQUEST — Apache returns the same code
	// (FindCoordinatorRequest.scala:91) and we mirror that to keep
	// the wire surface boring.
	var resolve func(string) (string, bool)
	var fixedErr int16
	switch req.KeyType {
	case 0:
		m.groupSrcMu.RLock()
		src := m.groupSrc
		m.groupSrcMu.RUnlock()
		if src == nil {
			fixedErr = int16(codec.ErrCoordinatorNotAvailable)
		} else {
			resolve = src.GroupCoordinator
		}
	case 1:
		m.txnSrcMu.RLock()
		src := m.txnSrc
		m.txnSrcMu.RUnlock()
		if src == nil {
			// Boot window: cluster_runtime hasn't called
			// SetTxnAssignmentSource yet. Retry-friendly so the
			// producer's Java client falls into its standard
			// markCoordinatorUnknown loop instead of hard-failing.
			fixedErr = int16(codec.ErrCoordinatorNotAvailable)
		} else {
			resolve = src.TxnCoordinator
		}
	default:
		fixedErr = int16(codec.ErrInvalidRequest)
	}

	lookupOne := func(key string) (int32, string, int32, int16) {
		if fixedErr != 0 {
			return 0, "", 0, fixedErr
		}
		brokerID, ok := resolve(key)
		if !ok {
			// Group/txn not yet in the cluster assignment. The
			// client should retry — the controller will register
			// the group on the next recompute (driven by
			// ActiveGroups in BrokerStatus). For the txn path
			// today, this means no broker is alive in the current
			// assignment (the gh #91 hash routing always answers
			// when at least one broker is alive).
			return 0, "", 0, int16(codec.ErrCoordinatorNotAvailable)
		}
		nodeID, host, port, ok := m.lookupBroker(brokerID)
		if !ok {
			return 0, "", 0, int16(codec.ErrCoordinatorNotAvailable)
		}
		return nodeID, host, port, 0
	}

	if len(req.CoordinatorKeys) > 0 {
		// v4+: multiple keys in one request.
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
		// v0–v3: single key.
		nodeID, host, port, errCode := lookupOne(req.Key)
		resp.NodeID = nodeID
		resp.Host = host
		resp.Port = port
		resp.ErrorCode = errCode
	}
	return resp
}

// JoinGroup dispatches a JoinGroupRequest to the per-group state
// machine. version is the wire protocol version (passed through from
// the handler) so the group can gate KIP-394 MEMBER_ID_REQUIRED on
// v4+. v0-v3 clients keep the pre-KIP-394 inline-memberID-assignment
// flow.
func (m *Manager) JoinGroup(req *api.JoinGroupRequest, version int16, clientID string) *api.JoinGroupResponse {
	if !m.isCoordinator(req.GroupID) {
		return &api.JoinGroupResponse{ErrorCode: int16(codec.ErrNotCoordinator), GenerationID: -1}
	}
	return m.getOrCreate(req.GroupID).join(req, version, clientID)
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
	// gh #21: carry per-partition metadata through the commit. Empty
	// strings are stored as "no metadata" — that's how OffsetFetch's
	// wire null sentinel round-trips.
	metadata := make(map[string]string)
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			k := OffsetKey(t.Name, p.PartitionIndex)
			offsets[k] = p.CommittedOffset
			if p.CommittedMetadata != "" {
				metadata[k] = p.CommittedMetadata
			}
		}
	}
	_ = m.offsets.CommitWithMetadata(req.GroupID, offsets, metadata)

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

// WireTxnOffsetHook installs the gh #24/#27 cross-coordinator hook
// on a TxnStateStore so that EndTxn(commit) materialises any pending
// offsets staged by TxnOffsetCommit, and EndTxn(abort) discards
// them. Wired from cmd/skafka/cluster_runtime.go after both the
// txn store and the offset coord are up.
//
// Same-broker scope: when txn coordinator and group coordinator
// happen to be the same broker (single-broker dev, or hash collision)
// the hook fires locally. Cross-broker dispatch — when the txn
// coordinator must signal another broker's offset store — lands
// with gh #114 WriteTxnMarkers.
func (m *Manager) WireTxnOffsetHook(s *TxnStateStore) {
	s.SetTxnOffsetHook(func(groupID string, pid int64, commit bool) {
		if !m.isCoordinator(groupID) {
			// Cross-broker — gh #114 follow-up will dispatch a
			// WriteTxnMarkers RPC here. Today the pending offsets
			// remain staged on the actual group-coord broker; a
			// future commit/abort RPC will reach them.
			return
		}
		if commit {
			_ = m.offsets.CommitPending(groupID, pid)
		} else {
			m.offsets.DiscardPending(groupID, pid)
		}
	})
}

// TxnOffsetCommit (API key 28) stages offsets from a transactional
// producer's `sendOffsetsToTransaction`. Mirrors Apache's
// GroupCoordinator.handleTxnCommitOffsets. gh #27.
//
// Validation:
//   - groupID empty → INVALID_GROUP_ID (per partition)
//   - this broker isn't the group coordinator → NOT_COORDINATOR
//   - txnID empty → INVALID_REQUEST
//
// Offsets are staged in the offset store's pending layer keyed by
// (groupID, producerID); they are NOT visible to OffsetFetch until
// EndTxn(commit) fires CommitPending via the TxnStateStore's
// txnOffsetHook. EndTxn(abort) calls DiscardPending instead.
//
// Producer epoch / member-identity validation is intentionally
// loose at this layer — the gh #91 TxnOwnership gate (when wired
// upstream) plus the storage-side (PID, epoch) match in EndTxn
// catch the load-bearing cases.
func (m *Manager) TxnOffsetCommit(req *api.TxnOffsetCommitRequest) *api.TxnOffsetCommitResponse {
	makeErrResp := func(errCode int16) *api.TxnOffsetCommitResponse {
		resp := &api.TxnOffsetCommitResponse{}
		for _, t := range req.Topics {
			tr := api.TxnOffsetCommitResponseTopic{Name: t.Name}
			for _, p := range t.Partitions {
				tr.Partitions = append(tr.Partitions, api.TxnOffsetCommitResponsePartition{
					PartitionIndex: p.PartitionIndex,
					ErrorCode:      errCode,
				})
			}
			resp.Topics = append(resp.Topics, tr)
		}
		return resp
	}

	if req.TransactionalID == "" {
		return makeErrResp(int16(codec.ErrInvalidRequest))
	}
	if req.GroupID == "" {
		return makeErrResp(int16(codec.ErrInvalidGroupID))
	}
	if !m.isCoordinator(req.GroupID) {
		return makeErrResp(int16(codec.ErrNotCoordinator))
	}

	// Stage offsets. Use a single map (no per-partition partial
	// failure today — the storage layer either takes them all or
	// fails fully).
	offsets := make(map[string]int64)
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			offsets[OffsetKey(t.Name, p.PartitionIndex)] = p.CommittedOffset
		}
	}
	m.offsets.StorePending(req.GroupID, req.ProducerID, offsets)

	// Per-partition response: every partition gets NONE on success.
	resp := &api.TxnOffsetCommitResponse{}
	for _, t := range req.Topics {
		tr := api.TxnOffsetCommitResponseTopic{Name: t.Name}
		for _, p := range t.Partitions {
			tr.Partitions = append(tr.Partitions, api.TxnOffsetCommitResponsePartition{
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
		metadata := m.offsets.FetchMetadata(groupID, specs)

		var trs []api.OffsetFetchTopicResponse
		for _, t := range topics {
			tr := api.OffsetFetchTopicResponse{Name: t.Name}
			for _, p := range t.PartitionIndexes {
				k := OffsetKey(t.Name, p)
				tr.Partitions = append(tr.Partitions, api.OffsetFetchPartitionResponse{
					PartitionIndex:  p,
					CommittedOffset: committed[k],
					// gh #21: metadata is the wire null sentinel
					// when empty — the codec handles "" → null on
					// both flexible and non-flexible versions.
					Metadata: metadata[k],
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

// DeleteOffsets removes specific (topic, partition) offset entries
// from a group without deleting the whole group (gh #100, OffsetDelete
// API 47). Used by AdminClient.deleteConsumerGroupOffsets() and
// kafka-consumer-groups.sh --delete-offsets.
//
// Returns either:
//   - groupErr != 0: top-level error (NOT_COORDINATOR / GROUP_ID_NOT_FOUND
//     / NON_EMPTY_GROUP / UNKNOWN_SERVER_ERROR). `removed` is nil. Caller
//     emits the error with no per-partition results.
//   - groupErr == 0: `removed` maps each requested key to whether it
//     existed before. Caller maps absent keys to UNKNOWN_TOPIC_OR_PARTITION
//     in the wire response.
//
// State guard matches DeleteGroups: only Empty / Dead groups are
// eligible. Apache rejects offset deletes on Stable / *Rebalance —
// active members would have committed offsets fenced mid-delete
// otherwise.
func (m *Manager) DeleteOffsets(groupID string, keys []string) (groupErr int16, removed map[string]bool) {
	if !m.isCoordinator(groupID) {
		return int16(codec.ErrNotCoordinator), nil
	}

	m.mu.Lock()
	g, inMemory := m.groups[groupID]
	m.mu.Unlock()

	// Same disk-cache-warmup as deleteGroup: a coordinator that just
	// took over may have offsets on disk but no in-memory group yet.
	_ = m.offsets.Load(groupID)
	hasOffsets := m.offsets.HasGroup(groupID)

	if !inMemory && !hasOffsets {
		return int16(codec.ErrGroupIDNotFound), nil
	}
	if inMemory {
		snap := g.describe()
		if snap.state != "Empty" && snap.state != "Dead" {
			return int16(codec.ErrNonEmptyGroup), nil
		}
	}

	rem, err := m.offsets.DeletePartitions(groupID, keys)
	if err != nil {
		return int16(codec.ErrUnknownServerError), nil
	}
	return 0, rem
}

func (m *Manager) DescribeGroups(req *api.DescribeGroupsRequest) *api.DescribeGroupsResponse {
	resp := &api.DescribeGroupsResponse{}
	for _, id := range req.Groups {
		dg := api.DescribedGroup{GroupID: id, AuthorizedOperations: -2147483648}

		// Symmetric with ListGroups: a non-coordinator broker
		// reports the group as Dead even if it has a stale
		// m.groups entry. Surface the truth (the broker's
		// authoritative role) instead of leaking transient state.
		if !m.isCoordinator(id) {
			dg.GroupState = "Dead"
			resp.Groups = append(resp.Groups, dg)
			continue
		}

		m.mu.Lock()
		g, ok := m.groups[id]
		m.mu.Unlock()

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
		// Each broker only reports the groups it currently owns
		// per the cluster assignment. The Java AdminClient unions
		// ListGroups responses across all brokers; if a stale
		// m.groups entry survives on a non-coordinator broker
		// (e.g. an assignment recompute moved the group elsewhere
		// before GroupTakeoverDriver's orphan sweep ran), filtering
		// here keeps the union strictly correct: at most one broker
		// reports any given group ID. Symmetric with the
		// isCoordinator gate on JoinGroup / OffsetCommit / etc.
		// (gh #89 follow-up).
		if !m.isCoordinator(id) {
			continue
		}
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
