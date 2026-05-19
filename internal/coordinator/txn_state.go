package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// nowUnixMillis is a package-local indirection so tests can pin
// time for the gh #28 txn-timeout reaper (AbortOverdue). Production
// callers get time.Now().UnixMilli().
var nowUnixMillis = func() int64 { return time.Now().UnixMilli() }

// DefaultNumSlots matches Apache Kafka's
// `transaction.state.log.num.partitions=50` default. Pinning the slot
// count to a fixed cluster-wide constant — instead of the StatefulSet
// replica count — decouples the storage layout from broker scale
// operations: scaling up or down changes which broker owns each slot
// (gh #91 hash routing), but every slot file remains valid. Same
// shape Apache uses: __transaction_state has a fixed 50 partitions;
// scaling brokers shifts leadership, never the partition count.
const DefaultNumSlots = 50

// TxnStateStore tracks (producerID, epoch) per transactional.id so
// InitProducerId can implement the epoch-fence-on-rejoin contract
// (gh #22). Apache Kafka's transactional producer relies on this:
// every reconnect of the same transactional.id returns the same
// PID with epoch+1, and any in-flight Produce from the previous
// session (still tagged with the old epoch) gets fenced by the
// idempotence check at storage.Append → ErrInvalidProducerEpoch
// → wire code 47.
//
// Persistence: sharded by hash(txnID) % numSlots, one JSON file per
// slot under <dataDir>/__cluster/txn_state/slot-N.json. Each broker
// owns only the slots routing to it under the gh #91 hash-and-alive-
// fallback in internal/broker/group_hash.go; on coordinator failover
// the new owner reads the slot file the prior owner wrote and
// continues from the same (PID, epoch) state. This closes the gh
// #108 correctness gap where a producer's preferred-slot broker
// dying caused the new owner to allocate a fresh PID with epoch=0
// and silently break the fence-on-rejoin contract.
//
// Mirrors Apache Kafka's __transaction_state internal topic:
// partition = slot, log replay = JSON file read. Skafka skips the
// log-replay step because the file is already the materialised map
// the Apache coordinator builds from compacted log records.
//
// Read-fresh-on-every-call semantics: each GetOrAllocate re-reads
// the slot file from disk before mutating, then writes back via
// atomic tmp+rename. NFS close-to-open consistency means a fresh
// os.Open sees the latest committed state from any other broker
// that recently wrote. Cost: ~2 file ops per InitProducerId (cold
// path; transactional producers init rarely).
//
// numSlots is a "set once at bootstrap" value. Apache enforces this
// by reading transaction.state.log.num.partitions at first cluster
// start and ignoring later changes; skafka has a softer guarantee —
// changing the value requires a re-shard pass that runs in
// migrateLayout() on every broker startup. The migration is
// idempotent: it walks every existing slot-*.json, computes each
// entry's expected slot under the current numSlots, moves any
// misplaced entry, and removes empty / out-of-range slot files.
// Best-effort during rolling upgrades: while old-version brokers
// still write to old-numSlots files, the new-version brokers' next
// startup migration catches them.
//
// Split-brain risk: during a controller transition (~15s window)
// two brokers can both think they own a slot. Last-write-wins on
// slot-N.json. Mitigated for the common case by the controller's
// ~5s lease refresh, fully closed by the gh #108 phase 2 cross-
// broker fence broadcast which also bumps every in-flight (PID,
// epoch) on the losing broker's partitions.
type TxnStateStore struct {
	dir      string
	numSlots int

	mu sync.Mutex

	// txnOffsetHook is the gh #24/#27 cross-coordinator hook fired
	// from EndTxn for each (groupID, producerID) that has staged
	// pending offsets. Optional — nil in tests / dev mode where the
	// offset store hasn't been wired.
	txnOffsetHook TxnOffsetHook
}

// TxnEntry is the persistent record of a transactional producer.
// PID stays stable across the lifetime of the entry; only Epoch
// moves. Once Epoch saturates int16 we rotate to a fresh PID
// (the InitProducerIdHandler does the rotation; TxnStateStore
// just records what it's told).
//
// Partitions is the gh #23 per-txn partition list — every (topic,
// partition) the producer has called AddPartitionsToTxn for in the
// current transaction. Empty when no txn is in progress (the
// implicit "Empty" state from Apache's TransactionState machine).
// Mirrors `TransactionMetadata.topicPartitions` in
// TransactionMetadata.scala. EndTxn (#25/#26) will clear this list
// after writing the commit/abort marker.
type TxnEntry struct {
	PID        int64      `json:"pid"`
	Epoch      int16      `json:"epoch"`
	Partitions []TxnTopic `json:"partitions,omitempty"`
	// OngoingSinceMs is the wall-clock UnixMilli the entry entered
	// the Ongoing state (gh #28). Together with TransactionTimeoutMs,
	// this is the input to AbortOverdue's deadline check. Zero in
	// states other than Ongoing.
	OngoingSinceMs int64 `json:"ongoingSinceMs,omitempty"`
	// TransactionTimeoutMs mirrors the InitProducerIdRequest field
	// (KIP-98). Apache aborts an Ongoing transaction whose
	// last-update + timeoutMs is in the past. gh #28's MVP wires
	// the field through to TxnStateStore + adds the AbortOverdue
	// sweep; a periodic goroutine call is a separate plumbing
	// task in cluster_runtime.go.
	TransactionTimeoutMs int32 `json:"transactionTimeoutMs,omitempty"`
	// State is the transaction state machine field — gh #25/#26.
	// Mirrors Apache's TransactionState (TransactionMetadata.scala):
	//
	//   "" or "Empty"   — no transaction in progress (default)
	//   "Ongoing"       — at least one AddPartitionsToTxn succeeded
	//   "PrepareCommit" — EndTxn(commit) accepted, transition in flight
	//   "PrepareAbort"  — EndTxn(abort) accepted, transition in flight
	//   "CompleteCommit"— commit finished (idempotent retries return NONE)
	//   "CompleteAbort" — abort finished
	//
	// Skafka transitions Prepare→Complete atomically in one EndTxn call
	// (no separate marker-write phase yet — that's the gh #27/#31
	// follow-up which adds WriteTxnMarkers + read-committed isolation).
	// The Prepare* states exist in the schema for forward compat.
	State string `json:"state,omitempty"`
	// Groups is the gh #24 per-txn consumer-group list — the
	// transactional producer called AddOffsetsToTxn(groupID) once per
	// group it intends to commit offsets to. On EndTxn the txn
	// coordinator signals each group's offset coordinator (via
	// WriteTxnMarkers gh #114 follow-up) to materialise or discard
	// the pending offsets. Apache's TransactionMetadata records this
	// implicitly by adding the __consumer_offsets[partitionFor(groupId)]
	// partition to topicPartitions; skafka tracks the groupID
	// directly since we don't have a __consumer_offsets topic.
	Groups []string `json:"groups,omitempty"`
}

// TxnTopic is one (topic, partitions) tuple inside a TxnEntry.
// Apache's wire/storage shape uses TopicPartition (a single
// (topic, int32) pair) but groups by topic on the wire to avoid
// repeating the topic name; we store the same grouped form.
type TxnTopic struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// NewTxnStateStore opens the per-cluster transactional-state dir.
// dir is typically <dataDir>/__cluster.
//
// numSlots ≤ 0 falls back to DefaultNumSlots (50). Pinning to a
// fixed cluster-wide constant — independent of broker count —
// keeps the storage layout stable across scale operations. Mirrors
// Apache's `transaction.state.log.num.partitions=50` default.
//
// Two migrations run on open, both idempotent:
//
//  1. Legacy single-file layout (pre-v0.1.81) — read
//     transactional_state.json, distribute entries to slot files,
//     delete the legacy file.
//  2. Slot-layout drift — re-shard any entry currently sitting in
//     slot-K.json where hash(txnID) % numSlots != K. Catches the
//     v0.1.81-v0.1.83 → v0.1.84 transition (numSlots was the
//     replica count, now pinned to 50) plus any future numSlots
//     change. Removes empty / out-of-range slot files.
func NewTxnStateStore(dir string, numSlots int) (*TxnStateStore, error) {
	if numSlots <= 0 {
		numSlots = DefaultNumSlots
	}
	slotDir := filepath.Join(dir, "txn_state")
	if err := os.MkdirAll(slotDir, 0o775); err != nil {
		return nil, err
	}
	s := &TxnStateStore{
		dir:      slotDir,
		numSlots: numSlots,
	}
	if err := s.migrateLegacy(dir); err != nil {
		return nil, err
	}
	if err := s.migrateLayout(); err != nil {
		return nil, err
	}
	return s, nil
}

// GetOrAllocate is the gh #22 contract: for txnID="foo" the first
// call returns a fresh PID with epoch=0; every subsequent call
// returns the SAME PID with epoch+1.
//
// alloc supplies a fresh PID — typically the same monotonic
// counter the non-transactional InitProducerId path uses, so PIDs
// stay globally distinct on this broker. alloc is only invoked
// the first time a txnID is seen, and on epoch rotation.
//
// Reads the slot file fresh on every call (gh #108): a producer
// rejoining after its preferred-slot broker failed over will hit
// the new coordinator, which reads the same slot-N.json the prior
// coordinator wrote and bumps from there.
//
// Concurrent callers within a single broker process serialise on
// s.mu, so two clients claiming the same transactional.id at
// exactly the same time get different epochs (one fences the
// other). Cross-broker concurrent claims are race-bounded to the
// brief controller-transition window; outside of that the gh #91
// OwnsTxn gate keeps each txn ID on a single broker.
func (s *TxnStateStore) GetOrAllocate(txnID string, alloc func() int64) (int64, int16, error) {
	return s.GetOrAllocateWithTimeout(txnID, 0, alloc)
}

// GetOrAllocateWithTimeout is GetOrAllocate plus the gh #28
// transaction.timeout.ms recording. The client's
// InitProducerIdRequest.TransactionTimeoutMs is preserved on the
// entry so AbortOverdue can age out crashed producers without
// having to re-derive the timeout at sweep time.
//
// timeoutMs <= 0 leaves the existing entry's timeout untouched
// (cooperative for the non-transactional fast path which doesn't
// know or care about the timeout dial).
func (s *TxnStateStore) GetOrAllocateWithTimeout(txnID string, timeoutMs int32, alloc func() int64) (int64, int16, error) {
	if txnID == "" {
		return 0, 0, errors.New("txn state store: empty transactional id")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	slot := s.slotFor(txnID)
	state, err := s.loadSlot(slot)
	if err != nil {
		return 0, 0, err
	}

	entry, ok := state[txnID]
	if !ok {
		entry = TxnEntry{PID: alloc(), Epoch: 0}
	} else if entry.Epoch == math.MaxInt16 {
		// Epoch overflow: rotate to a fresh PID. Apache Kafka
		// emits PRODUCER_FENCED here and forces the client to
		// re-init; for skafka without a transactional fence
		// surface, allocating a new PID achieves the same
		// effect — old in-flight writes can't match the new
		// (PID, epoch) pair so they're naturally fenced.
		entry = TxnEntry{PID: alloc(), Epoch: 0}
	} else {
		entry.Epoch++
	}
	if timeoutMs > 0 {
		entry.TransactionTimeoutMs = timeoutMs
	}
	state[txnID] = entry
	if err := s.persistSlot(slot, state); err != nil {
		return 0, 0, err
	}
	return entry.PID, entry.Epoch, nil
}

// AddPartitions records that the producer at (txnID, pid, epoch) has
// added the listed (topic, partitions) tuples to its in-progress
// transaction. gh #23 — mirrors Apache's
// TransactionCoordinator.handleAddPartitionsToTransaction.
//
// Validation order matches Apache:
//  1. txnID empty → ErrEmptyTxnID
//  2. No entry for txnID → ErrTxnUnknownProducer (caller maps to
//     INVALID_PRODUCER_ID_MAPPING)
//  3. PID mismatch → ErrTxnUnknownProducer
//  4. Epoch mismatch → ErrTxnEpochFenced (caller maps to
//     PRODUCER_FENCED)
//  5. Otherwise: union the partition list into entry.Partitions and
//     persist atomically. Idempotent — re-adding the same partitions
//     is a no-op success (matches Apache's
//     `partitions.subsetOf(txnMetadata.topicPartitions)` shortcut).
//
// Apache's full state machine has more rejection modes
// (CONCURRENT_TRANSACTIONS for PrepareCommit/PrepareAbort,
// pendingTransitionInProgress, etc). Skafka doesn't yet have a
// state field — that lands with #25/#26 EndTxn. For now,
// AddPartitions is always allowed when (txnID, PID, epoch) match.
func (s *TxnStateStore) AddPartitions(txnID string, pid int64, epoch int16, additions []TxnTopic) error {
	if txnID == "" {
		return ErrEmptyTxnID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	slot := s.slotFor(txnID)
	state, err := s.loadSlot(slot)
	if err != nil {
		return err
	}

	entry, ok := state[txnID]
	if !ok {
		return ErrTxnUnknownProducer
	}
	if entry.PID != pid {
		return ErrTxnUnknownProducer
	}
	if entry.Epoch != epoch {
		return ErrTxnEpochFenced
	}

	// gh #25/#26: a Prepare* state is mid-transition; refuse new
	// partitions. Apache returns CONCURRENT_TRANSACTIONS here.
	// Mid-Complete* state is fine — it means the previous txn finished
	// and this is a new transaction starting; advance state to Ongoing.
	switch entry.State {
	case TxnStatePrepareCommit, TxnStatePrepareAbort:
		return ErrTxnConcurrent
	}

	merged := mergePartitions(&entry, additions)

	// State machine: a fresh AddPartitionsToTxn starts a new
	// transaction. Empty/Complete* transitions to Ongoing. Ongoing
	// stays Ongoing.
	wasNotOngoing := entry.State != TxnStateOngoing
	if wasNotOngoing {
		entry.State = TxnStateOngoing
		// gh #28: stamp the wall-clock start so the txn-timeout
		// reaper (AbortOverdue) can age out producers that crashed
		// mid-transaction. Only stamped on the empty→ongoing edge.
		entry.OngoingSinceMs = nowUnixMillis()
	}

	if !merged && !wasNotOngoing {
		// Every requested (topic, partitions) tuple was already
		// recorded AND no state change — Apache's idempotent
		// shortcut, no log write.
		return nil
	}

	state[txnID] = entry
	return s.persistSlot(slot, state)
}

// AddOffsetsToTxn records that the producer will commit offsets to
// consumer group `groupID` as part of this transaction. gh #24
// (API key 25) — sibling of AddPartitionsToTxn for the offset path.
//
// Same validation pattern: (txnID, PID, epoch) must match an
// Ongoing-or-Empty entry; state transitions Empty/Complete* →
// Ongoing. Idempotent — re-adding the same group is a no-op.
//
// The recorded group list is used by EndTxn (and the future #114
// WriteTxnMarkers path) to know which offset coordinators need a
// commit/abort signal at txn completion time.
func (s *TxnStateStore) AddOffsetsToTxn(txnID string, pid int64, epoch int16, groupID string) error {
	if txnID == "" {
		return ErrEmptyTxnID
	}
	if groupID == "" {
		// Apache's INVALID_GROUP_ID — caller may map to
		// ErrInvalidGroupID, but the AddOffsetsToTxn wire spec
		// surfaces it as INVALID_REQUEST at the txn handler. Keep
		// ErrEmptyTxnID's behaviour shape: caller distinguishes.
		return ErrTxnInvalidState
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	slot := s.slotFor(txnID)
	state, err := s.loadSlot(slot)
	if err != nil {
		return err
	}

	entry, ok := state[txnID]
	if !ok {
		return ErrTxnUnknownProducer
	}
	if entry.PID != pid {
		return ErrTxnUnknownProducer
	}
	if entry.Epoch != epoch {
		return ErrTxnEpochFenced
	}
	switch entry.State {
	case TxnStatePrepareCommit, TxnStatePrepareAbort:
		return ErrTxnConcurrent
	}

	// Idempotent dedup of the group list.
	for _, g := range entry.Groups {
		if g == groupID {
			// State machine still needs to advance to Ongoing if it's
			// somehow in Empty/Complete* with a stale group entry.
			if entry.State != TxnStateOngoing {
				entry.State = TxnStateOngoing
				entry.OngoingSinceMs = nowUnixMillis()
				state[txnID] = entry
				return s.persistSlot(slot, state)
			}
			return nil
		}
	}
	entry.Groups = append(entry.Groups, groupID)
	if entry.State != TxnStateOngoing {
		entry.State = TxnStateOngoing
		entry.OngoingSinceMs = nowUnixMillis()
	}
	state[txnID] = entry
	return s.persistSlot(slot, state)
}

// TxnOffsetHook is the txn-coordinator → offset-coordinator signal
// that fires on every EndTxn transition. gh #24/#27: for each group
// that was registered via AddOffsetsToTxn, the txn coordinator must
// tell the group's offset store to either materialise
// (commit) or discard (abort) the pending offsets that
// TxnOffsetCommit staged earlier.
//
// In Apache, this signal travels via WriteTxnMarkers to the
// __consumer_offsets[partitionFor(groupId)] partition's leader.
// Skafka v0.1.97+ stages this as a local hook — when txn coord and
// group coord live on the same broker it fires directly; gh #114
// will add the cross-broker dispatch.
//
// commit=true → CommitPending, commit=false → DiscardPending.
type TxnOffsetHook func(groupID string, producerID int64, commit bool)

// SetTxnOffsetHook wires the cross-coordinator signal. Optional —
// when nil, EndTxn just clears the per-txn group list and the
// pending offsets remain staged (a follow-up commit/abort will
// reach them indirectly). Production wires manager.go's
// `(groupID, pid, commit) → m.OffsetStore.{Commit,Discard}Pending`.
func (s *TxnStateStore) SetTxnOffsetHook(h TxnOffsetHook) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txnOffsetHook = h
}

// EndTxn implements the EndTxn (API key 26) state transition.
// gh #25 (commit) + gh #26 (abort).
//
// Mirrors Apache's `TransactionCoordinator.endTransaction`:
//
//	Ongoing       → PrepareCommit → CompleteCommit  (commit=true)
//	Ongoing       → PrepareAbort  → CompleteAbort   (commit=false)
//	CompleteCommit + commit=true   → NONE (idempotent retry)
//	CompleteAbort  + commit=false  → NONE
//	CompleteCommit + commit=false  → ErrTxnInvalidState
//	CompleteAbort  + commit=true   → ErrTxnInvalidState
//	Empty / no-state               → ErrTxnInvalidState
//
// Skafka collapses Prepare→Complete into a single atomic transition
// because we don't yet write marker control batches (those land with
// the gh #27/#31 WriteTxnMarkers + read-committed isolation pair).
// Apache observes Prepare* externally only when the marker write
// fails halfway through; with no marker phase we never observe
// PrepareCommit or PrepareAbort from outside the lock.
//
// On success, clears entry.Partitions — Apache's
// completeTransitionTo(CompleteCommit/Abort) zeros topicPartitions
// since the next transaction starts fresh.
func (s *TxnStateStore) EndTxn(txnID string, pid int64, epoch int16, commit bool) error {
	if txnID == "" {
		return ErrEmptyTxnID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	slot := s.slotFor(txnID)
	state, err := s.loadSlot(slot)
	if err != nil {
		return err
	}

	entry, ok := state[txnID]
	if !ok {
		return ErrTxnUnknownProducer
	}
	if entry.PID != pid {
		return ErrTxnUnknownProducer
	}
	if entry.Epoch != epoch {
		return ErrTxnEpochFenced
	}

	// State machine. Apache's full table at TransactionMetadata.scala
	// validPreviousStates — we cover the cases reachable today.
	switch entry.State {
	case TxnStateOngoing:
		// Happy path — transition to Complete*.
		if commit {
			entry.State = TxnStateCompleteCommit
		} else {
			entry.State = TxnStateCompleteAbort
		}
		entry.Partitions = nil
		// gh #28: clear the timeout-reaper clock; AbortOverdue must
		// not re-trip on an already-completed transaction.
		entry.OngoingSinceMs = 0
		// gh #24/#27: fire the cross-coordinator hook for each
		// (groupID, pid) so its offset store either materialises or
		// discards the pending offsets staged by TxnOffsetCommit.
		// When txn coord = group coord (single broker, or same hash
		// slot), this is a local call. Cross-broker dispatch lands
		// with gh #114 WriteTxnMarkers.
		if s.txnOffsetHook != nil {
			for _, g := range entry.Groups {
				s.txnOffsetHook(g, pid, commit)
			}
		}
		entry.Groups = nil
	case TxnStateCompleteCommit:
		// Idempotent retry of commit — return NONE without persist.
		// Mismatched action (abort on committed txn) is invalid.
		if !commit {
			return ErrTxnInvalidState
		}
		return nil
	case TxnStateCompleteAbort:
		if commit {
			return ErrTxnInvalidState
		}
		return nil
	case TxnStatePrepareCommit, TxnStatePrepareAbort:
		// Apache returns CONCURRENT_TRANSACTIONS — Prepare* means
		// another EndTxn is mid-flight. Skafka transitions atomically
		// so this branch is unreachable today, but kept for forward
		// compat when marker writes split the transition into phases.
		return ErrTxnConcurrent
	default:
		// "" or "Empty" or anything else — no txn to end.
		// Apache: INVALID_TXN_STATE on EndTxn against Empty.
		return ErrTxnInvalidState
	}

	state[txnID] = entry
	return s.persistSlot(slot, state)
}

// AbortOverdue scans every slot owned by this broker and aborts any
// Ongoing transaction whose OngoingSinceMs + TransactionTimeoutMs is
// older than now. gh #28 — mirrors Apache's
// TransactionStateManager.abortTimedOutTransactions.
//
// Returns the list of (txnID, pid, epoch) tuples that were aborted
// so the caller can fire the same group-offset hook EndTxn fires (in
// practice this is done internally; the return slice is for
// observability/metrics + tests). Caller is expected to invoke this
// periodically — a 10s tick is the rough Apache cadence (controlled
// by `transaction.abort.timed.out.transaction.cleanup.interval.ms`,
// 10000 default; skafka's reaper loop in cluster_runtime fires on
// the same cadence).
//
// Bumps the producer epoch on abort. Apache's "writeTxnMarkers + bump
// epoch" sequence is what fences a stuck producer: when it eventually
// reconnects with the old (PID, epoch), the gh #22 fence-on-rejoin
// path returns PRODUCER_FENCED. Skafka transitions state to
// CompleteAbort atomically (no markers yet — that's gh #114) and
// bumps the epoch in the same persist.
//
// A 0 OngoingSinceMs is treated as "no clock set" and skipped — this
// keeps pre-#28 entries (which never got the stamp) from getting
// nuked on the first reaper tick. The same skip applies to entries
// whose TransactionTimeoutMs is 0 (client never told us how long it
// wanted, so we don't get to decide for them).
func (s *TxnStateStore) AbortOverdue(nowMs int64) []TxnAbortRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	var aborted []TxnAbortRecord
	for slot := 0; slot < s.numSlots; slot++ {
		state, err := s.loadSlot(slot)
		if err != nil {
			continue
		}
		changed := false
		for txnID, entry := range state {
			if entry.State != TxnStateOngoing {
				continue
			}
			if entry.OngoingSinceMs == 0 || entry.TransactionTimeoutMs <= 0 {
				continue
			}
			deadline := entry.OngoingSinceMs + int64(entry.TransactionTimeoutMs)
			if deadline > nowMs {
				continue
			}

			pid, epoch := entry.PID, entry.Epoch
			groups := append([]string(nil), entry.Groups...)

			entry.State = TxnStateCompleteAbort
			entry.Partitions = nil
			entry.OngoingSinceMs = 0
			if entry.Epoch == math.MaxInt16 {
				entry.Epoch = 0
			} else {
				entry.Epoch++
			}
			entry.Groups = nil
			state[txnID] = entry
			changed = true

			if s.txnOffsetHook != nil {
				for _, g := range groups {
					s.txnOffsetHook(g, pid, false)
				}
			}
			aborted = append(aborted, TxnAbortRecord{
				TxnID:    txnID,
				PID:      pid,
				OldEpoch: epoch,
				NewEpoch: entry.Epoch,
				Groups:   groups,
			})
		}
		if changed {
			_ = s.persistSlot(slot, state)
		}
	}
	return aborted
}

// TxnAbortRecord is the side-effect record AbortOverdue returns per
// aborted txn — feeds metrics and the gh #114 marker-writer (when it
// lands) and is used by tests to assert the sweep fired.
type TxnAbortRecord struct {
	TxnID    string
	PID      int64
	OldEpoch int16
	NewEpoch int16
	Groups   []string
}

// Transaction state constants — mirror Apache's TransactionState
// names so the persisted JSON is human-readable and aligns with
// debugging tooling expectations.
const (
	TxnStateEmpty          = "Empty"
	TxnStateOngoing        = "Ongoing"
	TxnStatePrepareCommit  = "PrepareCommit"
	TxnStatePrepareAbort   = "PrepareAbort"
	TxnStateCompleteCommit = "CompleteCommit"
	TxnStateCompleteAbort  = "CompleteAbort"
)

// mergePartitions unions additions into entry.Partitions in place.
// Returns true if anything new was added (caller persists), false
// if every (topic, partition) was already recorded (idempotent
// no-op success — matches Apache's `subsetOf` shortcut).
func mergePartitions(entry *TxnEntry, additions []TxnTopic) bool {
	changed := false
	for _, add := range additions {
		idx := -1
		for i := range entry.Partitions {
			if entry.Partitions[i].Topic == add.Topic {
				idx = i
				break
			}
		}
		if idx < 0 {
			// New topic — record all partitions.
			entry.Partitions = append(entry.Partitions, TxnTopic{
				Topic:      add.Topic,
				Partitions: append([]int32(nil), add.Partitions...),
			})
			changed = true
			continue
		}
		// Topic already tracked — union the partition list.
		existing := entry.Partitions[idx].Partitions
		for _, p := range add.Partitions {
			present := false
			for _, e := range existing {
				if e == p {
					present = true
					break
				}
			}
			if !present {
				existing = append(existing, p)
				changed = true
			}
		}
		entry.Partitions[idx].Partitions = existing
	}
	return changed
}

// Sentinel errors mapped to Kafka error codes by the
// AddPartitionsToTxnHandler (gh #23). Keeping the storage layer
// transport-free lets the handler choose between v0-3 (per-
// partition error code) and v4+ (top-level error code) shapes
// without leaking codec types into the coordinator package.
var (
	ErrEmptyTxnID         = errors.New("txn state: empty transactional id")
	ErrTxnUnknownProducer = errors.New("txn state: unknown txnID or pid mismatch")
	ErrTxnEpochFenced     = errors.New("txn state: producer epoch fenced")
	// ErrTxnConcurrent mirrors Apache's CONCURRENT_TRANSACTIONS (51):
	// another transition for this txnID is already in flight. gh #25.
	ErrTxnConcurrent = errors.New("txn state: concurrent transition in progress")
	// ErrTxnInvalidState mirrors Apache's INVALID_TXN_STATE: the
	// requested state transition is not allowed from the current
	// state (e.g., EndTxn against an Empty entry, or abort against
	// a CompleteCommit entry). gh #25/#26.
	ErrTxnInvalidState = errors.New("txn state: invalid state transition")
)

// txnEntriesEqual is a deep-equality helper. Necessary because
// TxnEntry contains a slice (Partitions) — direct struct comparison
// is a compile error.
func txnEntriesEqual(a, b TxnEntry) bool {
	if a.PID != b.PID || a.Epoch != b.Epoch || a.State != b.State {
		return false
	}
	if len(a.Partitions) != len(b.Partitions) {
		return false
	}
	for i := range a.Partitions {
		if a.Partitions[i].Topic != b.Partitions[i].Topic {
			return false
		}
		if len(a.Partitions[i].Partitions) != len(b.Partitions[i].Partitions) {
			return false
		}
		for j := range a.Partitions[i].Partitions {
			if a.Partitions[i].Partitions[j] != b.Partitions[i].Partitions[j] {
				return false
			}
		}
	}
	if len(a.Groups) != len(b.Groups) {
		return false
	}
	for i := range a.Groups {
		if a.Groups[i] != b.Groups[i] {
			return false
		}
	}
	return true
}

// Snapshot returns a copy of every txn entry across every slot.
// Used by tests to assert persistence + rejoin behaviour without
// poking into private fields.
func (s *TxnStateStore) Snapshot() map[string]TxnEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]TxnEntry)
	for slot := 0; slot < s.numSlots; slot++ {
		state, err := s.loadSlot(slot)
		if err != nil {
			continue
		}
		for k, v := range state {
			out[k] = v
		}
	}
	return out
}

// slotFor hashes txnID into [0, numSlots). Mirrors Apache's
// partitionFor(groupId) and skafka's broker.TxnCoordinatorSlot
// (FNV-1a 32-bit). The hash is purely local to disk-layout
// decisions; the broker-side coordinator routing uses its own
// hash in internal/broker/group_hash.go. The two hash functions
// happen to match (both FNV-1a 32 over the txnID bytes) but they
// don't have to — only the divisor (numSlots == numBrokers) and
// the deterministic mapping matter.
func (s *TxnStateStore) slotFor(txnID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(txnID))
	return int(h.Sum32()) % s.numSlots
}

func (s *TxnStateStore) slotPath(slot int) string {
	return filepath.Join(s.dir, fmt.Sprintf("slot-%d.json", slot))
}

func (s *TxnStateStore) loadSlot(slot int) (map[string]TxnEntry, error) {
	data, err := os.ReadFile(s.slotPath(slot))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return make(map[string]TxnEntry), nil
		}
		return nil, err
	}
	state := make(map[string]TxnEntry)
	if len(data) == 0 {
		return state, nil
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("txn state store: decode slot-%d: %w", slot, err)
	}
	return state, nil
}

func (s *TxnStateStore) persistSlot(slot int, state map[string]TxnEntry) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := s.slotPath(slot) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.slotPath(slot))
}

// migrateLegacy ingests <dataDir>/__cluster/transactional_state.json
// (the pre-#108 single-file layout) into the new slot-keyed dir
// and deletes the legacy file. Idempotent: returns nil if the
// legacy file is absent. Each entry is hashed to its slot via
// slotFor() and merged into the slot's existing map (so a warm
// broker that wrote some entries to the new layout already and
// then the legacy file resurfaces won't lose newer state).
func (s *TxnStateStore) migrateLegacy(parentDir string) error {
	legacy := filepath.Join(parentDir, "transactional_state.json")
	data, err := os.ReadFile(legacy)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	var legacyState map[string]TxnEntry
	if len(data) > 0 {
		if err := json.Unmarshal(data, &legacyState); err != nil {
			return fmt.Errorf("txn state store: decode legacy file: %w", err)
		}
	}
	for txnID, entry := range legacyState {
		slot := s.slotFor(txnID)
		state, err := s.loadSlot(slot)
		if err != nil {
			return err
		}
		// Don't overwrite a newer entry that may have landed in the
		// new layout while the legacy file lingered (race-bounded:
		// the legacy file is supposed to be deleted once, but if a
		// crash leaves it around between writes, the new-layout
		// entry's epoch will be ≥ legacy's).
		if existing, ok := state[txnID]; ok && existing.Epoch >= entry.Epoch && existing.PID == entry.PID {
			continue
		}
		state[txnID] = entry
		if err := s.persistSlot(slot, state); err != nil {
			return err
		}
	}
	return os.Remove(legacy)
}

// migrateLayout re-shards any entry sitting in a slot file that
// disagrees with the current numSlots — the case when an operator
// changes numSlots between boots, or when upgrading from a
// pre-v0.1.84 build that used a smaller numSlots (= broker count).
// Idempotent: running on an already-correct layout is a no-op.
//
// Five-phase algorithm:
//  1. Read every existing slot-*.json into memory.
//  2. Compute the new layout by hashing each entry under the
//     current numSlots. Higher-epoch wins on conflict (defensive
//     against the rolling-upgrade window).
//  3. Detect no-op: if the new layout is byte-identical to the
//     old, skip phases 4-5. Avoids spurious writes on warm
//     restarts.
//  4. Write every slot in the new layout. Atomic tmp+rename per
//     file. Source slots that also appear in the new layout get
//     overwritten cleanly here.
//  5. Delete source files that don't appear in the new layout
//     (either empty after relocation, or out-of-range under new
//     numSlots). Tolerates concurrent removal under rolling
//     upgrade.
//
// The phase ordering matters: writing the new layout BEFORE
// deleting source files means any concurrent reader sees a
// consistent view (either the old or the new layout, never a
// partial state with the entry deleted from old slot but not yet
// in new). Conversely, source-file deletion comes last so the
// migration is recoverable from a crash mid-write.
func (s *TxnStateStore) migrateLayout() error {
	dirEntries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	// Phase 1: load everything.
	oldLayout := make(map[int]map[string]TxnEntry) // slot → state
	for _, e := range dirEntries {
		name := e.Name()
		if !strings.HasPrefix(name, "slot-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		nstr := strings.TrimSuffix(strings.TrimPrefix(name, "slot-"), ".json")
		n, err := strconv.Atoi(nstr)
		if err != nil {
			continue
		}
		state, err := s.loadSlot(n)
		if err != nil {
			return err
		}
		oldLayout[n] = state
	}

	// Phase 2: compute the target layout. Walk every entry from
	// every old slot, place into its expected slot under current
	// numSlots. Empty slots aren't materialised — they don't
	// generate write or delete activity.
	newLayout := make(map[int]map[string]TxnEntry)
	for _, state := range oldLayout {
		for txnID, entry := range state {
			expected := s.slotFor(txnID)
			if existing, ok := newLayout[expected][txnID]; ok && existing.Epoch >= entry.Epoch {
				continue
			}
			if newLayout[expected] == nil {
				newLayout[expected] = make(map[string]TxnEntry)
			}
			newLayout[expected][txnID] = entry
		}
	}

	// Phase 3: idempotency check. If the layout already matches,
	// skip the rewrite. The check is map-equality on (slot →
	// txnID → entry).
	if layoutsEqual(oldLayout, newLayout) {
		return nil
	}

	// Phase 4: write every slot in the new layout. Source slots
	// that also appear here get overwritten cleanly (the new
	// content already includes any entries that hashed back to
	// the same slot).
	for slot, state := range newLayout {
		if err := s.persistSlot(slot, state); err != nil {
			return err
		}
	}

	// Phase 5: delete source files that no longer participate in
	// the new layout. Either the slot has no entries hashed to it
	// under current numSlots, or the slot index is out of range
	// (≥ numSlots).
	for slot := range oldLayout {
		if _, kept := newLayout[slot]; kept && slot < s.numSlots {
			continue
		}
		if err := os.Remove(s.slotPath(slot)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// layoutsEqual reports byte-for-byte equality between two
// (slot → state) maps. Used by migrateLayout's idempotency check.
func layoutsEqual(a, b map[int]map[string]TxnEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for slot, aState := range a {
		bState, ok := b[slot]
		if !ok || len(aState) != len(bState) {
			return false
		}
		for txnID, aEntry := range aState {
			bEntry, ok := bState[txnID]
			if !ok {
				return false
			}
			if !txnEntriesEqual(aEntry, bEntry) {
				return false
			}
		}
	}
	return true
}

// activeSlots returns the slot indices that currently have a file
// on disk (used by tests to inspect the on-disk shape).
func (s *TxnStateStore) activeSlots() ([]int, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var slots []int
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "slot-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		nstr := strings.TrimSuffix(strings.TrimPrefix(name, "slot-"), ".json")
		n, err := strconv.Atoi(nstr)
		if err != nil {
			continue
		}
		slots = append(slots, n)
	}
	return slots, nil
}
