package coordinator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// OffsetStore persists committed consumer group offsets to the shared PVC.
// Each group's offsets are stored as a JSON file under __consumer_offsets/.
// Writes are atomic (write tmp, os.Rename). Only the coordinator writes.
type OffsetStore struct {
	dataDir string
	mu      sync.RWMutex
	cache   map[string]map[string]int64 // groupID → "topic/partition" → offset

	// pending is the gh #27 in-flight transactional offset commit
	// buffer. Keyed by (groupID, producerID): the offsets a
	// transactional producer wrote via TxnOffsetCommit but the
	// transaction has not yet committed (EndTxn not yet seen).
	// Regular Fetch ignores these — they're invisible until the
	// txn coordinator signals CommitPending. EndTxn(abort) calls
	// DiscardPending.
	//
	// Persisted in memory only; on broker restart in-flight
	// transactional offsets are lost (correct: an unfinished txn
	// must rebuild from scratch). The committed Fetch view survives
	// restart via the regular cache/disk file.
	pending map[pendingKey]map[string]int64
}

// pendingKey is the (groupID, producerID) identity of a pending
// transactional offset-commit batch. Multiple producers can be
// mid-txn against the same group; each gets a separate slot.
type pendingKey struct {
	groupID    string
	producerID int64
}

// FetchSpec describes which topic partitions to fetch offsets for.
type FetchSpec struct {
	Topic      string
	Partitions []int32
}

func NewOffsetStore(dataDir string) *OffsetStore {
	return &OffsetStore{
		dataDir: dataDir,
		cache:   make(map[string]map[string]int64),
		pending: make(map[pendingKey]map[string]int64),
	}
}

// CommitPending stages offsets from a TxnOffsetCommit (API 28).
// They are NOT visible to OffsetFetch until CommitPending(committed=true)
// is called from the EndTxn path. Mirrors Apache's
// `GroupMetadataManager.storeOffsets(... producerId, producerEpoch ...)`
// which marks the offsets as in-flight in the offset-cache. gh #27.
func (s *OffsetStore) StorePending(groupID string, producerID int64, offsets map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := pendingKey{groupID, producerID}
	existing := s.pending[key]
	if existing == nil {
		existing = make(map[string]int64, len(offsets))
		s.pending[key] = existing
	}
	for k, v := range offsets {
		existing[k] = v
	}
}

// CommitPending materialises the staged offsets for (group, pid) as
// committed (merged into the visible cache + persisted). Called from
// the EndTxn(commit) handler — gh #27 / gh #114 follow-up will wire
// the cross-coordinator signal. Idempotent: no pending entry → nil.
func (s *OffsetStore) CommitPending(groupID string, producerID int64) error {
	s.mu.Lock()
	key := pendingKey{groupID, producerID}
	pendingOffsets, ok := s.pending[key]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	delete(s.pending, key)
	s.mu.Unlock()
	return s.Commit(groupID, pendingOffsets)
}

// DiscardPending drops staged offsets for (group, pid) without
// materialising. Called from EndTxn(abort). Idempotent.
func (s *OffsetStore) DiscardPending(groupID string, producerID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, pendingKey{groupID, producerID})
}

// PendingFor returns a snapshot of staged offsets for (group, pid).
// Exposed for tests; production code paths use CommitPending /
// DiscardPending. Returns nil when no pending entry exists.
func (s *OffsetStore) PendingFor(groupID string, producerID int64) map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.pending[pendingKey{groupID, producerID}]
	if src == nil {
		return nil
	}
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// OffsetKey is the "topic/partition" cache + on-disk JSON key. Exported
// so handlers and tests build keys identically to the coordinator
// (gh #100 added the per-partition OffsetDelete path that needs matching
// key construction at the wire layer).
func OffsetKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

func (s *OffsetStore) dir() string {
	return filepath.Join(s.dataDir, "__consumer_offsets")
}

// Commit atomically writes the committed offsets for a group.
// offsets maps "topic/partition" → committed offset.
func (s *OffsetStore) Commit(groupID string, offsets map[string]int64) error {
	if err := os.MkdirAll(s.dir(), 0o775); err != nil {
		return err
	}

	// Merge with existing cached offsets (only update the given keys).
	s.mu.Lock()
	existing := s.cache[groupID]
	if existing == nil {
		existing = make(map[string]int64)
		s.cache[groupID] = existing
	}
	for k, v := range offsets {
		existing[k] = v
	}
	merged := make(map[string]int64, len(existing))
	for k, v := range existing {
		merged[k] = v
	}
	s.mu.Unlock()

	data, err := json.Marshal(merged)
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir(), groupID+".tmp")
	final := filepath.Join(s.dir(), groupID+".json")
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

// Fetch returns committed offsets for the given group and topic partitions.
// Returns -1 for any partition with no committed offset.
func (s *OffsetStore) Fetch(groupID string, specs []FetchSpec) map[string]int64 {
	s.mu.RLock()
	group := s.cache[groupID]
	s.mu.RUnlock()

	result := make(map[string]int64)
	for _, spec := range specs {
		for _, p := range spec.Partitions {
			k := OffsetKey(spec.Topic, p)
			if group != nil {
				if v, ok := group[k]; ok {
					result[k] = v
					continue
				}
			}
			result[k] = -1
		}
	}
	return result
}

// HasGroup reports whether the in-memory cache has any offsets
// recorded for groupID. Used by Manager.DeleteGroups to detect a
// "group exists on disk but in-memory state is empty" case
// (typical for a coordinator that just took over the group and
// has Loaded the offsets but not yet seen a JoinGroup). Read-only;
// safe to call from the coordinator hot path.
func (s *OffsetStore) HasGroup(groupID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.cache[groupID]
	return ok
}

// Delete removes a group's offsets from cache and from disk. Used
// by Manager.DeleteGroups (gh #89). Idempotent: deleting an
// unknown group is a no-op (returns nil) so a partial-delete retry
// from the AdminClient doesn't surface spurious errors.
func (s *OffsetStore) Delete(groupID string) error {
	s.mu.Lock()
	delete(s.cache, groupID)
	s.mu.Unlock()

	path := filepath.Join(s.dir(), groupID+".json")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// DeletePartitions removes specific (topic, partition) offset entries
// from a group's committed offsets. Used by Manager.DeleteOffsets
// (gh #100) for OffsetDelete (API 47): kafka-consumer-groups
// --delete-offsets and AdminClient.deleteConsumerGroupOffsets.
//
// Returns the set of keys that were actually removed. Keys not present
// in the cache are silently absent — the wire-level UNKNOWN_TOPIC_OR_PARTITION
// mapping happens at the handler layer using the returned set.
//
// Mirrors Commit()'s lock-then-snap-then-write discipline: the disk
// write happens outside the cache lock to avoid extending hot-path
// contention into filesystem latency.
func (s *OffsetStore) DeletePartitions(groupID string, keys []string) (map[string]bool, error) {
	removed := make(map[string]bool, len(keys))
	s.mu.Lock()
	group := s.cache[groupID]
	if group == nil {
		s.mu.Unlock()
		return removed, nil
	}
	for _, k := range keys {
		if _, ok := group[k]; ok {
			delete(group, k)
			removed[k] = true
		}
	}
	snap := make(map[string]int64, len(group))
	for k, v := range group {
		snap[k] = v
	}
	s.mu.Unlock()

	if err := os.MkdirAll(s.dir(), 0o775); err != nil {
		return removed, err
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return removed, err
	}
	tmp := filepath.Join(s.dir(), groupID+".tmp")
	final := filepath.Join(s.dir(), groupID+".json")
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return removed, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return removed, err
	}
	return removed, nil
}

// Load reads a group's offsets from disk into the in-memory cache.
// Called when this broker becomes coordinator for the group.
func (s *OffsetStore) Load(groupID string) error {
	path := filepath.Join(s.dir(), groupID+".json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var offsets map[string]int64
	if err := json.Unmarshal(data, &offsets); err != nil {
		return err
	}
	s.mu.Lock()
	s.cache[groupID] = offsets
	s.mu.Unlock()
	return nil
}
