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
	// gh #21: parallel metadata map. Keyed identically to cache.
	// Empty string is the wire null sentinel ("no metadata"), so we
	// only store entries whose value is non-empty to keep the JSON
	// compact. Updates land alongside cache writes inside Commit.
	metadata map[string]map[string]string

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
		dataDir:  dataDir,
		cache:    make(map[string]map[string]int64),
		metadata: make(map[string]map[string]string),
		pending:  make(map[pendingKey]map[string]int64),
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

// Commit atomically writes the committed offsets for a group with no
// per-partition metadata. Equivalent to CommitWithMetadata(groupID,
// offsets, nil); preserved for callers that don't carry metadata
// (TxnOffsetCommit flow + internal compaction paths).
func (s *OffsetStore) Commit(groupID string, offsets map[string]int64) error {
	return s.CommitWithMetadata(groupID, offsets, nil)
}

// CommitWithMetadata atomically writes the committed offsets for a
// group, plus an optional per-partition metadata string (gh #21).
// metadata may be nil OR may contain a subset of the keys in offsets;
// any partition with an empty/missing metadata entry is stored without
// a metadata blob, which round-trips back as the wire null sentinel.
// Mirrors Apache Kafka's OffsetCommit semantics where metadata is
// opaque to the broker and round-trips per-partition.
func (s *OffsetStore) CommitWithMetadata(groupID string, offsets map[string]int64, metadata map[string]string) error {
	if err := os.MkdirAll(s.dir(), 0o775); err != nil {
		return err
	}

	// Merge with existing cached offsets + metadata (only update the
	// given keys). Both maps stay in sync; deleting a metadata entry
	// is done by passing "" — we store no entry so future fetches see
	// the empty string (== null on the wire).
	s.mu.Lock()
	existing := s.cache[groupID]
	if existing == nil {
		existing = make(map[string]int64)
		s.cache[groupID] = existing
	}
	existingMeta := s.metadata[groupID]
	if existingMeta == nil {
		existingMeta = make(map[string]string)
		s.metadata[groupID] = existingMeta
	}
	for k, v := range offsets {
		existing[k] = v
	}
	for k, v := range metadata {
		if v == "" {
			delete(existingMeta, k)
			continue
		}
		existingMeta[k] = v
	}
	mergedOffsets := make(map[string]int64, len(existing))
	for k, v := range existing {
		mergedOffsets[k] = v
	}
	mergedMeta := make(map[string]string, len(existingMeta))
	for k, v := range existingMeta {
		mergedMeta[k] = v
	}
	s.mu.Unlock()

	// On-disk schema (gh #21): {"offsets": {...}, "metadata": {...}}.
	// Older files (pre-#21) wrote a plain map[string]int64; load() in
	// readOffsetsFile parses both shapes for forward compatibility.
	payload := offsetFileV2{Offsets: mergedOffsets, Metadata: mergedMeta}
	data, err := json.Marshal(payload)
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

// offsetFileV2 is the gh #21 disk schema. The plain-map form remains
// readable via the unmarshalGroupFile helper.
type offsetFileV2 struct {
	Offsets  map[string]int64  `json:"offsets"`
	Metadata map[string]string `json:"metadata,omitempty"`
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

// FetchMetadata returns the per-partition metadata blob committed
// alongside each offset (gh #21). Keys missing from the returned map
// have no metadata (the wire null sentinel — empty string).
func (s *OffsetStore) FetchMetadata(groupID string, specs []FetchSpec) map[string]string {
	s.mu.RLock()
	group := s.metadata[groupID]
	s.mu.RUnlock()
	out := make(map[string]string)
	if group == nil {
		return out
	}
	for _, spec := range specs {
		for _, p := range spec.Partitions {
			k := OffsetKey(spec.Topic, p)
			if v, ok := group[k]; ok {
				out[k] = v
			}
		}
	}
	return out
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
// Supports both the gh #21 v2 schema (object with offsets + metadata)
// and the legacy v1 plain-map schema, so a pre-v0.1.163 group file
// loads cleanly after upgrade.
func (s *OffsetStore) Load(groupID string) error {
	path := filepath.Join(s.dir(), groupID+".json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	offsets, metadata, err := decodeOffsetsFile(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.cache[groupID] = offsets
	if metadata != nil {
		s.metadata[groupID] = metadata
	}
	s.mu.Unlock()
	return nil
}

// decodeOffsetsFile parses a __consumer_offsets/<group>.json blob.
// Tries the v2 envelope first; falls back to the legacy v1 plain
// map[string]int64 shape. Returning nil metadata for the legacy
// shape lets Load skip touching s.metadata for unmigrated groups.
func decodeOffsetsFile(data []byte) (map[string]int64, map[string]string, error) {
	var v2 offsetFileV2
	if err := json.Unmarshal(data, &v2); err == nil && v2.Offsets != nil {
		return v2.Offsets, v2.Metadata, nil
	}
	var v1 map[string]int64
	if err := json.Unmarshal(data, &v1); err != nil {
		return nil, nil, err
	}
	return v1, nil, nil
}
