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
	}
}

func offsetKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

func (s *OffsetStore) dir() string {
	return filepath.Join(s.dataDir, "__consumer_offsets")
}

// Commit atomically writes the committed offsets for a group.
// offsets maps "topic/partition" → committed offset.
func (s *OffsetStore) Commit(groupID string, offsets map[string]int64) error {
	if err := os.MkdirAll(s.dir(), 0755); err != nil {
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
			k := offsetKey(spec.Topic, p)
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
