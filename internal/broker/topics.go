package broker

import (
	"sort"
	"sync"

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
)

// CleanupPolicy is the per-topic retention/compaction strategy.
// Mirrors Apache Kafka's `cleanup.policy`: "delete" (the default,
// drop oldest segments by retention.ms / retention.bytes),
// "compact" (keep latest value per key — gh #48), or
// "compact,delete" (apply both — Streams uses this for changelog
// topics under EOS).
type CleanupPolicy string

const (
	CleanupPolicyDelete        CleanupPolicy = "delete"
	CleanupPolicyCompact       CleanupPolicy = "compact"
	CleanupPolicyCompactDelete CleanupPolicy = "compact,delete"
)

// IsCompact reports whether this policy involves compaction.
// Used by the cleaner to decide whether to dispatch through the
// compactor or stay on the retention-only path.
func (p CleanupPolicy) IsCompact() bool {
	return p == CleanupPolicyCompact || p == CleanupPolicyCompactDelete
}

// IsDelete reports whether this policy involves time/size-based
// deletion. Default-policy topics ("" or unset) count as delete
// because skafka's retention cleaner has always run on them.
func (p CleanupPolicy) IsDelete() bool {
	return p == "" || p == CleanupPolicyDelete || p == CleanupPolicyCompactDelete
}

// TopicMeta is the per-topic record TopicRegistry holds. Partitions
// is the count; Cleanup is the per-topic cleanup.policy when known
// (empty means "use the default = delete"). Other config fields
// will land here as #48 follow-ups need them.
type TopicMeta struct {
	Partitions int32
	Cleanup    CleanupPolicy
}

// TopicRegistry is a thread-safe in-memory cache of topic metadata.
// It satisfies handlers.TopicSource and handlers.TopicWriter.
type TopicRegistry struct {
	mu     sync.RWMutex
	topics map[string]TopicMeta
}

func NewTopicRegistry() *TopicRegistry {
	return &TopicRegistry{topics: make(map[string]TopicMeta)}
}

// Add registers a topic with default config (cleanup.policy=delete).
// Used by the protocol-handlers TopicWriter contract — admin
// CreateTopics calls this, and the post-config UpdateConfig follows
// from the TopicWatcher's CR observation.
func (r *TopicRegistry) Add(name string, partitions int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing := r.topics[name]
	existing.Partitions = partitions
	// Don't overwrite Cleanup if a watcher set it before Add fired
	// (CR-driven config can arrive ahead of the per-handler Add hint).
	r.topics[name] = existing
}

// SetCleanupPolicy is the gh #48 hook: TopicWatcher pushes the
// resolved cleanup.policy from the KafkaTopic CR's Spec.Config so
// the cleaner can dispatch correctly. Idempotent; called on every
// CR observation including no-op reconciles.
func (r *TopicRegistry) SetCleanupPolicy(name string, policy CleanupPolicy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing := r.topics[name]
	existing.Cleanup = policy
	r.topics[name] = existing
}

// CleanupPolicy returns the per-topic policy. Unknown topic ⇒
// empty string, which IsDelete()=true (the safe default — never
// silently start compacting a topic the broker doesn't know about).
func (r *TopicRegistry) CleanupPolicy(name string) CleanupPolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.topics[name].Cleanup
}

func (r *TopicRegistry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.topics, name)
}

// Get satisfies handlers.TopicSource.
func (r *TopicRegistry) Get(name string) (int32, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.topics[name]
	return t.Partitions, ok
}

// All satisfies handlers.TopicSource.
func (r *TopicRegistry) All() []handlers.TopicEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]handlers.TopicEntry, 0, len(r.topics))
	for name, t := range r.topics {
		out = append(out, handlers.TopicEntry{Name: name, Partitions: t.Partitions})
	}
	return out
}

// AllNames returns the topic names sorted lexically — used by the
// AssignmentLoop GroupSource adapter and other diagnostics that need
// stable ordering. Allocates a fresh slice; caller may mutate.
func (r *TopicRegistry) AllNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.topics))
	for name := range r.topics {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
