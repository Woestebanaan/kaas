package broker

import (
	"sort"
	"sync"

	"github.com/woestebanaan/skafka/internal/observability"
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
// is the count; Config carries the resolved KafkaTopic Spec.Config
// fields (gh #93) — every override that's been pushed by
// TopicWatcher and lives here lands in DescribeConfigs responses
// with ConfigSource=DYNAMIC_TOPIC_CONFIG. Pointer fields are nil
// when the CR didn't set the override; the cleaner / handler then
// fall through to the broker default.
type TopicMeta struct {
	Partitions int32
	Cleanup    CleanupPolicy
	Config     handlers.TopicConfig
	// TopicID carries the gh #105 / KIP-516 stable UUID (the
	// canonical hyphenated form the operator wrote to
	// KafkaTopic.Status.TopicID). Empty for topics the broker created
	// before the operator's status reconcile ran; the Metadata
	// handler falls back to the all-zero UUID on the wire.
	TopicID string
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
	existing := r.topics[name]
	existing.Partitions = partitions
	// Don't overwrite Cleanup if a watcher set it before Add fired
	// (CR-driven config can arrive ahead of the per-handler Add hint).
	r.topics[name] = existing
	r.mu.Unlock()
	// gh #115 / gh #121 PR1: baseline a zero observation as soon as
	// the broker knows the topic exists, so Grafana panels show it
	// even before the first Produce/Fetch. Apache parity — the MBean
	// meter registers on topic creation, not first traffic.
	observability.Global().TopicTraffic.Touch(name)
}

// SetCleanupPolicy is the gh #48 hook: TopicWatcher pushes the
// resolved cleanup.policy from the KafkaTopic CR's Spec.Config so
// the cleaner can dispatch correctly. Idempotent; called on every
// CR observation including no-op reconciles. Kept as a focused
// setter (separate from SetTopicConfig) for tests / callers that
// only care about the cleaner-relevant slice of the config.
func (r *TopicRegistry) SetCleanupPolicy(name string, policy CleanupPolicy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing := r.topics[name]
	existing.Cleanup = policy
	existing.Config.CleanupPolicy = string(policy)
	r.topics[name] = existing
}

// SetTopicConfig is the gh #93 hook: TopicWatcher pushes the
// resolved KafkaTopic CR Spec.Config so DescribeConfigs returns the
// effective per-topic values instead of broker defaults. Also
// updates Cleanup so SetTopicConfig fully supersedes the older
// SetCleanupPolicy contract — callers that have full config can
// call SetTopicConfig and skip the separate cleanup-policy push.
// Idempotent; safe to call on every CR observation.
func (r *TopicRegistry) SetTopicConfig(name string, cfg handlers.TopicConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing := r.topics[name]
	existing.Config = cfg
	existing.Cleanup = CleanupPolicy(cfg.CleanupPolicy)
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

// TopicConfig satisfies handlers.TopicConfigSource (gh #93). Returns
// the pushed-from-CR config plus an ok flag so the DescribeConfigs
// handler can fall through to broker defaults for unknown topics.
// The returned struct is a value copy — caller may mutate without
// affecting the registry.
func (r *TopicRegistry) TopicConfig(name string) (handlers.TopicConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.topics[name]
	if !ok {
		return handlers.TopicConfig{}, false
	}
	return t.Config, true
}

func (r *TopicRegistry) Remove(name string) {
	r.mu.Lock()
	delete(r.topics, name)
	r.mu.Unlock()
	// Drop the per-topic accumulator so the deleted topic stops
	// contributing a stale zero timeseries on every scrape.
	observability.Global().TopicTraffic.Forget(name)
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
		out = append(out, handlers.TopicEntry{Name: name, Partitions: t.Partitions, TopicID: t.TopicID})
	}
	return out
}

// SetTopicID stashes the gh #105 / KIP-516 stable UUID for a topic.
// Called from the TopicWatcher onEvent callback when a KafkaTopic's
// Status.TopicID is populated. Idempotent — the operator never
// rotates a topic's UUID, so reassignment is a no-op.
func (r *TopicRegistry) SetTopicID(name, topicID string) {
	if topicID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing := r.topics[name]
	existing.TopicID = topicID
	r.topics[name] = existing
}

// TopicID returns the KIP-516 UUID for a topic. Empty when unknown
// (pre-#105 CRs or topics created via local AddTopic without
// operator status reconcile).
func (r *TopicRegistry) TopicID(name string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.topics[name].TopicID
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
