package broker

import (
	"sync"

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
)

// TopicRegistry is a thread-safe in-memory cache of topic metadata.
// It satisfies handlers.TopicSource and handlers.TopicWriter.
type TopicRegistry struct {
	mu     sync.RWMutex
	topics map[string]int32 // name → partition count
}

func NewTopicRegistry() *TopicRegistry {
	return &TopicRegistry{topics: make(map[string]int32)}
}

func (r *TopicRegistry) Add(name string, partitions int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topics[name] = partitions
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
	p, ok := r.topics[name]
	return p, ok
}

// All satisfies handlers.TopicSource.
func (r *TopicRegistry) All() []handlers.TopicEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]handlers.TopicEntry, 0, len(r.topics))
	for name, p := range r.topics {
		out = append(out, handlers.TopicEntry{Name: name, Partitions: p})
	}
	return out
}
