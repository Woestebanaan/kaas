package k8s

import (
	"context"
	"log/slog"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	sigs_client "sigs.k8s.io/controller-runtime/pkg/client"

	operatorv1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

// TopicEventType describes the kind of change observed for a KafkaTopic.
type TopicEventType int

const (
	TopicAdded TopicEventType = iota
	TopicModified
	TopicDeleted
)

// TopicEvent is fired by TopicWatcher whenever the observed state of a KafkaTopic
// CR diverges from the watcher's last known state for that topic.
type TopicEvent struct {
	Type          TopicEventType
	Name          string
	Partitions    int32 // current count for Added/Modified, last-seen count for Deleted
	OldPartitions int32 // previous count (only set for Modified)
}

// TopicWatcher streams KafkaTopic CR changes from the API server and fires a
// callback whenever an observation differs from the watcher's cached state.
//
// Existing topics that the broker already discovered at startup should be
// announced via Prime before Run starts, so the watch-restart re-list does not
// re-fire callbacks for them.
type TopicWatcher struct {
	client    sigs_client.WithWatch
	namespace string
	onEvent   func(TopicEvent)
	backoff   time.Duration

	mu    sync.Mutex
	cache map[string]int32
}

// NewTopicWatcher builds a watcher bound to the KafkaTopic CRD in namespace.
func NewTopicWatcher(cfg *rest.Config, namespace string, onEvent func(TopicEvent)) (*TopicWatcher, error) {
	scheme := runtime.NewScheme()
	if err := operatorv1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	c, err := sigs_client.NewWithWatch(cfg, sigs_client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	return &TopicWatcher{
		client:    c,
		namespace: namespace,
		onEvent:   onEvent,
		backoff:   time.Second,
		cache:     make(map[string]int32),
	}, nil
}

// Prime seeds the watcher's cache so the first observation of name does not
// fire a TopicAdded callback.
func (w *TopicWatcher) Prime(name string, partitions int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cache[name] = partitions
}

// Run reconciles cache state against a List, then watches for changes.
// Loops with backoff on error until ctx is cancelled.
func (w *TopicWatcher) Run(ctx context.Context) error {
	for {
		rv, err := w.reconcile(ctx)
		if err != nil {
			slog.Error("topic watcher: list failed", "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.backoff):
				continue
			}
		}

		var list operatorv1.KafkaTopicList
		opts := &sigs_client.ListOptions{Namespace: w.namespace, Raw: &metav1.ListOptions{ResourceVersion: rv}}
		watcher, err := w.client.Watch(ctx, &list, opts)
		if err != nil {
			slog.Error("topic watcher: start watch failed", "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.backoff):
				continue
			}
		}
		if err := w.consume(ctx, watcher); err != nil {
			watcher.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				slog.Warn("topic watcher: restarting after error", "err", err)
			}
		}
	}
}

// reconcile lists all KafkaTopics in the namespace and fires divergence events
// against the cache. Returns the list's resourceVersion for the next Watch.
func (w *TopicWatcher) reconcile(ctx context.Context) (string, error) {
	var list operatorv1.KafkaTopicList
	if err := w.client.List(ctx, &list, &sigs_client.ListOptions{Namespace: w.namespace}); err != nil {
		return "", err
	}
	seen := make(map[string]struct{}, len(list.Items))
	for i := range list.Items {
		t := &list.Items[i]
		seen[t.Name] = struct{}{}
		w.handleUpsert(t)
	}
	// Fire Deleted for cached topics no longer present.
	w.mu.Lock()
	missing := make([]string, 0)
	for name := range w.cache {
		if _, ok := seen[name]; !ok {
			missing = append(missing, name)
		}
	}
	w.mu.Unlock()
	for _, name := range missing {
		w.handleDelete(&operatorv1.KafkaTopic{ObjectMeta: metav1.ObjectMeta{Name: name}})
	}
	return list.ResourceVersion, nil
}

func (w *TopicWatcher) consume(ctx context.Context, watcher watch.Interface) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-watcher.ResultChan():
			if !ok {
				return nil
			}
			t, ok := ev.Object.(*operatorv1.KafkaTopic)
			if !ok {
				continue
			}
			w.processEvent(ev.Type, t)
		}
	}
}

// processEvent applies a single watch event to the cache and fires onEvent if
// the observation diverges from cached state. Exposed for unit tests.
func (w *TopicWatcher) processEvent(eventType watch.EventType, t *operatorv1.KafkaTopic) {
	switch eventType {
	case watch.Added, watch.Modified:
		w.handleUpsert(t)
	case watch.Deleted:
		w.handleDelete(t)
	}
}

func (w *TopicWatcher) handleUpsert(t *operatorv1.KafkaTopic) {
	w.mu.Lock()
	old, existed := w.cache[t.Name]
	// Don't shrink the cached count — the operator rejects shrinks, and keeping
	// the larger value lets a later legitimate expansion still produce a
	// Modified event with the correct OldPartitions.
	if !existed || t.Spec.Partitions > old {
		w.cache[t.Name] = t.Spec.Partitions
	}
	w.mu.Unlock()

	switch {
	case !existed:
		w.fire(TopicEvent{Type: TopicAdded, Name: t.Name, Partitions: t.Spec.Partitions})
	case old < t.Spec.Partitions:
		w.fire(TopicEvent{Type: TopicModified, Name: t.Name, Partitions: t.Spec.Partitions, OldPartitions: old})
	case old > t.Spec.Partitions:
		slog.Warn("topic watcher: ignoring partition decrease", "topic", t.Name, "old", old, "new", t.Spec.Partitions)
	}
}

func (w *TopicWatcher) handleDelete(t *operatorv1.KafkaTopic) {
	w.mu.Lock()
	last, existed := w.cache[t.Name]
	delete(w.cache, t.Name)
	w.mu.Unlock()
	if !existed {
		return
	}
	w.fire(TopicEvent{Type: TopicDeleted, Name: t.Name, Partitions: last})
}

func (w *TopicWatcher) fire(ev TopicEvent) {
	if w.onEvent != nil {
		w.onEvent(ev)
	}
}
