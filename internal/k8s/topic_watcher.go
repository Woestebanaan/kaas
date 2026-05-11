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

	"github.com/woestebanaan/skafka/internal/protocol/handlers"
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
//
// CleanupPolicy carries the resolved Spec.Config.CleanupPolicy from
// the CR — empty when unset (which the broker treats as
// cleanup.policy=delete, the safe default). Used by gh #48 to decide
// which partitions enter the compactor's path.
//
// Config (gh #93) carries the resolved full per-topic CR config so
// the broker's TopicRegistry can serve DescribeConfigs with effective
// values instead of static broker defaults. CleanupPolicy is
// duplicated into both Config.CleanupPolicy and the typed
// CleanupPolicy field above to keep the gh #48 cleaner-dispatch
// callsites untouched while gh #93 plumbs the rest through.
type TopicEvent struct {
	Type           TopicEventType
	Name           string
	Partitions     int32 // current count for Added/Modified, last-seen count for Deleted
	OldPartitions  int32 // previous count (only set for Modified)
	CleanupPolicy  string
	Config         handlers.TopicConfig
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

	mu          sync.Mutex
	cache       map[string]watcherCacheEntry
	terminating map[string]struct{} // gh #76: track CRs we've already fired TopicDeleted for while finalizers churn, suppress duplicates
}

// watcherCacheEntry is the watcher's per-topic cached view. Keeping
// cleanupPolicy alongside partitions lets us fire TopicModified on
// policy mutation (gh #48) — Modified used to fire only on partition
// increase, but a CR change from cleanup.policy=delete to compact
// is just as broker-relevant. Config (gh #93) extends the same
// idea to retention.ms / retention.bytes / segment.bytes / etc.,
// so DescribeConfigs sees CR edits without a broker restart.
type watcherCacheEntry struct {
	Partitions    int32
	CleanupPolicy string
	Config        handlers.TopicConfig
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
		client:      c,
		namespace:   namespace,
		onEvent:     onEvent,
		backoff:     time.Second,
		cache:       make(map[string]watcherCacheEntry),
		terminating: make(map[string]struct{}),
	}, nil
}

// Prime seeds the watcher's cache so the first observation of name does not
// fire a TopicAdded callback.
func (w *TopicWatcher) Prime(name string, partitions int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cache[name] = watcherCacheEntry{Partitions: partitions}
}

// Run reconciles cache state against a List, then watches for changes.
// Loops with backoff on error until ctx is cancelled.
func (w *TopicWatcher) Run(ctx context.Context) error {
	for {
		rv, err := w.reconcile(ctx)
		if err != nil {
			slog.Error("topic watcher: listing KafkaTopic CRs from the API server failed (broker won't see new-topic-creation or delete events until reconcile succeeds; retries after backoff)",
				"backoff", w.backoff, "err", err)
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
		// Cache + event names key off EffectiveTopicName (gh #86 — a
		// CR with spec.topicName set has the literal Kafka name there,
		// not in metadata.name).
		seen[t.EffectiveTopicName()] = struct{}{}
		// Route through processEvent so Terminating CRs (with a non-nil
		// deletionTimestamp) reach handleTerminating during the initial
		// reconcile, not just during watch events. Without this, a
		// broker that comes up while topics are already mid-deletion
		// never fires TopicDeleted, and the operator's finalizer keeps
		// hitting NFS .nfsXXXX EBUSY because the broker re-opens the
		// segment files (gh #76).
		w.processEvent(watch.Modified, t)
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
		// The synthetic CR carries the Kafka name as metadata.name with
		// an empty Spec.TopicName, so EffectiveTopicName() inside
		// handleDelete falls back to metadata.name and matches the
		// cache key.
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
	// A CR with a non-nil deletionTimestamp is being torn down — fire
	// TopicDeleted now (rather than waiting for the K8s Deleted event
	// after finalizers clear) so the broker can close its log file
	// handles before the operator's finalizer tries to unlink the
	// partition dirs. On NFS, those open handles turn into .nfsXXXX
	// silly-renames that EBUSY the unlinkat (gh #76). Suppressed
	// across repeated Modified events for the same Terminating CR via
	// the terminating set.
	if t.DeletionTimestamp != nil {
		w.handleTerminating(t)
		return
	}
	switch eventType {
	case watch.Added, watch.Modified:
		w.handleUpsert(t)
	case watch.Deleted:
		w.handleDelete(t)
	}
}

// handleTerminating fires TopicDeleted once for a Terminating CR.
// Falls back to spec.partitions when cache is empty (broker just
// started, missed the Added event for a topic that's already
// Terminating).
func (w *TopicWatcher) handleTerminating(t *operatorv1.KafkaTopic) {
	name := t.EffectiveTopicName()
	w.mu.Lock()
	if _, alreadyFired := w.terminating[name]; alreadyFired {
		w.mu.Unlock()
		return
	}
	last, existed := w.cache[name]
	delete(w.cache, name)
	w.terminating[name] = struct{}{}
	w.mu.Unlock()

	if !existed {
		last = watcherCacheEntry{Partitions: t.Spec.Partitions}
	}
	if last.Partitions <= 0 {
		return
	}
	w.fire(TopicEvent{
		Type:          TopicDeleted,
		Name:          name,
		Partitions:    last.Partitions,
		CleanupPolicy: last.CleanupPolicy,
		Config:        last.Config,
	})
}

func (w *TopicWatcher) handleUpsert(t *operatorv1.KafkaTopic) {
	name := t.EffectiveTopicName()
	newPolicy := t.Spec.Config.CleanupPolicy
	newParts := t.Spec.Partitions
	newCfg := topicConfigFromCR(t)

	w.mu.Lock()
	old, existed := w.cache[name]
	// Compute the next cached state. Don't shrink Partitions —
	// operator rejects shrinks, and capping at max(old,new) lets a
	// later legitimate expansion still produce Modified with the
	// correct OldPartitions. Config is the only mutation we accept
	// in-place.
	next := old
	if !existed || newParts > old.Partitions {
		next.Partitions = newParts
	}
	next.CleanupPolicy = newPolicy
	next.Config = newCfg
	w.cache[name] = next
	w.mu.Unlock()

	switch {
	case !existed:
		w.fire(TopicEvent{
			Type:          TopicAdded,
			Name:          name,
			Partitions:    newParts,
			CleanupPolicy: newPolicy,
			Config:        newCfg,
		})
	case newParts > old.Partitions:
		w.fire(TopicEvent{
			Type:          TopicModified,
			Name:          name,
			Partitions:    newParts,
			OldPartitions: old.Partitions,
			CleanupPolicy: newPolicy,
			Config:        newCfg,
		})
	case !topicConfigEqual(old.Config, newCfg):
		// Config-only mutation (cleanup.policy, retention.ms,
		// segment.bytes, etc.): emit Modified with unchanged
		// partition count so the broker (gh #48) can re-dispatch
		// retention-vs-compactor and DescribeConfigs (gh #93)
		// returns the new effective values without a restart.
		w.fire(TopicEvent{
			Type:          TopicModified,
			Name:          name,
			Partitions:    old.Partitions,
			OldPartitions: old.Partitions,
			CleanupPolicy: newPolicy,
			Config:        newCfg,
		})
	case newParts < old.Partitions:
		slog.Warn("topic watcher: ignoring partition decrease", "topic", name, "old", old.Partitions, "new", newParts)
	}
}

// topicConfigFromCR projects KafkaTopicConfig (the operator type) onto
// handlers.TopicConfig (the broker-side type). Pointer fields are
// passed through verbatim — the broker treats nil as "no override,
// fall through to default".
func topicConfigFromCR(t *operatorv1.KafkaTopic) handlers.TopicConfig {
	c := t.Spec.Config
	return handlers.TopicConfig{
		CleanupPolicy:      c.CleanupPolicy,
		RetentionMs:        c.RetentionMs,
		RetentionBytes:     c.RetentionBytes,
		SegmentBytes:       c.SegmentBytes,
		MinCompactionLagMs: c.MinCompactionLagMs,
		DeleteRetentionMs:  c.DeleteRetentionMs,
	}
}

// topicConfigEqual is a value-equality check that follows pointers
// (so two distinct *int64 with the same target compare equal).
// reflect.DeepEqual would do the same, but rolling it inline keeps
// the watcher path off the reflect dependency for what is a hot
// per-event check.
func topicConfigEqual(a, b handlers.TopicConfig) bool {
	if a.CleanupPolicy != b.CleanupPolicy {
		return false
	}
	for _, pair := range []struct{ x, y *int64 }{
		{a.RetentionMs, b.RetentionMs},
		{a.RetentionBytes, b.RetentionBytes},
		{a.SegmentBytes, b.SegmentBytes},
		{a.MinCompactionLagMs, b.MinCompactionLagMs},
		{a.DeleteRetentionMs, b.DeleteRetentionMs},
	} {
		switch {
		case pair.x == nil && pair.y == nil:
			continue
		case pair.x == nil || pair.y == nil:
			return false
		case *pair.x != *pair.y:
			return false
		}
	}
	return true
}

func (w *TopicWatcher) handleDelete(t *operatorv1.KafkaTopic) {
	name := t.EffectiveTopicName()
	w.mu.Lock()
	last, existed := w.cache[name]
	delete(w.cache, name)
	delete(w.terminating, name)
	w.mu.Unlock()
	if !existed {
		return
	}
	w.fire(TopicEvent{
		Type:          TopicDeleted,
		Name:          name,
		Partitions:    last.Partitions,
		CleanupPolicy: last.CleanupPolicy,
		Config:        last.Config,
	})
}

func (w *TopicWatcher) fire(ev TopicEvent) {
	if w.onEvent != nil {
		w.onEvent(ev)
	}
}
