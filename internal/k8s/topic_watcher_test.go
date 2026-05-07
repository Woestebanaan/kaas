package k8s

import (
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	operatorv1 "github.com/woestebanaan/skafka/operator/api/v1alpha1"
)

func newTestWatcher() (*TopicWatcher, *recordedEvents) {
	rec := &recordedEvents{}
	w := &TopicWatcher{
		cache:       make(map[string]watcherCacheEntry),
		terminating: make(map[string]struct{}),
		onEvent:     rec.append,
	}
	return w, rec
}

type recordedEvents struct {
	mu     sync.Mutex
	events []TopicEvent
}

func (r *recordedEvents) append(ev TopicEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recordedEvents) snapshot() []TopicEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]TopicEvent, len(r.events))
	copy(out, r.events)
	return out
}

func topic(name string, partitions int32) *operatorv1.KafkaTopic {
	return &operatorv1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       operatorv1.KafkaTopicSpec{Partitions: partitions},
	}
}

// topicWithPolicy is the gh #48 test fixture: a CR with both
// partition count and an explicit cleanup.policy. Used to verify
// the watcher surfaces the policy on every event.
func topicWithPolicy(name string, partitions int32, policy string) *operatorv1.KafkaTopic {
	return &operatorv1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: operatorv1.KafkaTopicSpec{
			Partitions: partitions,
			Config:     operatorv1.KafkaTopicConfig{CleanupPolicy: policy},
		},
	}
}

// terminatingTopic builds a CR that has a non-nil deletionTimestamp,
// the K8s shape we see while finalizers are still present.
func terminatingTopic(name string, partitions int32) *operatorv1.KafkaTopic {
	now := metav1.Now()
	return &operatorv1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			DeletionTimestamp: &now,
			Finalizers:        []string{"skafka.io/topic-cleanup"},
		},
		Spec: operatorv1.KafkaTopicSpec{Partitions: partitions},
	}
}

func TestTopicWatcher_AddedFiresOnceForNewTopic(t *testing.T) {
	w, rec := newTestWatcher()

	w.processEvent(watch.Added, topic("smoke", 3))

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(got), got)
	}
	if got[0] != (TopicEvent{Type: TopicAdded, Name: "smoke", Partitions: 3}) {
		t.Errorf("unexpected event: %+v", got[0])
	}
}

func TestTopicWatcher_DuplicateAddedSuppressed(t *testing.T) {
	w, rec := newTestWatcher()

	w.processEvent(watch.Added, topic("smoke", 3))
	w.processEvent(watch.Added, topic("smoke", 3))

	if got := rec.snapshot(); len(got) != 1 {
		t.Fatalf("expected 1 event after duplicate, got %d: %+v", len(got), got)
	}
}

func TestTopicWatcher_PrimedTopicSuppressesAdded(t *testing.T) {
	w, rec := newTestWatcher()
	w.Prime("smoke", 3)

	w.processEvent(watch.Added, topic("smoke", 3))

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("expected no event for primed topic, got %+v", got)
	}
}

func TestTopicWatcher_PartitionExpansionFiresModified(t *testing.T) {
	w, rec := newTestWatcher()
	w.Prime("smoke", 3)

	w.processEvent(watch.Modified, topic("smoke", 5))

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(got), got)
	}
	want := TopicEvent{Type: TopicModified, Name: "smoke", Partitions: 5, OldPartitions: 3}
	if got[0] != want {
		t.Errorf("unexpected event: %+v want %+v", got[0], want)
	}
}

func TestTopicWatcher_ModifiedSamePartitionsSuppressed(t *testing.T) {
	w, rec := newTestWatcher()
	w.Prime("smoke", 3)

	w.processEvent(watch.Modified, topic("smoke", 3))

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("expected no event for unchanged Modified, got %+v", got)
	}
}

func TestTopicWatcher_PartitionDecreaseSuppressed(t *testing.T) {
	w, rec := newTestWatcher()
	w.Prime("smoke", 5)

	w.processEvent(watch.Modified, topic("smoke", 3))

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("expected no event for partition decrease, got %+v", got)
	}
	// Cache should NOT shrink — the operator already rejects shrinks.
	w.mu.Lock()
	cached := w.cache["smoke"]
	w.mu.Unlock()
	if cached.Partitions != 5 {
		t.Errorf("cache shrank: got %d want 5", cached.Partitions)
	}
}

func TestTopicWatcher_DeletedFiresWithLastPartitionCount(t *testing.T) {
	w, rec := newTestWatcher()
	w.Prime("smoke", 3)

	w.processEvent(watch.Deleted, topic("smoke", 0))

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(got), got)
	}
	want := TopicEvent{Type: TopicDeleted, Name: "smoke", Partitions: 3}
	if got[0] != want {
		t.Errorf("unexpected event: %+v want %+v", got[0], want)
	}
}

func TestTopicWatcher_DeletedUnknownTopicSuppressed(t *testing.T) {
	w, rec := newTestWatcher()

	w.processEvent(watch.Deleted, topic("ghost", 0))

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("expected no event for unknown topic, got %+v", got)
	}
}

// TestTopicWatcher_TerminatingFiresDeleteFromCache guards gh #76:
// when a CR's deletionTimestamp goes non-nil, the watcher must fire
// TopicDeleted immediately (before the K8s Deleted event, which only
// arrives after finalizers are gone) so the broker can close its
// log file handles. Without this, NFS silly-renames EBUSY the
// operator's unlinkat forever.
func TestTopicWatcher_TerminatingFiresDeleteFromCache(t *testing.T) {
	w, rec := newTestWatcher()
	w.Prime("smoke", 3)

	w.processEvent(watch.Modified, terminatingTopic("smoke", 3))

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(got), got)
	}
	want := TopicEvent{Type: TopicDeleted, Name: "smoke", Partitions: 3}
	if got[0] != want {
		t.Errorf("got %+v, want %+v", got[0], want)
	}
}

// TestTopicWatcher_TerminatingFallsBackToSpec covers the broker-startup
// case where the watcher's cache is empty (no Added event yet) but the
// initial reconcile observes a Terminating topic. The watcher must
// still fire so that broker-side handles get closed.
func TestTopicWatcher_TerminatingFallsBackToSpec(t *testing.T) {
	w, rec := newTestWatcher()

	w.processEvent(watch.Modified, terminatingTopic("audit-log", 1))

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(got), got)
	}
	if got[0] != (TopicEvent{Type: TopicDeleted, Name: "audit-log", Partitions: 1}) {
		t.Errorf("unexpected event: %+v", got[0])
	}
}

// TestTopicWatcher_TerminatingDedupsRepeatedEvents ensures the
// suppression set works: while the operator's finalizer churns
// (status updates, condition flips), K8s emits multiple Modified
// events for the same Terminating CR. We must not fire TopicDeleted
// every time — the broker would close already-closed handles, and
// every fire triggers a controller assignment recompute (gh #74).
func TestTopicWatcher_TerminatingDedupsRepeatedEvents(t *testing.T) {
	w, rec := newTestWatcher()
	w.Prime("smoke", 3)

	for i := 0; i < 4; i++ {
		w.processEvent(watch.Modified, terminatingTopic("smoke", 3))
	}

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 event after 4 Modified ticks, got %d", len(got))
	}
}

func TestTopicWatcher_PartitionDecreaseSuppressesCacheUpdate(t *testing.T) {
	// Important: after a suppressed shrink, a subsequent legitimate expansion
	// must compare against the old (larger) cached count, not the rejected new one.
	w, rec := newTestWatcher()
	w.Prime("smoke", 5)

	w.processEvent(watch.Modified, topic("smoke", 3))
	w.processEvent(watch.Modified, topic("smoke", 7))

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(got), got)
	}
	want := TopicEvent{Type: TopicModified, Name: "smoke", Partitions: 7, OldPartitions: 5}
	if got[0] != want {
		t.Errorf("unexpected event: %+v want %+v", got[0], want)
	}
}

// TestTopicWatcher_AddedSurfacesCleanupPolicy guards gh #48: a
// freshly-added CR with cleanup.policy=compact must propagate
// that to the broker via the TopicEvent's CleanupPolicy field.
// Without this, the broker's TopicRegistry never learns the
// policy and the cleaner dispatches every partition to retention
// — silently losing compaction for compact-topic users.
func TestTopicWatcher_AddedSurfacesCleanupPolicy(t *testing.T) {
	w, rec := newTestWatcher()
	w.processEvent(watch.Added, topicWithPolicy("changelog", 1, "compact"))

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	want := TopicEvent{Type: TopicAdded, Name: "changelog", Partitions: 1, CleanupPolicy: "compact"}
	if got[0] != want {
		t.Errorf("Added event: got %+v, want %+v", got[0], want)
	}
}

// TestTopicWatcher_PolicyMutationFiresModified guards the
// follow-up gh #48 case: a CR whose ONLY change is cleanup.policy
// (kubectl edit kafkatopic, or operator-side reconcile) must fire
// TopicModified so the broker re-dispatches the cleaner. Without
// it, a "kubectl edit kafkatopic foo --type merge -p ...
// cleanup.policy: compact" silently never starts compacting.
//
// Pre-#48 the watcher only fired Modified on partition increase,
// so this test is the regression guard for the new policy-only
// branch in handleUpsert.
func TestTopicWatcher_PolicyMutationFiresModified(t *testing.T) {
	w, rec := newTestWatcher()
	// First observation: delete-policy default.
	w.processEvent(watch.Added, topicWithPolicy("evolving", 1, "delete"))
	// Second observation: same partition count, policy flipped.
	w.processEvent(watch.Modified, topicWithPolicy("evolving", 1, "compact"))

	got := rec.snapshot()
	if len(got) != 2 {
		t.Fatalf("expected 2 events (Added + Modified), got %d: %+v", len(got), got)
	}
	if got[0].CleanupPolicy != "delete" {
		t.Errorf("Added event policy=%q, want delete", got[0].CleanupPolicy)
	}
	want := TopicEvent{
		Type:          TopicModified,
		Name:          "evolving",
		Partitions:    1,
		OldPartitions: 1,
		CleanupPolicy: "compact",
	}
	if got[1] != want {
		t.Errorf("Modified event on policy flip: got %+v, want %+v", got[1], want)
	}
}

// TestTopicWatcher_PartitionExpansionWithPolicy: a CR that both
// expands partitions AND changes policy in the same observation
// fires ONE Modified event carrying the new values. Catches a
// regression where the watcher fires duplicate Modified events
// (one per change axis).
func TestTopicWatcher_PartitionExpansionWithPolicy(t *testing.T) {
	w, rec := newTestWatcher()
	w.processEvent(watch.Added, topicWithPolicy("growing", 3, "delete"))
	w.processEvent(watch.Modified, topicWithPolicy("growing", 5, "compact"))

	got := rec.snapshot()
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d: %+v", len(got), got)
	}
	want := TopicEvent{
		Type:          TopicModified,
		Name:          "growing",
		Partitions:    5,
		OldPartitions: 3,
		CleanupPolicy: "compact",
	}
	if got[1] != want {
		t.Errorf("dual-axis Modified: got %+v, want %+v", got[1], want)
	}
}

// TestTopicWatcher_DeletedCarriesLastKnownPolicy: TopicDeleted
// must surface the cleanup.policy that was in effect at the
// moment of deletion. Lets the broker's onDelete handler decide
// whether the partition needs a final compaction pass before
// cleanup (a future hardening; today the broker just closes
// handles, but the field is in the contract).
func TestTopicWatcher_DeletedCarriesLastKnownPolicy(t *testing.T) {
	w, rec := newTestWatcher()
	w.processEvent(watch.Added, topicWithPolicy("ephemeral", 2, "compact"))
	w.processEvent(watch.Deleted, topicWithPolicy("ephemeral", 2, "compact"))

	got := rec.snapshot()
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[1].Type != TopicDeleted {
		t.Fatalf("second event type=%v, want Deleted", got[1].Type)
	}
	if got[1].CleanupPolicy != "compact" {
		t.Errorf("Deleted event policy=%q, want compact (last-known)", got[1].CleanupPolicy)
	}
}
