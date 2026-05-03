package broker

import (
	"context"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// ControllerWatch tracks the singleton skafka-controller Lease so brokers
// know (a) who the current controller is — for routing the heartbeat client
// — and (b) the current leaseTransitions value — for validating the epoch
// stamped on assignment.json.
//
// A simple poll loop (default 1s) is enough: brokers don't need sub-second
// awareness of the controller identity, and the assignment file's own
// fsnotify+poll already handles the fast path for assignment changes.
// Avoiding a full informer keeps the dependency graph small.
type ControllerWatch struct {
	client    kubernetes.Interface
	namespace string

	pollInterval time.Duration

	epoch  atomic.Int64
	holder atomic.Value // string
}

// NewControllerWatch builds a watcher rooted at the given namespace. The
// watcher Gets the controller Lease every 1s by default; pass a shorter
// interval in tests via WithPollInterval.
func NewControllerWatch(client kubernetes.Interface, namespace string) *ControllerWatch {
	w := &ControllerWatch{
		client:       client,
		namespace:    namespace,
		pollInterval: 1 * time.Second,
	}
	w.holder.Store("")
	return w
}

// WithPollInterval overrides the default poll cadence.
func (w *ControllerWatch) WithPollInterval(d time.Duration) *ControllerWatch {
	w.pollInterval = d
	return w
}

// CurrentEpoch returns the most recently observed leaseTransitions value.
// Returns 0 when no Lease has been observed yet.
func (w *ControllerWatch) CurrentEpoch() int64 {
	return w.epoch.Load()
}

// CurrentHolder returns the most recently observed Lease holder identity.
// Empty string when no Lease has been observed yet, or when the Lease
// exists but has no holderIdentity (a transient state during election).
func (w *ControllerWatch) CurrentHolder() string {
	v, _ := w.holder.Load().(string)
	return v
}

// Run blocks, polling the Lease until ctx is cancelled. The first poll
// happens immediately so callers see a populated CurrentEpoch quickly
// rather than waiting a full pollInterval.
func (w *ControllerWatch) Run(ctx context.Context) error {
	w.refresh(ctx)

	tick := time.NewTicker(w.pollInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			w.refresh(ctx)
		}
	}
}

// refresh issues a single Get against the Lease and updates atomic state.
// Errors are silently swallowed: the Lease may not exist yet (cluster
// startup, transient network blip) and we don't want to flap CurrentEpoch
// to 0 in that case.
func (w *ControllerWatch) refresh(ctx context.Context) {
	lease, err := w.client.CoordinationV1().Leases(w.namespace).Get(
		ctx, kafkaapi.ControllerLeaseName, metav1.GetOptions{},
	)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Lease not yet created — leave epoch/holder at their previous values.
			return
		}
		return
	}
	if lease.Spec.LeaseTransitions != nil {
		w.epoch.Store(int64(*lease.Spec.LeaseTransitions))
	}
	if lease.Spec.HolderIdentity != nil {
		w.holder.Store(*lease.Spec.HolderIdentity)
	} else {
		w.holder.Store("")
	}
}
