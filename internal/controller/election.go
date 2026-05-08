// Package controller implements the cluster controller role: the elected
// broker that computes the partition-to-broker assignment and writes it to
// /data/__cluster/assignment.json. Every broker runs an Election and, when
// it wins, runs the controller goroutines (assignment writer, gRPC heartbeat
// server, KafkaClusterAssignments CR mirror).
package controller

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/woestebanaan/skafka/internal/observability"
	"github.com/woestebanaan/skafka/pkg/kafkaapi"
)

// LeaseName is the singleton cluster-wide controller Lease. v3.3 plan §"The
// architecture in one paragraph" — exactly one Lease per cluster. The
// constant lives in pkg/kafkaapi so the broker side can reference the same
// name without importing this package; we re-export it here for in-package
// readability.
const LeaseName = kafkaapi.ControllerLeaseName

// AcquiredCallback is fired when this broker wins the controller Lease.
// epoch is the Lease's leaseTransitions value at the time of acquisition —
// the same monotonic counter the controller embeds in assignment.json so
// brokers can fence stale writes from a partitioned ex-controller. The
// passed ctx is the leader context: it is cancelled when leadership is lost
// (renewal failure, network partition, manual relinquish). Long-running
// controller work should anchor itself to ctx.
type AcquiredCallback func(ctx context.Context, epoch int64)

// LostCallback is fired when leadership is lost. It runs after the leader
// context has been cancelled and gives the broker a chance to wind down
// before the lease may be re-acquired.
type LostCallback func()

// Election wraps client-go/tools/leaderelection for the singleton
// skafka-controller Lease. Each broker constructs one Election and runs it
// for the lifetime of the broker process; the leader-elector library handles
// renewal, loss, and re-acquisition.
type Election struct {
	client    kubernetes.Interface
	namespace string
	identity  string

	onAcquired AcquiredCallback
	onLost     LostCallback

	// Timings — match v2.6 per-partition defaults so operators see consistent
	// failover behaviour. Tests override via WithTimings to keep runtime under
	// a few seconds.
	leaseDuration time.Duration
	renewDeadline time.Duration
	retryPeriod   time.Duration
}

// New constructs an Election. identity is typically the broker's pod name
// (already scoped per StatefulSet, so unique per process). Pass nil
// callbacks to disable.
func New(
	client kubernetes.Interface,
	namespace, identity string,
	onAcquired AcquiredCallback,
	onLost LostCallback,
) *Election {
	return &Election{
		client:        client,
		namespace:     namespace,
		identity:      identity,
		onAcquired:    onAcquired,
		onLost:        onLost,
		leaseDuration: 15 * time.Second,
		renewDeadline: 10 * time.Second,
		retryPeriod:   2 * time.Second,
	}
}

// WithTimings overrides the default Lease durations. Useful for tests; in
// production the defaults match Kubernetes' recommended renew/retry ratios.
func (e *Election) WithTimings(leaseDuration, renewDeadline, retryPeriod time.Duration) *Election {
	e.leaseDuration = leaseDuration
	e.renewDeadline = renewDeadline
	e.retryPeriod = retryPeriod
	return e
}

// Run blocks until ctx is cancelled, driving the leader-election state
// machine. On a transient loss (renewal failure, etc.) the elector is
// rebuilt and Run is called again so this broker re-enters the candidate
// pool — exactly one Run call covers the broker's whole lifetime.
func (e *Election) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := e.runOnce(ctx); err != nil {
			return err
		}
		// Brief settle before re-entering; if ctx was cancelled while we were
		// in runOnce, the next loop iteration returns nil.
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(e.retryPeriod):
		}
	}
}

// runOnce builds a fresh leaderelection.LeaderElector and runs it until ctx
// is cancelled OR leadership is lost. Returning nil means "loop and try
// again"; returning err means a configuration problem the caller should
// surface.
func (e *Election) runOnce(ctx context.Context) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      LeaseName,
			Namespace: e.namespace,
		},
		Client: e.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: e.identity,
		},
	}

	cfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   e.leaseDuration,
		RenewDeadline:   e.renewDeadline,
		RetryPeriod:     e.retryPeriod,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				epoch, err := e.fetchLeaseTransitions(leaderCtx)
				if err != nil {
					// Failed to read leaseTransitions immediately after winning.
					// This is rare (we just held a successful Update) — treat
					// as "epoch unknown" with 0 so the controller still starts
					// rather than silently abandoning the role. The first
					// re-acquisition will pick up the correct value.
					epoch = 0
				}
				// Trace the controller's tenure as a single span — its
				// duration tells operators how long this broker held the
				// Lease, and any child span emitted from a downstream
				// recompute / write picks up this span's parent context
				// so traces chain naturally.
				spanCtx, span := observability.Tracer().Start(leaderCtx,
					"controller.elected",
					trace.WithSpanKind(trace.SpanKindInternal),
					trace.WithAttributes(
						attribute.String("controller.identity", e.identity),
						attribute.Int64("controller.epoch", epoch),
					),
				)
				if e.onAcquired != nil {
					e.onAcquired(spanCtx, epoch)
				}
				<-leaderCtx.Done()
				span.End()
			},
			OnStoppedLeading: func() {
				if e.onLost != nil {
					e.onLost()
				}
			},
		},
	}

	le, err := leaderelection.NewLeaderElector(cfg)
	if err != nil {
		return fmt.Errorf("controller: build elector: %w", err)
	}
	le.Run(ctx)
	return nil
}

// fetchLeaseTransitions reads the current Lease object and returns its
// leaseTransitions field as an int64. The leaderelection library doesn't
// expose this through the callbacks, so we Get the Lease directly.
func (e *Election) fetchLeaseTransitions(ctx context.Context) (int64, error) {
	lease, err := e.client.CoordinationV1().Leases(e.namespace).Get(ctx, LeaseName, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	if lease.Spec.LeaseTransitions == nil {
		return 0, nil
	}
	return int64(*lease.Spec.LeaseTransitions), nil
}
