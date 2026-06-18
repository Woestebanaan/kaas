package lease

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// KubernetesLeaseManager implements LeaseManager using coordination.k8s.io/v1 Leases.
// One LeaderElector goroutine runs per partition; Kubernetes Lease TTL arbitrates ownership.
type KubernetesLeaseManager struct {
	client      kubernetes.Interface
	namespace   string
	selfID      string // pod name used as the elector identity
	selfOrdinal int32

	onStartedLeading func(topic string, partition int32, epoch int64)
	onStoppedLeading func(topic string, partition int32)

	mu          sync.RWMutex
	held        map[string]struct{}           // partitions for which OnStartedLeading fired
	leaders     map[string]int32              // "topic/partition" → leader ordinal
	cancels     map[string]context.CancelFunc // running elector contexts

	subMu       sync.Mutex
	subscribers []chan LeaderChange
}

// ParseOrdinalFromIdentity extracts the StatefulSet ordinal from an identity string
// such as "broker-2" (returns 2) or "skafka-broker-3" (returns 3).
func ParseOrdinalFromIdentity(identity string) int32 {
	parts := strings.Split(identity, "-")
	if len(parts) == 0 {
		return -1
	}
	n, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return -1
	}
	return int32(n)
}

// NewKubernetesLeaseManager creates the manager.
// onStartedLeading is called (with a long-lived context) when this broker wins a lease.
// onStoppedLeading is called when leadership is lost.
func NewKubernetesLeaseManager(
	client kubernetes.Interface,
	namespace string,
	selfID string,
	onStartedLeading func(topic string, partition int32, epoch int64),
	onStoppedLeading func(topic string, partition int32),
) *KubernetesLeaseManager {
	return &KubernetesLeaseManager{
		client:           client,
		namespace:        namespace,
		selfID:           selfID,
		selfOrdinal:      ParseOrdinalFromIdentity(selfID),
		onStartedLeading: onStartedLeading,
		onStoppedLeading: onStoppedLeading,
		held:             make(map[string]struct{}),
		leaders:          make(map[string]int32),
		cancels:          make(map[string]context.CancelFunc),
	}
}

func partKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

var nonAlphaNum = regexp.MustCompile(`[^a-z0-9-]`)

// leaseName returns a valid Kubernetes Lease name for the given partition.
// Format: "skafka-{sanitized-topic}-{partition}", max 253 chars total.
func leaseName(topic string, partition int32) string {
	safe := nonAlphaNum.ReplaceAllString(strings.ToLower(topic), "-")
	suffix := fmt.Sprintf("-%d", partition)
	prefix := "skafka-"
	maxSafe := 253 - len(prefix) - len(suffix)
	if len(safe) > maxSafe {
		safe = safe[:maxSafe]
	}
	return prefix + safe + suffix
}

// Acquire starts a LeaderElector goroutine for the given partition. Idempotent.
func (m *KubernetesLeaseManager) Acquire(ctx context.Context, topic string, partition int32) error {
	key := partKey(topic, partition)

	m.mu.Lock()
	if _, ok := m.cancels[key]; ok {
		m.mu.Unlock()
		return nil
	}
	elCtx, cancel := context.WithCancel(ctx)
	m.cancels[key] = cancel
	m.mu.Unlock()

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName(topic, partition),
			Namespace: m.namespace,
		},
		Client: m.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: m.selfID,
		},
	}

	cfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				epoch := time.Now().UnixNano()
				m.mu.Lock()
				m.held[key] = struct{}{}
				m.leaders[key] = m.selfOrdinal
				m.mu.Unlock()
				m.notify(LeaderChange{Topic: topic, Partition: partition, LeaderID: m.selfOrdinal})
				if m.onStartedLeading != nil {
					m.onStartedLeading(topic, partition, epoch)
				}
				<-leaderCtx.Done()
			},
			OnStoppedLeading: func() {
				m.mu.Lock()
				delete(m.held, key)
				m.leaders[key] = -1
				m.mu.Unlock()
				m.notify(LeaderChange{Topic: topic, Partition: partition, LeaderID: -1})
				if m.onStoppedLeading != nil {
					m.onStoppedLeading(topic, partition)
				}
			},
			OnNewLeader: func(identity string) {
				ordinal := ParseOrdinalFromIdentity(identity)
				m.mu.Lock()
				m.leaders[key] = ordinal
				m.mu.Unlock()
				m.notify(LeaderChange{Topic: topic, Partition: partition, LeaderID: ordinal})
			},
		},
	}

	le, err := leaderelection.NewLeaderElector(cfg)
	if err != nil {
		cancel()
		m.mu.Lock()
		delete(m.cancels, key)
		m.mu.Unlock()
		return err
	}
	go le.Run(elCtx)
	return nil
}

// Release cancels the elector for the given partition. With ReleaseOnCancel=true the
// Lease object is surrendered before the goroutine exits.
func (m *KubernetesLeaseManager) Release(topic string, partition int32) error {
	key := partKey(topic, partition)
	m.mu.Lock()
	cancel, ok := m.cancels[key]
	delete(m.cancels, key)
	m.mu.Unlock()
	if ok {
		cancel()
	}
	return nil
}

func (m *KubernetesLeaseManager) IsLeader(topic string, partition int32) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.held[partKey(topic, partition)]
	return ok
}

func (m *KubernetesLeaseManager) LeaderFor(topic string, partition int32) int32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, ok := m.leaders[partKey(topic, partition)]
	if !ok {
		return -1
	}
	return id
}

func (m *KubernetesLeaseManager) WatchLeaders(_ context.Context) (<-chan LeaderChange, error) {
	ch := make(chan LeaderChange, 64)
	m.subMu.Lock()
	m.subscribers = append(m.subscribers, ch)
	m.subMu.Unlock()
	return ch, nil
}

func (m *KubernetesLeaseManager) notify(lc LeaderChange) {
	m.subMu.Lock()
	defer m.subMu.Unlock()
	for _, ch := range m.subscribers {
		select {
		case ch <- lc:
		default:
		}
	}
}

// SetOnStartedLeading replaces the onStartedLeading callback after construction.
// Must be called before any Acquire.
func (m *KubernetesLeaseManager) SetOnStartedLeading(fn func(topic string, partition int32, epoch int64)) {
	m.onStartedLeading = fn
}

// SetOnStoppedLeading replaces the onStoppedLeading callback after construction.
// Must be called before any Acquire.
func (m *KubernetesLeaseManager) SetOnStoppedLeading(fn func(topic string, partition int32)) {
	m.onStoppedLeading = fn
}

// Verify interface at compile time.
var _ LeaseManager = (*KubernetesLeaseManager)(nil)
