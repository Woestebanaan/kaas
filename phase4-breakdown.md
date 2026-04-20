# Phase 4 Breakdown: Kubernetes Lease Manager

## Current State (end of Phase 3)

All tests pass:

```
ok  github.com/woestebanaan/skafka/internal/protocol        (server, dispatcher, frame codec)
ok  github.com/woestebanaan/skafka/internal/protocol/codec  (primitives, RecordBatch, CRC32C)
ok  github.com/woestebanaan/skafka/internal/protocol/codec/api  (all 21 API codecs)
ok  github.com/woestebanaan/skafka/tests/kafka-compat        (franz-go + kafka-go e2e tests)
ok  github.com/woestebanaan/skafka/tests/integration         (disk storage, recovery, watcher)
```

`DiskStorageEngine` is wired and working. The broker still uses three stubs that Phase 4
replaces:
- `LocalLeaseManager` — always reports self as leader; no Kubernetes interaction
- `LocalPartitionLock` — always reports locked; no filesystem locks acquired
- `broker.BrokerInfo` in `MetadataHandler` — hard-coded single broker; no cluster awareness

### Key files before starting Phase 4

| File | Role |
|---|---|
| `internal/lease/manager.go` | `LeaseManager` interface — needs `LeaderFor` added |
| `internal/broker/stubs.go` | `LocalLeaseManager`, `LocalPartitionLock` — replaced |
| `internal/broker/broker.go` | Wires everything; `BrokerInfo` passed to MetadataHandler |
| `internal/protocol/handlers/metadata.go` | Reports topology — needs multi-broker support |
| `internal/storage/engine.go` | `TakeoverPartition()` — called from `OnStartedLeading` |
| `cmd/skafka/main.go` | Entry point — gains `--init` flag and k8s client wiring |
| `operator/api/v1alpha1/kafkatopic_types.go` | CRD type for partition enumeration |

### Dependencies already in go.mod (no new additions needed)

```
k8s.io/client-go v0.35.0   → tools/leaderelection, tools/leaderelection/resourcelock
k8s.io/api v0.35.4          → coordination/v1 Lease, discovery/v1 EndpointSlice, core/v1 Pod
k8s.io/apimachinery v0.35.4 → metav1, types
sigs.k8s.io/controller-runtime → client (already used by operator)
```

---

## Interface change at the start of Phase 4

The `LeaseManager` interface currently has no way to answer "who is the leader" for a
partition — it can only say whether *this* broker is the leader. The `MetadataHandler`
needs to report the correct `LeaderID` (node ordinal) for each partition in a multi-broker
cluster. Add one method:

```go
// Before (current):
type LeaseManager interface {
    Acquire(ctx context.Context, topic string, partition int32) error
    Release(topic string, partition int32) error
    IsLeader(topic string, partition int32) bool
    WatchLeaders(ctx context.Context) (<-chan LeaderChange, error)
}

// After (Phase 4):
type LeaseManager interface {
    Acquire(ctx context.Context, topic string, partition int32) error
    Release(topic string, partition int32) error
    IsLeader(topic string, partition int32) bool
    LeaderFor(topic string, partition int32) int32  // returns node ordinal, -1 if unknown
    WatchLeaders(ctx context.Context) (<-chan LeaderChange, error)
}
```

Update `LocalLeaseManager` stub: `LeaderFor` returns 0 (the single broker is always
node 0). Update `MetadataHandler` to use `LeaderFor` instead of the
`IsLeader → self.NodeID` pattern.

This interface change is the **first thing to do** — it touches `manager.go`, `stubs.go`,
and `metadata.go`.

---

## Step 4.1 — Broker identity

File: `internal/k8s/broker.go`

```go
type BrokerIdentity struct {
    PodName    string // "broker-2"
    Ordinal    int32  // 2
    Namespace  string
    Host       string // "broker-2.skafka-headless.kafka.svc.cluster.local"
    Port       int32
}

func NewBrokerIdentity(namespace, headlessSvc string, port int32) (*BrokerIdentity, error) {
    podName := os.Getenv("MY_POD_NAME")
    if podName == "" {
        return nil, errors.New("MY_POD_NAME not set")
    }
    // Parse ordinal from last hyphen-separated segment: "broker-2" → 2
    parts := strings.Split(podName, "-")
    ordinal, err := strconv.Atoi(parts[len(parts)-1])
    if err != nil {
        return nil, fmt.Errorf("cannot parse ordinal from pod name %q: %w", podName, err)
    }
    host := fmt.Sprintf("%s.%s.%s.svc.cluster.local", podName, headlessSvc, namespace)
    return &BrokerIdentity{
        PodName:   podName,
        Ordinal:   int32(ordinal),
        Namespace: namespace,
        Host:      host,
        Port:      port,
    }, nil
}
```

For local dev without `MY_POD_NAME` set, fall back to `hostname` + ordinal 0. The
`NewBrokerIdentity` function can accept an optional override for this case.

**Done when:** unit test: `NewBrokerIdentity` with `MY_POD_NAME=broker-2` returns
`Ordinal=2` and the correct FQDN.

---

## Step 4.2 — EndpointSlice watcher

File: `internal/k8s/endpoints.go`

The `MetadataHandler` needs a live view of all broker pod addresses to populate the
`Brokers` list in Metadata responses. Without this, clients only ever see one broker
and cannot route produce/fetch to the right node.

```go
type BrokerEndpoint struct {
    NodeID int32  // StatefulSet ordinal
    Host   string // pod FQDN
    Port   int32
    Ready  bool
}

type BrokerRegistry struct {
    mu      sync.RWMutex
    brokers map[int32]BrokerEndpoint
    onChange func([]BrokerEndpoint) // called when set changes (for rebalancer)
}

func NewBrokerRegistry(onChange func([]BrokerEndpoint)) *BrokerRegistry

func (r *BrokerRegistry) All() []BrokerEndpoint    // sorted by NodeID
func (r *BrokerRegistry) Count() int
func (r *BrokerRegistry) Get(nodeID int32) (BrokerEndpoint, bool)

// Watch blocks until ctx is cancelled, streaming EndpointSlice updates from k8s.
func (r *BrokerRegistry) Watch(ctx context.Context, client kubernetes.Interface,
    namespace, headlessSvcName string) error
```

Watch implementation:
1. `client.DiscoveryV1().EndpointSlices(namespace).Watch(ctx, metav1.ListOptions{LabelSelector: "kubernetes.io/service-name="+headlessSvcName})`
2. On `ADDED`/`MODIFIED`: rebuild the in-memory map from `EndpointSlice.Endpoints`
   - Parse ordinal from endpoint's `hostname` field (same as pod name suffix)
   - `ready = endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready`
3. On `DELETED`: clear the map entries for that slice
4. Call `onChange` after each update (used in Step 4.4 for rebalancing)

**Done when:** test with a fake k8s client that sends EndpointSlice events, verifying
`BrokerRegistry.All()` reflects the correct set after each event.

---

## Step 4.3 — KubernetesLeaseManager

File: `internal/lease/k8s_manager.go`

```go
type KubernetesLeaseManager struct {
    client    kubernetes.Interface
    namespace string
    selfID    string // "{pod-name}-{monotonic-epoch}" — fencing token

    mu      sync.RWMutex
    leaders map[string]int32    // "topic/partition" → leader ordinal (-1 = unknown)
    held    map[string]struct{} // partitions for which OnStartedLeading has fired
    cancels map[string]context.CancelFunc

    subscribers []chan LeaderChange
    subMu       sync.Mutex

    onStartedLeading func(topic string, partition int32, epoch int64)
    onStoppedLeading func(topic string, partition int32)
}

func NewKubernetesLeaseManager(
    client kubernetes.Interface,
    namespace string,
    selfID string,
    onStartedLeading func(topic string, partition int32, epoch int64),
    onStoppedLeading func(topic string, partition int32),
) *KubernetesLeaseManager
```

### Lease naming

Kubernetes Lease names must be valid DNS subdomain names (max 253 chars). Use:

```
"skafka-" + sanitize(topic) + "-" + strconv.Itoa(int(partition))
```

Where `sanitize` replaces any character that is not `[a-z0-9-]` with `-` and truncates
to 240 chars before appending the partition suffix. This ensures uniqueness while staying
within the limit.

```go
func leaseName(topic string, partition int32) string {
    safe := regexp.MustCompile(`[^a-z0-9-]`).ReplaceAllString(strings.ToLower(topic), "-")
    if len(safe) > 240 {
        safe = safe[:240]
    }
    return fmt.Sprintf("skafka-%s-%d", safe, partition)
}
```

### Acquire

```go
func (m *KubernetesLeaseManager) Acquire(ctx context.Context, topic string, partition int32) error {
    key := partKey(topic, partition)

    m.mu.Lock()
    if _, ok := m.cancels[key]; ok {
        m.mu.Unlock()
        return nil // already running
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
        LockConfig: resourcelock.ResourceLockConfig{Identity: m.selfID},
    }

    cfg := leaderelection.LeaderElectionConfig{
        Lock:            lock,
        LeaseDuration:   15 * time.Second,
        RenewDeadline:   10 * time.Second,
        RetryPeriod:     2 * time.Second,
        ReleaseOnCancel: true,
        Callbacks: leaderelection.LeaderCallbacks{
            OnStartedLeading: func(ctx context.Context) {
                epoch := time.Now().UnixNano()
                m.mu.Lock()
                m.held[key] = struct{}{}
                m.leaders[key] = m.selfOrdinal()
                m.mu.Unlock()
                m.notify(LeaderChange{Topic: topic, Partition: partition, LeaderID: m.selfOrdinal()})
                if m.onStartedLeading != nil {
                    m.onStartedLeading(topic, partition, epoch)
                }
                <-ctx.Done() // block until released
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
                ordinal := parseOrdinalFromIdentity(identity)
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
        return err
    }
    go le.Run(elCtx)
    return nil
}
```

`selfOrdinal` parses the integer from `m.selfID` (e.g. `"broker-2-..."` → 2).

`parseOrdinalFromIdentity` does the same for an arbitrary holder identity string.

### Release

Cancel the elector context. With `ReleaseOnCancel: true`, the client-go elector will
release the Lease object before returning.

```go
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
```

### IsLeader / LeaderFor

```go
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
```

### WatchLeaders

Fan-out from the internal `notify` method to subscriber channels:

```go
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
        default: // drop if subscriber is slow
        }
    }
}
```

**Done when:** unit test with a fake k8s client verifying:
- `IsLeader` returns false before Acquire, true after `OnStartedLeading` fires
- `LeaderFor` returns the correct ordinal after `OnNewLeader` fires
- `Release` causes `OnStoppedLeading` to fire and `IsLeader` returns false

---

## Step 4.4 — Partition assignment + rebalancing

File: `internal/k8s/assignment.go`

On startup, this broker enumerates all KafkaTopic CRDs and attempts to acquire Leases
for a preferred subset based on consistent hashing. The Kubernetes Lease TTL handles
the actual arbitration — two brokers competing for the same Lease is safe because
`leaderelection` ensures only one wins.

```go
// preferred returns true if this broker should attempt to acquire the given partition,
// based on FNV hash modulo broker count.
func preferred(topic string, partition int32, selfOrdinal int32, numBrokers int) bool {
    if numBrokers <= 0 {
        return true
    }
    h := fnv.New32a()
    _, _ = fmt.Fprintf(h, "%s/%d", topic, partition)
    return int32(h.Sum32()%uint32(numBrokers)) == selfOrdinal
}
```

Startup flow (in `cmd/skafka/main.go`):

```go
// 1. List all KafkaTopic CRDs
topics, err := operatorClient.SkafkaV1alpha1().KafkaTopics(namespace).List(ctx, metav1.ListOptions{})

// 2. For each partition, create storage dir and attempt Lease
for _, topic := range topics.Items {
    for p := int32(0); p < topic.Spec.Partitions; p++ {
        _ = engine.CreatePartition(topic.Name, p)
        if preferred(topic.Name, p, identity.Ordinal, brokerRegistry.Count()) {
            _ = leaseManager.Acquire(ctx, topic.Name, p)
        }
    }
}

// 3. Also attempt non-preferred partitions (backup, avoids liveness issue on small clusters)
//    Lease arbitration ensures this is safe.
for _, topic := range topics.Items {
    for p := int32(0); p < topic.Spec.Partitions; p++ {
        if !leaseManager.IsLeader(topic.Name, p) {
            _ = leaseManager.Acquire(ctx, topic.Name, p)
        }
    }
}
```

Rebalancing on broker join/leave:

```go
// BrokerRegistry.onChange callback — called when EndpointSlice changes.
// If a new broker appears and this broker holds more than its fair share,
// voluntarily release some leases to allow redistribution.
func rebalance(leaseManager *lease.KubernetesLeaseManager, topics []KafkaTopic,
    selfOrdinal int32, numBrokers int) {
    for _, topic := range topics {
        for p := int32(0); p < topic.Spec.Partitions; p++ {
            if leaseManager.IsLeader(topic.Name, p) &&
               !preferred(topic.Name, p, selfOrdinal, numBrokers) {
                _ = leaseManager.Release(topic.Name, p)
            }
        }
    }
}
```

**Done when:** test: two concurrent `KubernetesLeaseManager` instances (using envtest
or a fake client) competing for the same Lease, verifying only one holds it at a time.

---

## Step 4.5 — ReadinessGate

File: `internal/k8s/readiness.go`

```go
// ReadinessUpdater patches the broker's own Pod condition "skafka.io/PartitionsReady".
// The pod only joins the headless service (and receives client traffic) when this is True.
type ReadinessUpdater struct {
    client    kubernetes.Interface
    podName   string
    namespace string
}

const ReadinessCondition = "skafka.io/PartitionsReady"

func (r *ReadinessUpdater) SetReady(ctx context.Context, ready bool) error {
    status := corev1.ConditionFalse
    msg := "waiting for partition leases"
    if ready {
        status = corev1.ConditionTrue
        msg = "all assigned partitions are ready"
    }
    patch := []byte(fmt.Sprintf(`{"status":{"conditions":[{"type":%q,"status":%q,"message":%q,"lastTransitionTime":%q}]}}`,
        ReadinessCondition, status, msg, time.Now().UTC().Format(time.RFC3339)))
    _, err := r.client.CoreV1().Pods(r.namespace).Patch(ctx, r.podName,
        types.MergePatchType, patch, metav1.PatchOptions{}, "status")
    return err
}
```

The ReadinessGate must be declared in the Pod spec (StatefulSet manifest) under
`spec.readinessGates`:
```yaml
readinessGates:
  - conditionType: "skafka.io/PartitionsReady"
```

Trigger readiness: after Acquire is called for all assigned partitions, poll
`leaseManager.IsLeader` for each. When all partitions this broker attempted to acquire
are either held or confirmed held by another broker (no unresolved), call
`SetReady(ctx, true)`. Run this check in a goroutine watching `WatchLeaders`.

**Done when:** test verifies that the PATCH call fires with `status=True` after all
expected `OnStartedLeading` callbacks have fired.

---

## Step 4.6 — Init container

Flag `--init` added to `cmd/skafka/main.go`.

```go
if os.Args[1] == "--init" {
    runInit(ctx)
    return
}

func runInit(ctx context.Context) {
    dataDir := os.Getenv("SKAFKA_DATA_DIR")
    // build k8s client
    topics, _ := operatorClient.SkafkaV1alpha1().KafkaTopics(namespace).List(ctx, metav1.ListOptions{})
    for _, t := range topics.Items {
        for p := int32(0); p < t.Spec.Partitions; p++ {
            dir := filepath.Join(dataDir, t.Name, strconv.Itoa(int(p)))
            if err := os.MkdirAll(dir, 0755); err != nil {
                slog.Error("init: mkdir failed", "dir", dir, "err", err)
                os.Exit(1)
            }
        }
    }
    slog.Info("init complete")
}
```

The StatefulSet manifest uses this as an init container:
```yaml
initContainers:
  - name: partition-init
    image: same as broker
    args: ["--init"]
    env:
      - name: MY_POD_NAME
        valueFrom: {fieldRef: {fieldPath: metadata.name}}
    volumeMounts:
      - name: data
        mountPath: /data
```

**Done when:** `go run ./cmd/skafka --init` with a temp dir creates the expected
partition directory tree.

---

## Step 4.7 — Update MetadataHandler and wire everything

### MetadataHandler changes (internal/protocol/handlers/metadata.go)

Replace the single `BrokerInfo` with a `BrokerSource` interface:

```go
type BrokerSource interface {
    Self() BrokerEndpoint
    All() []BrokerEndpoint
}

type BrokerEndpoint struct {
    NodeID int32
    Host   string
    Port   int32
}

type MetadataHandler struct {
    brokers BrokerSource
    topics  TopicSource
    leases  lease.LeaseManager
}
```

In the response, populate `resp.Brokers` from `brokers.All()` and use
`leases.LeaderFor(topic, partition)` for each partition's `LeaderID`.

A `singleBrokerSource` adapter wraps the existing `BrokerInfo` for backward compat in
local-dev mode (no k8s).

### Wire in cmd/skafka/main.go

```
IF MY_POD_NAME is set (running in Kubernetes):
    identity = NewBrokerIdentity(...)
    k8sClient = rest.InClusterConfig → kubernetes.NewForConfig
    brokerRegistry = NewBrokerRegistry(rebalancer callback)
    go brokerRegistry.Watch(ctx, k8sClient, namespace, headlessSvc)
    leaseManager = NewKubernetesLeaseManager(k8sClient, namespace, identity.PodName, onStarted, onStopped)
    readiness = NewReadinessUpdater(k8sClient, identity.PodName, namespace)
    [enumerate KafkaTopic CRDs, call Acquire for each partition]
    [watch WatchLeaders for readiness, call SetReady when all leases settled]
ELSE (local dev):
    identity.Ordinal = 0
    brokerRegistry = static single-entry registry
    leaseManager = LocalLeaseManager
```

**Done when:** a two-broker envtest integration test verifies:
1. Both brokers start and call `Acquire` for all partitions of a topic
2. Each partition's lease is held by exactly one broker
3. `MetadataResponse.Brokers` contains both brokers
4. `MetadataResponse.Partitions[n].LeaderID` matches the actual Lease holder

---

## Testing strategy

| Test type | Where | Tools |
|---|---|---|
| Unit: broker identity parsing | `internal/k8s/broker_test.go` | plain Go |
| Unit: EndpointSlice parsing | `internal/k8s/endpoints_test.go` | fake client |
| Unit: KubernetesLeaseManager | `internal/lease/k8s_manager_test.go` | fake client |
| Unit: consistent hash assignment | `internal/k8s/assignment_test.go` | plain Go |
| Integration: two-broker Lease contention | `tests/integration/lease_test.go` | envtest |
| Integration: MetadataHandler multi-broker | `tests/integration/metadata_test.go` | envtest |

For envtest: `sigs.k8s.io/controller-runtime/pkg/envtest` spins up a real API server
locally. The Lease tests can run in CI without a live cluster.

---

## Step order summary

| Step | File(s) | Depends on |
|---|---|---|
| 4.0 Interface change | `lease/manager.go`, `broker/stubs.go`, `handlers/metadata.go` | nothing |
| 4.1 Broker identity | `k8s/broker.go` | nothing |
| 4.2 EndpointSlice watcher | `k8s/endpoints.go` | 4.1 |
| 4.3 KubernetesLeaseManager | `lease/k8s_manager.go` | 4.0 |
| 4.4 Partition assignment | `k8s/assignment.go` | 4.2, 4.3 |
| 4.5 ReadinessGate | `k8s/readiness.go` | 4.1 |
| 4.6 Init container | `cmd/skafka/main.go` | 4.1 |
| 4.7 Wire + MetadataHandler | `handlers/metadata.go`, `cmd/skafka/main.go` | 4.0–4.6 |

Start with 4.0 (interface change), then 4.1–4.3 in parallel, then the rest in order.
