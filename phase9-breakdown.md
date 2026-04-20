# Phase 9 Breakdown: External Access via Per-Broker TLS Passthrough

## What Changed From Previous Design Iterations

Earlier drafts of Phase 9 explored two other approaches that were discarded:

1. **Custom router binary** (`skafka-router`) — a stateless Kafka-protocol-aware
   proxy that rewrote Metadata and maintained a Lease routing table. Rejected
   because it terminated TLS at an intermediate proxy and added ~600 lines of
   custom code without delivering anything the protocol can't do itself.

2. **Single external hostname + leader forwarding** — all brokers advertised
   `kafka.example.com:9093`, and non-leader brokers forwarded Produce requests
   to the current lease holder. Rejected because forwarding is redundant with
   the Kafka protocol's native `NOT_LEADER_FOR_PARTITION` retry — the clients
   already know how to handle wrong-leader situations, as long as Metadata
   responses tell them how to reach each specific broker.

**What Phase 9 actually is:**

Each broker pod gets its own advertised hostname. Clients receive per-broker
addresses in the Metadata response and use the standard Kafka protocol retry
mechanism to reach the correct leader. A single Gateway (or LoadBalancer) in
TLS passthrough mode routes by SNI hostname to the correct broker pod.

No forwarding, no proxy, no custom binary. Just DNS and the existing protocol.

---

## Current State (end of Phase 8)

The broker and operator ship as container images on GHCR, the Helm chart renders
16 resources, and a 3-replica broker StatefulSet behind a ClusterIP headless
Service works for in-cluster clients. External clients still have no path — that
is what Phase 9 adds.

Internal clients are unaffected. They continue using headless Service DNS:

```
skafka-0.skafka-headless.kafka.svc.cluster.local:9092
skafka-1.skafka-headless.kafka.svc.cluster.local:9092
skafka-2.skafka-headless.kafka.svc.cluster.local:9092
```

---

## Architecture

```
External client (TLS + SNI)
        │
        ▼
Gateway / LoadBalancer (TLS passthrough, SNI-based routing)
        │
        ├─ SNI = broker-0.kafka.example.com → broker-0 pod
        ├─ SNI = broker-1.kafka.example.com → broker-1 pod
        └─ SNI = broker-2.kafka.example.com → broker-2 pod
                        │
                  (TLS ends here — on the broker pod)
                        │
                  If request is for a partition this broker does not lead,
                  broker returns NOT_LEADER_FOR_PARTITION. Client refreshes
                  Metadata, reconnects to the actual leader using its own
                  hostname. Standard Kafka protocol behaviour.
```

**Bootstrap address:** clients can use any broker hostname or a wildcard
that resolves to all of them. Clients only need one to learn the full list
from a Metadata response.

**DNS requirement:** a wildcard DNS record `*.kafka.example.com → <LB IP>`
OR explicit records per broker. One A/AAAA record per broker is sufficient
and keeps things explicit; a wildcard is operationally simpler.

**Certificate requirement:** a single wildcard certificate (`*.kafka.example.com`)
OR a certificate with per-broker SANs (`broker-0.kafka.example.com`,
`broker-1.kafka.example.com`, ...). cert-manager handles either.

---

## File Layout for Phase 9

```
# New files
internal/protocol/tls.go             ← Step 9.0: cert watcher

# Extended files
internal/protocol/server.go          ← Step 9.0: TLS listener
operator/controllers/
  kafkacluster_controller.go         ← Step 9.1: listener reconciliation
operator/api/v1alpha1/
  kafkacluster_types.go              ← Step 9.1: listeners spec
deploy/helm/skafka/templates/
  listener-certificate.yaml          ← Step 9.2: wildcard or SAN cert
  listener-tlsroutes.yaml            ← Step 9.2: one TLSRoute per broker (SNI)
  listener-services.yaml             ← Step 9.2: per-broker Services
deploy/helm/skafka/
  values.yaml                        ← Step 9.2: listeners.* values
```

No new Go dependencies. No new binaries. cert-manager is an optional dependency
for TLS certificate management — document as a prerequisite if
`listeners.external.tls.certManager` is enabled.

---

## Step 9.0 — Broker TLS Listener

Brokers need a TLS listener. This was deferred from Phase 7 because in-cluster
operation doesn't require it.

File: `internal/protocol/server.go` (extend)

```go
// Each broker runs two listeners:
//   - PLAINTEXT on :9092 (headless Service, in-cluster clients)
//   - TLS on :9093 (exposed externally via Gateway / LB)
//
// The broker selects which advertised host to put in Metadata responses
// based on which listener received the request.

type ListenerConfig struct {
    Addr           string      // ":9092" or ":9093"
    TLS            *tls.Config // nil for plaintext
    AdvertisedHost string      // hostname to advertise in Metadata for connections on this listener
    AdvertisedPort int32
}

func (s *Server) AddListener(cfg ListenerConfig) error
```

File: `internal/protocol/tls.go`

The broker loads TLS material from disk and reloads on rotation using fsnotify,
so a cert-manager rollover does not require a pod restart.

```go
func WatchingCertificate(certFile, keyFile string) *tls.Config {
    var cert atomic.Pointer[tls.Certificate]
    loadInitial(&cert, certFile, keyFile)

    // fsnotify watcher → reload cert on WRITE to either file; swap atomic pointer.

    return &tls.Config{
        GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
            return cert.Load(), nil
        },
        MinVersion: tls.VersionTLS13,
    }
}
```

TLS 1.3 minimum. Support for TLS 1.2 can be added if legacy clients require it;
document as a security downgrade.

**Per-broker advertised host.** Each broker's `ADVERTISED_HOST` env var for the
external listener is computed from its StatefulSet ordinal:

```
EXTERNAL_ADVERTISED_HOST = broker-{ordinal}.kafka.example.com
```

These are stable and known at StatefulSet create time — no waiting for
LoadBalancer IP provisioning, no rolling restart needed later. The value comes
from Helm via a template that substitutes `{ordinal}`.

**Done when:**
- Broker starts with both listeners active
- `openssl s_client -connect broker-0.kafka.example.com:9093 -servername broker-0.kafka.example.com`
  returns the broker's certificate
- Plaintext client on `:9092` unaffected
- Metadata response on the external listener includes `broker-{ordinal}.kafka.example.com`
  for each broker row; on the internal listener includes the headless DNS

---

## Step 9.1 — Operator: Listener Reconciliation

File: `operator/api/v1alpha1/kafkacluster_types.go` (extended)
File: `operator/controllers/kafkacluster_controller.go` (extended)

### KafkaCluster spec additions

```yaml
apiVersion: skafka.io/v1alpha1
kind: KafkaCluster
metadata:
  name: skafka
  namespace: kafka
spec:
  replicas: 3
  storage:
    className: ceph-filesystem
    size: 500Gi

  listeners:
    # Internal listener — always active, stable DNS, no configuration.
    internal:
      port: 9092

    # External listener — opt-in.
    external:
      enabled: false
      port: 9093

      # Hostname pattern. {ordinal} is substituted per broker.
      # Example: "broker-{ordinal}.kafka.example.com"
      hostnamePattern: broker-{ordinal}.kafka.example.com

      # Bootstrap hostname (optional). Used for clients that want a single
      # address. Typically a wildcard DNS record pointing at the same LB
      # or a CNAME to broker-0. Purely a convenience — not required.
      bootstrapHostname: kafka.example.com

      tls:
        certManager:
          enabled: true
          issuerRef:
            name: letsencrypt-prod
            kind: ClusterIssuer
          # Certificate covers all hostnames:
          # SANs:
          #   - broker-0.kafka.example.com
          #   - broker-1.kafka.example.com
          #   - broker-2.kafka.example.com
          #   - kafka.example.com (bootstrap, if set)
          # OR a wildcard: *.kafka.example.com

      gateway:
        enabled: true
        gatewayRef:
          name: skafka-gateway
          namespace: kafka
        # Gateway must have a TLS-passthrough listener on port 9093.

      service:
        annotations: {}

status:
  # Populated once external listener is ready.
  bootstrapServers:
    - broker-0.kafka.example.com:9093
    - broker-1.kafka.example.com:9093
    - broker-2.kafka.example.com:9093
  conditions:
    - type: ExternalListenerReady
      status: "True"
```

### Reconciliation logic

```go
func (r *KafkaClusterReconciler) reconcileListeners(
    ctx context.Context, cluster *v1alpha1.KafkaCluster,
) error {

    // Internal: always active. Each broker's INTERNAL_ADVERTISED_HOST is a
    // headless DNS name derived from its ordinal at StatefulSet create time.
    // No reconciliation needed beyond the existing StatefulSet.

    if !cluster.Spec.Listeners.External.Enabled {
        return r.deleteExternalListenerResources(ctx, cluster)
    }

    // 1. One Certificate covering all per-broker hostnames + optional bootstrap.
    if cluster.Spec.Listeners.External.TLS.CertManager.Enabled {
        if err := r.reconcileBrokerCertificate(ctx, cluster); err != nil {
            return err
        }
    }

    // 2. One LoadBalancer Service per broker ordinal, selecting only that pod
    //    via statefulset.kubernetes.io/pod-name label.
    //    Alternative: a single Service + N TLSRoutes with SNI matching.
    //    The TLSRoute+SNI approach is what Phase 9 uses (simpler infra).

    // 3. One TLSRoute per broker, matching by SNI hostname, backed by the
    //    per-broker Service (pod-name selector).
    for i := int32(0); i < cluster.Spec.Replicas; i++ {
        if err := r.reconcileBrokerService(ctx, cluster, i); err != nil {
            return err
        }
        if cluster.Spec.Listeners.External.Gateway.Enabled {
            if err := r.reconcileBrokerTLSRoute(ctx, cluster, i); err != nil {
                return err
            }
        }
    }

    // 4. StatefulSet env vars don't need operator-injected patching because
    //    the hostname pattern is known at install time. Helm renders it.
    //    (This is the biggest operational win over the single-hostname design.)

    return r.updateExternalListenerStatus(ctx, cluster)
}
```

**No LB-IP wait, no rolling restart.** Because each broker's external hostname
is derived from its ordinal and the static hostname pattern in `values.yaml`,
it is known at pod creation time. The operator never needs to patch the
StatefulSet env vars after the fact.

This is the concrete operational benefit of the per-broker-hostname design
versus the single-hostname design that required operator-driven post-creation
env-var injection.

---

## Step 9.2 — Helm Templates

### listener-certificate.yaml

One Certificate covering all broker hostnames, plus the bootstrap hostname if
configured.

```yaml
{{- if and .Values.listeners.external.enabled
          .Values.listeners.external.tls.certManager.enabled }}
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {{ include "skafka.fullname" . }}-broker-tls
spec:
  secretName: {{ include "skafka.fullname" . }}-broker-tls
  dnsNames:
    {{- range $i, $e := until (int .Values.broker.replicaCount) }}
    - {{ printf $.Values.listeners.external.hostnamePattern (toString $i) | quote }}
    {{- end }}
    {{- with .Values.listeners.external.bootstrapHostname }}
    - {{ . | quote }}
    {{- end }}
  issuerRef:
    name: {{ .Values.listeners.external.tls.certManager.issuerRef.name }}
    kind: {{ .Values.listeners.external.tls.certManager.issuerRef.kind }}
{{- end }}
```

**All broker pods mount the same Secret.** The certificate covers every
per-broker SAN. cert-manager rotates it; `WatchingCertificate` in the broker
hot-reloads it.

### listener-services.yaml

One Service per broker pod. Each Service selects only that specific pod by
matching `statefulset.kubernetes.io/pod-name`. Backed by the Gateway (or
LoadBalancer) through the TLSRoute.

```yaml
{{- if .Values.listeners.external.enabled }}
{{- range $i, $e := until (int .Values.broker.replicaCount) }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ include "skafka.fullname" $ }}-broker-{{ $i }}
spec:
  type: ClusterIP
  selector:
    {{- include "skafka.brokerSelectorLabels" $ | nindent 4 }}
    statefulset.kubernetes.io/pod-name: {{ include "skafka.fullname" $ }}-{{ $i }}
  ports:
    - name: kafka-tls
      port: {{ $.Values.listeners.external.port }}
      targetPort: kafka-tls
{{- end }}
{{- end }}
```

### listener-tlsroutes.yaml

One TLSRoute per broker, matched by SNI hostname.

```yaml
{{- if and .Values.listeners.external.enabled
          .Values.listeners.external.gateway.enabled }}
{{- range $i, $e := until (int .Values.broker.replicaCount) }}
---
apiVersion: gateway.networking.k8s.io/v1alpha2
kind: TLSRoute
metadata:
  name: {{ include "skafka.fullname" $ }}-broker-{{ $i }}
spec:
  hostnames:
    - {{ printf $.Values.listeners.external.hostnamePattern (toString $i) | quote }}
  rules:
    - backendRefs:
        - name: {{ include "skafka.fullname" $ }}-broker-{{ $i }}
          port: {{ $.Values.listeners.external.port }}
  parentRefs:
    - name: {{ $.Values.listeners.external.gateway.gatewayRef.name }}
      namespace: {{ $.Values.listeners.external.gateway.gatewayRef.namespace }}
      sectionName: kafka-tls
{{- end }}
{{- end }}
```

**SNI-based routing** — the Gateway reads the SNI hostname from the ClientHello
to pick which TLSRoute applies, then forwards bytes unchanged. TLS terminates
at the broker pod. The Gateway never holds the private key.

### values.yaml additions

```yaml
listeners:
  internal:
    port: 9092

  external:
    enabled: false
    port: 9093
    hostnamePattern: broker-{ordinal}.kafka.example.com
    bootstrapHostname: ""   # optional; if set, included in certificate SANs

    tls:
      certManager:
        enabled: true
        issuerRef:
          name: letsencrypt-prod
          kind: ClusterIssuer

    gateway:
      enabled: true
      gatewayRef:
        name: skafka-gateway
        namespace: kafka
```

### StatefulSet env additions

The broker container gets its external advertised host from the pod ordinal:

```yaml
- name: EXTERNAL_ADVERTISED_HOST
  value: "broker-$(POD_ORDINAL).kafka.example.com"
- name: POD_ORDINAL
  valueFrom:
    fieldRef:
      fieldPath: metadata.labels['apps.kubernetes.io/pod-index']
```

`apps.kubernetes.io/pod-index` is a built-in downward-API label on StatefulSet
pods (Kubernetes 1.28+). No init container or entrypoint script needed.

### Helm NOTES.txt

```
{{- if .Values.listeners.external.enabled }}
External access is enabled with end-to-end TLS.

Wait for the broker certificate to be ready:

  kubectl get certificate {{ include "skafka.fullname" . }}-broker-tls \
    -n {{ .Release.Namespace }}

Configure your Kafka clients with bootstrap servers:
  {{- range $i, $e := until (int .Values.broker.replicaCount) }}
  {{ printf $.Values.listeners.external.hostnamePattern (toString $i) }}:{{ $.Values.listeners.external.port }}
  {{- end }}

Any one of these is sufficient for bootstrap — the client learns the full list
from Metadata.
{{- else }}
External access is disabled. In-cluster clients connect directly to:
  {{- range $i, $e := until (int .Values.broker.replicaCount) }}
  {{ include "skafka.fullname" . }}-{{ $i }}.{{ include "skafka.headlessName" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ .Values.listeners.internal.port }}
  {{- end }}
{{- end }}
```

---

## Step 9.3 — Integration Tests

File: `tests/integration/external_access_test.go`

### In-cluster (kind + Rook-Ceph + Envoy Gateway)

1. **TLS passthrough connectivity** — deploy with external listener enabled;
   `openssl s_client -connect broker-0.kafka.example.com:9093 -servername
   broker-0.kafka.example.com`; verify the returned certificate has the
   expected CN or SAN.

2. **Produce + consume via TLS** — franz-go client connects to
   `broker-0.kafka.example.com:9093` as bootstrap; produces 10,000 records;
   consumes all; verifies order.

3. **Metadata response carries per-broker hostnames** — verify Metadata
   response contains `broker-0.kafka.example.com`, `broker-1.kafka.example.com`,
   `broker-2.kafka.example.com` (not headless DNS, not a single hostname).

4. **NOT_LEADER redirect** — produce to a partition led by broker-2 via the
   broker-0 address; verify the client receives `NOT_LEADER_FOR_PARTITION`,
   refreshes Metadata, reconnects to `broker-2.kafka.example.com`, succeeds.
   Verify this happens automatically with the Kafka client's default retry
   settings — no application-level handling required.

5. **Broker failover** — `kubectl delete pod skafka-broker-1`; verify:
   - Client sees the broker drop out of Metadata
   - Partitions formerly led by broker-1 migrate to other brokers via Lease
   - Client reconnects to the new leader transparently
   - No data loss

6. **Certificate hot-reload** — rotate the cert-manager Certificate; verify all
   broker pods pick up the new cert on their next TLS handshake (within 5s of
   Secret update) without pod restarts.

7. **Internal client unaffected** — with external listener enabled, verify a
   pod inside the cluster using `skafka-0.skafka-headless.kafka.svc:9092`
   connects and produces without TLS.

8. **Bootstrap hostname CNAME** — set `listeners.external.bootstrapHostname =
   kafka.example.com` as a CNAME to `broker-0.kafka.example.com`; verify
   clients using `kafka.example.com:9093` as bootstrap learn the three
   `broker-N.kafka.example.com` addresses via Metadata and use them normally.

9. **Wildcard DNS** — configure `*.kafka.example.com` as a wildcard DNS pointing
   at the Gateway's LB address; verify SNI routing still works correctly
   without explicit per-broker A records.

---

## Step Order Summary

| Step | File(s) | Depends on |
|---|---|---|
| 9.0 Broker TLS listener | `internal/protocol/server.go`, `internal/protocol/tls.go` | Phase 7 |
| 9.1 Operator listeners | `operator/controllers/kafkacluster_controller.go`, CRD types | 9.0 |
| 9.2 Helm templates | `listener-*.yaml`, values | 9.1 |
| 9.3 Integration tests | `tests/integration/external_access_test.go` | 9.0–9.2 |

9.0 and 9.1 are mostly independent — 9.0 adds the TLS listener to the broker,
9.1 adds the CRD fields and operator reconcile logic. They can be written in
parallel with mutual stubs. 9.2 depends on 9.1 for the value structure. Tests
(9.3) run last, using the real chart.

---

## Comparison With Discarded Alternatives

| Concern | Custom router | Single hostname + forwarding | **Per-broker hostnames (this)** |
|---|---|---|---|
| TLS endpoint | Router pod | Broker pod | Broker pod |
| Custom Go code | ~600 lines | ~150 lines (forwarding) | **0 lines beyond TLS listener** |
| Kafka protocol retry | Bypassed by router | Bypassed by forwarding | **Used as designed** |
| Operator complexity | High (LB IP watch) | Medium (env patch + restart) | **Low (Helm-rendered hostnames)** |
| DNS requirement | One record | One record | Wildcard or one per broker |
| Certificate | Single cert | Single cert | Wildcard or SAN-per-broker |
| Gateway routes | One TLSRoute | One TLSRoute | N TLSRoutes (one per broker) |
| Post-install restart | Yes (env patch) | Yes (env patch) | **No (hostnames known upfront)** |

The per-broker-hostname approach trades slightly more YAML (N TLSRoutes + N
Services) for significantly less runtime complexity and fewer moving parts.
Helm renders everything at install time; the operator's job shrinks to just
reconciling the certificate and the static TLSRoutes.
