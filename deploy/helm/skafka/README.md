# skafka Helm chart

Deploys the skafka broker StatefulSet and operator Deployment backed by a single
shared ReadWriteMany PersistentVolumeClaim.

> **Note (mid-rewrite).** skafka is being rewritten from Go to Rust (see
> `../../../docs/rewrite.md`). The chart itself is language-agnostic and is
> reused unchanged by both flavors. Phase 9 of the rewrite will add an
> `image.flavor` value (`go` | `rust`) so a single chart version can deploy
> either; until then the chart only points at the Go images shipped from
> `archive/`.

## Prerequisites

- Kubernetes >= 1.27
- A `ReadWriteMany` StorageClass (see **StorageClass guidance** below)
- Helm >= 3.8 (for OCI chart support)

## Installation

The chart is published as an OCI artifact to GHCR. No `helm repo add` needed.

The chart bundles its CRDs under `crds/`, so Helm installs them automatically on
first install. The chart is always pushed under the name `skafka` (from
`Chart.yaml`); pre-release tags (`vX.Y.Z-*`) only rename the *images* to their
`*-preview` variants. Only `v0.1.0-preview` has been tagged so far, so the
install command below uses that:

```bash
helm install my-skafka oci://ghcr.io/woestebanaan/charts/skafka \
  --version 0.1.0-preview \
  --namespace kafka --create-namespace \
  --set image.repository=ghcr.io/woestebanaan/skafka-preview \
  --set operator.image.repository=ghcr.io/woestebanaan/skafka-operator-preview \
  --set storage.className=ceph-filesystem \
  --set broker.replicaCount=3
```

See available versions:

```bash
helm show all oci://ghcr.io/woestebanaan/charts/skafka --version 0.1.0-preview
```

### CRDs on upgrade

Helm [deliberately does not upgrade CRDs](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/)
that it installed from the chart's `crds/` directory. When a release upgrades
CRDs, apply them explicitly before `helm upgrade`:

```bash
# Pull the new chart version locally, then apply the CRDs it ships:
helm pull oci://ghcr.io/woestebanaan/charts/skafka --version 0.1.0-preview --untar
kubectl apply -f skafka/crds/

# Or apply them straight from the repo at a specific ref:
REF=v0.1.0-preview
BASE=https://raw.githubusercontent.com/Woestebanaan/skafka/${REF}/deploy/crds
for f in skafka.io_kafkaclusters.yaml \
         skafka.io_kafkatopics.yaml \
         skafka.io_kafkausers.yaml \
         skafka.io_kafkausergroups.yaml \
         skafka.io_kafkaacls.yaml; do
  kubectl apply -f "${BASE}/${f}"
done
```

## StorageClass guidance

skafka stores all partition data on a single shared PVC. The StorageClass must
support `ReadWriteMany` and provide NFSv4-class semantics: atomic same-directory
rename, fsync durability, and close-to-open consistency.

Phase 4 dropped flock entirely — single-writer enforcement now comes from
epoch-prefixed segment filenames + the BrokerCoordinator's ownership decision
(see `archive/internal/controller/` in the Go tree, or `crates/sk-controller`
once the Rust port lands — see `../../../docs/rewrite.md`), so the StorageClass no
longer needs to support `flock()`. Any RWX volume that meets NFSv4-class
semantics works.

| StorageClass | Status | Notes |
|---|---|---|
| **CephFS (Rook / ceph-csi)** | ✅ Production | Strong same-directory rename atomicity; recommended. |
| **csi-driver-nfs / NFSv4.1 server** | ✅ Production | Use `nconnect=4-8` and `acregmax=1` for sub-second mtime freshness on assignment.json polling. |
| **AWS EFS / Azure Files Premium NFS / GCP Filestore** | ✅ Production | All offer NFSv4-class semantics. |
| **Longhorn / OpenEBS RWX** | ✅ Production | Block-backed RWX. |
| **Local / hostPath** | ✅ Single-pod dev | Not RWX; only works with `broker.replicaCount: 1`. |

### NFS mount options

For any NFS-backed StorageClass (csi-driver-nfs, EFS, Filestore, etc.) set
`mountOptions` on the StorageClass — not on the PVC, since `mountOptions` is a
StorageClass field that the CSI driver translates into NFS mount flags at
attach time. Example:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: skafka-nfs
provisioner: nfs.csi.k8s.io
mountOptions:
  - nfsvers=4.1
  - nconnect=8        # parallel TCP connections; faster fsync
  - acregmax=1        # sub-second mtime freshness on assignment.json polling
  - hard              # block on server unavailability instead of returning EIO
parameters:
  server: nfs.example.com
  share: /export/skafka
```

The `acregmax=1` setting matters most: the broker polls assignment.json's
mtime as the fast-failover signal, and the default NFS attribute cache
(60s) would delay every controller failover. `nconnect` raises throughput
under concurrent fsyncs from multiple brokers.

## External access

The external listener uses **explicit per-broker hostnames** with a
**SAN-per-broker certificate** — the chart materialises a single
cert-manager `Certificate` whose `dnsNames` list includes
`broker-0.kafka.example.com`, `broker-1.kafka.example.com`, …, plus
the optional `bootstrapHostname`. Both choices are deliberate:

- **Per-broker hostnames, not wildcard.** Wildcard hostnames
  (`*.kafka.example.com`) would simplify DNS but require a DNS-01 ACME
  challenge — which adds an external dependency on a DNS provider that
  cert-manager can program. Explicit per-broker hostnames work with
  HTTP-01 (Gateway-fronted) or any pre-existing DNS-managed by
  whoever runs the cluster. The cost is one DNS record per broker
  pod, only changing when `broker.replicaCount` changes.
- **SAN-per-broker, not separate cert-per-broker.** Issuing one
  certificate per broker would multiply ACME issuance cost and
  rotation churn for no gain — every broker pod mounts the same
  Secret, and the in-process TLS listener picks the right SNI from
  the cert's SAN list. cert-manager rotates this single Secret
  in-place; the broker fsnotify-watches the mount and hot-reloads
  without a pod restart.

If you scale `broker.replicaCount` up at runtime, the operator
re-reconciles the `KafkaCluster` CR and updates the Certificate's
`dnsNames` to add the new SAN; cert-manager then re-issues the cert.
This is a one-time cost per scale event, not per request.

## Configuration

See `values.yaml` for the full set of tunables. Common overrides:

| Key | Default | Purpose |
|---|---|---|
| `broker.replicaCount` | 3 | Number of broker pods |
| `storage.className` | ceph-filesystem | RWX StorageClass |
| `storage.size` | 500Gi | PVC capacity |
| `auth.enabled` | true | Enable credentials.json/acls.json loading |
| `auth.requireSasl` | false | Reject non-SASL requests |
| `auth.tls.enabled` | false | Bind TLS listener on port 9093 |
| `listeners.external.enabled` | false | Enable per-broker external TLS listener |
| `listeners.external.tls.clientCA.enabled` | false | Require every TLS client to present a cert (mTLS) |
| `listeners.external.tls.clientCA.existingSecret` | `""` | Secret holding the CA bundle for client cert verification |
| `broker.controllerLease.durationSeconds` | 15 | Cluster-controller Lease lifetime; lower = faster failover, more etcd writes |
| `podDisruptionBudget.maxUnavailable` | 1 | Equivalent to Kafka min-ISR guarantee |

## Smoke test

```bash
# Port-forward to the client Service (release name + "-skafka"):
kubectl -n kafka port-forward svc/my-skafka-skafka 9092:9092 &

# Create a topic:
cat <<EOF | kubectl apply -f -
apiVersion: skafka.io/v1alpha1
kind: KafkaTopic
metadata:
  name: test
  namespace: kafka
spec:
  partitions: 3
EOF

# Produce and consume with kcat:
echo "hello" | kcat -b localhost:9092 -t test -P
kcat -b localhost:9092 -t test -C -o beginning -e
```

## Uninstall

```bash
helm uninstall my-skafka -n kafka
```

**Note:** the PVC is NOT deleted on uninstall (`helm.sh/resource-policy: keep`
annotation). Delete it manually if you want to reclaim the storage:

```bash
kubectl -n kafka delete pvc my-skafka-skafka-data
```

**Note:** the `KafkaCluster` CR has a finalizer that the operator must
process to clean up the cert-manager Certificate and Gateway TLSRoutes.
Because `helm uninstall` deletes the operator Deployment in parallel
with the `KafkaCluster` CR, the operator may terminate before the
finalizer fires, leaving the CR stuck in `Terminating`. Delete the CR
explicitly first:

```bash
kubectl -n kafka delete kafkacluster my-skafka --wait
helm uninstall my-skafka -n kafka
```
