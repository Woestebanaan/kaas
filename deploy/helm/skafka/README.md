# skafka Helm chart

Deploys the skafka broker StatefulSet and operator Deployment backed by a single
shared ReadWriteMany PersistentVolumeClaim.

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
support `ReadWriteMany` AND the broker's partition lock mechanism.

| StorageClass | Lock backend | Safe? | Notes |
|---|---|---|---|
| **CephFS (Rook / ceph-csi)** | `flock` | ✅ Production | `flock()` propagates cluster-wide. Recommended. |
| **Longhorn / OpenEBS RWX** | `flock` | ✅ Production | Block-backed RWX; `flock()` works. |
| **NFS (nfs-csi, subdir-external-provisioner)** | `nfs` | ⚠️ Advisory only | `flock()` over NFS is unreliable. Split-brain risk during network partitions. Not recommended for production. |
| **Local / hostPath** | `flock` | ✅ Single-pod dev | Not RWX; only works with `broker.replicaCount: 1`. |

Select the lock backend explicitly:

```yaml
lock:
  backend: flock   # or "nfs"
```

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
