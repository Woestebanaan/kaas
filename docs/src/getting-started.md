# Getting Started

Deploy kaas onto a cluster with the Helm chart, or run a single broker locally in dev mode with in-memory storage.

## Deploy on Kubernetes

Prerequisites: Kubernetes ≥ 1.27, Helm ≥ 3.8, and — for more than one
broker — a `ReadWriteMany` StorageClass with NFSv4-class semantics (see
[Storage substrate requirements](operations/storage.md)).

```bash
helm install my-kaas oci://ghcr.io/woestebanaan/charts/kaas \
  --version 0.2.4-preview \
  --namespace kafka --create-namespace \
  --set storage.className=<your-rwx-class> \
  --set broker.replicaCount=3
```

Topics are Kubernetes resources:

```yaml
apiVersion: kaas.rs/v1alpha1
kind: KafkaTopic
metadata:
  name: test
  namespace: kafka
spec:
  partitions: 3
```

Then talk to it like any Kafka:

```bash
kubectl -n kafka port-forward svc/my-kaas-kaas 9092:9092 &
echo "hello" | kcat -b localhost:9092 -t test -P
kcat -b localhost:9092 -t test -C -o beginning -e
```

The chart's default listeners, authentication options, and external-access
plumbing are covered in [Helm chart & listener
configuration](operations/helm.md); single-broker dev clusters can run on
a plain `ReadWriteOnce` local-path class.

## Run locally in dev mode

The broker binary detects dev mode by the absence of the `MY_POD_NAME`
env var (which the StatefulSet always sets): storage flips to in-memory,
no Kubernetes API is needed, and the broker treats itself as leader of
every partition.

```bash
cargo run -p kaas
```

Point any Kafka client at `localhost:9092`. Nothing is persisted and
nothing is cluster-aware — it's a protocol-correct scratchpad for client
development and codec work, not a single-node production mode.

## Build the book and the code

```bash
cargo build --workspace          # toolchain is pinned; rustup auto-installs
cargo test  --workspace --all-features
cargo xtask docs --serve         # this book, live-reloading
```
