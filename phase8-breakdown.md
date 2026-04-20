# Phase 8 Breakdown: Kubernetes Deployment

## Current State (end of Phase 7)

All code for a functional single-namespace deployment exists. What's missing is
everything that makes it runnable on a real cluster: container images, manifests,
a Helm chart, and a health-probe endpoint for readiness checks.

### What already exists in `deploy/`

| Path | Purpose |
|---|---|
| `deploy/crds/skafka.io_kafka*.yaml` | Generated CustomResourceDefinitions for all four CRDs |
| `deploy/rbac/broker-clusterrole.yaml` | Broker ClusterRole (Leases, EndpointSlices, CRD watch, Secrets, pods/status) |
| `deploy/rbac/operator-clusterrole.yaml` | Operator ClusterRole (full CRD CRUD, Secrets, Jobs, Leases, cert-manager) |
| `deploy/helm/` | **Empty** — chart lives here in Phase 8 |
| `deploy/grafana/` | Reserved for Phase 10 dashboards |

### One code gap to close first

The plan's StatefulSet spec declares:
```yaml
readinessProbe:
  httpGet: {path: /healthz, port: 8080}
```

The broker binary doesn't currently serve HTTP on `:8080`. Step 8.0 adds a tiny health
probe server — just `/healthz` and `/readyz` returning 200 OK once the TCP listener is up.

---

## Step 8.0 — Broker health probe endpoint

File: `cmd/skafka/main.go` (extend)

A minimal `net/http` server on `:8080` (configurable via `SKAFKA_HEALTH_ADDR`) with two
handlers:
- `/healthz` → 200 OK if the process is alive (always, after startup)
- `/readyz` → 200 OK if the TCP listener is bound AND the broker has acquired any
  assigned partition leases (or `PartitionsReady` condition is set). In local-dev mode
  this just mirrors `/healthz`.

```go
func startHealthServer(ctx context.Context, addr string, ready func() bool) {
    mux := http.NewServeMux()
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
        w.WriteHeader(http.StatusOK)
    })
    mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
        if !ready() {
            w.WriteHeader(http.StatusServiceUnavailable)
            return
        }
        w.WriteHeader(http.StatusOK)
    })
    srv := &http.Server{Addr: addr, Handler: mux}
    go func() { _ = srv.ListenAndServe() }()
    go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
}
```

**Done when:** `curl localhost:8080/healthz` returns 200 while the broker is running.

---

## Step 8.1 — Broker Dockerfile

File: `Dockerfile` (repo root)

```dockerfile
# syntax=docker/dockerfile:1

# Digest-pinned to prevent silent drift. Dependabot/Renovate bumps these.
FROM golang:1.22-alpine@sha256:<pinned-digest> AS builder
WORKDIR /src

# Cache go module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOFLAGS="-trimpath" \
    go build -ldflags="-s -w" -o /skafka ./cmd/skafka

FROM gcr.io/distroless/static-debian12:nonroot@sha256:<pinned-digest>
COPY --from=builder /skafka /skafka

# Non-root, no shell, no package manager.
USER nonroot:nonroot
EXPOSE 9092 9093 8080
ENTRYPOINT ["/skafka"]
```

**Why digest-pinned:** `:nonroot` is a floating tag; tomorrow's build of the same
git commit could produce a different binary than today's. Pinning by SHA256 digest
makes builds bit-for-bit reproducible. Dependabot/Renovate will file PRs to bump the
digest on upstream releases.

Target image size: <20MB (distroless static base is ~2MB, Go binary ~15–18MB).

### Operator Dockerfile

File: `Dockerfile.operator`

Identical pattern with `./cmd/skafka-operator` as the build target and port `:8081`
exposed (health probes).

### `.dockerignore`

File: `.dockerignore`

```
.git
.github
deploy
phase*-breakdown.md
shared-storage-kafka-plan.md
*.md
tests/kafka-compat
skafka
skafka-operator
```

### No local Docker required — images build in CI

All container builds happen in GitHub Actions (Step 8.8). The Dockerfiles are authored
here but validated by CI, never by running `docker build` locally. A CI job on every
pull request runs `docker build` without pushing to catch Dockerfile regressions before
merge.

**Done when:** the PR-time workflow (`.github/workflows/ci.yml`, added in Step 8.8)
successfully builds both images without errors.

---

## Step 8.2 — Helm chart skeleton

File: `deploy/helm/skafka/Chart.yaml`

```yaml
apiVersion: v2
name: skafka
description: Kafka-protocol-compatible broker on shared storage with Kubernetes-native coordination
type: application
version: 0.1.0
appVersion: "0.1.0"
kubeVersion: ">=1.27.0-0"
keywords: [kafka, streaming, messaging]
sources:
  - https://github.com/yourorg/skafka
```

### Directory layout

```
deploy/helm/skafka/
├── Chart.yaml
├── values.yaml
├── README.md
├── crds/
│   ├── kafkatopics.yaml           ← copies of deploy/crds/*, installed once by Helm
│   ├── kafkausers.yaml
│   ├── kafkausergroups.yaml
│   └── kafkaacls.yaml
└── templates/
    ├── _helpers.tpl
    ├── NOTES.txt
    ├── broker-statefulset.yaml
    ├── broker-service.yaml
    ├── broker-pvc.yaml
    ├── broker-pdb.yaml
    ├── broker-serviceaccount.yaml
    ├── broker-role.yaml
    ├── broker-rolebinding.yaml
    ├── broker-clusterrole.yaml       ← from deploy/rbac/, templated
    ├── broker-clusterrolebinding.yaml
    ├── operator-deployment.yaml
    ├── operator-serviceaccount.yaml
    ├── operator-clusterrole.yaml
    ├── operator-clusterrolebinding.yaml
    └── configmap.yaml
```

CRDs under `crds/` are installed automatically by Helm on first install and are NOT
upgraded by `helm upgrade` (Helm's intentional behavior — CRD schema changes go through
a separate `kubectl apply -f crds/`).

---

## Step 8.3 — values.yaml

File: `deploy/helm/skafka/values.yaml`

```yaml
image:
  repository: ghcr.io/yourorg/skafka
  tag: ""               # defaults to Chart.appVersion
  pullPolicy: IfNotPresent

operator:
  enabled: true
  image:
    repository: ghcr.io/yourorg/skafka-operator
    tag: ""
    pullPolicy: IfNotPresent
  resources:
    requests: {cpu: 100m, memory: 128Mi}
    limits:   {cpu: 500m, memory: 256Mi}

broker:
  replicaCount: 3
  clusterID: skafka-local
  ports:
    kafka: 9092
    tls: 9093
    health: 8080
  resources:
    requests: {cpu: 500m, memory: 1Gi}
    limits:   {cpu: 2,    memory: 4Gi}
  config:
    segmentBytes: 1073741824     # 1 GB
    retentionHours: 168          # 7 days
    numPartitions: 3
    rebalanceTimeoutMs: 60000
  lease:
    durationSeconds: 15
    renewDeadlineSeconds: 10
    retryPeriodSeconds: 2

storage:
  className: ceph-filesystem     # or nfs-csi
  size: 500Gi
  accessMode: ReadWriteMany
  mountPath: /data

lock:
  backend: flock                 # "flock" for CephFS; "nfs" for advisory over NFS

auth:
  enabled: true
  mechanisms: [SCRAM-SHA-512]
  requireSasl: false             # set to true to reject non-SASL requests
  tls:
    enabled: false
    existingSecret: ""           # Secret containing tls.crt + tls.key
    certManagerIssuer: ""

podDisruptionBudget:
  enabled: true
  maxUnavailable: 1

serviceAccount:
  broker:
    create: true
    name: ""                     # defaults to {release}-broker
  operator:
    create: true
    name: ""

autoscaling:
  enabled: false
  minReplicas: 3
  maxReplicas: 10
  targetConsumerLagMessages: 100000
```

---

## Step 8.4 — StatefulSet template

File: `deploy/helm/skafka/templates/broker-statefulset.yaml`

Key points:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ include "skafka.fullname" . }}
spec:
  replicas: {{ .Values.broker.replicaCount }}
  serviceName: {{ include "skafka.headlessName" . }}
  podManagementPolicy: Parallel     # all brokers are identical; no startup ordering needed
  selector:
    matchLabels: {{- include "skafka.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels: {{- include "skafka.selectorLabels" . | nindent 8 }}
    spec:
      serviceAccountName: {{ include "skafka.brokerSAName" . }}
      readinessGates:
        - conditionType: "skafka.io/PartitionsReady"
      initContainers:
        - name: partition-init
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
          args: ["--init"]
          env:
            - name: SKAFKA_DATA_DIR
              value: {{ .Values.storage.mountPath }}
            - name: SKAFKA_NAMESPACE
              valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
          volumeMounts:
            - name: data
              mountPath: {{ .Values.storage.mountPath }}
      containers:
        - name: broker
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
          ports:
            - {name: kafka,  containerPort: {{ .Values.broker.ports.kafka }}}
            - {name: tls,    containerPort: {{ .Values.broker.ports.tls }}}
            - {name: health, containerPort: {{ .Values.broker.ports.health }}}
          env:
            - name: MY_POD_NAME
              valueFrom: {fieldRef: {fieldPath: metadata.name}}
            - name: SKAFKA_NAMESPACE
              valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
            - name: SKAFKA_HEADLESS_SVC
              value: {{ include "skafka.headlessName" . }}
            - name: SKAFKA_DATA_DIR
              value: {{ .Values.storage.mountPath }}
            - name: SKAFKA_CLUSTER_ID
              value: {{ .Values.broker.clusterID }}
            - name: SKAFKA_PORT
              value: "{{ .Values.broker.ports.kafka }}"
            {{- if .Values.auth.requireSasl }}
            - name: SKAFKA_REQUIRE_SASL
              value: "true"
            {{- end }}
          livenessProbe:
            tcpSocket: {port: kafka}
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet: {path: /readyz, port: health}
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: {{ .Values.storage.mountPath }}
            {{- if .Values.auth.tls.enabled }}
            - name: tls
              mountPath: /tls
              readOnly: true
            {{- end }}
          resources: {{- toYaml .Values.broker.resources | nindent 12 }}
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: {{ include "skafka.pvcName" . }}
        {{- if .Values.auth.tls.enabled }}
        - name: tls
          secret:
            secretName: {{ .Values.auth.tls.existingSecret }}
        {{- end }}
```

Key choices:
- `podManagementPolicy: Parallel` — all brokers are identical; there's no leader-follower
  ordering constraint like in vanilla Kafka.
- Single shared PVC (volume `data`) — NOT a volumeClaimTemplate. Every pod mounts the
  same volume at the same path. This is the defining architectural choice of skafka.
- ReadinessGate `skafka.io/PartitionsReady` — Phase 4 already implements the PATCH.

---

## Step 8.5 — Service, PVC, PDB templates

### `broker-service.yaml`

Two services:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "skafka.headlessName" . }}   # e.g. skafka-headless
spec:
  clusterIP: None                                # headless: gives pod-FQDN DNS
  publishNotReadyAddresses: false                # respects readinessGates
  selector: {{- include "skafka.selectorLabels" . | nindent 4 }}
  ports:
    - {name: kafka, port: 9092, targetPort: kafka}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ include "skafka.fullname" . }}        # client-facing Service
spec:
  type: ClusterIP
  selector: {{- include "skafka.selectorLabels" . | nindent 4 }}
  ports:
    - {name: kafka, port: 9092, targetPort: kafka}
    {{- if .Values.auth.tls.enabled }}
    - {name: tls,   port: 9093, targetPort: tls}
    {{- end }}
```

The client Service is a stopgap until Phase 9 (router) ships. It load-balances to any
ready broker; the client handles `NOT_LEADER_FOR_PARTITION` retries (franz-go and
kafka-go both do this automatically).

### `broker-pvc.yaml`

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ include "skafka.pvcName" . }}
  annotations:
    helm.sh/resource-policy: keep   # never auto-delete on helm uninstall
spec:
  accessModes: [{{ .Values.storage.accessMode }}]
  storageClassName: {{ .Values.storage.className }}
  resources:
    requests:
      storage: {{ .Values.storage.size }}
```

### `broker-pdb.yaml`

```yaml
{{- if .Values.podDisruptionBudget.enabled }}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "skafka.fullname" . }}
spec:
  maxUnavailable: {{ .Values.podDisruptionBudget.maxUnavailable }}
  selector:
    matchLabels: {{- include "skafka.selectorLabels" . | nindent 6 }}
{{- end }}
```

---

## Step 8.6 — RBAC templates

Convert `deploy/rbac/broker-clusterrole.yaml` and `operator-clusterrole.yaml` into
Helm templates (add `{{ .Release.Name }}` prefix to names). Add matching
`ServiceAccount` + `ClusterRoleBinding` pairs.

Namespaced resources (Secrets, Pods) get a `Role`/`RoleBinding` scoped to the release
namespace; cluster-wide resources (CRDs, Leases across ns if needed) stay on
`ClusterRole`/`ClusterRoleBinding`.

Files:
- `broker-serviceaccount.yaml`
- `broker-clusterrole.yaml`
- `broker-clusterrolebinding.yaml`
- `broker-role.yaml` (Secrets + pods/status in own namespace)
- `broker-rolebinding.yaml`
- `operator-serviceaccount.yaml`
- `operator-clusterrole.yaml`
- `operator-clusterrolebinding.yaml`

---

## Step 8.7 — Operator Deployment

File: `deploy/helm/skafka/templates/operator-deployment.yaml`

```yaml
{{- if .Values.operator.enabled }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "skafka.fullname" . }}-operator
spec:
  replicas: 1
  selector:
    matchLabels: {{- include "skafka.operatorSelectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels: {{- include "skafka.operatorSelectorLabels" . | nindent 8 }}
    spec:
      serviceAccountName: {{ include "skafka.operatorSAName" . }}
      containers:
        - name: operator
          image: "{{ .Values.operator.image.repository }}:{{ .Values.operator.image.tag | default .Chart.AppVersion }}"
          env:
            - name: SKAFKA_DATA_DIR
              value: {{ .Values.storage.mountPath }}
            - name: SKAFKA_NAMESPACE
              valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
          ports:
            - {name: metrics, containerPort: 8080}
            - {name: health,  containerPort: 8081}
          livenessProbe:
            httpGet: {path: /healthz, port: health}
          readinessProbe:
            httpGet: {path: /readyz, port: health}
          volumeMounts:
            - name: data
              mountPath: {{ .Values.storage.mountPath }}
          resources: {{- toYaml .Values.operator.resources | nindent 12 }}
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: {{ include "skafka.pvcName" . }}
{{- end }}
```

The operator mounts the SAME PVC as the brokers — it needs to write
`credentials.json` and `acls.json`, plus create partition directories.

---

## Step 8.8 — CI workflow

File: `.github/workflows/release.yml`

Builds and pushes both container images AND the Helm chart to GHCR on Git tag.
GHCR has supported OCI Helm charts since Helm 3.8 — no separate `gh-pages` branch,
no GitHub Pages setup, same registry for images and charts.

### Four safety guardrails

Coupling artifact versions to git tags is the industry standard but has sharp edges.
This workflow closes four of them:

1. **Tag protection** (one-time GitHub UI setup, outside the workflow):
   Settings → Rules → New tag ruleset → target pattern `v*` → Restrict updates +
   Restrict deletions + Require approvals for tag creation. Prevents
   `git push --force --tags` and accidental deletion.
2. **Release environment with required reviewers** — the workflow references
   `environment: release` and won't proceed without a manual approval click.
3. **Pre-release detection** — tags with a hyphen (`v1.0.0-rc.1`) publish to a
   separate preview image name so they can't accidentally be pulled as stable.
4. **Keyless cosign signing** — every published artifact is signed using GitHub's
   OIDC identity. No keys to manage; verify later with
   `cosign verify ghcr.io/...`.

```yaml
name: release
on:
  push:
    tags: ['v*']
jobs:
  release:
    runs-on: ubuntu-latest
    environment: release             # requires manual approval per (2)
    permissions:
      contents: read
      packages: write
      id-token: write                # needed for keyless cosign (4)
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-qemu-action@v3
      - uses: docker/setup-buildx-action@v3
      - uses: sigstore/cosign-installer@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      # Detect pre-releases: tags with a hyphen are preview builds (3).
      - name: Compute image names
        id: names
        run: |
          VERSION=${{ github.ref_name }}
          OWNER=${{ github.repository_owner }}
          if [[ "$VERSION" == *-* ]]; then
            echo "broker_image=ghcr.io/$OWNER/skafka-preview" >> $GITHUB_OUTPUT
            echo "operator_image=ghcr.io/$OWNER/skafka-operator-preview" >> $GITHUB_OUTPUT
            echo "prerelease=true" >> $GITHUB_OUTPUT
          else
            echo "broker_image=ghcr.io/$OWNER/skafka" >> $GITHUB_OUTPUT
            echo "operator_image=ghcr.io/$OWNER/skafka-operator" >> $GITHUB_OUTPUT
            echo "prerelease=false" >> $GITHUB_OUTPUT
          fi

      # --- Container images ---
      - name: Broker image
        id: broker
        uses: docker/build-push-action@v5
        with:
          context: .
          file: Dockerfile
          platforms: linux/amd64,linux/arm64
          push: true
          tags: ${{ steps.names.outputs.broker_image }}:${{ github.ref_name }}
      - name: Operator image
        id: operator
        uses: docker/build-push-action@v5
        with:
          context: .
          file: Dockerfile.operator
          platforms: linux/amd64,linux/arm64
          push: true
          tags: ${{ steps.names.outputs.operator_image }}:${{ github.ref_name }}

      # Sign images by digest with keyless OIDC (4).
      - name: Cosign images
        run: |
          cosign sign --yes ${{ steps.names.outputs.broker_image }}@${{ steps.broker.outputs.digest }}
          cosign sign --yes ${{ steps.names.outputs.operator_image }}@${{ steps.operator.outputs.digest }}

      # --- Helm chart as OCI artifact ---
      - uses: azure/setup-helm@v4
        with: {version: 'v3.14.0'}
      - name: Package + push chart
        id: chart
        env:
          VERSION: ${{ github.ref_name }}
        run: |
          CHART_VERSION=${VERSION#v}   # Helm SemVer has no v-prefix
          helm package deploy/helm/skafka \
            --version "$CHART_VERSION" \
            --app-version "$CHART_VERSION"
          helm registry login ghcr.io \
            -u ${{ github.actor }} \
            -p ${{ secrets.GITHUB_TOKEN }}
          PUSH_OUT=$(helm push skafka-${CHART_VERSION}.tgz \
            oci://ghcr.io/${{ github.repository_owner }}/charts 2>&1)
          DIGEST=$(echo "$PUSH_OUT" | awk '/Digest:/ {print $2}')
          echo "chart_ref=ghcr.io/${{ github.repository_owner }}/charts/skafka@${DIGEST}" >> $GITHUB_OUTPUT

      # Sign the chart by digest (4).
      - name: Cosign chart
        run: cosign sign --yes ${{ steps.chart.outputs.chart_ref }}
```

Result: three artifacts per release tag, all discoverable under the same GitHub org:
- `ghcr.io/yourorg/skafka:v0.1.0` (broker image)
- `ghcr.io/yourorg/skafka-operator:v0.1.0` (operator image)
- `ghcr.io/yourorg/charts/skafka:0.1.0` (Helm chart — note no `v` prefix)

### PR-time CI (`.github/workflows/ci.yml`)

Runs on every push and pull request. Validates everything that a local contributor
couldn't check without Docker:

```yaml
name: ci
on: {push: {branches: [main]}, pull_request: {}}
jobs:
  go:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: {go-version: '1.22'}
      - run: go vet ./...
      - uses: golangci/golangci-lint-action@v6
      - run: go test ./... -timeout 120s
      - run: make manifests && git diff --exit-code   # fail if CRDs drifted

  docker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - name: Build broker image (no push)
        uses: docker/build-push-action@v5
        with: {context: ., file: Dockerfile, push: false}
      - name: Build operator image (no push)
        uses: docker/build-push-action@v5
        with: {context: ., file: Dockerfile.operator, push: false}

  helm:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: azure/setup-helm@v4
      - run: helm lint deploy/helm/skafka
      - run: helm template deploy/helm/skafka > /tmp/rendered.yaml   # catches template errors
```

This way the Dockerfile is exercised on every PR even though no maintainer ever runs
`docker build` on their laptop.

---

## Step 8.9 — README and StorageClass docs

File: `deploy/helm/skafka/README.md`

Covers:
- **Prerequisites**: Kubernetes ≥1.27, a ReadWriteMany StorageClass (CephFS recommended)
- **Installation** (chart is published as an OCI artifact to GHCR — no `helm repo add`
  needed, Helm 3.8+ required):
  ```bash
  # CRDs are installed automatically by Helm on first install (helm looks in chart/crds/).
  # For upgrades, re-apply CRDs explicitly:
  kubectl apply -f https://raw.githubusercontent.com/yourorg/skafka/main/deploy/crds/

  helm install my-kafka oci://ghcr.io/yourorg/charts/skafka \
    --version 0.1.0 \
    --set storage.className=ceph-filesystem \
    --set broker.replicaCount=3
  ```

  To see available chart versions: `helm show all oci://ghcr.io/yourorg/charts/skafka --version 0.1.0`
- **Configuration reference** — auto-generated from `values.yaml` via `helm-docs`
- **StorageClass prerequisites**:
  - **CephFS (recommended)**: Rook or ceph-csi; `flock()` works cluster-wide; use
    `broker.lock.backend: flock`
  - **NFS**: `nfs-csi` or `nfs-subdir-external-provisioner`; `flock()` is unreliable
    across nodes; use `broker.lock.backend: nfs` (advisory only — **warning: split-brain
    risk during network partitions, not recommended for production**)
  - **Longhorn / OpenEBS (block-backed RWX)**: `flock()` works; use `flock`
- **Smoke test**: produce/consume with `kcat` via `kubectl port-forward`

---

## Step 8.10 — Integration test (kind + rook-ceph)

Optional but high-value: a CI job that spins up kind + Rook-Ceph, installs the chart,
and runs the `tests/kafka-compat/` suite against it end-to-end.

File: `.github/workflows/e2e.yml`

Skipping details — standard pattern with `helm/kind-action` and
`rook/rook`'s single-node Ceph manifests. Roughly 15 minutes of cluster setup plus
the existing ~2s of test time.

---

## Step order summary

| Step | File(s) | Depends on |
|---|---|---|
| 8.0 Health probe endpoint | `cmd/skafka/main.go` | nothing |
| 8.1 Dockerfiles | `Dockerfile`, `Dockerfile.operator`, `.dockerignore` | 8.0 |
| 8.2 Chart skeleton + CRDs | `deploy/helm/skafka/{Chart.yaml,values.yaml,crds/*}` | nothing |
| 8.3 values.yaml | `deploy/helm/skafka/values.yaml` | 8.2 |
| 8.4 StatefulSet | `templates/broker-statefulset.yaml` + helpers | 8.3 |
| 8.5 Services + PVC + PDB | `templates/broker-{service,pvc,pdb}.yaml` | 8.3 |
| 8.6 RBAC | `templates/broker-{sa,role,rolebinding,clusterrole,clusterrolebinding}.yaml` | 8.3 |
| 8.7 Operator Deployment | `templates/operator-deployment.yaml` + RBAC | 8.3 |
| 8.8 CI workflow | `.github/workflows/release.yml`, `ci.yml` | 8.1 |
| 8.9 README + docs | `deploy/helm/skafka/README.md` | 8.4–8.7 |
| 8.10 kind + Rook-Ceph E2E | `.github/workflows/e2e.yml` | 8.1–8.9 |

Steps 8.2–8.7 (chart templates) are all independent after 8.3 and can be written in
parallel. 8.1 (Dockerfiles) depends only on 8.0. 8.8 (CI) depends on the Dockerfiles.
