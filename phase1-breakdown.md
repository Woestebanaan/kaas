# Phase 1 Breakdown: Foundation

## Environment Status

| Tool | Status | Notes |
|---|---|---|
| Go | ✅ 1.26.1 | Exceeds the 1.22+ requirement |
| kubectl | ✅ v1.28.15 | |
| helm | ✅ v3.20.1 | |
| make | ✅ 4.3 | |
| git | ✅ 2.39.5 | |
| kubebuilder | ❌ missing | Needed for CRD scaffolding |
| controller-gen | ❌ missing | Needed for `make generate`/`make manifests` |
| golangci-lint | ❌ missing | Needed for CI linting |
| kind | ❌ missing | Needed for integration tests |
| docker | ➡️ deferred | Image builds handled by GitHub Actions + GHCR |
| kcat | ❌ missing | Needed for compatibility tests (Phase 2) |

kubebuilder and controller-gen block CRD scaffolding (Step 1.3) — both now installed. Docker is not needed locally; images are built and published via GitHub Actions to ghcr.io/woestebanaan/skafka.

---

## Step 1.1 — Install missing tools

- Install `kubebuilder` (binary download, v3.x)
- Install `controller-gen` via `go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest`
- Install `golangci-lint` via the official install script
- Install `kind` binary
- Document which tools need docker (kind cluster creation, image builds) — defer those

**Done when:** kubebuilder, controller-gen, golangci-lint, and kind all respond to `--version`

---

## Step 1.2 — Initialize Go module

- `go mod init github.com/yourorg/skafka` (confirm org name before running)
- Create top-level directory structure: `cmd/`, `internal/`, `operator/`, `pkg/`, `deploy/`, `tests/`
- Add a minimal `Makefile` with placeholder targets: `generate`, `manifests`, `build`, `test`, `lint`

**Done when:** `go build ./...` succeeds on an empty module

---

## Step 1.3 — kubebuilder init + CRD scaffolding

- `kubebuilder init --domain skafka.io --repo github.com/yourorg/skafka`
- Create all four API kinds: `KafkaTopic`, `KafkaUser`, `KafkaUserGroup`, `KafkaAcl`
- Fill in Go struct fields for each CRD's `Spec` and `Status` (based on the YAML examples in the main plan)
- Run `make generate && make manifests` to produce DeepCopy methods and CRD YAML

**Done when:** `deploy/crds/` contains valid CRD YAML for all four kinds, `make generate` is clean

---

## Step 1.4 — Core interfaces

Define the four interfaces (no implementation yet) in their respective packages:

- `internal/storage/engine.go` → `StorageEngine`
- `internal/lease/manager.go` → `LeaseManager`
- `internal/lock/lock.go` → `PartitionLock`
- `internal/auth/auth.go` → `AuthEngine`

Also define shared types they depend on: `Record`, `Credentials`, `Principal`, `Resource`, `Operation`, `LeaderChange`.

**Done when:** `go build ./...` passes, interfaces are complete, no implementations yet

---

## Step 1.5 — RBAC manifests

Write `deploy/rbac/broker-clusterrole.yaml` and `deploy/rbac/operator-clusterrole.yaml` based on the permission lists in the main plan. Pure YAML — no code required.

**Done when:** both files exist and `kubectl apply --dry-run=client` passes against them

---

## Step 1.6 — CI pipeline (GitHub Actions)

Create `.github/workflows/ci.yml` with:

- `go vet ./...`
- `golangci-lint run`
- `go test ./...`
- `make manifests` + diff check (fail if CRD YAML drifted from generated)
- Placeholder integration test job (disabled until kind + docker are available)

**Done when:** CI runs green on a push to `main`
