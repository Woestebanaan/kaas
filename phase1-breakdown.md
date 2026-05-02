# Phase 1 Foundation — Breakdown vs. Current Repo

Comparison of `skafka-plan-v3.md` (now **v3.3**, Bytes-Are-Opaque Architecture) §"Phase 1: Foundation (Week 1–2)" (lines 596–632) against the state of `main` at commit `c03eb64` ("feat(v3): phase 1 foundation — types, interfaces, RBAC, scaffolds").

> **v3.3 deltas affecting Phase 1** (from prior v3.2):
> - `StorageEngine.Append` now takes `epoch uint32, batchBytes []byte` — byte-opacity is no longer optional.
> - New critical constraint #22 forbids a decoded `RecordBatch` struct with `[]Record` *anywhere* in the codec, even off the hot path.
> - New Phase 1 open question (#12): mmap vs `pread()` for fetch-path segment reads.
> - New `tests/byte-opacity/` directory in the project layout (real tests, deferred to Phase 3 storage work).

Legend: ✅ done · 🟡 partial / minor deviation · ❌ not yet · ➕ extra (not in plan)

---

## 1. Go module

| Plan | Repo |
|---|---|
| `go mod init github.com/yourorg/skafka` | `module github.com/woestebanaan/skafka` (`go.mod:1`) |
| Go 1.22+ | Go 1.26.1 (`go.mod:3`) |

✅ Module exists. Org name is `woestebanaan`, not the placeholder `yourorg`.

---

## 2. CRD scaffolding

Plan listed six CRDs. All present under `operator/api/v1alpha1/`:

| CRD | Type file | Rendered manifest |
|---|---|---|
| KafkaCluster | `kafkacluster_types.go` | `deploy/crds/skafka.io_kafkaclusters.yaml` |
| KafkaTopic | `kafkatopic_types.go` | `deploy/crds/skafka.io_kafkatopics.yaml` |
| KafkaUser | `kafkauser_types.go` | `deploy/crds/skafka.io_kafkausers.yaml` |
| KafkaUserGroup | `kafkausergroup_types.go` | `deploy/crds/skafka.io_kafkausergroups.yaml` |
| KafkaAcl | `kafkaacl_types.go` | `deploy/crds/skafka.io_kafkaacls.yaml` |
| KafkaClusterAssignments | `kafkaclusterassignments_types.go` | `deploy/crds/skafka.io_kafkaclusterassignments.yaml` |

`KafkaClusterAssignments` is correctly modeled as the v3.2 debug-only mirror: empty `Spec`, all state in `Status`, `Truncated bool`, brokers/partitions/consumerGroups (`kafkaclusterassignments_types.go:23-58`). Generated `zz_generated.deepcopy.go` and per-type deepcopy files are in place.

✅ All six CRDs.

---

## 3. Core interfaces

Plan defined five contracts. Mapping:

### `StorageEngine` — 🟡 signature gap under v3.3
`internal/storage/engine.go:26-50`. Repo has `Append(ctx, topic, partition int32, rawBatch []byte) (baseOffset, error)`. v3.3 mandates `Append(ctx, topic, partition int32, epoch uint32, batchBytes []byte)` — the byte-opacity half is already satisfied, but **the `epoch uint32` parameter is missing on Append**. Under v3.2 (where the plan still showed `[]Record`) this was a reasonable deviation; under v3.3 the plan has converged on bytes and explicitly added per-Append epoch fencing, so this is now a real gap to close. `TakeOver(ctx, topic, partition, epoch uint32)` and `Relinquish` are present alongside the v2.6 `TakeoverPartition`/`RelinquishPartition`.

### `AssignmentStore` — ✅ matches plan
`pkg/kafkaapi/assignment.go:75-88`. Read / Write / Watch surface as specified.

### `Controller` — ✅ matches plan (with epoch on Start)
`pkg/kafkaapi/controller.go:42-59`. `Start(ctx, epoch int64)` takes the `leaseTransitions` value as required by the takeover/fence design (plan Phase 4 §"Controller election" mandates this; Phase 1's listing simply omitted it).

### `BrokerCoordinator` — ✅ matches plan
`pkg/kafkaapi/controller.go:65-91`. `Owns`, `CurrentEpoch`, `OnAssignmentChange`, `LastHeartbeat` all present.

### `AuthEngine` — ✅ already existed
`internal/auth/auth.go:48`. Pre-existing from v2.6; plan says "unchanged".

➕ Supporting types added: `Assignment`, `BrokerAssignment`, `PartitionAssignment`, `ConsumerGroupAssignment`, `BrokerHealth`, `PartitionRole`, `AssignmentChange`, `AssignmentChangeReason`, `AssignmentChangeHandler` (`pkg/kafkaapi/assignment.go`, `pkg/kafkaapi/controller.go`).

---

## 4. RBAC for broker ServiceAccount

`deploy/rbac/broker-clusterrole.yaml`:

| Plan | Status | Where |
|---|---|---|
| get/list/watch KafkaTopic, KafkaUser, KafkaUserGroup, KafkaAcl | ✅ | lines 20–22 |
| get/list/watch Secrets | ✅ | lines 34–36 |
| get/list/watch + create/update Lease (skafka-controller) | ✅ | lines 12–14 (verbs cover both v2.6 per-partition leases and v3 singleton) |
| get/list/watch + update KafkaClusterAssignments | ✅ | lines 27–32 (also covers `/status`) |
| get/patch own Pod (ReadinessGate) | ✅ | lines 38–40 (`pods/status` patch) |
| get EndpointSlices | ✅ | lines 16–18 |
| File system: read/write to /data PVC mount | n/a here | volume mount, not RBAC — belongs to Phase 8 Helm |

✅ Complete. `deploy/rbac/operator-clusterrole.yaml` separately gained `kafkaclusterassignments` create/delete (operator lifecycle).

---

## 5. CI

`.github/workflows/ci.yml` and `release.yml`:

| Plan | Status | Notes |
|---|---|---|
| `go vet` | ✅ | ci.yml |
| `golangci-lint` | 🟡 | step removed with comment: latest released `golangci-lint` is built with Go 1.24 and refuses go.mod ≥ 1.26. Re-enable when a Go 1.26 build ships. |
| `go test ./...` | ✅ | ci.yml |
| `make manifests` (CRD drift check) | ✅ | runs controller-gen + `git diff --exit-code -- deploy/crds/` |
| Multi-arch Docker images on tag | ✅ | `docker-publish.yml` / `release.yml` |
| Integration matrix: kind + csi-driver-nfs / Rook-Ceph | ❌ | no matrix job in CI yet |
| Controller-failover test job | ❌ | only the `tests/controller-failover/placeholder_test.go` `t.Skip` stub exists |
| Stale-controller-race test job (new in v3.2) | ❌ | only the `tests/stale-controller-race/placeholder_test.go` `t.Skip` stub exists |

The three ❌ items are deferred until Phase 4 has real implementations to exercise.

---

## Extras delivered in c03eb64 (beyond plan §Phase 1)

- ➕ `proto/heartbeat.proto` — `ControllerHeartbeat` bidi-stream schema with `BrokerStatus` / `ControllerCommand` (PING / LEAVING / ASSIGNMENT_CHANGED). `proto/buf.yaml`, `proto/buf.gen.yaml`, and `Makefile` targets `proto` / `proto-tools` are wired up; generated stubs deferred to Phase 4.
- ➕ `pkg/heartbeatpb/doc.go` — empty package so `go build ./...` is happy before stubs are generated.
- ➕ `cmd/skafka-fsync-check/` and `cmd/skafka-failover-probe/` — `os.Exit(0)` stubs that print "not yet implemented (Phase 3/4)". Lets Dockerfiles and CI matrices reference these binaries unconditionally.
- ➕ `tests/controller-failover/`, `tests/stale-controller-race/` — `t.Skip` placeholders so `go test ./...` exits clean.
- ➕ `internal/broker/stubs.go` — `MemoryStorage`, `LocalLeaseManager`, `LocalPartitionLock`, `AllowAllAuthEngine`, `DenyAllAuthEngine` + compile-time interface assertions (`var _ storage.StorageEngine = ...`).

---

## Phase 1 Open Questions (plan §lines 1445–1484)

| # | Question | Owner in repo | State |
|---|---|---|---|
| 1 | fsync durability across RWX providers | `cmd/skafka-fsync-check` | stub only — Phase 3 |
| 2 | Heartbeat timeout calibration | `cmd/skafka-failover-probe` | stub only — Phase 4 |
| 3 | NFS mtime resolution / `acregmax=1` | docs in Helm chart README | not yet |
| 4 | Controller balancer algorithm (strict-stability v1) | `internal/controller/balancer.go` | dir doesn't exist — Phase 4 |
| 5 | `__consumer_offsets` retention model | `internal/coordinator/` | v2.6 model in place |
| 6 | Operator PVC access | `operator/` + Helm | docs not yet |
| 7 | Min API version range (Kafka 2.6+) | `internal/protocol/handlers/api_versions.go` | already enforced from v2.6 |
| 8 | DNS strategy for per-broker hostnames | Helm chart | Phase 8/9 |
| 9 | Cert strategy for per-broker hostnames | Helm chart | Phase 8/9 |
| 10 | Idempotent producer accept-without-dedup | `internal/protocol/handlers/produce.go` | already in place |
| 11 | Dedicated controller mode | — | deferred to v4 |
| 12 | **mmap vs `pread()` for fetch segment reads** (NEW in v3.3) | `internal/storage/segment.go` | research item — verify per-RWX-provider during Phase 3 |

---

## v3.3 byte-opacity audit (new in this revision)

v3.3 elevated byte-opacity from a v1.5 optimization to a Phase 1 architectural constraint. Status of the existing v2.6 codepaths against the new constraints (plan §"Critical Constraints" 21–24):

| Constraint | Repo status |
|---|---|
| 21. RecordBatch payloads opaque end-to-end; on-disk == wire format | ✅ already true: `internal/storage/engine.go:25` and `:227` operate on `rawBatch []byte`; `internal/protocol/handlers/produce.go:126` only parses the 61-byte header |
| 22. **No decoded `RecordBatch` struct with `[]Record` field anywhere in the codec** | ❌ `internal/protocol/codec/types.go:50-77` defines `Record`, `RecordBatch{... Records []Record}`, `EncodeRecordBatch`, `DecodeRecordBatch`. Only called from `types_test.go` — **but the constraint forbids the *type* existing, not just hot-path usage**. Phase 1 cleanup item under v3.3. |
| 23. Produce hot path: per-batch allocations, never per-record | ✅ produce handler does not iterate records |
| 24. Fetch path passes segment bytes directly into response framing, no re-encode, no re-CRC | ✅ confirmed — `engine.go:298` returns `[]byte`, handlers wrap it |

The plan also says the v3.3 project layout has `tests/byte-opacity/` (line 581). Not present in repo. Per local convention (placeholder `t.Skip` packages for Phase 4 CI hooks), an empty placeholder would let `go test ./...` reference it; but the byte-opacity tests are real (round-trip + CPU-profile assertions + tripwire metrics) and depend on Phase 3 storage being real, so deferring the directory is reasonable.

---

## Action items introduced by v3.3

1. **Add `epoch uint32` to `StorageEngine.Append`** (and propagate to `MemoryStorage.Append` and `DiskStorageEngine.Append` and all callers in `internal/protocol/handlers/produce.go`). Append-time epoch check is what fences out a stale leader on the very next batch when self-fencing alone would take up to `heartbeatTimeout` to react.
2. **Remove `RecordBatch`/`Record` decoded structs** from `internal/protocol/codec/types.go` (lines 50-77) along with `EncodeRecordBatch` / `DecodeRecordBatch`. Migrate `types_test.go` to either header-only assertions or hand-built byte fixtures. Add the tripwire metrics `skafka_codec_record_decode_total` and `skafka_codec_batch_reencode_total` (plan line 1352-1354) — but only if a useful place to increment them remains; if the symbols are gone, the metrics flat-line at zero by construction, which is the desired property.
3. **Add `tests/byte-opacity/` placeholder** (optional in Phase 1; required by Phase 3).
4. **Document open question #12** (mmap vs `pread()`) — no code change in Phase 1, but the question now needs an owner and a per-provider answer ahead of Phase 3.

---

## Summary

Phase 1 is **substantively complete as a foundation pass**. Module, CRD set, RBAC, and the new contracts (`AssignmentStore`, `Controller`, `BrokerCoordinator`, plus the `Assignment` struct) are landed.

Under the v3.2 plan I had two reasonable deviations to flag. v3.3 closes one of them and opens two new gaps:

1. ❌ `StorageEngine.Append` is now byte-batch *with* per-Append `epoch uint32`. Repo has the bytes but not the epoch parameter. Real gap.
2. ❌ Constraint #22: codec must not contain a decoded `RecordBatch` / `[]Record` type. Repo still does, in `codec/types.go`. Real gap (even if test-only today).
3. ✅ `Controller.Start(ctx, epoch int64)` was a v3.2 deviation; v3.3 doesn't change it.

The deferred items — golangci-lint re-enable, RWX-provider integration matrix, controller-failover and stale-controller-race CI jobs — still wait on Phase 3/4 implementations.

Recommended ordering: close the two byte-opacity gaps before moving to Phase 4, since both touch the storage and codec interfaces that Phase 4's broker-coordinator and self-fencing code will build against.
