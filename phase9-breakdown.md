# Phase 9 Breakdown — External Access via Per-Broker TLS Passthrough

> Plan reference: `skafka-plan-v3.md` lines 1095–1107.
>
> v3.3 keeps Phase 9 unchanged from v3.0/v3.1/v3.2: per-broker hostnames,
> Gateway API TLSRoute SNI passthrough, cert-manager Certificate, no
> custom router, no leader forwarding. The v3.3-only note is that kTLS
> is deferred to v1.5 — TLS encryption stays in user space for v1, which
> means no `sendfile(2)` through the TLS socket. Otherwise the design
> is the v3.0 design.

## What Phase 9 actually means

The internal listener (port 9092, plaintext) is the in-cluster
fast path; clients there reach brokers via the headless service's per-pod
DNS (`skafka-N.skafka-headless...`). Phase 9 is the *external* listener:
clients outside the cluster need to talk to the leader of each partition,
and to do that they need:

1. A direct, addressable hostname per broker pod.
2. End-to-end TLS to that pod (no Gateway-terminated TLS — the proxy
   layer has to be SNI passthrough or it'd need to be Kafka-aware).
3. A Metadata response that hands the client those per-broker hostnames
   when the request arrives over the TLS listener, *not* the in-cluster
   ones (otherwise NOT_LEADER retry would point at the wrong target).
4. Fallback semantics when a request lands on the wrong broker:
   `NOT_LEADER_FOR_PARTITION`. Standard Kafka clients then retry at
   the right per-broker hostname they got from Metadata.

Once those four pieces are in place, the external listener is a
straight passthrough: the broker pod runs `crypto/tls`, the Gateway
proxies bytes, and the client gets full Kafka semantics over TLS.

## Plan vs. current state

| Plan element | Where it lives today | Status |
|---|---|---|
| Per-broker `Service` (ClusterIP, pod-name selector) | `deploy/helm/skafka/templates/listener-services.yaml` + `operator/controllers/kafkacluster_controller.go` | ✅ — duplicated, see Gap #1 |
| TLSRoute per broker (Gateway API SNI passthrough) | `deploy/helm/skafka/templates/listener-tlsroutes.yaml` + same operator | ✅ — duplicated, see Gap #1 |
| cert-manager `Certificate` with per-broker SANs | `deploy/helm/skafka/templates/listener-certificate.yaml` + same operator | ✅ — duplicated, see Gap #1 |
| TLS listener on broker (port 9093) | `cmd/skafka/main.go:319-340` (`SKAFKA_TLS_CERT_FILE`/`KEY_FILE`/`PORT`) | ✅ |
| Hot-reload on cert rotation | `internal/protocol/tls.go` (fsnotify + `WatchingCertificate`) | ✅ + unit test `TestWatchingCertificateReload` |
| Metadata returns per-broker external hostnames | `internal/protocol/handlers/metadata.go:25-30` (`addressFor`) | ✅ + `TestMetadataHandlerExternal*` |
| Listener-tagged conn state drives Metadata routing | `internal/protocol/server.go:127-133` + `internal/connstate/connstate.go` | ✅ |
| `K8sBrokerSource.ExtHostPattern`/`ExtPort` populated from env | `cmd/skafka/main.go:103-110` | ✅ |
| `NOT_LEADER_FOR_PARTITION` returned when broker isn't leader | `internal/protocol/handlers/produce.go:105`, `fetch.go:76`, `list_offsets.go:36` | ✅ — wiring exists |
| mTLS (opt-in client cert verification) | `internal/protocol/tls.go` `WithRequireClientCert` + Helm `clientCA.*` (Phase 7/8) | ✅ |
| TLS handshake / cert-reload metrics | `internal/observability/metrics.go` (`TLSHandshakes`, `CertReloads`) | ✅ |
| `bootstrapHostname` SAN | values.yaml + chart Certificate; operator reconciler too | ✅ |
| Integration test: external listener round-trip via per-broker SNI | — | ❌ Gap #2 |
| Integration test: NOT_LEADER redirect end-to-end | `tests/kafka-compat/` only covers the happy path; produce path's NOT_LEADER is unit-tested but never exercised through the actual cluster runtime | ❌ Gap #3 |
| Cert hot-reload exercised under live TLS handshake | unit test only — no test that reloads a cert and verifies the *next* TLS handshake picks it up | ⚠️ Gap #4 (low priority) |
| Decision: chart vs operator owns external resources | both create them — see Gap #1 | ❌ Gap #1 |

## Gaps to close

### Gap #1: Resolve chart/operator duplication of external resources (P0) — DONE

> **Status:** shipped. Chart now templates a single `KafkaCluster` CR
> (`templates/kafkacluster.yaml`) and the three duplicate
> `templates/listener-{services,tlsroutes,certificate}.yaml` are
> deleted. The operator's `KafkaClusterReconciler` materializes the
> Certificate, per-broker Services, TLSRoutes, and the
> `KafkaClusterAssignments` mirror from the CR.
>
> **Known follow-up (not blocking Gap #2):** the operator's
> `kafkaClusterFinalizer` cleanup path runs `deleteExternalResources`
> before removing the finalizer, but `helm uninstall` deletes the
> operator Deployment in parallel with the `KafkaCluster` CR. If the
> operator pod terminates before the finalizer fires, the CR gets
> stuck in `Terminating`. Workaround documented in the chart README
> (`kubectl delete kafkacluster/<name>` first, then `helm uninstall`).
> Permanent fix: set ownerReferences on the operator-created
> Certificate + TLSRoute so k8s cascades on `KafkaCluster` deletion
> (the per-broker Service already has them).



The chart's three `listener-*.yaml` templates and the operator's
`KafkaClusterReconciler` both create the **same** Certificate +
per-broker Service + per-broker TLSRoute. Today this works because:

- Fresh `helm install` ships the resources via the chart, with Helm's
  ownership labels. The operator running afterwards calls
  `controllerutil.CreateOrUpdate` and tries to overwrite — but because
  the chart's Service is plain (no `KafkaCluster` ownerReference), the
  operator may either silently take ownership or hit a label/annotation
  fight on every reconcile.

This is a Phase 9 ownership question that has to be answered before the
external listener is "production ready":

**Option A — chart only, drop the operator reconciler.**
Simplest. The chart already templates everything; the
`KafkaClusterReconciler` becomes dead code. This abandons the v3.0/v3.1
design that envisioned a `KafkaCluster` CR as the surface a *user*
edits at runtime (e.g. to add a hostname or change replicas without
re-running `helm upgrade`).

**Option B — operator only, gut the chart's listener-*.yaml templates.**
The chart instead creates a `KafkaCluster` CR (`templates/kafkacluster.yaml`),
and the operator reconciles all external resources from it. Day-2
hostname / cert / replica changes happen via `kubectl edit
kafkacluster/skafka` — the v3.0/v3.1 intended UX. The chart still has
to pre-stage CRDs (already does).

**Recommendation: Option B.** The CRD already exists, the operator
reconciler already has tests, and the operator scenario is the one the
plan's resource table calls out (line 511–515). Dropping the chart's
three `listener-*.yaml` templates and adding a single
`kafkacluster.yaml` template is a small diff, removes the duplication,
and makes the external listener config day-2 editable without
`helm upgrade`.

**Scope**:
- Add `deploy/helm/skafka/templates/kafkacluster.yaml` that materializes
  a `KafkaCluster` CR from `values.yaml` (replicaCount, listeners.\*,
  storage.className, image refs, controllerLease.\*).
- Delete `listener-services.yaml`, `listener-tlsroutes.yaml`,
  `listener-certificate.yaml`.
- Verify `helm template` shows the CR; the existing operator test
  proves the reconciler then materializes the three resource types.
- Update `phase8-breakdown.md` (or a NOTE in `phase9-breakdown.md`) to
  reflect the new ownership model.

**Edge cases** to think through before committing:
- Operator must be running before listener resources appear → chicken-and-egg
  on first install. The operator Deployment lands in the same chart,
  but Helm doesn't guarantee ordering. Solution: the operator chart
  should not block on these resources existing — it just won't be able
  to create them until the operator pod is Ready, which is fine
  because the broker StatefulSet doesn't depend on them either (the
  broker only mounts the Secret that cert-manager produces, and that
  Secret is created by cert-manager regardless of whether the
  Certificate was provisioned by Helm or the operator).
- The `helm uninstall` story: who deletes the externally-owned CR's
  child resources? If the `KafkaCluster` CR has a finalizer (`kafkaClusterFinalizer`
  is already defined on line 21 of `kafkacluster_controller.go`), the
  reconciler should clean up child resources on delete — verify the
  finalizer codepath actually does this.

### Gap #2: External listener integration test (P0)

There is no end-to-end test that drives a Kafka client through the TLS
listener with a per-broker hostname pattern and verifies:

1. Initial bootstrap connection succeeds via TLS to one broker.
2. Metadata response carries the *external* hostnames.
3. A subsequent Produce to a partition whose leader is on a different
   broker uses the external hostname for that broker (not the internal
   one).

**Scope**: Add `tests/kafka-compat/external_listener_test.go` that:
- Spins up two `*broker.Broker` instances (similar to `mtls_test.go`'s
  pattern), each with its own TLS listener using `tlscerts` testutil to
  generate certs whose SANs match `broker-0.localhost` /
  `broker-1.localhost`.
- Configures each broker with `EXTERNAL_HOSTNAME_PATTERN=broker-%d.localhost`
  and a TLS listener bound to `127.0.0.1:port`.
- Uses `franz-go` with `TLS()` and `SeedBrokers("broker-0.localhost:port")`,
  with `/etc/hosts`-style resolution stubbed via the franz-go `Dialer`
  hook (a custom `net.Dialer` that resolves `broker-N.localhost` to the
  real loopback port — same trick the mTLS test uses).
- Assertion 1: Metadata response brokers list contains the
  `broker-N.localhost` hostnames, not `127.0.0.1`.
- Assertion 2: produce a record to a topic whose leader is on broker-1,
  starting from broker-0 — observe the second connection lands on
  `broker-1.localhost:port` (verifiable via the dialer hook capturing
  the requested host).

**Why this matters**: this is the exact MVP checklist item *"Metadata
response carries per-broker hostnames"* and *"TLS passthrough + SNI
verified"* — it's the test that proves the chain end-to-end.

### Gap #3: NOT_LEADER_FOR_PARTITION end-to-end coverage (P1)

`internal/protocol/handlers/produce_test.go:200` asserts that the
*legacy lease* path returns the error when not leader, but there's no
test that exercises this through the v3 BrokerCoordinator path:
broker A receives a Produce for a partition whose `assignment.json`
says broker B owns it → broker A returns NOT_LEADER →
client (franz-go's stdlib retry) re-fetches Metadata and retries at
broker B's external hostname.

**Scope**: Either extend Gap #2's test to cover this redirect (preferred —
one test exercising both the Metadata path and the retry path), or add
`tests/integration/not_leader_redirect_test.go`. franz-go handles the
retry automatically; the test just has to assert that the Produce
eventually succeeds *and* that broker A logged a NOT_LEADER response.

### Gap #4: Cert hot-reload during live handshake (P2, low value)

`TestWatchingCertificateReload` confirms the in-process `tls.Config`
reload but does not exercise the full path: rotate the on-disk cert,
do a *new* TLS handshake, observe the new cert is presented. This is
mostly redundant with the unit test (the `tls.Config.GetCertificate`
hook is what `tls.Listen` calls per handshake) but a small integration
test would close the MVP checklist item *"cert-manager Certificate
rotates without pod restart"* without requiring cert-manager.

**Scope**: a `tests/kafka-compat/cert_rotation_test.go` that:
- Generates two cert pairs (cert-A, cert-B) under the same CA via
  `tlscerts`.
- Starts a broker with cert-A.
- Connects a TLS client, observes cert-A's serial number.
- Atomically renames cert-B onto disk in the broker's TLS dir.
- Waits for the watcher's reload (250–500ms is enough — there's a
  debounce in `watchLoop`).
- Connects a new TLS client, asserts cert-B's serial number.

**Defer to Phase 10 or skip** if the unit test is judged sufficient.

### Gap #5: Bootstrap hostname surfaced in NOTES.txt (P2, polish)

`values.yaml` exposes `listeners.external.bootstrapHostname` and the
chart Certificate adds it as a SAN, but `NOTES.txt` doesn't tell the
operator that it's the recommended client bootstrap. A one-line
addition: when `bootstrapHostname` is set, lead the bootstrap servers
list with it.

### Gap #6: DNS / cert strategy decision (P2, doc-only)

Plan questions 8 and 9 (lines 1465–1468) are open in the plan: wildcard
vs explicit per-broker DNS, wildcard vs SAN-per-broker certs. The
chart commits to *explicit per-broker SAN-per-broker* — document this
choice in `deploy/helm/skafka/README.md` with the rationale (wildcard
SANs require a DNS-01 challenge and a wildcard hostname pattern;
explicit SANs work with HTTP-01 and arbitrary hostnames; the cost is
re-issuing the cert when `replicaCount` grows, which is rare).

## Recommended ordering

1. **Gap #1** — resolve chart/operator duplication via Option B. Low risk,
   small diff, unblocks day-2 UX.
2. **Gap #2** — write the external listener integration test. This is
   the single most valuable Phase 9 deliverable; it's what makes the
   checklist items *"per-broker hostnames in Metadata"* and *"TLS
   passthrough + SNI verified"* truthy.
3. **Gap #3** — fold NOT_LEADER coverage into the same test, OR a
   sibling test if it makes the first too tangled.
4. **Gap #5** + **Gap #6** — small docs/NOTES nits, do together in one
   commit.
5. **Gap #4** — only if there's appetite. The unit test in
   `internal/protocol/tls_test.go` already exercises the reload logic;
   the live-handshake integration test is belt-and-braces.

## Out of scope for Phase 9

- **kTLS / `sendfile(2)` through the TLS socket** — explicitly deferred
  to v1.5 per plan line 1101–1107. The v1 design is correct (byte-opacity
  preserved up to the TLS socket); v1.5 just changes the encryption
  layer's location.
- **Multi-listener auth differentiation** — there's only one auth engine
  per broker; both listeners run the same SCRAM/mTLS/anonymous logic.
  Future "internal=plaintext-anonymous, external=SCRAM-required" splits
  would land later.
- **OAuth / OIDC over the external listener** — the plan keeps SASL/SCRAM
  + mTLS as the only external auth mechanisms.
- **External-listener throttling / quota enforcement** — Phase 10's
  `skafka_quota_throttle_total` is a metric scaffold; actual per-listener
  quota policy is post-v1.

## Acceptance criteria for "Phase 9 done"

- [ ] Chart uses `KafkaCluster` CR; `listener-{services,tlsroutes,certificate}.yaml`
      gone (Gap #1).
- [ ] `tests/kafka-compat/external_listener_test.go` passes end-to-end:
      bootstrap → external Metadata → cross-broker produce via
      per-broker hostnames (Gaps #2 + #3).
- [ ] `helm install` on a kind cluster (with cert-manager + Gateway
      API CRDs pre-installed) produces a working external endpoint
      that `kcat -b broker-0.kafka.example.com:9093 -X
      security.protocol=SSL ...` can hit. Manual smoke test, not CI.
- [ ] README documents the explicit-per-broker DNS/SAN choice (Gap #6).
- [ ] NOTES.txt prints the bootstrap hostname when set (Gap #5).

## What this leaves for Phase 10

Observability is the next phase: the metric scaffolds for
`skafka_external_connections_total{mode, broker_id}`,
`skafka_tls_handshakes_total{broker, result}`, and
`skafka_not_leader_returned_total{topic, partition}` exist as plan items
(lines 1183–1186) but the broker side doesn't emit them yet — and the
NOT_LEADER tripwire emerges naturally out of Gap #3's wiring. Phase 10
picks all of that up.
