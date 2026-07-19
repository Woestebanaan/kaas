# Book Phase 3 — Architecture Prose Port

Part of the [mdbook documentation plan](./book-plan.md) (§6, milestone 3).

- **Status**: **done** (2026-07-19). All ten Part I chapters carry the ported +
  fact-checked prose; ARCHITECTURE.md is a pointer stub; CLAUDE.md + README updated.
  Corrections applied during the sweep (beyond the 7 phase-2 divergences, all of which
  are now reflected in prose): **(8)** gh #105 TopicID wire propagation is *unshipped* —
  the production topic watch (`run_topic_watch`) inserts all-zero registry entries, so
  Metadata serves nil topic IDs; CLAUDE.md's "TopicID propagation" section rewritten,
  and KIP-516 must land as **partial** in the phase-4 KIP index (15/6/8, not 16/5/8).
  Also fixed: the stale `fetch.rs` docstring (claimed read-uncommitted-only), CLAUDE.md's
  committer-FD-clone, manifest-persist-sites, reaper-gating, and KafkaTopic-delete
  claims.
- **Depends on**: [Phase 2](./book-phase-2-diagrams.md) (diagrams already in place)
- **Delivers as**: one commit on `main`
- **Exit state**: Part I is the authoritative architecture doc; `docs/ARCHITECTURE.md` is a
  pointer stub; nothing else in the repo links to the old content by section.

## Goal

Port the remaining prose of `docs/ARCHITECTURE.md` (672 lines) into the ten Part I chapters,
merging it with the phase 2 diagrams. This is a *port with a fact-check sweep*, not a copy:
ARCHITECTURE.md predates several subsystem rewrites and at least one stale claim is already
known (the gh #114 marker RPC, fixed in phase 2's diagram — the surrounding prose needs the
same fix).

## Chapter mapping

| Part I chapter (plan §2) | Sources to port |
|---|---|
| System overview | "At a glance" + "Process topology" |
| Broker/operator runtime independence | "Process topology" (broker/operator/PVC subsections) + CLAUDE.md's "runtime-independent" section (the strongest statement of the invariant — the book becomes its canonical home) |
| Controller, leases & assignment.json | "Control plane" section |
| Storage engine hot path | "Data plane: Produce" + "Storage architecture" (group commit, segments, manifest lag semantics, `KAAS_FLUSH_INTERVAL_MESSAGES`) |
| File-handle ownership & takeover | "Single-FD ownership (gh #76)" + "Manifest + producer snapshot" + SIGTERM drain (gh #61/#139) |
| Consumer-group coordination | gh #92 hash routing, two-tier ownership, `GroupTakeoverDriver` orphan sweep |
| Transactions & idempotence | "Idempotent producer + transactions" + the txn state store / marker queue / fence log triad |
| Listeners, authentication, authorization | gh #124/#125/#126 material (per-listener auth vs cluster-wide authz, Metadata port advertisement, quota debt-carry) |
| Kubernetes integration | CRDs (4 types incl. the Strimzi-divergent quota field naming), reconcile-time cleanup (no finalizers — the ArgoCD deadlock story), broker RBAC (`update,patch` on kafkatopics), readiness gate |
| Observability | `kaas-observability`: OTLP push, `/healthz` runtime state, byte-opacity tripwires |

## Tasks

1. Port each section per the mapping, rewriting transitions so chapters stand alone (the book
   has navigation; chapters shouldn't rely on ARCHITECTURE.md's single-scroll reading order).
2. **Stale-claim sweep during the port** — verify every gh-issue reference, filename, and
   mechanism against the current tree as its section is ported. Known suspects: anything
   referencing wire RPCs between brokers (heartbeat gRPC is the *only* one; markers and fences
   go via PVC files), and any pre-gh #135 CRD names (`KafkaACL` / `KafkaUserGroup` are gone).
3. Shrink `docs/ARCHITECTURE.md` to a pointer stub (~10 lines: one-paragraph summary + link to
   the built book / `docs/src/` chapters). Per plan §8 the default decision is **stub**, not
   duplicate.
4. Update every reference to ARCHITECTURE.md:
   - `CLAUDE.md` ("System-level architecture in docs/ARCHITECTURE.md" → point at the book).
   - Root `README.md` (from phase 1).
   - Any crate `//!` doc comments that deep-link ARCHITECTURE.md sections
     (`grep -rn "ARCHITECTURE" crates bins`).
5. Keep the Part I chapter on non-goals aligned with plan §4's list (the full non-goals *page*
   is phase 4; Part I just needs the architectural trio: no KRaft, no replication/ISR, no
   literal `__transaction_state` topic).

## Out of scope

- The compatibility section (phase 4/5).
- Deleting ARCHITECTURE.md outright — it stays as a stub so inbound links and muscle memory
  don't 404.

## Verification

- [ ] `grep -rn "ARCHITECTURE.md" . --include="*.md" --include="*.rs"` — every hit is either
      the stub itself or intentionally points at it.
- [ ] Stale-claim sweep documented: commit message lists sections where prose was *corrected*
      (not just moved), so reviewers can spot-check.
- [ ] `cargo xtask docs` zero warnings; `mdbook-linkcheck` green (the port creates many new
      internal cross-links — this is the phase where linkcheck starts earning its keep).
- [ ] CLAUDE.md reference updated and still accurate.
