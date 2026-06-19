# Phase 0 — Bootstrap

Detailed work plan for the first phase of the Rust rewrite. Companion to
[`rewrite.md`](./rewrite.md); the high-level summary lives there.

**Goal.** Land a green CI on `main` where a Rust workspace at the repo root
builds cleanly alongside the frozen Go tree under `archive/`. No production
behaviour ships in this phase — every Rust crate is an empty `lib.rs`. What
ships is the **scaffolding** every later phase will lean on: build, test,
lint, generate, package, release.

**Length.** ~1 week, single engineer.

**Out of scope for Phase 0.** Wire codec (Phase 1), storage (Phase 2),
server bring-up (Phase 3). Don't write production logic in this phase even
if it feels obvious — the gate is "scaffolding is correct and CI is fast",
not "we've started porting".

---

## Workstreams

Eight workstreams, executable mostly in parallel after the workspace
skeleton (A) lands:

- **A** — Workspace skeleton
- **B** — Toolchain & lints
- **C** — Dependency manifest
- **D** — gRPC stubs via `tonic-build`
- **E** — `xtask` runner
- **F** — Dockerfiles
- **G** — CI rewrite (with `legacy-go` job)
- **H** — Docs + exit criteria

Dependencies: A blocks everything; B and C can land together; D, E, F can
land in any order once A is in; G lands last and gates the merge.

---

## A — Workspace skeleton

Create at the repo root:

```
Cargo.toml                       # [workspace]
rust-toolchain.toml
rustfmt.toml
clippy.toml
.cargo/config.toml               # workspace-wide build flags
crates/
  sk-codec/Cargo.toml
  sk-codec/src/lib.rs
  sk-protocol/Cargo.toml
  sk-protocol/src/lib.rs
  sk-storage/Cargo.toml
  sk-storage/src/lib.rs
  sk-coordinator/Cargo.toml
  sk-coordinator/src/lib.rs
  sk-broker/Cargo.toml
  sk-broker/src/lib.rs
  sk-broker/build.rs              # tonic-build entry (see D)
  sk-controller/Cargo.toml
  sk-controller/src/lib.rs
  sk-auth/Cargo.toml
  sk-auth/src/lib.rs
  sk-k8s/Cargo.toml
  sk-k8s/src/lib.rs
  sk-observability/Cargo.toml
  sk-observability/src/lib.rs
  sk-operator-api/Cargo.toml
  sk-operator-api/src/lib.rs
  sk-operator-controllers/Cargo.toml
  sk-operator-controllers/src/lib.rs
  sk-test-harness/Cargo.toml
  sk-test-harness/src/lib.rs
bins/
  skafka/Cargo.toml
  skafka/src/main.rs              # fn main() { println!("skafka stub"); }
  skafka/Dockerfile               # see F
  skafka-operator/Cargo.toml
  skafka-operator/src/main.rs
  skafka-operator/Dockerfile
xtask/
  Cargo.toml
  src/main.rs                     # see E
```

Root `Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
    "crates/*",
    "bins/*",
    "xtask",
]

[workspace.package]
edition = "2021"
license = "Apache-2.0"
repository = "https://github.com/Woestebanaan/skafka"
rust-version = "1.85"

# [workspace.dependencies] populated in workstream C.

[workspace.lints.clippy]
unwrap_used        = "deny"
expect_used        = "deny"
panic              = "deny"
as_conversions     = "deny"
cast_possible_truncation = "deny"
cast_sign_loss     = "deny"

[workspace.lints.rust]
unsafe_code        = "forbid"   # one crate (sk-storage) overrides this with allow + a comment
missing_debug_implementations = "warn"
```

Each crate's `Cargo.toml` minimally:

```toml
[package]
name = "sk-codec"
version = "0.0.0"
edition.workspace      = true
license.workspace      = true
repository.workspace   = true
rust-version.workspace = true
publish = false

[lints]
workspace = true
```

Each crate's initial `src/lib.rs`:

```rust
//! sk-codec — Kafka wire frames, primitives, CRC32C, KIP-482 tagged fields.
//!
//! Populated in Phase 1 of the rewrite. See `phase-0.md` for scaffolding rules.
```

`.gitignore` additions:

```
/target/
**/*.rs.bk
.cargo/.package-cache
```

**Exit:** `cargo build --workspace` and `cargo test --workspace` both pass.
Tree under `archive/` still untouched.

---

## B — Toolchain & lints

`rust-toolchain.toml`:

```toml
[toolchain]
channel    = "1.85.0"
components = ["rustfmt", "clippy", "rust-src"]
profile    = "minimal"
```

(Pin to whatever is current-stable at start; do NOT track `stable` —
reproducible CI is worth the bump cadence. 1.85 is the floor because
`getrandom ≥ 0.4` requires Rust edition 2024.)

`rustfmt.toml`:

```toml
edition                  = "2021"
max_width                = 100
reorder_imports          = true
use_field_init_shorthand = true
# imports_granularity and group_imports are nightly-only as of 1.85 — keep
# disabled until they stabilize.
```

`clippy.toml`:

```toml
msrv             = "1.85.0"
allow-unwrap-in-tests = true
allow-expect-in-tests = true
```

`.cargo/config.toml`:

```toml
[build]
rustflags = ["-D", "warnings"]   # warnings are errors in this workspace

[target.x86_64-unknown-linux-gnu]
linker = "clang"                  # faster link; harmless on CI runners that have it
```

**Exit:** `cargo clippy --workspace --all-targets -- -D warnings` passes.
`cargo fmt --check` passes.

---

## C — Dependency manifest

Populate `[workspace.dependencies]` in the root `Cargo.toml`. Pin minor
versions; resolve `Cargo.lock` and commit it (binary workspace).

```toml
[workspace.dependencies]
# async + I/O
tokio          = { version = "1.43", features = ["full"] }
tokio-rustls   = "0.26"
tokio-util     = { version = "0.7", features = ["io", "codec"] }
bytes          = "1.9"
futures        = "0.3"
async-trait    = "0.1"
parking_lot    = "0.12"
dashmap        = "6.1"
arc-swap       = "1.7"
notify         = "7.0"

# wire / serde / encoding
serde          = { version = "1", features = ["derive"] }
serde_json     = "1"
prost          = "0.13"
tonic          = "0.12"
prost-build    = "0.13"
tonic-build    = "0.12"
crc32c         = "0.6"
uuid           = { version = "1.11", features = ["v4", "serde"] }
base64         = "0.22"
hmac           = "0.12"
sha2           = "0.10"
regex          = "1.11"
bytestring     = "1.4"

# error / tracing / metrics
thiserror      = "2.0"
anyhow         = "1"
tracing        = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
tracing-opentelemetry = "0.28"
opentelemetry  = "0.27"
opentelemetry-otlp = { version = "0.27", features = ["grpc-tonic"] }
opentelemetry_sdk = { version = "0.27", features = ["rt-tokio"] }

# kubernetes
kube           = { version = "0.96", features = ["client", "derive", "runtime", "rustls-tls"] }
k8s-openapi    = { version = "0.23", features = ["latest"] }
schemars       = "0.8"

# TLS
rustls         = { version = "0.23", default-features = false, features = ["ring"] }
rcgen          = "0.13"

# axum (healthz, metrics endpoints)
axum           = "0.7"

# dev-deps shared across crates
proptest       = "1.6"
insta          = { version = "1.41", features = ["json", "yaml"] }
tokio-test     = "0.4"
tempfile       = "3.14"
wiremock       = "0.6"
```

Each crate consumes via:

```toml
[dependencies]
tokio = { workspace = true }
bytes = { workspace = true }
```

**Exit:** `cargo update --dry-run` is a no-op; `cargo deny check`
(see workstream G) passes; lockfile is committed.

---

## D — gRPC stubs via tonic-build

The existing `proto/buf.gen.yaml` generates Go stubs into
`../pkg/heartbeatpb` (now `archive/pkg/heartbeatpb`). The Go tree is frozen,
so we do **not** regenerate Go stubs from Rust CI. The buf config stays
exactly where it is — usable manually from inside `archive/` if a port-fix
ever needs to re-run it.

For Rust, generate at compile time via `tonic-build` inside `sk-broker`.

`crates/sk-broker/build.rs`:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Vendored protoc means CI / contributors never apt-install protobuf-compiler.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../../proto/heartbeat.proto"], &["../../proto"])?;
    println!("cargo:rerun-if-changed=../../proto/heartbeat.proto");
    Ok(())
}
```

`crates/sk-broker/Cargo.toml`:

```toml
[dependencies]
tonic = { workspace = true }
prost = { workspace = true }

[build-dependencies]
tonic-build         = { workspace = true }
protoc-bin-vendored = { workspace = true }
```

`crates/sk-broker/src/lib.rs`:

```rust
pub mod heartbeatpb {
    // tonic emits `as i32` and similar patterns that trip the workspace
    // clippy gate. Generated code is not subject to hand-written style rules.
    #![allow(
        clippy::all, clippy::pedantic, clippy::nursery,
        clippy::as_conversions, clippy::cast_possible_truncation, clippy::cast_sign_loss,
        missing_debug_implementations
    )]
    tonic::include_proto!("skafka.heartbeat.v1");
}
```

Rust CI runs `cargo build --workspace`, which transitively runs `build.rs`,
which generates and compiles the stubs into `target/`. Nothing is checked
in. If a developer needs to inspect the generated code, `cargo expand` or
`target/debug/build/sk-broker-*/out/skafka.heartbeat.v1.rs` is the answer.

**Decision: do NOT use buf for Rust.** Adding a Rust buf plugin is
double-toolchain (buf binary + buf modules) for zero gain — tonic-build is
the standard in the ecosystem, lives in Cargo, and runs on every cabal
runner without any extra install.

**Exit:** `cargo build -p sk-broker` produces a target dir whose generated
file contains `ControllerHeartbeat` service code; a unit test in
`sk-broker/tests/proto_smoke.rs` builds a `BrokerStatus` struct and
asserts a field is reachable.

---

## E — xtask runner

A single binary crate that wraps repo-wide chores so they're discoverable
via `cargo xtask <subcommand>` instead of a `Makefile`.

`xtask/Cargo.toml`:

```toml
[package]
name = "xtask"
version = "0.0.0"
edition.workspace = true
publish = false

[dependencies]
anyhow = { workspace = true }
```

`xtask/src/main.rs`:

```rust
use std::{env, process::Command};

fn main() -> anyhow::Result<()> {
    let task = env::args().nth(1).unwrap_or_default();
    match task.as_str() {
        "gen-proto"     => gen_proto(),
        "gen-crds"      => gen_crds(),
        "check-crd-drift" => check_crd_drift(),
        "fmt-check"     => run(&["cargo", "fmt", "--check"]),
        "ci"            => ci(),
        other => Err(anyhow::anyhow!("unknown xtask: {other}")),
    }
}

fn gen_proto() -> anyhow::Result<()> {
    // tonic-build runs inside sk-broker's build.rs; this just forces a rebuild.
    run(&["cargo", "build", "-p", "sk-broker"])
}

fn gen_crds() -> anyhow::Result<()> {
    // Stub for Phase 0. Phase 7 replaces this with a schemars walker over
    // sk-operator-api that writes deploy/crds/*.yaml + mirrors into the chart.
    eprintln!("gen-crds: stub — implemented in Phase 7");
    Ok(())
}

fn check_crd_drift() -> anyhow::Result<()> {
    gen_crds()?;
    run(&["git", "diff", "--exit-code", "deploy/crds", "deploy/helm/skafka/crds"])
}

fn ci() -> anyhow::Result<()> {
    run(&["cargo", "fmt", "--check"])?;
    run(&["cargo", "clippy", "--workspace", "--all-targets", "--", "-D", "warnings"])?;
    run(&["cargo", "test", "--workspace"])?;
    run(&["cargo", "build", "--release", "--workspace", "--bins"])?;
    Ok(())
}

fn run(argv: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(argv[0]).args(&argv[1..]).status()?;
    if !status.success() { anyhow::bail!("{:?} failed: {status}", argv); }
    Ok(())
}
```

Add a `[alias]` in `.cargo/config.toml`:

```toml
[alias]
xtask = "run --quiet --package xtask --"
```

So `cargo xtask gen-proto` works without typing `--package xtask --`.

**Exit:** `cargo xtask ci` runs locally end-to-end and exits zero.

---

## F — Dockerfiles

Two slim multistage builds. Match the Go Dockerfile patterns under
`archive/` (Debian-slim runtime, non-root user) so the chart's
`image.repository` swap in Phase 9 is a one-line change with no Pod
spec churn.

`bins/skafka/Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1.7
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin skafka && \
    cp target/release/skafka /skafka

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 skafka
COPY --from=build /skafka /usr/local/bin/skafka
USER 10001
EXPOSE 9092 9094 8080
ENTRYPOINT ["/usr/local/bin/skafka"]
```

`bins/skafka-operator/Dockerfile` — identical shape, swaps `--bin
skafka-operator`. No port exposure.

Root `.dockerignore`:

```
/target
/archive/dist
/archive/bin
.git
.github
*.md
```

(Don't exclude `archive/`: even though we don't compile it, excluding it
from the build context would make any future `archive/<thing>` lookup from
Phase 9 misbehave. Excluding `archive/dist` and `archive/bin` keeps build
output out without hiding source.)

**Exit:** `docker buildx build -f bins/skafka/Dockerfile .` succeeds and
the resulting image runs the stub binary.

---

## G — CI rewrite

Replace `.github/workflows/ci.yml` with a workflow that runs both the new
Rust pipeline and a `legacy-go` job against `archive/`. Keep
`docker-publish.yml` as a separate workflow but update its build paths to
`archive/Dockerfile*`.

`.github/workflows/ci.yml` (Phase 0 shape):

```yaml
name: ci
on:
  pull_request:
  push:
    branches: [main]

jobs:
  rust:
    runs-on: arc-runner-set
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --check
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace --all-features
      - run: cargo build --release --workspace --bins

  legacy-go:
    runs-on: arc-runner-set
    defaults:
      run:
        working-directory: archive
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: archive/go.mod
          cache: true
          cache-dependency-path: archive/go.sum
      - run: go vet ./...
      - run: go test ./...

  helm:
    runs-on: arc-runner-set
    steps:
      - uses: actions/checkout@v4
      - uses: azure/setup-helm@v4
      - run: helm lint deploy/helm/skafka
      - run: helm template deploy/helm/skafka > /dev/null

  crd-drift:
    runs-on: arc-runner-set
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo xtask check-crd-drift   # Phase 0: stub, no-op; Phase 7: real
```

Update `.github/workflows/docker-publish.yml` (Phase 0 shape — paths only):

```yaml
# ... existing on/permissions/env blocks ...
jobs:
  publish:
    runs-on: arc-runner-set
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        # ... existing GHCR login ...

      - name: Build & push broker (go, from archive/)
        uses: docker/build-push-action@v5
        with:
          context: archive
          file: archive/Dockerfile
          push: true
          tags: ghcr.io/woestebanaan/skafka-preview:${{ env.VERSION }}

      - name: Build & push operator (go, from archive/)
        uses: docker/build-push-action@v5
        with:
          context: archive
          file: archive/Dockerfile.operator
          push: true
          tags: ghcr.io/woestebanaan/skafka-operator-preview:${{ env.VERSION }}

      # Rust image stanzas remain commented-out until Phase 9 flips the default flavor.
      # - name: Build & push broker (rust)
      #   uses: docker/build-push-action@v5
      #   with:
      #     context: .
      #     file: bins/skafka/Dockerfile
      #     push: true
      #     tags: ghcr.io/woestebanaan/skafka-rs-preview:${{ env.VERSION }}
```

Add a `cargo-deny.toml` at root and a `deny` job (optional, but worth it
from day one):

```toml
[advisories]
yanked = "deny"
ignore = []

[licenses]
allow = ["Apache-2.0", "MIT", "BSD-3-Clause", "ISC", "Unicode-DFS-2016"]
confidence-threshold = 0.93

[bans]
multiple-versions = "warn"

[sources]
unknown-registry = "deny"
unknown-git      = "deny"
```

**Exit:** PR with this workflow file is green on a clean checkout. The
`legacy-go` job builds and tests `archive/` without any path tweaks beyond
the `defaults.run.working-directory` override.

---

## H — Docs + exit criteria

### Root `README.md`

Either create one (does not currently exist) or leave for later. Recommend
a 10-line file pointing at `rewrite.md`, `phase-0.md`, and `CLAUDE.md`.

### Per-crate `README.md`

One sentence per crate, lifted from the `lib.rs` doc-comment.

### Update `CLAUDE.md` "Common commands"

Once Phase 0 lands, append:

```bash
cargo build --workspace        # build the Rust workspace
cargo test --workspace         # run Rust unit tests
cargo xtask ci                 # full local CI (fmt/clippy/test/build)
cargo xtask gen-proto          # regenerate gRPC stubs (tonic-build)
cargo xtask gen-crds           # stub until Phase 7
```

(Don't remove the `cd archive` block — both pipelines coexist until
Phase 9.)

### Memory note

After Phase 0 merges, save a feedback memory: "Phase 0 chose tonic-build
over buf for Rust gRPC stubs. Reason: avoids dual-toolchain install on CI;
buf stays for the Go tree under archive/." So a later me doesn't second-
guess the choice.

---

## Phase 0 exit criteria (all must hold)

1. `cargo build --workspace` succeeds on a clean clone in < 90 s (cold) /
   < 5 s (warm).
2. `cargo test --workspace` runs and reports 0 tests except the
   `sk-broker` proto-smoke test.
3. `cargo clippy --workspace --all-targets -- -D warnings` passes.
4. `cargo fmt --check` passes.
5. `cargo xtask ci` runs all four steps green.
6. `cd archive && go vet ./... && go test ./...` still passes.
7. `helm lint deploy/helm/skafka` passes.
8. `docker buildx build -f bins/skafka/Dockerfile .` produces an image
   that runs the stub binary and exits 0.
9. `docker buildx build -f archive/Dockerfile archive` still produces a
   working Go broker image.
10. CI `ci` workflow on a clean PR is green and finishes within 4 minutes
    for the empty workspace.
11. The Helm chart, CRDs, scripts, and `proto/heartbeat.proto` are
    bit-identical to their pre-Phase-0 contents.

If any of these fail, do not merge Phase 0 — fix and re-run.

---

## Risks & mitigations

- **MSRV drift between CI and `rust-toolchain.toml`.** Pin the toolchain
  file; CI uses `dtolnay/rust-toolchain@stable` with `with: toolchain:`
  unspecified so it honours the toolchain file. Verified via
  `rustc --version` in the first CI step.
- **Linker missing on the runner.** `.cargo/config.toml` sets `linker =
  "clang"`; if the runner doesn't have clang installed, the workflow
  fails fast. Mitigation: remove the linker override before merge if the
  in-cluster ARC runners don't ship clang; revisit when we have a
  measurable build-time gap.
- **`Swatinem/rust-cache` ineffective on first PR.** Cold-build budget
  baked into exit criterion #1 already accounts for an empty cache.
- **`tonic-build` requires `protoc` on the runner.** Modern `tonic-build`
  vendors `protoc` via the `protobuf-src` crate; if not, install via
  `actions/setup-protoc` in the `rust` job. Decide based on the
  shipped tonic version at Phase 0 start.

---

## What this enables for Phase 1

After Phase 0 merges, Phase 1 lands by:

1. Writing real code into `crates/sk-codec/src/*.rs`.
2. Adding fixture files under `crates/sk-codec/tests/fixtures/`.
3. Adding a new CI job for the codec proptest sweep if it grows long
   enough to warrant a separate runner.

No further toolchain, lint, or workflow changes — Phase 1 is pure
implementation against a stable harness.
