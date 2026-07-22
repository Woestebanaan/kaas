//! Cluster-unique, restart-safe producer-ID allocation (gh #219).
//!
//! # Why this exists
//!
//! Apache Kafka hands out producer IDs from a **global** counter whose
//! next block is persisted (ZooKeeper's `/latest_producer_id_block`,
//! KRaft's `ProducerIdsRecord`), so a PID is never reused — not after a
//! broker restart, not across brokers. kaas has no metadata quorum
//! (explicit non-goal), so before gh #219 `Broker::next_producer_id`
//! was a bare `AtomicI64` seeded at 1 on every boot. That reissued the
//! same PIDs after every restart, and every broker in the cluster
//! issued the *same* PIDs concurrently.
//!
//! Reissuing a PID is not cosmetic — the partition's idempotence state
//! (`producer-state.snapshot`, restored on `Partition::open`) is keyed
//! by `(pid, epoch)`. A fresh producer that draws a recycled `pid=5,
//! epoch=0` lands on the *previous* producer's dedupe window, so its
//! batches are classified against a sequence history that isn't its
//! own:
//!
//! - sequence range matches a cached batch → `Duplicate`: the records
//!   are silently dropped and a stale base offset is echoed (produce
//!   "succeeds", consumers read nothing);
//! - otherwise → `OUT_OF_ORDER_SEQUENCE_NUMBER` (wire error 45).
//!
//! Both symptoms are what gh #219 saw after a topic delete→recreate
//! cycle, but nothing about them is Streams-specific: any restart of a
//! broker with existing partition data reproduces it.
//!
//! # Layout
//!
//! The PID space is partitioned by broker ordinal instead of
//! coordinated through a quorum:
//!
//! ```text
//! pid = (broker_id + 1) * PID_STRIDE + local
//! ```
//!
//! Each broker is therefore the **single writer** of its own slice, and
//! of its own block file `/data/__cluster/producer_ids/kaas-<id>.json`
//! — no cross-broker read-modify-write on the shared volume (NFS rule
//! 3). `+ 1` keeps broker 0 clear of the low PIDs the pre-gh #219
//! counter handed out, so an upgrade can't collide with producer state
//! already on disk.
//!
//! `local` advances in blocks of [`PID_BLOCK`]: the block *end* is
//! persisted (tmp + fsync + rename, NFS rule 1) **before** any PID in
//! that block is handed out, so a crash can only ever skip PIDs
//! forward, never rewind them.

use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::warn;

use kaas_storage::atomic_write::atomic_write_json;
use kaas_storage::fs::{Fs, RealFs};

/// Sub-directory of `/data/__cluster/` holding the per-broker block
/// files. One file per broker; each broker only ever writes its own.
pub const PRODUCER_ID_DIR: &str = "producer_ids";

/// Size of the PID slice reserved to each broker. 2^40 PIDs per broker
/// leaves room for ~2^23 brokers inside a positive `i64` — both bounds
/// are unreachable in practice.
pub const PID_STRIDE: i64 = 1 << 40;

/// PIDs reserved per persisted block. Matches Apache's
/// `PRODUCER_ID_BLOCK_SIZE`.
pub const PID_BLOCK: i64 = 1000;

/// On-disk block file. Records the first `local` value this broker has
/// *not* yet reserved; everything below it is spent.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlockFile {
    next_block: i64,
}

#[derive(Debug)]
struct State {
    /// Next `local` value to hand out.
    next: i64,
    /// First `local` value past the reserved block.
    block_end: i64,
}

/// Hands out producer IDs that are unique across brokers and across
/// restarts. See the module docs for the layout.
pub struct ProducerIdAllocator {
    fs: RealFs,
    dir: PathBuf,
    filename: String,
    /// `(broker_id + 1) * PID_STRIDE` — the low end of this broker's
    /// slice.
    base: i64,
    state: Mutex<State>,
}

impl std::fmt::Debug for ProducerIdAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let st = self.state.lock();
        f.debug_struct("ProducerIdAllocator")
            .field("base", &self.base)
            .field("next", &(self.base + st.next))
            .field("block_end", &(self.base + st.block_end))
            .finish()
    }
}

impl ProducerIdAllocator {
    /// Open (or create) this broker's block file under
    /// `<cluster_dir>/producer_ids/` and reserve the first block.
    ///
    /// `cluster_dir` is the `/data/__cluster` directory. A corrupt or
    /// unreadable block file is treated as "unknown high-water" and the
    /// allocator refuses to start rather than risk reissuing spent PIDs
    /// — the caller falls back to the in-memory counter and logs it.
    pub fn open(cluster_dir: &Path, broker_id: i32) -> std::io::Result<Self> {
        let fs = RealFs::new();
        let dir = cluster_dir.join(PRODUCER_ID_DIR);
        let filename = format!("kaas-{broker_id}.json");
        let persisted = read_block_file(&fs, &dir, &filename)?;

        let base = i64::from(broker_id.max(0))
            .saturating_add(1)
            .saturating_mul(PID_STRIDE);

        let me = Self {
            fs,
            dir,
            filename,
            base,
            state: Mutex::new(State {
                next: persisted,
                block_end: persisted,
            }),
        };
        // Reserve the first block up front so a broker that never
        // produces still burns its block — the next boot then starts
        // above anything this one could have handed out.
        me.reserve(&mut me.state.lock());
        Ok(me)
    }

    /// Next producer ID. Never returns 0 (the client-side "unset"
    /// sentinel) and never returns a PID a previous boot of this broker
    /// — or any other broker — could have handed out.
    pub fn next(&self) -> i64 {
        let mut st = self.state.lock();
        if st.next >= st.block_end {
            self.reserve(&mut st);
        }
        let pid = self.base.saturating_add(st.next);
        st.next += 1;
        pid
    }

    /// Persist the new block end, then adopt it. Persisting first is
    /// what makes a crash skip PIDs forward instead of rewinding them.
    ///
    /// A write failure is logged and the block adopted anyway:
    /// refusing to allocate would fail every `InitProducerId` on the
    /// broker for what is usually a transient NFS blip. The exposure is
    /// bounded — PIDs stay unique for the life of this process, and are
    /// only reusable if the write never succeeds *and* the broker
    /// restarts.
    fn reserve(&self, st: &mut State) {
        let new_end = st.block_end.saturating_add(PID_BLOCK);
        if let Err(err) = atomic_write_json(
            &self.fs,
            &self.dir,
            &self.filename,
            &BlockFile {
                next_block: new_end,
            },
        ) {
            warn!(
                %err,
                dir = %self.dir.display(),
                file = self.filename.as_str(),
                "producer-id block not persisted; PIDs stay unique for this process \
                 but a restart may reuse them"
            );
        }
        st.block_end = new_end;
    }
}

/// Read the persisted block end. A missing file is `0` (first boot).
fn read_block_file(fs: &dyn Fs, dir: &Path, filename: &str) -> std::io::Result<i64> {
    let path = dir.join(filename);
    match fs.open_read(&path) {
        Ok(mut f) => {
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut f, &mut buf)?;
            let bf: BlockFile = serde_json::from_slice(&buf).map_err(std::io::Error::other)?;
            Ok(bf.next_block.max(0))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pids_are_monotonic_and_never_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let a = ProducerIdAllocator::open(tmp.path(), 0).unwrap();
        let first = a.next();
        assert!(first > 0, "0 is the client-side unset sentinel");
        for _ in 0..(PID_BLOCK * 3) {
            let next = a.next();
            assert!(next > first);
        }
    }

    /// The whole point: a restart must not reissue PIDs the previous
    /// boot handed out — that is what makes a fresh producer land on a
    /// dead producer's dedupe window.
    #[test]
    fn restart_never_reissues_a_pid() {
        let tmp = tempfile::tempdir().unwrap();
        let mut seen = std::collections::HashSet::new();

        for _boot in 0..4 {
            let a = ProducerIdAllocator::open(tmp.path(), 0).unwrap();
            // Draw more than one block so the in-flight reservation
            // path is exercised too.
            for _ in 0..(PID_BLOCK + 7) {
                assert!(seen.insert(a.next()), "PID reissued across restart");
            }
        }
    }

    /// A crash between `open` and the next boot loses at most the
    /// unspent tail of the reserved block — it never rewinds.
    #[test]
    fn boot_starts_above_the_previous_block() {
        let tmp = tempfile::tempdir().unwrap();
        let first = {
            let a = ProducerIdAllocator::open(tmp.path(), 0).unwrap();
            a.next()
        };
        let second = ProducerIdAllocator::open(tmp.path(), 0).unwrap().next();
        assert_eq!(
            second - first,
            PID_BLOCK,
            "second boot starts one whole block above the first"
        );
    }

    #[test]
    fn brokers_draw_from_disjoint_slices() {
        let tmp = tempfile::tempdir().unwrap();
        let a = ProducerIdAllocator::open(tmp.path(), 0).unwrap();
        let b = ProducerIdAllocator::open(tmp.path(), 1).unwrap();
        let c = ProducerIdAllocator::open(tmp.path(), 7).unwrap();

        let mut seen = std::collections::HashSet::new();
        for _ in 0..(PID_BLOCK + 3) {
            assert!(seen.insert(a.next()));
            assert!(seen.insert(b.next()));
            assert!(seen.insert(c.next()));
        }
        // Slices are far enough apart that no amount of allocation on
        // one broker walks into another's.
        assert_eq!(b.next() - a.next(), PID_STRIDE);
    }

    /// Upgrade safety: the pre-gh #219 counter handed out 1, 2, 3, …
    /// and those PIDs are still in `producer-state.snapshot` files on
    /// the volume. Broker 0 must not start there.
    #[test]
    fn broker_zero_starts_clear_of_legacy_low_pids() {
        let tmp = tempfile::tempdir().unwrap();
        let a = ProducerIdAllocator::open(tmp.path(), 0).unwrap();
        assert!(a.next() >= PID_STRIDE);
    }

    #[test]
    fn corrupt_block_file_is_an_error_not_a_rewind() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join(PRODUCER_ID_DIR);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("kaas-0.json"), b"{not json").unwrap();
        assert!(ProducerIdAllocator::open(tmp.path(), 0).is_err());
    }
}
