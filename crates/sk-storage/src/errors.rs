//! Top-level storage error sentinels.
//!
//! Mirrors the sentinel set the Go `internal/storage` package exposes —
//! Phase 2 commits map them onto wire error codes in `sk-protocol`'s
//! Produce/Fetch handlers.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    /// The producer's epoch is older than the partition's current epoch.
    #[error("epoch mismatch")]
    EpochMismatch,

    /// Idempotent-producer guard: the batch's sequence has a gap or
    /// starts after a non-zero value on a fresh PID. Wire error 45.
    #[error("out of order sequence number")]
    OutOfOrderSequence,

    /// Idempotent-producer guard: the batch's PID/epoch is older than
    /// what's tracked. Wire error 47.
    #[error("invalid producer epoch")]
    InvalidProducerEpoch,

    /// Idempotent-producer guard: the batch's (firstSeq, lastSeq) tuple
    /// is already in the dedupe window. Wire error 46.
    #[error("duplicate sequence number")]
    DuplicateSequence,

    /// The fsync watchdog (gh #95) deadline elapsed. Subsequent Append
    /// calls fail fast until the engine drops the partition.
    #[error("storage stalled")]
    Stalled,

    /// Topic / partition does not exist in the engine.
    #[error("unknown topic or partition")]
    UnknownTopicOrPartition,

    /// Read offset is outside `[log_start_offset, high_watermark)`.
    #[error("offset out of range")]
    OffsetOutOfRange,

    /// Partition was relinquished (closed) while the request was in
    /// flight.
    #[error("partition closed")]
    Closed,

    /// Producer snapshot or manifest decoded as a future schema version
    /// that this binary doesn't understand. Recoverable — the caller
    /// starts fresh.
    #[error("unknown on-disk schema version: {0}")]
    UnknownSchemaVersion(i64),

    /// Underlying filesystem I/O error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// JSON parse error from manifest, producer snapshot, or topic
    /// config.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}
