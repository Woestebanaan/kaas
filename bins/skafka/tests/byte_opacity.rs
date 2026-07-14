//! Two layers of protection against the "broker is a byte mover, not
//! a byte interpreter" invariant slipping:
//!
//! 1. **Storage round-trip is byte-identical** — bytes handed to
//!    [`MemoryStorage::append`] come back from
//!    [`MemoryStorage::read`] unchanged, across several batches with
//!    distinct compression-attribute bits. The compression codec bit
//!    is declarative (no actual compression happens in the test);
//!    the point is that the broker treats the payload as opaque.
//! 2. **Tripwire counters read zero after the smoke** — the
//!    [`sk_codec::tripwires`] process-atomics stay at their reset
//!    baseline. If a future refactor decodes a record or re-encodes
//!    a batch, the corresponding
//!    `sk_codec::tripwires::bump_codec_*` call fires and this test
//!    turns red.
//!
//! Runs in-process against `MemoryStorage` — no live broker, no OTel
//! collector required.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use bytes::Bytes;
use sk_codec::tripwires;
use sk_storage::{MemoryStorage, StorageEngine};

/// Build a minimal v2 RecordBatch carrying `num_records` records with
/// the given `compression_bits` in the attributes field. Bytes outside
/// the offset / delta / attribute fields are zeros — the codec never
/// looks at the record payload.
///
/// Covers the fields the
/// broker actually inspects (baseOffset, batchLength, magic,
/// lastOffsetDelta). Everything else is filler; the storage engine
/// only reads bytes `[0..8]` (baseOffset), `[8..12]` (batchLength),
/// `[16]` (magic), and `[23..27]` (lastOffsetDelta) — matches
/// `crates/sk-storage/src/memory.rs:80-126`.
fn build_batch(base_offset: i64, num_records: i32, compression_bits: i16) -> Bytes {
    // 12 bytes header (baseOffset + batchLength) + 65 bytes body:
    // enough to carry a valid `lastOffsetDelta` at [23..27] and a
    // non-zero attributes word at [21..23]. The remaining bytes stay
    // zero — they're not inspected by MemoryStorage.
    let body_size: usize = 65;
    let total = 12 + body_size;
    let mut buf = vec![0u8; total];
    buf[0..8].copy_from_slice(&base_offset.to_be_bytes());
    let body_len_i32 = i32::try_from(body_size).expect("body_size fits in i32");
    buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
    // magic = 2 at offset 16 (mirrors MemoryStorage's assertion).
    buf[16] = 2;
    // attributes (i16 BE) at [21..23]. Compression codec lives in
    // the low 3 bits per KIP-32. The broker never reads this field —
    // the whole point of the test.
    buf[21..23].copy_from_slice(&compression_bits.to_be_bytes());
    // lastOffsetDelta at [23..27] = num_records - 1.
    let last_offset_delta = num_records - 1;
    buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
    Bytes::from(buf)
}

#[tokio::test]
async fn storage_round_trip_is_byte_identical() {
    tripwires::reset_for_test();

    let store = MemoryStorage::new();
    let topic = "byteopacity-topic";

    // (name, num_records, compression_bits). Compression bits map to
    // KIP-32: 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd. The broker
    // treats them as opaque — nothing here actually compresses.
    let cases: &[(&str, i32, i16)] = &[
        ("snappy", 5, 2),
        ("none", 3, 0),
        ("gzip", 4, 1),
        ("lz4", 7, 3),
        ("zstd", 2, 4),
    ];

    let mut base_offset = 0_i64;
    let mut combined: Vec<u8> = Vec::new();
    for (name, num_records, compression) in cases {
        let batch = build_batch(base_offset, *num_records, *compression);
        base_offset += i64::from(*num_records);

        store
            .append(topic, 0, 0, -1, batch.clone())
            .await
            .unwrap_or_else(|e| panic!("[{name}] append: {e}"));
        combined.extend_from_slice(&batch);
    }

    let got = store
        .read(topic, 0, 0, combined.len() + 1024)
        .await
        .expect("read failed");

    // Byte-for-byte equality. If the storage engine ever decodes and
    // re-serialises, the byte-level structure (padding, ordering,
    // filler bytes) will differ and this fails.
    assert_eq!(
        got.len(),
        combined.len(),
        "byte-identical round-trip length mismatch: got {} bytes, want {} bytes",
        got.len(),
        combined.len()
    );

    if got.as_ref() != combined.as_slice() {
        // Give the failure a useful hint for the easy case.
        let mismatch_idx = got
            .iter()
            .zip(combined.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(usize::MAX);
        panic!(
            "byte-identical round-trip content mismatch: first differ at byte {mismatch_idx} \
             (got 0x{:02x}, want 0x{:02x})",
            got[mismatch_idx], combined[mismatch_idx]
        );
    }

    // Tripwires must stay at zero — no code path in the storage
    // engine decoded records or re-encoded a batch during the
    // round-trip.
    assert_eq!(
        tripwires::record_decode_count(),
        0,
        "byte-opacity violated: sk_codec::tripwires::record_decode_count > 0. \
         Some code path called bump_codec_record_decode — see the tracing::warn line \
         from sk_observability::byteopacity for the site."
    );
    assert_eq!(
        tripwires::batch_reencode_count(),
        0,
        "byte-opacity violated: sk_codec::tripwires::batch_reencode_count > 0. \
         Some code path called bump_codec_batch_reencode."
    );
}

/// Meta-test: prove that IF a future violator calls
/// `bump_codec_record_decode` / `bump_codec_batch_reencode`, the
/// counters do increment. This is the only test in the file that
/// calls the `bump_*` helpers — production code must never call
/// them.
#[test]
fn bumps_are_observable() {
    tripwires::reset_for_test();

    let before_r = tripwires::record_decode_count();
    let before_b = tripwires::batch_reencode_count();
    tripwires::bump_codec_record_decode("byteopacity_test_meta");
    tripwires::bump_codec_batch_reencode("byteopacity_test_meta");
    assert_eq!(tripwires::record_decode_count(), before_r + 1);
    assert_eq!(tripwires::batch_reencode_count(), before_b + 1);
}
