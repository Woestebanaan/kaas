//! v2 RecordBatch encoder for transactional COMMIT / ABORT markers.
//!
//! Port of `archive/internal/protocol/handlers/control_batch.go` —
//! byte-for-byte equivalent so a Go-written marker batch and a
//! Rust-written one decode to the same record by any Kafka 3.x
//! client (and vice versa).
//!
//! ### Wire shape
//!
//! ```text
//!   0..8   baseOffset (i64)            = 0  (broker rewrites on Append)
//!   8..12  batchLength (i32)
//!  12..16  partitionLeaderEpoch (i32)  = 0
//!  16..17  magic (i8)                  = 2
//!  17..21  crc (u32 CRC32C / Castagnoli of bytes 21..)
//!  21..23  attributes (i16)            = 0x30 (control | transactional)
//!  23..27  lastOffsetDelta (i32)       = 0
//!  27..35  baseTimestamp (i64)         = 0
//!  35..43  maxTimestamp (i64)          = 0
//!  43..51  producerId (i64)
//!  51..53  producerEpoch (i16)
//!  53..57  baseSequence (i32)          = -1   (control batches are
//!                                              idempotence-exempt)
//!  57..61  recordCount (i32)           = 1
//!  61..    one varint-prefixed record body
//! ```
//!
//! Record body:
//! - `attributes` (i8) = `0`
//! - `timestampDelta` (varlong) = `0`
//! - `offsetDelta` (varint) = `0`
//! - `keyLen` (varint) = `4`
//! - `key` = `[0, 0, 0, control_type]` where `control_type` is `0`
//!   (ABORT) or `1` (COMMIT)
//! - `valueLen` (varint) = `6`
//! - `value` = `[0, 0, coordinator_epoch (i32 BE)]`
//! - `headersCount` (varint) = `0`

use bytes::{BufMut, BytesMut};

use sk_codec::crc;
use sk_codec::primitives::write_varint;

const MAGIC_V2: i8 = 2;
const ATTR_IS_TRANSACTIONAL: i16 = 1 << 4;
const ATTR_IS_CONTROL: i16 = 1 << 5;
const ATTRIBUTES: i16 = ATTR_IS_TRANSACTIONAL | ATTR_IS_CONTROL;
const CONTROL_TYPE_ABORT: u16 = 0;
const CONTROL_TYPE_COMMIT: u16 = 1;
const END_TXN_MARKER_VERSION: u16 = 0;
const BASE_SEQUENCE_CONTROL: i32 = -1;

/// Encode a single-record transactional COMMIT or ABORT marker batch.
///
/// `commit = true` → COMMIT marker; `commit = false` → ABORT marker.
/// The broker rewrites the `baseOffset` field to the assigned HWM on
/// `Append`, so the leading 8 bytes are emitted as zero.
pub fn build_control_batch(
    producer_id: i64,
    producer_epoch: i16,
    commit: bool,
    coordinator_epoch: i32,
) -> Vec<u8> {
    let control_type = if commit {
        CONTROL_TYPE_COMMIT
    } else {
        CONTROL_TYPE_ABORT
    };

    // ---- Record key (4 bytes): version (i16) + type (i16) -----------
    let mut key = [0u8; 4];
    key[0..2].copy_from_slice(&0u16.to_be_bytes());
    key[2..4].copy_from_slice(&control_type.to_be_bytes());

    // ---- Record value (6 bytes): EndTxnMarker schema --------------
    // i16 schema version + i32 coordinatorEpoch.
    let mut value = [0u8; 6];
    value[0..2].copy_from_slice(&END_TXN_MARKER_VERSION.to_be_bytes());
    value[2..6].copy_from_slice(&coordinator_epoch.to_be_bytes());

    let record_body = encode_record(&key, &value);

    // ---- CRC payload (attributes onward) ----------------------------
    let mut payload = BytesMut::new();
    payload.put_i16(ATTRIBUTES);
    payload.put_i32(0); // lastOffsetDelta — single record
    payload.put_i64(0); // baseTimestamp
    payload.put_i64(0); // maxTimestamp
    payload.put_i64(producer_id);
    payload.put_i16(producer_epoch);
    payload.put_i32(BASE_SEQUENCE_CONTROL);
    payload.put_i32(1); // recordCount
    payload.extend_from_slice(&record_body);

    let crc = crc::compute(&payload);

    // batchLength covers: partitionLeaderEpoch (4) + magic (1) +
    // crc (4) + everything after the CRC field.
    let body_len = 4 + 1 + 4 + payload.len();
    let batch_length = i32::try_from(body_len).unwrap_or(i32::MAX);

    let mut batch = BytesMut::with_capacity(8 + 4 + body_len);
    batch.put_i64(0); // baseOffset placeholder — broker rewrites
    batch.put_i32(batch_length);
    batch.put_i32(0); // partitionLeaderEpoch
    batch.put_i8(MAGIC_V2);
    batch.put_u32(crc);
    batch.extend_from_slice(&payload);
    batch.to_vec()
}

/// Encode one v2 record (varint-prefixed length + body).
fn encode_record(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i8(0); // record-level attributes — always 0 for control
    write_varint(&mut body, 0); // timestampDelta
    write_varint(&mut body, 0); // offsetDelta
    write_varint(&mut body, i64::try_from(key.len()).unwrap_or(0));
    body.extend_from_slice(key);
    write_varint(&mut body, i64::try_from(value.len()).unwrap_or(0));
    body.extend_from_slice(value);
    write_varint(&mut body, 0); // headers count

    let mut prefixed = BytesMut::new();
    write_varint(&mut prefixed, i64::try_from(body.len()).unwrap_or(0));
    prefixed.extend_from_slice(&body);
    prefixed.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_marker_has_control_and_transactional_bits() {
        let batch = build_control_batch(42, 7, true, 3);
        // Attributes live at offset 21..23 (BE i16).
        let attrs = i16::from_be_bytes([batch[21], batch[22]]);
        assert_eq!(attrs & ATTR_IS_CONTROL, ATTR_IS_CONTROL);
        assert_eq!(attrs & ATTR_IS_TRANSACTIONAL, ATTR_IS_TRANSACTIONAL);
    }

    #[test]
    fn base_sequence_is_minus_one_for_idempotence_exemption() {
        let batch = build_control_batch(42, 7, true, 3);
        // baseSequence at offset 53..57.
        let bs = i32::from_be_bytes([batch[53], batch[54], batch[55], batch[56]]);
        assert_eq!(bs, -1);
    }

    #[test]
    fn producer_id_and_epoch_round_trip() {
        let batch = build_control_batch(0x1234_5678_9abc, 0x4321, false, 0);
        let pid = i64::from_be_bytes([
            batch[43], batch[44], batch[45], batch[46], batch[47], batch[48], batch[49], batch[50],
        ]);
        let epoch = i16::from_be_bytes([batch[51], batch[52]]);
        assert_eq!(pid, 0x1234_5678_9abc);
        assert_eq!(epoch, 0x4321);
    }

    #[test]
    fn crc_validates() {
        let batch = build_control_batch(42, 7, true, 3);
        sk_codec::crc::verify_batch_crc(&batch).expect("self-encoded batch must verify");
    }

    #[test]
    fn record_count_is_one() {
        let batch = build_control_batch(42, 7, false, 0);
        let n = i32::from_be_bytes([batch[57], batch[58], batch[59], batch[60]]);
        assert_eq!(n, 1);
    }

    #[test]
    fn commit_and_abort_keys_differ_only_in_type_byte() {
        let commit = build_control_batch(1, 0, true, 0);
        let abort = build_control_batch(1, 0, false, 0);
        // The two batches differ only in the CRC and the control-type
        // byte (offset 21 + attributes + ... + record-body key byte 3).
        // We check the type byte indirectly by re-decoding the key:
        // record body starts after batch header at offset 61. Layout:
        //   varint bodyLen | i8 attrs=0 | varlong tsDelta=0 | varint offDelta=0 |
        //   varint keyLen=4 | i32 key | ...
        // With all zigzag-zero one-byte fields, key starts at 61 + 5 = 66.
        let commit_type = u16::from_be_bytes([commit[68], commit[69]]);
        let abort_type = u16::from_be_bytes([abort[68], abort[69]]);
        assert_eq!(commit_type, CONTROL_TYPE_COMMIT);
        assert_eq!(abort_type, CONTROL_TYPE_ABORT);
    }
}
