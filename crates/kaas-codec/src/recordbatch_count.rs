//! Counting records inside a v2 RecordBatch stream without decoding them.
//!
//! Layout of a v2 batch (61-byte header):
//!
//! ```text
//!   [baseOffset:8][batchLength:4][partitionLeaderEpoch:4][magic:1]
//!   [crc:4][attrs:2][lastOffsetDelta:4][baseTimestamp:8][maxTimestamp:8]
//!   [producerId:8][producerEpoch:2][baseSequence:4][numRecords:4]
//! ```
//!
//! Wire length of a batch = `12 (baseOffset + batchLength) + value of batchLength`.
//!
//! Both helpers below read **only the 61-byte header** of each batch. The
//! records payload is never touched, so the storage engine's `sendfile(2)`
//! splice path is not undone by counting.

use std::io::{self, Read, Seek, SeekFrom};

const BATCH_HEADER_SIZE: usize = 61;
const BATCH_LENGTH_OFFSET: usize = 8;
const BATCH_PREFIX_SIZE: usize = 12;
const BATCH_NUM_RECORDS_OFFSET: usize = 57;

/// Smallest valid `batchLength` field value: the header tail (49 bytes from
/// `partitionLeaderEpoch` through `numRecords`) when the records list is
/// empty. A smaller value means the bytes aren't a v2 batch.
const MIN_BATCH_LENGTH_FIELD: i32 = 49;

fn read_be_i32(b: &[u8], off: usize) -> i32 {
    let mut tmp = [0u8; 4];
    tmp.copy_from_slice(&b[off..off + 4]);
    i32::from_be_bytes(tmp)
}

/// Walk a contiguous in-memory stream of v2 RecordBatches and return the
/// total `numRecords` across all complete batches. A truncated final batch
/// at the tail is ignored — Kafka clients tolerate truncation at the end of
/// a Fetch response by spec.
pub fn count_records_in_batches(b: &[u8]) -> i64 {
    let mut total: i64 = 0;
    let mut pos: usize = 0;
    while pos + BATCH_HEADER_SIZE <= b.len() {
        let batch_len = read_be_i32(b, pos + BATCH_LENGTH_OFFSET);
        if batch_len < MIN_BATCH_LENGTH_FIELD {
            return total;
        }
        let Ok(batch_len_us) = usize::try_from(batch_len) else {
            return total;
        };
        let wire_len = BATCH_PREFIX_SIZE.saturating_add(batch_len_us);
        if pos.saturating_add(wire_len) > b.len() {
            return total;
        }
        let records = read_be_i32(b, pos + BATCH_NUM_RECORDS_OFFSET);
        if records > 0 {
            total = total.saturating_add(i64::from(records));
        }
        pos = pos.saturating_add(wire_len);
    }
    total
}

/// Walk a v2 RecordBatch stream that lives on disk (or any seekable
/// reader). Reads only the 61-byte header of each batch — records payload
/// stays where it is, so the storage engine's splice path is not undone.
///
/// `pos` is the starting byte offset; `length` bounds how far to walk.
pub fn count_records_in_batches_at<R: Read + Seek>(
    r: &mut R,
    pos: u64,
    length: usize,
) -> io::Result<i64> {
    let mut total: i64 = 0;
    let mut cur = pos;
    let length_u64 = u64::try_from(length).unwrap_or(u64::MAX);
    let end = cur.saturating_add(length_u64);
    let mut hdr = [0u8; BATCH_HEADER_SIZE];

    let hdr_u64 = u64::try_from(BATCH_HEADER_SIZE).unwrap_or(u64::MAX);
    while cur.saturating_add(hdr_u64) <= end {
        r.seek(SeekFrom::Start(cur))?;
        match r.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(total),
            Err(e) => return Err(e),
        }
        let batch_len = read_be_i32(&hdr, BATCH_LENGTH_OFFSET);
        if batch_len < MIN_BATCH_LENGTH_FIELD {
            return Ok(total);
        }
        let Ok(batch_len_us) = usize::try_from(batch_len) else {
            return Ok(total);
        };
        let wire_len = BATCH_PREFIX_SIZE.saturating_add(batch_len_us);
        let wire_len_u64 = u64::try_from(wire_len).unwrap_or(u64::MAX);
        if cur.saturating_add(wire_len_u64) > end {
            return Ok(total);
        }
        let records = read_be_i32(&hdr, BATCH_NUM_RECORDS_OFFSET);
        if records > 0 {
            total = total.saturating_add(i64::from(records));
        }
        cur = cur.saturating_add(wire_len_u64);
    }
    Ok(total)
}

// ---------------------------------------------------------------------------
//
// Tests use a hand-built minimal v2 batch: just the 61-byte header with a
// zero-length records section. The CRC field is left zero; these helpers
// never validate it.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal v2 batch header carrying `num_records`.
    fn batch_header(num_records: i32) -> Vec<u8> {
        let mut v = vec![0u8; BATCH_HEADER_SIZE];
        // batchLength: MIN_BATCH_LENGTH_FIELD = 49 (zero records payload)
        v[BATCH_LENGTH_OFFSET..BATCH_LENGTH_OFFSET + 4]
            .copy_from_slice(&MIN_BATCH_LENGTH_FIELD.to_be_bytes());
        v[BATCH_NUM_RECORDS_OFFSET..BATCH_NUM_RECORDS_OFFSET + 4]
            .copy_from_slice(&num_records.to_be_bytes());
        v
    }

    #[test]
    fn empty_stream_is_zero() {
        assert_eq!(count_records_in_batches(&[]), 0);
    }

    #[test]
    fn single_batch_sum() {
        let buf = batch_header(7);
        assert_eq!(count_records_in_batches(&buf), 7);
    }

    #[test]
    fn multi_batch_sum() {
        let mut buf = Vec::new();
        for n in &[3i32, 5, 11] {
            buf.extend_from_slice(&batch_header(*n));
        }
        assert_eq!(count_records_in_batches(&buf), 3 + 5 + 11);
    }

    #[test]
    fn truncated_tail_is_ignored() {
        let mut buf = batch_header(4);
        // Strip the last 5 bytes of the header so the batch is incomplete.
        buf.truncate(buf.len() - 5);
        assert_eq!(count_records_in_batches(&buf), 0);
    }

    #[test]
    fn invalid_batch_length_field_stops_walk() {
        let mut buf = batch_header(99);
        // Stomp batchLength to a value < MIN_BATCH_LENGTH_FIELD.
        buf[BATCH_LENGTH_OFFSET..BATCH_LENGTH_OFFSET + 4].copy_from_slice(&5i32.to_be_bytes());
        assert_eq!(count_records_in_batches(&buf), 0);
    }

    #[test]
    fn at_variant_matches_in_memory_variant() {
        let mut buf = Vec::new();
        for n in &[2i32, 4, 6, 8] {
            buf.extend_from_slice(&batch_header(*n));
        }
        let mut cur = Cursor::new(&buf);
        let total = count_records_in_batches_at(&mut cur, 0, buf.len()).unwrap();
        assert_eq!(total, count_records_in_batches(&buf));
    }
}
