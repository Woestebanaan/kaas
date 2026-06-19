//! CRC32C (Castagnoli) wrapper used to validate v2 RecordBatch CRC fields.
//!
//! Thin pass-through over the `crc32c` crate, which engages SSE4.2 CRC32
//! intrinsics on x86_64 and ARMv8 CRC instructions on aarch64. The Go side
//! uses `hash/crc32.MakeTable(crc32.Castagnoli)` which compiles to the same
//! instructions on the same hardware — outputs are byte-identical.

use thiserror::Error;

/// Castagnoli CRC32C of `data`.
pub fn compute(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("CRC32C mismatch: got {got:08x} want {want:08x}")]
pub struct CrcError {
    pub got: u32,
    pub want: u32,
}

/// Compute the CRC of `data` and compare against `expected`.
pub fn validate(data: &[u8], expected: u32) -> Result<(), CrcError> {
    let got = compute(data);
    if got == expected {
        Ok(())
    } else {
        Err(CrcError {
            got,
            want: expected,
        })
    }
}

/// Validate the CRC32C field of a v2 RecordBatch. Layout (the 4-byte CRC
/// field starts at offset 21 within the batch, and covers everything from
/// the attributes field at offset 25 onward):
///
/// ```text
///   0..8  baseOffset (i64)
///   8..12 batchLength (i32)
///  12..16 partitionLeaderEpoch (i32)
///  16..17 magic (i8)
///  17..21 crc (u32)          <- field validated here
///  21..   attributes onward  <- bytes covered by the CRC
/// ```
///
/// This function reads the bytes from offset 21 to the end as opaque input
/// to CRC32C. It does NOT decode the records — byte opacity is preserved.
pub fn verify_batch_crc(batch: &[u8]) -> Result<(), CrcError> {
    const CRC_FIELD_OFFSET: usize = 17;
    const CRC_COVERED_FROM: usize = 21;
    if batch.len() < CRC_COVERED_FROM {
        return Err(CrcError { got: 0, want: 0 });
    }
    let mut crc_bytes = [0u8; 4];
    crc_bytes.copy_from_slice(&batch[CRC_FIELD_OFFSET..CRC_FIELD_OFFSET + 4]);
    let expected = u32::from_be_bytes(crc_bytes);
    validate(&batch[CRC_COVERED_FROM..], expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Canonical Castagnoli test vector — same value the Go side asserts.
    #[test]
    fn known_vector_123456789() {
        assert_eq!(compute(b"123456789"), 0xE306_9283);
    }

    /// Catches the common bug of using the IEEE polynomial by mistake. IEEE
    /// CRC32 of "123456789" is 0xCBF43926.
    #[test]
    fn not_ieee() {
        assert_ne!(compute(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn empty_input() {
        assert_eq!(compute(&[]), 0);
    }

    #[test]
    fn single_byte_sensitivity() {
        assert_ne!(compute(&[0xAB]), compute(&[0xAA]));
    }

    /// RFC 3720 (iSCSI) test vectors — independent confirmation of polynomial.
    #[test]
    fn rfc3720_vectors() {
        assert_eq!(compute(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(compute(&[0xFFu8; 32]), 0x62A8_AB43);
    }

    #[test]
    fn validate_pass_fail() {
        let data = b"skafka test data";
        let crc = compute(data);
        assert!(validate(data, crc).is_ok());
        assert!(validate(data, crc.wrapping_add(1)).is_err());
    }
}
