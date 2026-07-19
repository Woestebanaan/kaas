//! Codec-level errors. Framing-layer I/O errors live in [`crate::frame::FrameError`].

use thiserror::Error;

/// Error returned by every `read_*` / `write_*` primitive and per-API codec
/// function. Total over an in-memory buffer — there is no I/O at this layer.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    /// The buffer ran out before the read could complete.
    #[error("unexpected EOF in codec")]
    UnexpectedEof,

    /// A string field carried bytes that are not valid UTF-8.
    #[error("invalid UTF-8 in codec string")]
    InvalidUtf8,

    /// A length prefix was negative for a non-nullable variant, or larger than
    /// the remaining buffer.
    #[error("invalid length {got} (max {max})")]
    InvalidLength { got: i64, max: usize },

    /// A uvarint exceeded 10 wire bytes or had a high bit set on byte 9 with
    /// payload > 1 (would overflow u64).
    #[error("invalid uvarint encoding")]
    InvalidUvarint,

    /// A UUID field was not exactly 16 bytes.
    #[error("invalid uuid length")]
    InvalidUuid,

    /// A non-nullable field carried the null sentinel (length = -1 for legacy,
    /// uvarint = 0 for compact).
    #[error("unexpected null value")]
    UnexpectedNull,

    /// A field length does not fit the target type's positive range
    /// (e.g. encoding a 100 KiB string into an i16 prefix).
    #[error("length overflow during encoding")]
    LengthOverflow,
}
