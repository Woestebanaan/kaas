//! sk-codec — Kafka wire frames, primitives, CRC32C, KIP-482 tagged fields.
//!
//! Port of `archive/internal/protocol/codec/`. Bit-perfect equivalent of the
//! Go codec: every API key/version the Go broker registers encodes/decodes
//! byte-identically against captured Apache Kafka 3.7 fixtures.
//!
//! # Byte-opacity contract
//!
//! RecordBatch payloads are byte-opaque throughout sk-codec. There is **no
//! `Record` struct** in this crate — record-batch bytes flow as
//! `Option<bytes::Bytes>` and are never parsed. The only function that
//! touches batch bytes beyond the 61-byte v2 header is [`crc::verify_batch_crc`],
//! which treats them as opaque input to CRC32C.
//!
//! Any future code that does decode a record or re-encode a batch MUST bump
//! the matching counter in [`tripwires`]. The integration tests in
//! `crates/sk-codec/tests/` assert both counters read zero after every run.
//!
//! See `phase-1.md` for the full scope and the workstream breakdown that
//! produced these modules.

pub mod api;
pub mod crc;
pub mod errors;
pub mod frame;
pub mod headers;
pub mod primitives;
pub mod recordbatch_count;
pub mod tagged;
pub mod tripwires;

pub use bytes::{Bytes, BytesMut};
pub use errors::CodecError;
pub use frame::{read_frame, write_frame, FrameError, FrameReader};
pub use headers::{
    decode_request_header, encode_response_header, HeaderVersion, RequestHeader, ResponseHeader,
};
