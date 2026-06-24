//! Per-API request/response types and codecs.
//!
//! One module per Kafka API key, snake-cased after the Apache name. Phase 1
//! seeds the module tree with [`api_versions`] (key 18) — the first slice;
//! later commits land the remaining 39 keys.
//!
//! Each module exposes:
//!
//! - `Request` and `Response` types,
//! - `decode_request(&mut Bytes, version: i16) -> Result<Request, CodecError>`,
//! - `encode_response(&mut BytesMut, &Response, version: i16) -> Result<(), CodecError>`,
//! - a `VERSIONS: (i16, i16)` constant carrying `(min, max)`.
//!
//! The (api_key, api_version) → header version mapping for each module is
//! registered through [`registry::ALL`] so [`api_versions`] can emit a
//! correct ApiVersions response without per-module bookkeeping.

pub mod api_versions;
pub mod common;
pub mod fetch;
pub mod init_producer_id;
pub mod list_offsets;
pub mod metadata;
pub mod produce;
pub mod registry;
pub mod sasl_authenticate;
pub mod sasl_handshake;
