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

pub mod add_offsets_to_txn;
pub mod add_partitions_to_txn;
pub mod api_versions;
pub mod common;
pub mod delete_groups;
pub mod describe_groups;
pub mod end_txn;
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod init_producer_id;
pub mod join_group;
pub mod leave_group;
pub mod list_groups;
pub mod list_offsets;
pub mod metadata;
pub mod offset_commit;
pub mod offset_delete;
pub mod offset_fetch;
pub mod produce;
pub mod registry;
pub mod sasl_authenticate;
pub mod sasl_handshake;
pub mod sync_group;
pub mod txn_offset_commit;
pub mod write_txn_markers;
