//! Helpers shared across per-API codec modules.
//!
//! These wrappers switch between legacy and KIP-482 flexible
//! representations of the same field based on the per-call `flexible`
//! flag. They mirror the `readString` / `nullableString` /
//! `writeString` / `readArray` helpers in
//! `archive/internal/protocol/codec/api/produce.go` and friends.
//!
//! Names deliberately shadow the underlying primitives in
//! [`crate::primitives`]; the qualifier `api::common::` at the call
//! site (e.g. `use crate::api::common::read_str;`) signals that the
//! variant is flex-aware.

use bytes::BytesMut;

use crate::errors::CodecError;
use crate::primitives;
use crate::Bytes;

/// Read a non-nullable string, switching on flexible.
pub fn read_str(buf: &mut Bytes, flexible: bool) -> Result<String, CodecError> {
    if flexible {
        primitives::read_compact_string(buf)
    } else {
        primitives::read_string(buf)
    }
}

/// Read a nullable string, switching on flexible. `None` ↔ wire null.
pub fn read_nullable_str(buf: &mut Bytes, flexible: bool) -> Result<Option<String>, CodecError> {
    if flexible {
        primitives::read_compact_nullable_string(buf)
    } else {
        primitives::read_nullable_string(buf)
    }
}

/// Write a non-nullable string, switching on flexible.
pub fn write_str(buf: &mut BytesMut, s: &str, flexible: bool) -> Result<(), CodecError> {
    if flexible {
        primitives::write_compact_string(buf, s)
    } else {
        primitives::write_string(buf, s)
    }
}

/// Write a nullable string, switching on flexible. `None` ↔ wire null.
pub fn write_nullable_str(
    buf: &mut BytesMut,
    s: Option<&str>,
    flexible: bool,
) -> Result<(), CodecError> {
    if flexible {
        primitives::write_compact_nullable_string(buf, s)
    } else {
        primitives::write_nullable_string(buf, s)
    }
}

/// Read an array length prefix, switching on flexible. Compact arrays
/// encode `len + 1` as an unsigned varint (`0` = null); the flexible
/// branch unwinds that and returns the actual element count.
pub fn read_array_len(buf: &mut Bytes, flexible: bool) -> Result<usize, CodecError> {
    if flexible {
        primitives::read_compact_array_len(buf)
    } else {
        primitives::read_array_len(buf)
    }
}

/// Write an array length prefix, switching on flexible.
pub fn write_array_len(buf: &mut BytesMut, count: usize, flexible: bool) -> Result<(), CodecError> {
    if flexible {
        primitives::write_compact_array_len(buf, count)
    } else {
        primitives::write_array_len(buf, count)
    }
}

/// Read nullable bytes, switching on flexible. Zero-copy view into
/// the source buffer.
pub fn read_nullable_bytes(buf: &mut Bytes, flexible: bool) -> Result<Option<Bytes>, CodecError> {
    if flexible {
        primitives::read_compact_nullable_bytes(buf)
    } else {
        primitives::read_nullable_bytes(buf)
    }
}

/// Write nullable bytes, switching on flexible.
pub fn write_nullable_bytes(
    buf: &mut BytesMut,
    b: Option<&[u8]>,
    flexible: bool,
) -> Result<(), CodecError> {
    if flexible {
        primitives::write_compact_nullable_bytes(buf, b)
    } else {
        primitives::write_nullable_bytes(buf, b)
    }
}
