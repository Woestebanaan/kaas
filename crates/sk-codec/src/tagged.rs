//! KIP-482 tagged fields (the "flexible version" extension block at the tail
//! of every flexible request/response).
//!
//! Wire shape: `uvarint(num_fields) || for each: uvarint(tag) ||
//! uvarint(value_len) || value_bytes`.
//!
//! Most Apache 3.7 code paths emit `num_fields = 0`. Write that with
//! [`write_empty`]; read by [`read`] which discards unknown tags (Apache's
//! documented forward-compat contract).

use bytes::{Bytes, BytesMut};

use crate::errors::CodecError;
use crate::primitives::{read_uvarint, write_uvarint};

/// A KIP-482 tagged field. The value is opaque bytes — interpretation is
/// the responsibility of the per-API decoder once it picks the tag out
/// of the [`Vec`] returned by [`read_into`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggedField {
    pub tag: u64,
    pub value: Bytes,
}

/// Read and discard every tagged field. Mirrors Go's `ReadTaggedFields` —
/// unknown tags are silently consumed, and known tags are not surfaced to
/// the caller. Use [`read_into`] when a handler needs to inspect specific
/// tags.
pub fn read(buf: &mut Bytes) -> Result<(), CodecError> {
    let n = read_uvarint(buf)?;
    for _ in 0..n {
        let _tag = read_uvarint(buf)?;
        let size = read_uvarint(buf)?;
        let size_us = usize::try_from(size).map_err(|_| CodecError::LengthOverflow)?;
        if buf.len() < size_us {
            return Err(CodecError::UnexpectedEof);
        }
        let _ = buf.split_to(size_us);
    }
    Ok(())
}

/// Read every tagged field into a [`Vec`]. Used by handlers that need to
/// inspect specific tag values (CreateTopics v7+ TopicID, etc.).
pub fn read_into(buf: &mut Bytes) -> Result<Vec<TaggedField>, CodecError> {
    let n = read_uvarint(buf)?;
    let n_us = usize::try_from(n).map_err(|_| CodecError::LengthOverflow)?;
    let mut out = Vec::with_capacity(n_us);
    for _ in 0..n_us {
        let tag = read_uvarint(buf)?;
        let size = read_uvarint(buf)?;
        let size_us = usize::try_from(size).map_err(|_| CodecError::LengthOverflow)?;
        if buf.len() < size_us {
            return Err(CodecError::UnexpectedEof);
        }
        let value = buf.split_to(size_us);
        out.push(TaggedField { tag, value });
    }
    Ok(out)
}

/// The single-byte `uvarint(0)` that marks "no tagged fields". By far the
/// most common case on the wire.
pub fn write_empty(buf: &mut BytesMut) {
    write_uvarint(buf, 0);
}

/// Write a non-empty tagged-field block. Fields are written in the order
/// given; Apache expects ascending tag order but does not strictly enforce
/// it on read. Callers that care should sort before calling.
pub fn write(buf: &mut BytesMut, fields: &[TaggedField]) -> Result<(), CodecError> {
    let count = u64::try_from(fields.len()).map_err(|_| CodecError::LengthOverflow)?;
    write_uvarint(buf, count);
    for f in fields {
        write_uvarint(buf, f.tag);
        let len = u64::try_from(f.value.len()).map_err(|_| CodecError::LengthOverflow)?;
        write_uvarint(buf, len);
        buf.extend_from_slice(&f.value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_block_roundtrips() {
        let mut buf = BytesMut::new();
        write_empty(&mut buf);
        let mut b = buf.freeze();
        assert!(read(&mut b).is_ok());
        assert!(b.is_empty());
    }

    #[test]
    fn empty_block_via_read_into() {
        let mut buf = BytesMut::new();
        write_empty(&mut buf);
        let mut b = buf.freeze();
        let fields = read_into(&mut b).unwrap();
        assert!(fields.is_empty());
    }

    #[test]
    fn multi_field_roundtrip() {
        let fields = vec![
            TaggedField {
                tag: 0,
                value: Bytes::from_static(&[1, 2, 3]),
            },
            TaggedField {
                tag: 5,
                value: Bytes::from_static(&[]),
            },
            TaggedField {
                tag: 99,
                value: Bytes::from_static(&[7, 7, 7, 7]),
            },
        ];
        let mut buf = BytesMut::new();
        write(&mut buf, &fields).unwrap();
        let mut b = buf.freeze();
        let got = read_into(&mut b).unwrap();
        assert_eq!(got, fields);
        assert!(b.is_empty());
    }

    #[test]
    fn unknown_tags_discarded_by_read() {
        let fields = vec![TaggedField {
            tag: 42,
            value: Bytes::from_static(&[0xff, 0xee, 0xdd]),
        }];
        let mut buf = BytesMut::new();
        write(&mut buf, &fields).unwrap();
        let mut b = buf.freeze();
        assert!(read(&mut b).is_ok());
        assert!(b.is_empty());
    }
}
