//! LeaveGroup — API key 13.
//!
//! Versions 0..=5. Flexible (KIP-482) from v4. v3+ replaces the
//! single `member_id` request field with a `members[]` batch
//! (KIP-345); v1+ adds `throttle_time_ms` on the response.
//!
//! Port of `archive/internal/protocol/codec/api/leave_group.go`.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::headers::HeaderVersion;
use crate::primitives::{read_i16, read_i32, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 5);
pub const MIN_FLEXIBLE: i16 = 4;

const fn header_for(version: i16) -> HeaderVersion {
    if version >= MIN_FLEXIBLE {
        HeaderVersion::V2
    } else {
        HeaderVersion::V1
    }
}

fn request_hdr(version: i16) -> HeaderVersion {
    header_for(version)
}

fn response_hdr(version: i16) -> HeaderVersion {
    header_for(version)
}

pub const SPEC: ApiSpec = ApiSpec {
    key: 13,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
    request_hdr,
    response_hdr,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub group_id: String,
    /// v0..=v2 single-member form. Empty at v3+.
    pub member_id: String,
    /// v3+ batch form.
    pub members: Vec<LeaveMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveMember {
    pub member_id: String,
    /// `None` ↔ wire null.
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v1+. `0` when unset.
    pub throttle_time_ms: i32,
    pub error_code: i16,
    /// v3+.
    pub members: Vec<LeaveMemberResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveMemberResponse {
    pub member_id: String,
    /// `None` ↔ wire null.
    pub group_instance_id: Option<String>,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let group_id = read_str(buf, flexible)?;
    if version <= 2 {
        let member_id = read_str(buf, flexible)?;
        if flexible {
            tagged::read(buf)?;
        }
        return Ok(Request {
            group_id,
            member_id,
            members: Vec::new(),
        });
    }
    let n = read_array_len(buf, flexible)?;
    let mut members = Vec::with_capacity(n);
    for _ in 0..n {
        let member_id = read_str(buf, flexible)?;
        let group_instance_id = read_nullable_str(buf, flexible)?;
        if flexible {
            tagged::read(buf)?;
        }
        members.push(LeaveMember {
            member_id,
            group_instance_id,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request {
        group_id,
        member_id: String::new(),
        members,
    })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    if version >= 1 {
        write_i32(buf, resp.throttle_time_ms);
    }
    write_i16(buf, resp.error_code);
    if version >= 3 {
        write_array_len(buf, resp.members.len(), flexible)?;
        for m in &resp.members {
            write_str(buf, &m.member_id, flexible)?;
            write_nullable_str(buf, m.group_instance_id.as_deref(), flexible)?;
            write_i16(buf, m.error_code);
            if flexible {
                tagged::write_empty(buf);
            }
        }
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut resp = Response::default();
    if version >= 1 {
        resp.throttle_time_ms = read_i32(buf)?;
    }
    resp.error_code = read_i16(buf)?;
    if version >= 3 {
        let n = read_array_len(buf, flexible)?;
        for _ in 0..n {
            let member_id = read_str(buf, flexible)?;
            let group_instance_id = read_nullable_str(buf, flexible)?;
            let error_code = read_i16(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            resp.members.push(LeaveMemberResponse {
                member_id,
                group_instance_id,
                error_code,
            });
        }
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_str(buf, &req.group_id, flexible)?;
    if version <= 2 {
        write_str(buf, &req.member_id, flexible)?;
        if flexible {
            tagged::write_empty(buf);
        }
        return Ok(());
    }
    write_array_len(buf, req.members.len(), flexible)?;
    for m in &req.members {
        write_str(buf, &m.member_id, flexible)?;
        write_nullable_str(buf, m.group_instance_id.as_deref(), flexible)?;
        if flexible {
            tagged::write_empty(buf);
        }
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request(version: i16) -> Request {
        let mut req = Request {
            group_id: "g1".to_owned(),
            ..Request::default()
        };
        if version <= 2 {
            req.member_id = "consumer-1".to_owned();
        } else {
            req.members = vec![
                LeaveMember {
                    member_id: "consumer-1".to_owned(),
                    group_instance_id: Some("inst-1".to_owned()),
                },
                LeaveMember {
                    member_id: "consumer-2".to_owned(),
                    group_instance_id: None,
                },
            ];
        }
        req
    }

    fn sample_response(version: i16) -> Response {
        let mut resp = Response::default();
        if version >= 3 {
            resp.members = vec![LeaveMemberResponse {
                member_id: "consumer-1".to_owned(),
                group_instance_id: Some("inst-1".to_owned()),
                error_code: 0,
            }];
        }
        resp
    }

    fn roundtrip(version: i16) {
        let req = sample_request(version);
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());

        let resp = sample_response(version);
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn v0_single_member() {
        roundtrip(0);
    }

    #[test]
    fn v3_batch_members() {
        roundtrip(3);
    }

    #[test]
    fn v4_is_flexible() {
        roundtrip(4);
    }

    #[test]
    fn v5_roundtrip() {
        roundtrip(5);
    }
}
