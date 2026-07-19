//! DescribeLogDirs — API key 35.
//!
//! Versions 0..=1, both non-flexible (flexible encoding starts at v2,
//! which also adds TotalBytes/UsableBytes — clients negotiate down
//! from the max we advertise). A null topics array on the request
//! means "describe every log dir, every topic".

use bytes::BytesMut;

use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::headers::HeaderVersion;
use crate::primitives::{
    read_bool, read_i16, read_i32, read_i64, read_string, write_bool, write_i16, write_i32,
    write_i64, write_string,
};
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 1);

fn request_hdr(_version: i16) -> HeaderVersion {
    HeaderVersion::V1
}

fn response_hdr(_version: i16) -> HeaderVersion {
    HeaderVersion::V0
}

pub const SPEC: ApiSpec = ApiSpec {
    key: 35,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: None,
    request_hdr,
    response_hdr,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    /// `None` ↔ the request's topics array was null ("describe
    /// everything").
    pub topics: Option<Vec<RequestTopic>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub results: Vec<LogDirResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogDirResult {
    pub error_code: i16,
    pub log_dir: String,
    pub topics: Vec<ResponseTopic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseTopic {
    pub name: String,
    pub partitions: Vec<ResponsePartition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResponsePartition {
    pub partition_index: i32,
    pub partition_size: i64,
    pub offset_lag: i64,
    pub is_future_key: bool,
}

pub fn decode_request(buf: &mut Bytes, _version: i16) -> Result<Request, CodecError> {
    let count = read_i32(buf)?;
    if count < 0 {
        return Ok(Request { topics: None });
    }
    let mut topics = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
    for _ in 0..count {
        let name = read_string(buf)?;
        let p_count = read_i32(buf)?;
        let mut partitions = Vec::with_capacity(usize::try_from(p_count).unwrap_or(0));
        for _ in 0..p_count {
            partitions.push(read_i32(buf)?);
        }
        topics.push(RequestTopic { name, partitions });
    }
    Ok(Request {
        topics: Some(topics),
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, _version: i16) -> Result<(), CodecError> {
    let Some(topics) = req.topics.as_ref() else {
        write_i32(buf, -1);
        return Ok(());
    };
    write_i32(buf, i32::try_from(topics.len()).unwrap_or(i32::MAX));
    for t in topics {
        write_string(buf, &t.name)?;
        write_i32(buf, i32::try_from(t.partitions.len()).unwrap_or(i32::MAX));
        for p in &t.partitions {
            write_i32(buf, *p);
        }
    }
    Ok(())
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    _version: i16,
) -> Result<(), CodecError> {
    write_i32(buf, resp.throttle_time_ms);
    write_i32(buf, i32::try_from(resp.results.len()).unwrap_or(i32::MAX));
    for r in &resp.results {
        write_i16(buf, r.error_code);
        write_string(buf, &r.log_dir)?;
        write_i32(buf, i32::try_from(r.topics.len()).unwrap_or(i32::MAX));
        for t in &r.topics {
            write_string(buf, &t.name)?;
            write_i32(buf, i32::try_from(t.partitions.len()).unwrap_or(i32::MAX));
            for p in &t.partitions {
                write_i32(buf, p.partition_index);
                write_i64(buf, p.partition_size);
                write_i64(buf, p.offset_lag);
                write_bool(buf, p.is_future_key);
            }
        }
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, _version: i16) -> Result<Response, CodecError> {
    let throttle_time_ms = read_i32(buf)?;
    let n = read_i32(buf)?;
    let mut results = Vec::with_capacity(usize::try_from(n).unwrap_or(0));
    for _ in 0..n {
        let error_code = read_i16(buf)?;
        let log_dir = read_string(buf)?;
        let nt = read_i32(buf)?;
        let mut topics = Vec::with_capacity(usize::try_from(nt).unwrap_or(0));
        for _ in 0..nt {
            let name = read_string(buf)?;
            let np = read_i32(buf)?;
            let mut partitions = Vec::with_capacity(usize::try_from(np).unwrap_or(0));
            for _ in 0..np {
                partitions.push(ResponsePartition {
                    partition_index: read_i32(buf)?,
                    partition_size: read_i64(buf)?,
                    offset_lag: read_i64(buf)?,
                    is_future_key: read_bool(buf)?,
                });
            }
            topics.push(ResponseTopic { name, partitions });
        }
        results.push(LogDirResult {
            error_code,
            log_dir,
            topics,
        });
    }
    Ok(Response {
        throttle_time_ms,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(version: i16) {
        let req = Request {
            topics: Some(vec![RequestTopic {
                name: "orders".into(),
                partitions: vec![0, 1, 2],
            }]),
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());

        let resp = Response {
            throttle_time_ms: 0,
            results: vec![LogDirResult {
                error_code: 0,
                log_dir: "/data".into(),
                topics: vec![ResponseTopic {
                    name: "orders".into(),
                    partitions: vec![ResponsePartition {
                        partition_index: 0,
                        partition_size: 4096,
                        offset_lag: 0,
                        is_future_key: false,
                    }],
                }],
            }],
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn v0_roundtrip() {
        roundtrip(0);
    }

    #[test]
    fn v1_roundtrip() {
        roundtrip(1);
    }

    #[test]
    fn null_topics_means_describe_everything() {
        let mut w = BytesMut::new();
        encode_request(&mut w, &Request { topics: None }, 0).unwrap();
        assert_eq!(&w[..], &(-1i32).to_be_bytes());
        let mut r = w.freeze();
        let got = decode_request(&mut r, 0).unwrap();
        assert_eq!(got.topics, None);
    }
}
