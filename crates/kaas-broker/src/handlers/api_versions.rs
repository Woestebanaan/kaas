//! ApiVersions handler (key 18).
//!
//! Builds the response from [`kaas_codec::api::registry::ALL`] so the
//! client sees every key the broker actually has wired up — no
//! per-version bookkeeping per handler.

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_codec::api::api_versions;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

#[derive(Debug, Default)]
pub struct ApiVersionsHandler;

impl ApiVersionsHandler {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Handler for ApiVersionsHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        _body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let resp = api_versions::response_from_registry(0);
        let mut out = BytesMut::new();
        api_versions::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    #[tokio::test]
    async fn response_contains_six_phase3_keys() {
        let body = ApiVersionsHandler::new()
            .handle(&conn(), 3, Bytes::new())
            .await
            .unwrap();
        let mut r = body.freeze();
        let resp = api_versions::decode_response(&mut r, 3).unwrap();
        assert_eq!(resp.error_code, 0);
        let keys: Vec<i16> = resp.api_versions.iter().map(|v| v.api_key).collect();
        for expected in [0, 1, 2, 3, 18, 22] {
            assert!(keys.contains(&expected), "missing key {expected}");
        }
    }
}
