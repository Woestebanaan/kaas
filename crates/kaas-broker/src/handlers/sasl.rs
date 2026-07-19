//! SASL handlers — `SaslHandshake` (17) + `SaslAuthenticate` (36).
//!
//! `SaslHandshakeHandler` advertises the broker's enabled mechanisms
//! and stamps the picked mechanism on `ConnState`. The
//! `SaslAuthenticateHandler` drives the per-listener engine's
//! `SaslExchange` state machine until `done = true`, then stamps
//! `principal` + `sasl_done` so the dispatcher's pre-auth gate stops
//! rejecting subsequent requests.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use kaas_auth::selector::AuthEngineSelector;
use kaas_codec::api::{sasl_authenticate, sasl_handshake};
use kaas_protocol::{ConnState, Handler, HandlerError};

// Wire error codes — same numeric values Apache uses.
const ERR_NETWORK_EXCEPTION: i16 = 13;
const ERR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const ERR_SASL_AUTHENTICATION_FAILED: i16 = 58;

/// Mechanisms advertised on the `SaslHandshake` response. Order
/// matters: clients pick the first matching entry.
const MECHANISMS: &[&str] = &["SCRAM-SHA-512", "PLAIN"];

#[derive(Debug)]
pub struct SaslHandshakeHandler {
    mechanisms: Vec<String>,
}

impl SaslHandshakeHandler {
    pub fn new() -> Self {
        Self {
            mechanisms: MECHANISMS.iter().map(|s| (*s).to_owned()).collect(),
        }
    }
}

impl Default for SaslHandshakeHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Handler for SaslHandshakeHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = sasl_handshake::decode_request(&mut body, version)?;

        let supported = self.mechanisms.iter().any(|m| m == &req.mechanism);
        let error_code = if supported {
            // Record the picked mechanism so SaslAuthenticate
            // instantiates the right exchange on the first call.
            conn.lock().sasl_mechanism = Some(req.mechanism.clone());
            0
        } else {
            ERR_UNSUPPORTED_SASL_MECHANISM
        };

        let resp = sasl_handshake::Response {
            error_code,
            mechanisms: self.mechanisms.clone(),
        };
        let mut out = BytesMut::new();
        sasl_handshake::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[derive(Debug)]
pub struct SaslAuthenticateHandler {
    engines: Arc<dyn AuthEngineSelector>,
}

impl SaslAuthenticateHandler {
    pub fn new(engines: Arc<dyn AuthEngineSelector>) -> Self {
        Self { engines }
    }
}

#[async_trait]
impl Handler for SaslAuthenticateHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = sasl_authenticate::decode_request(&mut body, version)?;
        let auth_bytes = req.auth_bytes.unwrap_or_default();

        let (listener_name, mechanism, is_tls, has_state) = {
            let cs = conn.lock();
            (
                cs.listener_name.clone(),
                cs.sasl_mechanism.clone(),
                cs.is_tls,
                cs.sasl_state.is_some(),
            )
        };

        // Reject PLAIN over a non-TLS connection. Password lives in the bytes the
        // client is about to send; refuse before instantiating the
        // exchange.
        if let Some(mech) = mechanism.as_deref() {
            if mech == "PLAIN" && !is_tls {
                return encode_err(
                    version,
                    ERR_NETWORK_EXCEPTION,
                    Some("PLAIN mechanism requires TLS".to_owned()),
                );
            }
        }

        // Instantiate the exchange on the first call. Default to
        // SCRAM-SHA-512 if the client skipped the handshake — the
        // long-standing permissive default.
        if !has_state {
            let mech = mechanism.as_deref().unwrap_or("SCRAM-SHA-512");
            let eng = self.engines.for_listener(&listener_name);
            match eng.new_sasl_exchange(mech) {
                Ok(state) => conn.lock().sasl_state = Some(state),
                Err(err) => {
                    tracing::warn!(%err, listener = listener_name.as_str(), mechanism = mech, "sasl: unknown mechanism");
                    return encode_err(
                        version,
                        ERR_UNSUPPORTED_SASL_MECHANISM,
                        Some(err.to_string()),
                    );
                }
            }
        }

        // Step the exchange. We take the state out under the lock so
        // the async-step call doesn't hold parking_lot.
        let mut state = match conn.lock().sasl_state.take() {
            Some(s) => s,
            None => {
                return encode_err(
                    version,
                    ERR_NETWORK_EXCEPTION,
                    Some("missing SASL state".to_owned()),
                );
            }
        };

        let (server_msg, done) = match state.step(&auth_bytes) {
            Ok(out) => out,
            Err(err) => {
                tracing::warn!(%err, listener = listener_name.as_str(), "sasl: step failed");
                // Don't put the failed state back — a future request
                // hitting `sasl_state.is_some()` would re-enter step
                // and confuse the state machine.
                return encode_err(
                    version,
                    ERR_SASL_AUTHENTICATION_FAILED,
                    Some(err.to_string()),
                );
            }
        };

        if done {
            let principal = state.principal().cloned();
            let mut cs = conn.lock();
            cs.principal = principal;
            cs.sasl_done = true;
            // Drop the exchange — it's consumed.
        } else {
            // Multi-step mechanism (SCRAM); put state back for the
            // next round trip.
            conn.lock().sasl_state = Some(state);
        }

        let resp = sasl_authenticate::Response {
            error_code: 0,
            error_message: None,
            auth_bytes: Bytes::from(server_msg),
            session_lifetime_ms: 0,
        };
        let mut out = BytesMut::new();
        sasl_authenticate::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

fn encode_err(
    version: i16,
    error_code: i16,
    message: Option<String>,
) -> Result<BytesMut, HandlerError> {
    let resp = sasl_authenticate::Response {
        error_code,
        error_message: message,
        auth_bytes: Bytes::new(),
        session_lifetime_ms: 0,
    };
    let mut out = BytesMut::new();
    sasl_authenticate::encode_response(&mut out, &resp, version)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaas_auth::credentials::TestCred;
    use kaas_auth::engine::RealAuthEngine;
    use kaas_auth::selector::SingleAuthEngine;
    use kaas_auth::{AuthEngine, CredentialLoader, PrincipalMapper};
    use kaas_codec::api::common::write_str;
    use kaas_codec::primitives::write_compact_bytes;
    use kaas_codec::tagged;
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn(is_tls: bool) -> Mutex<ConnState> {
        let mut cs = ConnState::new("authed", SocketAddr::from_str("127.0.0.1:9092").unwrap());
        cs.is_tls = is_tls;
        Mutex::new(cs)
    }

    fn selector_with_real(creds: Arc<CredentialLoader>) -> Arc<dyn AuthEngineSelector> {
        let engine: Arc<dyn AuthEngine> = Arc::new(RealAuthEngine::new(
            creds,
            Arc::new(PrincipalMapper::default()),
        ));
        Arc::new(SingleAuthEngine::new(engine))
    }

    fn plain_creds(username: &str, password: &str) -> Arc<CredentialLoader> {
        let l = CredentialLoader::new("/tmp/sasl-handler-test");
        l.install_for_test(vec![TestCred {
            username: username.to_owned(),
            auth_type: "plain".to_owned(),
            plain_password: Some(password.to_owned()),
            ..TestCred::default()
        }]);
        Arc::new(l)
    }

    fn handshake_body(mechanism: &str) -> Bytes {
        // v1 (non-flexible): just a non-compact string.
        let mut w = BytesMut::new();
        write_str(&mut w, mechanism, false).unwrap();
        w.freeze()
    }

    fn authenticate_body_v2(payload: &[u8]) -> Bytes {
        // v2 is flexible: compact-nullable-bytes then empty tagged.
        let mut w = BytesMut::new();
        write_compact_bytes(&mut w, payload).unwrap();
        tagged::write_empty(&mut w);
        w.freeze()
    }

    #[tokio::test]
    async fn handshake_known_mechanism_accepted() {
        let h = SaslHandshakeHandler::new();
        let c = conn(false);
        let out = h
            .handle(&c, 1, handshake_body("SCRAM-SHA-512"))
            .await
            .unwrap();
        let mut r = out.freeze();
        let resp = sasl_handshake::decode_response(&mut r, 1).unwrap();
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.mechanisms, vec!["SCRAM-SHA-512", "PLAIN"]);
        assert_eq!(c.lock().sasl_mechanism.as_deref(), Some("SCRAM-SHA-512"));
    }

    #[tokio::test]
    async fn handshake_unknown_mechanism_returns_33() {
        let h = SaslHandshakeHandler::new();
        let c = conn(false);
        let out = h.handle(&c, 1, handshake_body("GSSAPI")).await.unwrap();
        let mut r = out.freeze();
        let resp = sasl_handshake::decode_response(&mut r, 1).unwrap();
        assert_eq!(resp.error_code, ERR_UNSUPPORTED_SASL_MECHANISM);
        // mechanism NOT stamped on conn — client must retry handshake.
        assert!(c.lock().sasl_mechanism.is_none());
    }

    #[tokio::test]
    async fn plain_over_non_tls_returns_network_exception() {
        let sel = selector_with_real(plain_creds("svc", "hunter2"));
        let h = SaslAuthenticateHandler::new(sel);
        let c = conn(false);
        c.lock().sasl_mechanism = Some("PLAIN".to_owned());
        // Payload doesn't matter — we reject before reading it.
        let out = h
            .handle(&c, 2, authenticate_body_v2(b"\0svc\0hunter2"))
            .await
            .unwrap();
        let mut r = out.freeze();
        let resp = sasl_authenticate::decode_response(&mut r, 2).unwrap();
        assert_eq!(resp.error_code, ERR_NETWORK_EXCEPTION);
        assert!(!c.lock().sasl_done);
    }

    #[tokio::test]
    async fn plain_over_tls_completes() {
        let sel = selector_with_real(plain_creds("svc", "hunter2"));
        let h = SaslAuthenticateHandler::new(sel);
        let c = conn(true);
        c.lock().sasl_mechanism = Some("PLAIN".to_owned());
        let out = h
            .handle(&c, 2, authenticate_body_v2(b"\0svc\0hunter2"))
            .await
            .unwrap();
        let mut r = out.freeze();
        let resp = sasl_authenticate::decode_response(&mut r, 2).unwrap();
        assert_eq!(resp.error_code, 0);
        let cs = c.lock();
        assert!(cs.sasl_done);
        assert_eq!(cs.principal.as_ref().unwrap().name, "svc");
    }

    #[tokio::test]
    async fn plain_bad_password_returns_58() {
        let sel = selector_with_real(plain_creds("svc", "right"));
        let h = SaslAuthenticateHandler::new(sel);
        let c = conn(true);
        c.lock().sasl_mechanism = Some("PLAIN".to_owned());
        let out = h
            .handle(&c, 2, authenticate_body_v2(b"\0svc\0wrong"))
            .await
            .unwrap();
        let mut r = out.freeze();
        let resp = sasl_authenticate::decode_response(&mut r, 2).unwrap();
        assert_eq!(resp.error_code, ERR_SASL_AUTHENTICATION_FAILED);
        assert!(!c.lock().sasl_done);
    }
}
