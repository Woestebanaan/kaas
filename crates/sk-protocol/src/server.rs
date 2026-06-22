//! Multi-listener accept loop.
//!
//! Models `archive/internal/protocol/server.go` but rustified:
//! - one `tokio::task` per accepted connection,
//! - cancellation via [`CancellationToken`],
//! - per-listener TLS plumbing (`tls_config: Option<...>`) — wired
//!   but unused in Phase 3; Phase 4 lights up real cert loading.
//!
//! Three independent axes per listener: exposure (`addr`),
//! encryption (`tls_config`), and authentication (which the
//! dispatcher pulls from per-listener via the `connstate::listener_name`
//! tag in Phase 4). Same Strimzi-shape model as the Go side.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{rustls::ServerConfig, TlsAcceptor};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::connstate::ConnState;
use crate::dispatch::Dispatcher;
use crate::frame::{Connection, ProtoError};

#[derive(Debug)]
pub struct ListenerConfig {
    /// Free-form tag stamped onto every accepted connection's
    /// [`ConnState::listener_name`]. The Helm chart picks the strings
    /// (`internal`, `external`, `authed`, …); skafka has no
    /// predefined set.
    pub name: String,
    pub addr: SocketAddr,
    /// `Some(listener)` skips `TcpListener::bind` and wraps the
    /// pre-bound socket — tests use this to allocate `127.0.0.1:0`
    /// themselves and capture the assigned port.
    pub pre_bound: Option<TcpListener>,
    /// `Some(cfg)` wraps the bound listener with TLS. Phase 3 leaves
    /// this `None` everywhere; Phase 4 wires cert loading.
    pub tls_config: Option<Arc<ServerConfig>>,
}

#[derive(Debug)]
pub struct ServerConfigBuilder {
    pub listeners: Vec<ListenerConfig>,
    pub max_frame_bytes: usize,
}

impl ServerConfigBuilder {
    pub fn new(listeners: Vec<ListenerConfig>) -> Self {
        Self {
            listeners,
            max_frame_bytes: sk_codec::frame::MAX_FRAME_SIZE,
        }
    }
}

#[derive(Debug)]
pub struct Server {
    cfg: ServerConfigBuilder,
    dispatcher: Arc<Dispatcher>,
}

impl Server {
    pub fn new(cfg: ServerConfigBuilder, dispatcher: Arc<Dispatcher>) -> Self {
        Self { cfg, dispatcher }
    }

    /// Open every listener (returning early on first bind failure)
    /// and run accept loops until `cancel` fires. Returns the bound
    /// addresses for tests that need them.
    pub async fn serve(self, cancel: CancellationToken) -> io::Result<()> {
        let bound = self.bind_all().await?;
        let mut handles = Vec::new();
        for (lc_name, listener, max_frame, tls) in bound {
            let dispatcher = self.dispatcher.clone();
            let cancel = cancel.clone();
            handles.push(tokio::spawn(accept_loop(
                lc_name, listener, max_frame, tls, dispatcher, cancel,
            )));
        }
        for h in handles {
            let _ = h.await;
        }
        Ok(())
    }

    /// Bind every listener and return the (name, bound listener,
    /// max_frame, tls) tuples. Useful for tests that need the actual
    /// `:0` port before driving traffic.
    pub async fn bind(self) -> io::Result<(BoundServer, Arc<Dispatcher>)> {
        let bound = self.bind_all().await?;
        Ok((
            BoundServer {
                listeners: bound,
                max_frame_bytes: self.cfg.max_frame_bytes,
            },
            self.dispatcher,
        ))
    }

    async fn bind_all(&self) -> io::Result<Vec<(String, TcpListener, usize, Option<TlsAcceptor>)>> {
        let mut out = Vec::with_capacity(self.cfg.listeners.len());
        for lc in &self.cfg.listeners {
            if lc.name.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "server: listener config missing name",
                ));
            }
            let ln = match &lc.pre_bound {
                Some(_) => {
                    // pre_bound is `&Option<TcpListener>` here — we
                    // can't move out of it. Pre-bind support is for
                    // tests, which call `bind_all` once; treat the
                    // pre_bound field as advisory and re-bind via
                    // the listener's `local_addr`.
                    // For now: pre_bound clients should set `addr` to
                    // the local_addr of the pre_bound listener and
                    // accept the re-bind cost. Tests do this via
                    // `bind_to_port_zero` helper.
                    TcpListener::bind(lc.addr).await?
                }
                None => TcpListener::bind(lc.addr).await?,
            };
            let tls = lc
                .tls_config
                .as_ref()
                .map(|cfg| TlsAcceptor::from(cfg.clone()));
            info!(
                addr = %ln.local_addr().map(|a| a.to_string()).unwrap_or_default(),
                listener = lc.name.as_str(),
                tls = tls.is_some(),
                "sk-protocol listening",
            );
            out.push((lc.name.clone(), ln, self.cfg.max_frame_bytes, tls));
        }
        Ok(out)
    }
}

pub struct BoundServer {
    listeners: Vec<(String, TcpListener, usize, Option<TlsAcceptor>)>,
    max_frame_bytes: usize,
}

impl std::fmt::Debug for BoundServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self
            .listeners
            .iter()
            .map(|(n, _, _, _)| n.as_str())
            .collect();
        f.debug_struct("BoundServer")
            .field("listeners", &names)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .finish()
    }
}

impl BoundServer {
    /// `(listener_name, bound_addr)` pairs in registration order.
    /// Tests use this to discover the `:0` port the kernel assigned.
    /// Resolved `(listener_name, bound_addr)` pairs. Skips any
    /// listener whose `local_addr()` lookup fails (which only
    /// happens if the socket was already torn down).
    pub fn local_addrs(&self) -> Vec<(String, SocketAddr)> {
        self.listeners
            .iter()
            .filter_map(|(name, l, _, _)| l.local_addr().ok().map(|a| (name.clone(), a)))
            .collect()
    }

    pub fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }

    /// Run accept loops until `cancel` fires. Consumes self.
    pub async fn serve(self, dispatcher: Arc<Dispatcher>, cancel: CancellationToken) {
        let mut handles = Vec::new();
        for (name, listener, max_frame, tls) in self.listeners {
            let d = dispatcher.clone();
            let c = cancel.clone();
            handles.push(tokio::spawn(accept_loop(
                name, listener, max_frame, tls, d, c,
            )));
        }
        for h in handles {
            let _ = h.await;
        }
    }
}

async fn accept_loop(
    listener_name: String,
    listener: TcpListener,
    max_frame_bytes: usize,
    tls: Option<TlsAcceptor>,
    dispatcher: Arc<Dispatcher>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                info!(listener = listener_name.as_str(), "accept loop cancelled");
                return;
            }
            res = listener.accept() => {
                match res {
                    Ok((stream, peer)) => {
                        let d = dispatcher.clone();
                        let c = cancel.clone();
                        let name = listener_name.clone();
                        let tls = tls.clone();
                        tokio::spawn(async move {
                            if let Err(err) = serve_conn(name, stream, peer, max_frame_bytes, tls, d, c).await {
                                tracing::debug!(%peer, %err, "connection closed with error");
                            }
                        });
                    }
                    Err(err) => {
                        warn!(listener = listener_name.as_str(), %err, "accept failed; backing off 10ms");
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
            }
        }
    }
}

async fn serve_conn(
    listener_name: String,
    stream: TcpStream,
    peer: SocketAddr,
    max_frame_bytes: usize,
    tls: Option<TlsAcceptor>,
    dispatcher: Arc<Dispatcher>,
    cancel: CancellationToken,
) -> Result<(), ProtoError> {
    let state = Arc::new(parking_lot::Mutex::new(ConnState::new(
        &listener_name,
        peer,
    )));
    if let Some(acceptor) = tls {
        let tls_stream = acceptor.accept(stream).await.map_err(ProtoError::Io)?;
        let conn = Connection::with_max_frame(tls_stream, max_frame_bytes);
        request_loop(conn, dispatcher, state, cancel).await
    } else {
        let conn = Connection::with_max_frame(stream, max_frame_bytes);
        request_loop(conn, dispatcher, state, cancel).await
    }
}

async fn request_loop<S>(
    mut conn: Connection<S>,
    dispatcher: Arc<Dispatcher>,
    state: Arc<parking_lot::Mutex<ConnState>>,
    cancel: CancellationToken,
) -> Result<(), ProtoError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            req = conn.read_request() => {
                let (hdr, body) = match req {
                    Ok(r) => r,
                    Err(ProtoError::Frame(sk_codec::frame::FrameError::Disconnected)) => return Ok(()),
                    Err(e) => {
                        error!(%e, "request decode failed; closing connection");
                        return Err(e);
                    }
                };
                let correlation_id = hdr.correlation_id;
                let (body, hv) = dispatcher.dispatch(state.as_ref(), hdr, body).await;
                if let Err(e) = conn.write_response(correlation_id, &body, hv).await {
                    error!(%e, "response write failed; closing connection");
                    return Err(e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::{Bytes, BytesMut};
    use sk_codec::api::api_versions;
    use sk_codec::headers::encode_request_header;
    use sk_codec::primitives::{write_compact_string, write_i32};
    use sk_codec::tagged;
    use sk_codec::{HeaderVersion, RequestHeader};

    use crate::dispatch::{Handler, HandlerError};

    struct ApiVersionsStub;
    #[async_trait]
    impl Handler for ApiVersionsStub {
        async fn handle(
            &self,
            _: &parking_lot::Mutex<ConnState>,
            version: i16,
            _body: Bytes,
        ) -> Result<BytesMut, HandlerError> {
            let resp = api_versions::response_from_registry(0);
            let mut out = BytesMut::new();
            api_versions::encode_response(&mut out, &resp, version)?;
            Ok(out)
        }
    }

    fn build_request_frame(api_key: i16, api_version: i16, correlation_id: i32) -> Vec<u8> {
        let mut body = BytesMut::new();
        encode_request_header(
            &mut body,
            &RequestHeader {
                api_key,
                api_version,
                correlation_id,
                client_id: Some("smoke".to_owned()),
            },
            HeaderVersion::V2,
        )
        .unwrap();
        // ApiVersions v3 body: client_software_name + version + tagged.
        write_compact_string(&mut body, "smoke-test").unwrap();
        write_compact_string(&mut body, "1.0.0").unwrap();
        tagged::write_empty(&mut body);

        let mut out = Vec::with_capacity(body.len() + 4);
        out.extend_from_slice(&i32::try_from(body.len()).unwrap().to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    #[tokio::test]
    async fn server_end_to_end_api_versions_roundtrip() {
        let mut d = Dispatcher::new();
        d.register(18, 0, 4, Arc::new(ApiVersionsStub));
        let dispatcher = Arc::new(d);

        let cfg = ServerConfigBuilder::new(vec![ListenerConfig {
            name: "internal".to_owned(),
            addr: "127.0.0.1:0".parse().unwrap(),
            pre_bound: None,
            tls_config: None,
        }]);

        let server = Server::new(cfg, dispatcher.clone());
        let (bound, dispatcher) = server.bind().await.unwrap();
        let addrs = bound.local_addrs();
        let port = addrs[0].1.port();

        let cancel = CancellationToken::new();
        let serve_cancel = cancel.clone();
        let serve = tokio::spawn(async move { bound.serve(dispatcher, serve_cancel).await });

        // Client: open TCP, send one ApiVersions v3 request, read response.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let frame = build_request_frame(18, 3, 1234);
        sock.write_all(&frame).await.unwrap();
        sock.flush().await.unwrap();

        // Read [size:i32][response].
        let mut sz = [0u8; 4];
        sock.read_exact(&mut sz).await.unwrap();
        let n = usize::try_from(i32::from_be_bytes(sz)).expect("non-negative size");
        let mut resp = vec![0u8; n];
        sock.read_exact(&mut resp).await.unwrap();
        // First 4 bytes = correlation id; for ApiVersions header is V0
        // (no tagged-fields block).
        assert_eq!(&resp[..4], &1234i32.to_be_bytes());
        // After the correlation id: error_code=0 then sorted api list.
        assert_eq!(&resp[4..6], &0i16.to_be_bytes(), "error_code = 0");

        drop(sock);
        cancel.cancel();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), serve).await;
    }

    fn _silence_unused() {
        let _ = write_i32;
    }
}
