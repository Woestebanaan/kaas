//! Controller-side `ControllerHeartbeat` gRPC server.
//!
//! Port of `archive/internal/controller/heartbeat_server.go`. The
//! Go side wires the bidirectional `Stream` rpc onto a `sync.Map`
//! of per-broker `clientState`; we use a `parking_lot::Mutex<HashMap>`
//! over the same shape. Each connected broker's send half is a
//! bounded `tokio::sync::mpsc::Sender<ControllerCommand>` — slow
//! consumers get their `ASSIGNMENT_CHANGED` push dropped (the 1 s
//! mtime poll picks up the change as a safety net) instead of
//! stalling the send-loop and back-pressuring every other broker
//! (gh #77 reactive-rebalance vector).
//!
//! Three public surfaces are exposed via the [`HeartbeatServer`]
//! struct (registered into a tonic server by
//! `sk_broker::heartbeatpb::controller_heartbeat_server::ControllerHeartbeatServer::new`):
//!
//! - [`HeartbeatServer::push_assignment_changed`] —
//!   AssignmentLoop hook; broadcasts a fresh assignment version.
//! - [`HeartbeatServer::active_groups`] — union over every
//!   connected broker's most-recent
//!   `BrokerStatus.active_groups[]`. Feeds the assignment-loop's
//!   `GroupSource`.
//! - [`HeartbeatServer::connected_brokers`] — diagnostic snapshot
//!   used by the K8s mirror + `BrokerSource` impls.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use sk_broker::heartbeatpb::controller_command::Type as ControllerCmdType;
use sk_broker::heartbeatpb::controller_heartbeat_server::ControllerHeartbeat;
use sk_broker::heartbeatpb::{BrokerStatus, ControllerCommand};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

/// Default 1 s ping cadence — matches the Go side's
/// `time.NewTicker(time.Second)`.
const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(1);

/// Per-direction mpsc buffer size. 4 slots is enough for one
/// `ASSIGNMENT_CHANGED` + a few PINGs in flight; matches Go's
/// `make(chan ..., 4)`. Slow consumers drop their push and pick
/// up the change via the 1 s file poll.
const SEND_BUFFER: usize = 4;

/// State the server keeps per connected broker.
struct ClientState {
    send: mpsc::Sender<ControllerCommand>,
    last_seen_ms: i64,
    last_seen_assignment_version: u64,
    active_groups: Vec<String>,
    last_broker_ts_ms: i64,
}

impl std::fmt::Debug for ClientState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientState")
            .field("last_seen_ms", &self.last_seen_ms)
            .field(
                "last_seen_assignment_version",
                &self.last_seen_assignment_version,
            )
            .field("active_groups", &self.active_groups.len())
            .finish()
    }
}

#[derive(Debug)]
pub struct HeartbeatServer {
    ping_interval: Duration,
    /// Most-recently-pushed assignment version. Re-sent to fresh
    /// connections so a broker reconnecting catches up immediately.
    pending_version: Mutex<u64>,
    clients: Mutex<HashMap<String, Arc<Mutex<ClientState>>>>,
}

impl HeartbeatServer {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            ping_interval: DEFAULT_PING_INTERVAL,
            pending_version: Mutex::new(0),
            clients: Mutex::new(HashMap::new()),
        })
    }

    /// Override the ping cadence (test hook).
    pub fn with_ping_interval(mut self: Arc<Self>, d: Duration) -> Arc<Self> {
        // Arc::get_mut works only when this Arc is unique. The
        // typical wiring is `let s = HeartbeatServer::new(); let s
        // = s.with_ping_interval(...);` immediately after
        // construction.
        if let Some(inner) = Arc::get_mut(&mut self) {
            inner.ping_interval = d;
        }
        self
    }

    /// Broadcast `ASSIGNMENT_CHANGED(version)` to every connected
    /// broker. Best-effort: slow consumers (mpsc full) get the push
    /// dropped — the 1 s mtime poll catches the change.
    pub fn push_assignment_changed(&self, version: u64) {
        *self.pending_version.lock() = version;
        let clients: Vec<Arc<Mutex<ClientState>>> = self.clients.lock().values().cloned().collect();
        let cmd = ControllerCommand {
            timestamp_ms: now_ms(),
            r#type: ControllerCmdType::AssignmentChanged.into(),
            assignment_version: version,
            broker_status_timestamp_ms: 0,
        };
        for c in clients {
            let send = c.lock().send.clone();
            // try_send so a full buffer doesn't block the broadcast.
            let _ = send.try_send(cmd);
        }
        // One counter per broadcast, not per recipient. Alerting cares
        // about "did we tell peers" — the per-recipient drop rate is
        // observable via the client-side heartbeat.rtt.
        sk_observability::metrics::global()
            .assignment_pushes
            .add(1, &[]);
    }

    /// Notify every broker that the controller is shutting down
    /// gracefully. Brokers switch to looking for a new controller
    /// via the Lease informer rather than waiting for heartbeat
    /// timeout.
    pub fn push_leaving(&self) {
        let clients: Vec<Arc<Mutex<ClientState>>> = self.clients.lock().values().cloned().collect();
        let cmd = ControllerCommand {
            timestamp_ms: now_ms(),
            r#type: ControllerCmdType::Leaving.into(),
            assignment_version: 0,
            broker_status_timestamp_ms: 0,
        };
        for c in clients {
            let send = c.lock().send.clone();
            let _ = send.try_send(cmd);
        }
    }

    /// Snapshot of currently-connected broker IDs.
    pub fn connected_brokers(&self) -> Vec<String> {
        self.clients.lock().keys().cloned().collect()
    }

    /// Union of consumer-group IDs every connected broker reports
    /// in `BrokerStatus.active_groups`. Feeds the assignment loop's
    /// `GroupSource`.
    pub fn active_groups(&self) -> Vec<String> {
        let mut seen: HashSet<String> = HashSet::new();
        for state in self.clients.lock().values() {
            for g in &state.lock().active_groups {
                seen.insert(g.clone());
            }
        }
        seen.into_iter().collect()
    }

    /// Wall-clock (ms since epoch) of the most recent BrokerStatus
    /// from `broker_id`. `None` when the broker is not currently
    /// connected.
    pub fn broker_last_seen(&self, broker_id: &str) -> Option<i64> {
        self.clients
            .lock()
            .get(broker_id)
            .map(|c| c.lock().last_seen_ms)
    }
}

/// tonic-facing wrapper around an `Arc<HeartbeatServer>`. The
/// orphan rule blocks `impl ControllerHeartbeat for Arc<T>` since
/// both sides live outside this crate; this newtype keeps the
/// impl local. `Clone` is `Arc::clone` — cheap.
#[derive(Debug, Clone)]
pub struct HeartbeatService(pub Arc<HeartbeatServer>);

impl HeartbeatService {
    pub fn new(inner: Arc<HeartbeatServer>) -> Self {
        Self(inner)
    }
}

#[tonic::async_trait]
impl ControllerHeartbeat for HeartbeatService {
    type StreamStream = Pin<Box<dyn Stream<Item = Result<ControllerCommand, Status>> + Send>>;

    async fn stream(
        &self,
        request: Request<Streaming<BrokerStatus>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let me = &self.0;
        let mut inbound = request.into_inner();

        // First message carries the broker identity.
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("missing first BrokerStatus"))?;
        let broker_id = first.broker_id.clone();
        if broker_id.is_empty() {
            return Err(Status::invalid_argument(
                "first BrokerStatus must carry broker_id",
            ));
        }

        let (tx, rx) = mpsc::channel::<ControllerCommand>(SEND_BUFFER);
        let state = Arc::new(Mutex::new(ClientState {
            send: tx,
            last_seen_ms: now_ms(),
            last_seen_assignment_version: first.last_seen_assignment_version,
            active_groups: first.active_groups.clone(),
            last_broker_ts_ms: first.timestamp_ms,
        }));
        // Replace any prior stream for the same broker — reconnect
        // wins.
        me.clients.lock().insert(broker_id.clone(), state.clone());

        // Catch up the new client with the most-recent assignment
        // version so a reconnect doesn't have to wait for the next
        // push.
        let pending = *me.pending_version.lock();
        if pending > 0 {
            let cmd = ControllerCommand {
                timestamp_ms: now_ms(),
                r#type: ControllerCmdType::AssignmentChanged.into(),
                assignment_version: pending,
                broker_status_timestamp_ms: first.timestamp_ms,
            };
            let _ = state.lock().send.try_send(cmd);
        }

        // Spawn recv loop.
        let me_recv = self.0.clone();
        let state_recv = state.clone();
        let bid_recv = broker_id.clone();
        tokio::spawn(async move {
            while let Some(msg) = inbound.next().await {
                let Ok(msg) = msg else { break };
                let mut s = state_recv.lock();
                s.last_seen_ms = now_ms();
                s.last_seen_assignment_version = msg.last_seen_assignment_version;
                s.active_groups = msg.active_groups;
                s.last_broker_ts_ms = msg.timestamp_ms;
            }
            // Remove this client on stream end, but only if we're
            // still the registered state — a reconnect during this
            // task's lifetime may have replaced us.
            let mut map = me_recv.clients.lock();
            if let Some(cur) = map.get(&bid_recv) {
                if Arc::ptr_eq(cur, &state_recv) {
                    map.remove(&bid_recv);
                }
            }
            debug!(broker = bid_recv.as_str(), "heartbeat stream closed");
        });

        // Send loop — fan in mpsc commands + a periodic PING.
        let ping_interval = self.0.ping_interval;
        let state_send = state.clone();
        let outbound = async_stream::stream! {
            let mut rx = ReceiverStream::new(rx);
            let mut tick = tokio::time::interval(ping_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    cmd = rx.next() => match cmd {
                        Some(c) => yield Ok(c),
                        None => break,
                    },
                    _ = tick.tick() => {
                        let echo = state_send.lock().last_broker_ts_ms;
                        yield Ok(ControllerCommand {
                            timestamp_ms: now_ms(),
                            r#type: ControllerCmdType::Ping .into(),
                            assignment_version: 0,
                            broker_status_timestamp_ms: echo,
                        });
                    }
                }
            }
        };
        let boxed: Self::StreamStream = Box::pin(outbound);
        Ok(Response::new(boxed))
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_server_has_no_clients() {
        let s = HeartbeatServer::new();
        assert!(s.connected_brokers().is_empty());
        assert!(s.active_groups().is_empty());
        assert_eq!(s.broker_last_seen("skafka-0"), None);
    }

    #[test]
    fn push_assignment_changed_updates_pending_version() {
        let s = HeartbeatServer::new();
        s.push_assignment_changed(7);
        assert_eq!(*s.pending_version.lock(), 7);
    }

    #[test]
    fn push_leaving_against_zero_clients_is_no_op() {
        let s = HeartbeatServer::new();
        s.push_leaving();
        assert!(s.connected_brokers().is_empty());
    }
}
