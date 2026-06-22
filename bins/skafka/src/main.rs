//! skafka broker binary.
//!
//! Phase 3: single-broker, no auth, no cluster. Listens on every
//! configured listener, serves Produce / Fetch / ListOffsets /
//! Metadata / ApiVersions / InitProducerId against either an
//! on-disk or in-memory storage engine.
//!
//! Storage selection:
//! - `SKAFKA_DATA_DIR` set → `DiskStorageEngine` rooted there.
//! - unset → `MemoryStorage` (dev mode; same path the legacy Go
//!   broker takes when `MY_POD_NAME` is unset).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use sk_broker::{
    ApiVersionsHandler, Broker, Cli, FetchHandler, InitProducerIdHandler, ListOffsetsHandler,
    MetadataHandler, ProduceHandler, TopicRegistry,
};
use sk_protocol::{Dispatcher, ListenerConfig, Server, ServerConfigBuilder};
use sk_storage::{DiskStorageEngine, MemoryStorage, PartitionConfig, RealFs, StorageEngine};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::from_env().context("parsing SKAFKA_* env")?;
    init_tracing(&cli.log_level);
    info!(
        broker_id = cli.broker_id,
        cluster_id = cli.cluster_id.as_str(),
        listeners = cli.listeners.len(),
        data_dir = ?cli.data_dir,
        "skafka starting",
    );

    let engine = build_engine(cli.data_dir.clone(), cli.flush_interval_messages)?;
    let topics =
        Arc::new(TopicRegistry::from_env_json(&cli.topics_seed).context("parsing SKAFKA_TOPICS")?);
    if topics.is_empty() {
        warn!(
            "SKAFKA_TOPICS is empty — broker will serve metadata for zero topics. \
             Set the env var to a JSON array, e.g. [{{\"name\":\"t1\",\"partitions\":1}}]"
        );
    }
    let broker = Arc::new(Broker::new(
        engine.clone(),
        topics,
        cli.cluster_id.clone(),
        cli.broker_id,
    ));

    let dispatcher = build_dispatcher(broker.clone(), &cli.listeners);

    let listeners = parse_listeners(&cli.listeners)?;
    let server = Server::new(ServerConfigBuilder::new(listeners), Arc::new(dispatcher));

    let cancel = CancellationToken::new();
    let serve_cancel = cancel.clone();
    let serve = tokio::spawn(async move { server.serve(serve_cancel).await });

    wait_for_shutdown_signal().await?;
    info!("shutdown signal received; cancelling listeners");
    cancel.cancel();
    match serve.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => warn!(%err, "server task ended with error"),
        Err(join_err) => warn!(%join_err, "server task join error"),
    }

    info!("draining storage engine");
    if let Err(err) = engine.drain().await {
        warn!(%err, "engine drain reported error");
    }
    info!("skafka exited cleanly");
    Ok(())
}

fn init_tracing(log_level: &str) {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}

fn build_engine(
    data_dir: Option<PathBuf>,
    _flush_interval_messages: i64,
) -> Result<Arc<dyn StorageEngine>> {
    match data_dir {
        Some(dir) => {
            std::fs::create_dir_all(&dir).context("creating SKAFKA_DATA_DIR")?;
            // PartitionConfig::default uses the conservative defaults
            // the Phase 2 plan landed (4 KiB index interval, 1 GiB
            // segment cap, group-commit on). SKAFKA_FLUSH_INTERVAL_MESSAGES
            // wires into PartitionConfig in a Phase 2 follow-up; for now
            // the engine honours its struct default.
            let cfg = PartitionConfig::default();
            let engine = DiskStorageEngine::new(Arc::new(RealFs), dir, cfg);
            Ok(Arc::new(engine))
        }
        None => Ok(Arc::new(MemoryStorage::new())),
    }
}

fn build_dispatcher(broker: Arc<Broker>, listeners: &[sk_broker::ListenerEntry]) -> Dispatcher {
    let mut d = Dispatcher::new();
    d.register(0, 3, 9, Arc::new(ProduceHandler::new(broker.clone())));
    d.register(1, 4, 12, Arc::new(FetchHandler::new(broker.clone())));
    d.register(2, 1, 7, Arc::new(ListOffsetsHandler::new(broker.clone())));
    d.register(
        3,
        1,
        10,
        Arc::new(MetadataHandler::new(broker.clone(), listeners)),
    );
    d.register(18, 0, 4, Arc::new(ApiVersionsHandler::new()));
    d.register(22, 0, 4, Arc::new(InitProducerIdHandler::new(broker)));
    d
}

fn parse_listeners(entries: &[sk_broker::ListenerEntry]) -> Result<Vec<ListenerConfig>> {
    entries
        .iter()
        .map(|e| {
            let addr = e
                .addr
                .parse::<std::net::SocketAddr>()
                .with_context(|| format!("parsing listener addr {:?} for {}", e.addr, e.name))?;
            Ok(ListenerConfig {
                name: e.name.clone(),
                addr,
                pre_bound: None,
                tls_config: None,
            })
        })
        .collect()
}

async fn wait_for_shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let mut int = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    tokio::select! {
        _ = term.recv() => info!("SIGTERM received"),
        _ = int.recv()  => info!("SIGINT received"),
    }
    Ok(())
}
