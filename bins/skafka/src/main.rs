//! skafka broker binary.
//!
//! Phase 5: per-listener auth + cluster-wide authorization +
//! quotas + consumer-group coordinator + assignment-driven
//! ownership. Listens on every configured listener, serves
//! Produce / Fetch / ListOffsets / Metadata / ApiVersions /
//! InitProducerId / SASL + the full key 8–16 / 42 / 47
//! consumer-group surface against either an on-disk or in-memory
//! storage engine.
//!
//! Cluster bring-up lives in [`cluster::install`] — see that
//! module for the dev / single-broker-disk / cluster mode
//! decision tree.
//!
//! Storage selection:
//! - `SKAFKA_DATA_DIR` set → `DiskStorageEngine` rooted there.
//! - unset → `MemoryStorage` (dev mode).
//!
//! Auth selection (per-listener via `SKAFKA_LISTENERS`, cluster-wide
//! via `SKAFKA_AUTH_DISABLED` / `SKAFKA_AUTHORIZATION_TYPE` /
//! `SKAFKA_SUPER_USERS` / `SKAFKA_SSL_PRINCIPAL_MAPPING_RULES`).
//! `auth_disabled=true` forces `AllowAllAuthorizer` + `NoQuotaChecker`
//! across the board.

mod cluster;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig as RustlsServerConfig};
use sk_auth::{
    AclEngine, AllowAllAuthEngine, AllowAllAuthorizer, AuthEngine, AuthEngineSelector, Authorizer,
    CredentialLoader, NoQuotaChecker, PerListenerAuthEngine, PrincipalMapper, QuotaChecker,
    QuotaEnforcer, RealAuthEngine, SuperUserAuthorizer,
};
use sk_broker::{
    AddOffsetsToTxnHandler, AddPartitionsToTxnHandler, AlterClientQuotasHandler,
    ApiVersionsHandler, Broker, Cli, CliTlsConfig, CreatePartitionsHandler, CreateTopicsHandler,
    DeleteGroupsHandler, DescribeClientQuotasHandler, DescribeConfigsHandler,
    DescribeGroupsHandler, EndTxnHandler, FetchHandler, FindCoordinatorHandler, HeartbeatHandler,
    IncrementalAlterConfigsHandler, InitProducerIdHandler, JoinGroupHandler, LeaveGroupHandler,
    ListGroupsHandler, ListOffsetsHandler, ListenerEntry, MetadataHandler, OffsetCommitHandler,
    OffsetDeleteHandler, OffsetFetchHandler, ProduceHandler, SaslAuthenticateHandler,
    SaslHandshakeHandler, SyncGroupHandler, TopicRegistry, TxnOffsetCommitHandler,
    WriteTxnMarkersHandler,
};
use sk_protocol::{Dispatcher, ListenerConfig, MtlsConfig, Server, ServerConfigBuilder};
use sk_storage::{DiskStorageEngine, MemoryStorage, PartitionConfig, RealFs, StorageEngine};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Hot-reload interval for credentials.json + acls.json. 10 s
/// matches the Go side's mtime poll. The Phase 4 plan calls out a
/// follow-up to swap in `notify` inotify during Phase 8.
const RELOAD_INTERVAL_SECS: u64 = 10;

/// Init-container entry point. Mirrors
/// `archive/cmd/skafka/main.go::runInit` — chown/chmod the data dir
/// to the broker uid/gid so the broker container (uid=65532) can
/// mkdir topic dirs at runtime even when the CSI provisioner
/// silently skipped fsGroup-driven perms (skafka#110).
///
/// Skips the KafkaTopic CR walk the Go side did — the operator
/// creates partition dirs on first reconcile, and the storage
/// engine mkdirs lazily on Produce; the CR walk was an optimisation
/// to have the dirs pre-warm, not a correctness requirement.
fn run_init() -> Result<()> {
    let data_dir = std::env::var("SKAFKA_DATA_DIR").context("init: SKAFKA_DATA_DIR not set")?;
    let uid: u32 = std::env::var("SKAFKA_BROKER_UID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(65532);
    let gid: u32 = std::env::var("SKAFKA_BROKER_GID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(65532);

    let data_path = Path::new(&data_dir);
    std::fs::create_dir_all(data_path)
        .with_context(|| format!("init: mkdir {}", data_path.display()))?;

    ensure_data_dir_perms(data_path, uid, gid)
        .with_context(|| format!("init: chown/chmod {}", data_path.display()))?;
    eprintln!(
        "skafka init: data_dir={} uid={} gid={} ok",
        data_dir, uid, gid
    );
    Ok(())
}

/// Layer B of the skafka#110 defence-in-depth stack: kubelet's
/// fsGroup (layer A) can silently fail on non-cooperating CSI
/// drivers; this init routine runs as root and makes the data dir
/// writable by the broker process. Layer C (the storage engine's
/// mkdir_all mode 0o775 at runtime) covers new subdirs.
///
/// Walk semantics: chown every entry to (uid, gid); chmod every
/// directory to 0o775 (setgid + 0775 so children inherit the dir's
/// group). Files keep their existing mode. Non-root dev-mode
/// invocations skip the chown pass — chmod EPERM is tolerated.
fn ensure_data_dir_perms(root: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::{chown, PermissionsExt};
    // Root-level chmod first — always run so a warm restart on a
    // CSI-cooperating substrate still fixes a mis-configured dir.
    // Tolerate EPERM so dev-mode invocations don't fail.
    if let Err(e) = std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o775)) {
        if e.kind() != std::io::ErrorKind::PermissionDenied {
            return Err(e);
        }
    }
    // Probe: try to chown the root. If we're non-root we get EPERM;
    // skip the recursive fix (dev mode already owns the dir).
    match chown(root, Some(uid), Some(gid)) {
        Ok(()) => walk_and_fix(root, uid, gid),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => Ok(()),
        Err(e) => Err(e),
    }
}

fn walk_and_fix(dir: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::{chown, PermissionsExt};
    // Root already chowned by ensure_data_dir_perms; just chmod +
    // descend.
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o775))?;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        chown(&path, Some(uid), Some(gid))?;
        if file_type.is_dir() {
            walk_and_fix(&path, uid, gid)?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // `--init` mode short-circuits everything else. Called by the
    // Helm chart's `partition-init` init container.
    if std::env::args().nth(1).as_deref() == Some("--init") {
        return run_init();
    }

    // rustls 0.23 requires a CryptoProvider to be picked before the
    // first TLS build. We enabled `ring` at workspace level; the
    // provider isn't auto-installed. Any second-hand rustls user
    // (kube-rs → hyper-rustls) would panic without this. Ignore the
    // Err — it only fires if a provider was already installed by
    // another crate.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::from_env().context("parsing SKAFKA_* env")?;

    // Bring up OTel BEFORE the first tracing event so the OTel layer
    // sees every subsequent span. Bootstrap installs the global
    // MeterProvider + TracerProvider and populates the Metrics
    // registry; install_tracing then routes tracing macros through
    // the freshly-built tracer.
    let obs_cancel = CancellationToken::new();
    let providers = sk_observability::bootstrap("skafka", obs_cancel.clone())
        .await
        .context("initialising observability")?;
    let log_format = std::env::var("SKAFKA_LOG_FORMAT").unwrap_or_else(|_| "json".into());
    sk_observability::install_tracing(&cli.log_level, &log_format, providers.tracer.clone());

    info!(
        broker_id = cli.broker_id,
        cluster_id = cli.cluster_id.as_str(),
        listeners = cli.listeners.len(),
        data_dir = ?cli.data_dir,
        auth_disabled = cli.auth_disabled,
        authorization_type = cli.authorization_type.as_str(),
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

    let auth = build_auth(&cli)?;
    let broker = Arc::new(Broker::with_auth(
        engine.clone(),
        topics.clone(),
        cli.cluster_id.clone(),
        cli.broker_id,
        auth.authorizer.clone(),
        auth.quotas.clone(),
    ));

    // In cluster mode (MY_POD_NAME + SKAFKA_NAMESPACE set), wire the
    // kube-backed TopicCRWriter so admin handlers (CreateTopics,
    // CreatePartitions, IncrementalAlterConfigs) can patch KafkaTopic
    // CRs. Dev-mode leaves cr_writer as None and those handlers
    // return CLUSTER_AUTHORIZATION_FAILED.
    if std::env::var("MY_POD_NAME").is_ok() {
        let ns = std::env::var("SKAFKA_NAMESPACE").unwrap_or_else(|_| "default".into());
        match kube::Client::try_default().await {
            Ok(client) => {
                let writer = sk_broker::topic_cr_writer::KubeTopicCRWriter::new(client, ns);
                broker.install_cr_writer(Arc::new(writer));
                info!("installed KubeTopicCRWriter for admin handlers");
            }
            Err(err) => {
                warn!(%err, "kube client init failed; CreateTopics + admin handlers will refuse");
            }
        }
    }

    let cancel = CancellationToken::new();

    // Phase 5: bring up the consumer-group Manager + (in disk
    // mode) the Coordinator + AssignmentLoop + takeover drivers.
    // install() is a no-op for the Phase-3/4 surface — it only
    // adds capabilities, never replaces them.
    let cluster_rt = cluster::install(
        broker.clone(),
        topics.clone(),
        engine.clone(),
        cli.data_dir.clone(),
        cli.broker_id,
        &cli.cluster_id,
        cancel.clone(),
    )?;

    // Spawn the credential / ACL reloader before the listeners go up
    // so the first served request sees the latest disk state.
    let _reload_task = spawn_reloader(auth.creds.clone(), auth.acls.clone());

    let dispatcher = build_dispatcher(broker.clone(), &cli.listeners, auth.engines.clone());
    let listeners = parse_listeners(&cli.listeners, &auth.engines, &auth.principal_mapper)?;
    let server = Server::new(ServerConfigBuilder::new(listeners), Arc::new(dispatcher));

    let serve_cancel = cancel.clone();
    let serve = tokio::spawn(async move { server.serve(serve_cancel).await });

    // Spawn the axum /healthz + /readyz server on SKAFKA_HEALTH_ADDR
    // (chart default `:8080`). The chart's readinessProbe hits
    // http://<pod>:8080/readyz — without this, pods stay 0/1
    // Ready forever.
    let health_addr = std::env::var("SKAFKA_HEALTH_ADDR").unwrap_or_else(|_| ":8080".into());
    let health_addr = if let Some(stripped) = health_addr.strip_prefix(':') {
        format!("0.0.0.0:{stripped}")
    } else {
        health_addr
    };
    let health_cfg = sk_observability::health::HealthConfig {
        broker_id: format!("skafka-{}", cli.broker_id),
        listeners: cli.listeners.iter().map(|l| l.name.clone()).collect(),
        tls: None,
        source: None,
    };
    let health_cancel = cancel.clone();
    tokio::spawn(async move {
        let router = sk_observability::health::health_router(health_cfg);
        match tokio::net::TcpListener::bind(&health_addr).await {
            Ok(listener) => {
                info!(addr = %health_addr, "health server listening");
                let _ = axum::serve(listener, router)
                    .with_graceful_shutdown(async move { health_cancel.cancelled().await })
                    .await;
            }
            Err(err) => warn!(%err, %health_addr, "health server bind failed"),
        }
    });

    // Flip /readyz once the accept loop is up. The chart's
    // readinessProbe gates on this. If a listener fails to bind, we
    // never call this and the pod stays unready until SIGTERM.
    sk_observability::set_ready(true);

    wait_for_shutdown_signal().await?;
    info!("shutdown signal received; cancelling listeners");
    cancel.cancel();
    match serve.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => warn!(%err, "server task ended with error"),
        Err(join_err) => warn!(%join_err, "server task join error"),
    }

    // Abort cluster background tasks before draining storage so
    // their in-flight writes don't race the drain.
    for handle in cluster_rt.tasks {
        handle.abort();
    }

    info!("draining storage engine");
    if let Err(err) = engine.drain().await {
        warn!(%err, "engine drain reported error");
    }

    // Flush pending OTLP pushes + span exports before the process
    // dies. Best-effort — errors go to stderr but don't fail exit.
    if let Err(err) = providers.shutdown() {
        warn!(%err, "observability shutdown reported error");
    }
    obs_cancel.cancel();

    info!("skafka exited cleanly");
    Ok(())
}

struct AuthSetup {
    authorizer: Arc<dyn Authorizer>,
    quotas: Arc<dyn QuotaChecker>,
    engines: Arc<PerListenerAuthEngine>,
    creds: Option<Arc<CredentialLoader>>,
    acls: Option<Arc<AclEngine>>,
    principal_mapper: Arc<PrincipalMapper>,
}

fn build_auth(cli: &Cli) -> Result<AuthSetup> {
    let mapper = Arc::new(
        PrincipalMapper::parse(&cli.ssl_principal_mapping_rules)
            .context("parsing SKAFKA_SSL_PRINCIPAL_MAPPING_RULES")?,
    );

    if cli.auth_disabled {
        info!("auth disabled — using AllowAllAuthorizer + NoQuotaChecker on every listener");
        let allow_all: Arc<dyn AuthEngine> = Arc::new(AllowAllAuthEngine);
        let engines = Arc::new(PerListenerAuthEngine::new(allow_all));
        return Ok(AuthSetup {
            authorizer: Arc::new(AllowAllAuthorizer),
            quotas: Arc::new(NoQuotaChecker),
            engines,
            creds: None,
            acls: None,
            principal_mapper: mapper,
        });
    }

    let cluster_dir = cli
        .data_dir
        .as_ref()
        .map(|d| d.join("__cluster"))
        .unwrap_or_else(|| PathBuf::from("/data/__cluster"));
    let creds_path = cluster_dir.join("credentials.json");
    let acls_path = cluster_dir.join("acls.json");

    let creds = Arc::new(CredentialLoader::new(creds_path.clone()));
    if let Err(err) = creds.reload() {
        warn!(%err, path = %creds_path.display(), "auth: initial credentials reload failed (continuing)");
    }
    let acls = Arc::new(AclEngine::new(acls_path.clone()));
    if let Err(err) = acls.reload() {
        warn!(%err, path = %acls_path.display(), "auth: initial acls reload failed (continuing)");
    }

    // Per-listener engine map. Anonymous listeners (auth_type unset
    // or "none") use AllowAllAuthEngine; scram/plain/mtls listeners
    // use RealAuthEngine wrapped around the credential store.
    let allow_all_engine: Arc<dyn AuthEngine> = Arc::new(AllowAllAuthEngine);
    let real_engine: Arc<dyn AuthEngine> =
        Arc::new(RealAuthEngine::new(creds.clone(), mapper.clone()));
    let mut engines_map = PerListenerAuthEngine::new(allow_all_engine.clone());
    for lc in &cli.listeners {
        let engine_for_listener: Arc<dyn AuthEngine> =
            match lc.authentication_type.as_deref().unwrap_or("none") {
                "none" => allow_all_engine.clone(),
                "scram-sha-512" | "plain" | "mtls" => real_engine.clone(),
                other => {
                    warn!(
                        listener = lc.name.as_str(),
                        authentication_type = other,
                        "unknown authentication_type — falling back to AllowAllAuthEngine"
                    );
                    allow_all_engine.clone()
                }
            };
        engines_map.insert(lc.name.clone(), engine_for_listener);
    }
    let engines = Arc::new(engines_map);

    // Authorizer: AclEngine if "simple", AllowAll otherwise. Wrap in
    // SuperUserAuthorizer when super_users is non-empty.
    let base_authorizer: Arc<dyn Authorizer> = match cli.authorization_type.as_str() {
        "simple" => acls.clone(),
        "" => Arc::new(AllowAllAuthorizer),
        other => {
            warn!(
                authorization_type = other,
                "unknown SKAFKA_AUTHORIZATION_TYPE — falling back to AllowAll"
            );
            Arc::new(AllowAllAuthorizer)
        }
    };
    let authorizer: Arc<dyn Authorizer> = if cli.super_users.is_empty() {
        base_authorizer
    } else {
        Arc::new(SuperUserAuthorizer::new(
            cli.super_users.clone(),
            base_authorizer,
        ))
    };

    // Quotas: backed by the credential store so per-user limits in
    // credentials.json take effect.
    let quotas: Arc<dyn QuotaChecker> = Arc::new(QuotaEnforcer::new(creds.clone()));

    Ok(AuthSetup {
        authorizer,
        quotas,
        engines,
        creds: Some(creds),
        acls: Some(acls),
        principal_mapper: mapper,
    })
}

fn spawn_reloader(
    creds: Option<Arc<CredentialLoader>>,
    acls: Option<Arc<AclEngine>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let creds = creds?;
    let acls = acls?;
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(RELOAD_INTERVAL_SECS));
        // First tick fires immediately; skip it since we reloaded at
        // boot.
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Err(err) = creds.reload() {
                warn!(%err, "auth: credentials hot-reload failed");
            }
            if let Err(err) = acls.reload() {
                warn!(%err, "auth: acls hot-reload failed");
            }
        }
    }))
}

fn build_engine(
    data_dir: Option<PathBuf>,
    _flush_interval_messages: i64,
) -> Result<Arc<dyn StorageEngine>> {
    match data_dir {
        Some(dir) => {
            std::fs::create_dir_all(&dir).context("creating SKAFKA_DATA_DIR")?;
            let cfg = PartitionConfig::default();
            let engine = DiskStorageEngine::new(Arc::new(RealFs), dir, cfg);
            Ok(Arc::new(engine))
        }
        None => Ok(Arc::new(MemoryStorage::new())),
    }
}

fn build_dispatcher(
    broker: Arc<Broker>,
    listeners: &[ListenerEntry],
    engines: Arc<PerListenerAuthEngine>,
) -> Dispatcher {
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
    // Phase 5 consumer-group surface (keys 8-16, 42, 47).
    d.register(8, 0, 8, Arc::new(OffsetCommitHandler::new(broker.clone())));
    d.register(9, 1, 8, Arc::new(OffsetFetchHandler::new(broker.clone())));
    d.register(
        10,
        0,
        4,
        Arc::new(FindCoordinatorHandler::new(broker.clone())),
    );
    d.register(11, 0, 9, Arc::new(JoinGroupHandler::new(broker.clone())));
    d.register(12, 0, 4, Arc::new(HeartbeatHandler::new(broker.clone())));
    d.register(13, 0, 5, Arc::new(LeaveGroupHandler::new(broker.clone())));
    d.register(14, 0, 5, Arc::new(SyncGroupHandler::new(broker.clone())));
    d.register(
        15,
        0,
        5,
        Arc::new(DescribeGroupsHandler::new(broker.clone())),
    );
    d.register(16, 0, 4, Arc::new(ListGroupsHandler::new(broker.clone())));
    d.register(17, 0, 1, Arc::new(SaslHandshakeHandler::new()));
    d.register(18, 0, 4, Arc::new(ApiVersionsHandler::new()));
    d.register(
        22,
        0,
        4,
        Arc::new(InitProducerIdHandler::new(broker.clone())),
    );
    // Phase 6 transactional surface (keys 24–28).
    d.register(
        24,
        0,
        3,
        Arc::new(AddPartitionsToTxnHandler::new(broker.clone())),
    );
    d.register(
        25,
        0,
        3,
        Arc::new(AddOffsetsToTxnHandler::new(broker.clone())),
    );
    d.register(26, 0, 3, Arc::new(EndTxnHandler::new(broker.clone())));
    d.register(
        27,
        0,
        1,
        Arc::new(WriteTxnMarkersHandler::new(broker.clone())),
    );
    d.register(
        28,
        0,
        3,
        Arc::new(TxnOffsetCommitHandler::new(broker.clone())),
    );
    d.register(
        36,
        0,
        2,
        Arc::new(SaslAuthenticateHandler::new(engines.clone())),
    );
    d.register(42, 0, 2, Arc::new(DeleteGroupsHandler::new(broker.clone())));
    // Phase 7 admin handlers (workstream D). All five take an
    // Arc<Broker>; the broker resolves the optional TopicCRWriter /
    // QuotaEnforcer slots per-call so dev-mode (slots unset) maps
    // cleanly to the documented wire error codes (31, 35).
    d.register(
        32,
        0,
        4,
        Arc::new(DescribeConfigsHandler::new(broker.clone())),
    );
    d.register(19, 0, 7, Arc::new(CreateTopicsHandler::new(broker.clone())));
    d.register(
        37,
        0,
        3,
        Arc::new(CreatePartitionsHandler::new(broker.clone())),
    );
    d.register(
        44,
        0,
        1,
        Arc::new(IncrementalAlterConfigsHandler::new(broker.clone())),
    );
    d.register(
        48,
        0,
        1,
        Arc::new(DescribeClientQuotasHandler::new(broker.clone())),
    );
    d.register(
        49,
        0,
        1,
        Arc::new(AlterClientQuotasHandler::new(broker.clone())),
    );
    d.register(47, 0, 0, Arc::new(OffsetDeleteHandler::new(broker)));
    d.set_auth(engines);
    d
}

fn parse_listeners(
    entries: &[ListenerEntry],
    engines: &Arc<PerListenerAuthEngine>,
    mapper: &Arc<PrincipalMapper>,
) -> Result<Vec<ListenerConfig>> {
    entries
        .iter()
        .map(|e| {
            let addr = e
                .addr
                .parse::<std::net::SocketAddr>()
                .with_context(|| format!("parsing listener addr {:?} for {}", e.addr, e.name))?;
            let tls_config = match &e.tls {
                None => None,
                Some(tc) => {
                    Some(Arc::new(load_tls(tc).with_context(|| {
                        format!("loading TLS for listener {}", e.name)
                    })?))
                }
            };
            let mtls = if matches!(e.authentication_type.as_deref(), Some("mtls")) {
                let engine = engines.for_listener(&e.name);
                Some(MtlsConfig {
                    engine,
                    mapper: mapper.clone(),
                })
            } else {
                None
            };
            Ok(ListenerConfig {
                name: e.name.clone(),
                addr,
                pre_bound: None,
                tls_config,
                mtls,
            })
        })
        .collect()
}

fn load_tls(cfg: &CliTlsConfig) -> Result<RustlsServerConfig> {
    let certs = load_certs(&cfg.cert_path)?;
    let key = load_private_key(&cfg.key_path)?;
    let builder = RustlsServerConfig::builder();
    let server = if let Some(ca_path) = &cfg.client_ca_path {
        let mut roots = RootCertStore::empty();
        for cert in load_certs(ca_path)? {
            roots
                .add(cert)
                .context("installing client-CA cert into trust store")?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .context("building client cert verifier")?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    server
        .with_single_cert(certs, key)
        .context("rustls server config with cert + key")
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let f = std::fs::File::open(path)
        .with_context(|| format!("opening cert file {}", path.display()))?;
    let mut r = std::io::BufReader::new(f);
    let mut out = Vec::new();
    for cert in rustls_pemfile::certs(&mut r) {
        out.push(cert.with_context(|| format!("parsing cert in {}", path.display()))?);
    }
    if out.is_empty() {
        anyhow::bail!("no PEM certificates found in {}", path.display());
    }
    Ok(out)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let f = std::fs::File::open(path)
        .with_context(|| format!("opening key file {}", path.display()))?;
    let mut r = std::io::BufReader::new(f);
    let key = rustls_pemfile::private_key(&mut r)
        .with_context(|| format!("parsing private key in {}", path.display()))?
        .ok_or_else(|| anyhow::anyhow!("no private key in {}", path.display()))?;
    Ok(key)
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

// HashMap import is unused in some builds — silence its dead-import
// warning by referencing the type once.
#[allow(dead_code)]
fn _silence_unused_hashmap() -> HashMap<u8, u8> {
    HashMap::new()
}
