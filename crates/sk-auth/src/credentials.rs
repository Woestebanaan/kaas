//! `credentials.json` loader + `CredentialStore` trait.
//!
//! Mirrors `archive/internal/auth/loader.go` 1:1. JSON shape is the
//! Strimzi-compat file the operator writes to
//! `/data/__cluster/credentials.json`. Missing file is non-fatal
//! (returns an empty store).
//!
//! `CredentialLoader` holds three `RwLock`-guarded maps:
//! `by_username`, `by_cn` (CN → username, mTLS reverse lookup),
//! `by_sa` (populated but unused in Phase 4 — Phase 7 wires the K8s
//! ServiceAccount path).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use parking_lot::RwLock;
use serde::Deserialize;

use crate::errors::AuthError;
use crate::types::Quotas;

/// Look up SASL/mTLS credentials by username/CN/(namespace,name).
pub trait CredentialStore: Send + Sync + std::fmt::Debug + 'static {
    /// Returns `(stored_key, server_key, salt, iterations)` for a
    /// SCRAM-SHA-512 user; `None` if no such user or wrong auth type.
    fn lookup_scram(&self, username: &str) -> Option<ScramCreds>;

    /// Returns the username registered for a given TLS subject CN.
    fn lookup_tls(&self, cn: &str) -> Option<String>;

    /// `true` iff the (namespace, name) pair is a registered
    /// ServiceAccount. Phase 4 evaluates this against the loader's
    /// `by_sa` map but no PLAIN exchange exercises it; Phase 7 wires
    /// the K8s TokenReview path.
    fn lookup_sa(&self, namespace: &str, name: &str) -> bool;

    /// Per-user throughput limits; `None` if no quotas configured.
    fn lookup_quotas(&self, username: &str) -> Option<Quotas>;

    /// Optional: the static-credential PLAIN password (Phase 4 only —
    /// `credentials.json` from the operator never carries this).
    fn lookup_plain_password(&self, username: &str) -> Option<String> {
        let _ = username;
        None
    }

    /// Optional enumeration for admin APIs (Phase 5/7 surfaces).
    fn list_all_scram_users(&self) -> HashMap<String, ScramInfo> {
        HashMap::new()
    }

    fn list_all_quotas(&self) -> HashMap<String, Quotas> {
        HashMap::new()
    }
}

/// SCRAM key material returned by `lookup_scram`.
#[derive(Debug, Clone)]
pub struct ScramCreds {
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub salt: Vec<u8>,
    pub iterations: i32,
}

/// Safe-to-expose SCRAM metadata for `DescribeUserScramCredentials`
/// (gh #104, KIP-554). Leaking the salt or stored_key would let a
/// privileged operator harvest credentials for offline attack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramInfo {
    pub mechanism: String,
    pub iterations: i32,
}

#[derive(Debug, Deserialize)]
struct CredFile {
    #[serde(default)]
    version: u32,
    #[serde(default)]
    users: Vec<CredUser>,
}

#[derive(Debug, Deserialize)]
struct CredUser {
    username: String,
    #[serde(rename = "authType")]
    auth_type: String,
    #[serde(default)]
    scram: Option<ScramJson>,
    #[serde(rename = "tlsCN", default)]
    tls_cn: Option<String>,
    #[serde(rename = "serviceAccount", default)]
    sa: Option<SaJson>,
    #[serde(default)]
    quotas: Option<QuotasJson>,
    /// Static PLAIN password (Phase 4 only). Production operators
    /// never write this; tests and dev-mode opt in by hand.
    #[serde(rename = "plainPassword", default)]
    plain_password: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScramJson {
    salt: String,
    #[serde(rename = "storedKey")]
    stored_key: String,
    #[serde(rename = "serverKey")]
    server_key: String,
    iterations: i32,
}

#[derive(Debug, Deserialize)]
struct SaJson {
    name: String,
    namespace: String,
}

#[derive(Debug, Deserialize)]
struct QuotasJson {
    #[serde(rename = "producerMaxByteRatePerBroker", default)]
    producer: Option<i64>,
    #[serde(rename = "consumerMaxByteRatePerBroker", default)]
    consumer: Option<i64>,
    #[serde(rename = "requestPercentage", default)]
    request_percentage: Option<i32>,
}

#[derive(Debug, Clone, Default)]
struct LoadedCred {
    auth_type: String,
    scram: Option<ScramCreds>,
    tls_cn: Option<String>,
    sa_namespace: String,
    sa_name: String,
    quotas: Option<Quotas>,
    plain_password: Option<String>,
}

#[derive(Debug)]
struct State {
    by_username: HashMap<String, LoadedCred>,
    by_cn: HashMap<String, String>,
    by_sa: HashMap<String, bool>,
}

impl State {
    fn empty() -> Self {
        Self {
            by_username: HashMap::new(),
            by_cn: HashMap::new(),
            by_sa: HashMap::new(),
        }
    }
}

/// Loads `credentials.json` and exposes it as a `CredentialStore`.
/// `reload()` atomically swaps the in-memory data.
#[derive(Debug)]
pub struct CredentialLoader {
    path: PathBuf,
    state: RwLock<State>,
}

impl CredentialLoader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            state: RwLock::new(State::empty()),
        }
    }

    /// Returns the credentials file path the loader is bound to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the file and replace in-memory state atomically. Missing
    /// file is `Ok(())` so an empty operator-side state doesn't fail
    /// broker boot.
    pub fn reload(&self) -> Result<(), AuthError> {
        let data = match std::fs::read(&self.path) {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                *self.state.write() = State::empty();
                return Ok(());
            }
            Err(err) => return Err(AuthError::Io(err)),
        };
        let parsed: CredFile = serde_json::from_slice(&data)?;
        let _ = parsed.version; // surface only on read; no schema gate yet

        let mut by_username = HashMap::with_capacity(parsed.users.len());
        let mut by_cn = HashMap::new();
        let mut by_sa = HashMap::new();

        for u in parsed.users {
            let mut c = LoadedCred {
                auth_type: u.auth_type.clone(),
                quotas: u.quotas.map(|q| Quotas {
                    producer_max_byte_rate_per_broker: q.producer,
                    consumer_max_byte_rate_per_broker: q.consumer,
                    request_percentage: q.request_percentage,
                }),
                plain_password: u.plain_password,
                ..LoadedCred::default()
            };
            match u.auth_type.as_str() {
                "scram-sha-512" => {
                    if let Some(s) = u.scram {
                        c.scram = Some(ScramCreds {
                            stored_key: BASE64.decode(&s.stored_key)?,
                            server_key: BASE64.decode(&s.server_key)?,
                            salt: BASE64.decode(&s.salt)?,
                            iterations: s.iterations,
                        });
                    }
                }
                "tls" => {
                    if let Some(cn) = &u.tls_cn {
                        if !cn.is_empty() {
                            by_cn.insert(cn.clone(), u.username.clone());
                        }
                    }
                    c.tls_cn = u.tls_cn;
                }
                "kubernetes-serviceaccount" => {
                    if let Some(sa) = u.sa {
                        by_sa.insert(format!("{}/{}", sa.namespace, sa.name), true);
                        c.sa_namespace = sa.namespace;
                        c.sa_name = sa.name;
                    }
                }
                other => {
                    tracing::warn!(
                        username = u.username.as_str(),
                        auth_type = other,
                        "credentials: unknown authType — entry kept but auth will fail",
                    );
                }
            }
            by_username.insert(u.username, c);
        }

        let mut state = self.state.write();
        state.by_username = by_username;
        state.by_cn = by_cn;
        state.by_sa = by_sa;
        Ok(())
    }

    /// Replace the in-memory state with the given users without
    /// touching disk. Used by tests to inject fixtures.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn install_for_test(&self, users: Vec<TestCred>) {
        let mut state = self.state.write();
        state.by_username.clear();
        state.by_cn.clear();
        state.by_sa.clear();
        for u in users {
            if let Some(cn) = &u.tls_cn {
                state.by_cn.insert(cn.clone(), u.username.clone());
            }
            state.by_username.insert(
                u.username.clone(),
                LoadedCred {
                    auth_type: u.auth_type,
                    scram: u.scram,
                    tls_cn: u.tls_cn,
                    sa_namespace: String::new(),
                    sa_name: String::new(),
                    quotas: u.quotas,
                    plain_password: u.plain_password,
                },
            );
        }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[derive(Debug, Default, Clone)]
pub struct TestCred {
    pub username: String,
    pub auth_type: String,
    pub scram: Option<ScramCreds>,
    pub tls_cn: Option<String>,
    pub quotas: Option<Quotas>,
    pub plain_password: Option<String>,
}

impl CredentialStore for CredentialLoader {
    fn lookup_scram(&self, username: &str) -> Option<ScramCreds> {
        let s = self.state.read();
        let c = s.by_username.get(username)?;
        if c.auth_type != "scram-sha-512" {
            return None;
        }
        c.scram.clone()
    }

    fn lookup_tls(&self, cn: &str) -> Option<String> {
        self.state.read().by_cn.get(cn).cloned()
    }

    fn lookup_sa(&self, namespace: &str, name: &str) -> bool {
        self.state
            .read()
            .by_sa
            .contains_key(&format!("{namespace}/{name}"))
    }

    fn lookup_quotas(&self, username: &str) -> Option<Quotas> {
        let s = self.state.read();
        s.by_username.get(username).and_then(|c| c.quotas.clone())
    }

    fn lookup_plain_password(&self, username: &str) -> Option<String> {
        let s = self.state.read();
        s.by_username
            .get(username)
            .and_then(|c| c.plain_password.clone())
    }

    fn list_all_scram_users(&self) -> HashMap<String, ScramInfo> {
        let s = self.state.read();
        s.by_username
            .iter()
            .filter_map(|(u, c)| {
                if c.auth_type != "scram-sha-512" {
                    return None;
                }
                let iterations = c.scram.as_ref().map(|s| s.iterations).unwrap_or(0);
                Some((
                    u.clone(),
                    ScramInfo {
                        mechanism: "SCRAM-SHA-512".to_owned(),
                        iterations,
                    },
                ))
            })
            .collect()
    }

    fn list_all_quotas(&self) -> HashMap<String, Quotas> {
        let s = self.state.read();
        s.by_username
            .iter()
            .filter_map(|(u, c)| c.quotas.clone().map(|q| (u.clone(), q)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn write_file(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut f, content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn missing_file_is_ok() {
        let loader = CredentialLoader::new("/no/such/path.json");
        loader.reload().unwrap();
        assert!(loader.lookup_scram("anyone").is_none());
    }

    #[test]
    fn loads_scram_user() {
        let f = write_file(
            r#"{
                "version": 1,
                "users": [{
                    "username": "alice",
                    "authType": "scram-sha-512",
                    "scram": {
                        "salt": "c2FsdA==",
                        "storedKey": "c3RvcmVk",
                        "serverKey": "c2VydmVy",
                        "iterations": 4096
                    },
                    "quotas": {
                        "producerMaxByteRatePerBroker": 1048576,
                        "consumerMaxByteRatePerBroker": 2097152
                    }
                }]
            }"#,
        );
        let loader = CredentialLoader::new(f.path());
        loader.reload().unwrap();

        let c = loader.lookup_scram("alice").unwrap();
        assert_eq!(c.salt, b"salt");
        assert_eq!(c.stored_key, b"stored");
        assert_eq!(c.server_key, b"server");
        assert_eq!(c.iterations, 4096);

        let q = loader.lookup_quotas("alice").unwrap();
        assert_eq!(q.producer_max_byte_rate_per_broker, Some(1_048_576));
        assert_eq!(q.consumer_max_byte_rate_per_broker, Some(2_097_152));

        assert_eq!(loader.list_all_scram_users().len(), 1);
        assert_eq!(loader.list_all_quotas().len(), 1);
    }

    #[test]
    fn loads_tls_user_with_reverse_cn_lookup() {
        let f = write_file(r#"{"users":[{"username":"bob","authType":"tls","tlsCN":"CN=bob"}]}"#);
        let loader = CredentialLoader::new(f.path());
        loader.reload().unwrap();
        assert_eq!(loader.lookup_tls("CN=bob").as_deref(), Some("bob"));
        assert!(loader.lookup_tls("CN=eve").is_none());
    }

    #[test]
    fn loads_sa_user() {
        let f = write_file(
            r#"{"users":[{"username":"sa","authType":"kubernetes-serviceaccount",
                "serviceAccount":{"name":"app","namespace":"default"}}]}"#,
        );
        let loader = CredentialLoader::new(f.path());
        loader.reload().unwrap();
        assert!(loader.lookup_sa("default", "app"));
        assert!(!loader.lookup_sa("default", "other"));
    }

    #[test]
    fn loads_plain_password() {
        let f = write_file(
            r#"{"users":[{"username":"svc","authType":"plain","plainPassword":"hunter2"}]}"#,
        );
        let loader = CredentialLoader::new(f.path());
        loader.reload().unwrap();
        assert_eq!(
            loader.lookup_plain_password("svc").as_deref(),
            Some("hunter2")
        );
    }

    #[test]
    fn reload_swaps_state_atomically() {
        let path = NamedTempFile::new().unwrap().into_temp_path();
        std::fs::write(
            &path,
            r#"{"users":[{"username":"a","authType":"tls","tlsCN":"CN=a"}]}"#,
        )
        .unwrap();
        let loader = CredentialLoader::new(&path);
        loader.reload().unwrap();
        assert!(loader.lookup_tls("CN=a").is_some());

        std::fs::write(
            &path,
            r#"{"users":[{"username":"b","authType":"tls","tlsCN":"CN=b"}]}"#,
        )
        .unwrap();
        loader.reload().unwrap();
        assert!(loader.lookup_tls("CN=a").is_none());
        assert!(loader.lookup_tls("CN=b").is_some());
    }
}
