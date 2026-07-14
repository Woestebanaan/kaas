//! Broker identity derived from the K8s downward API.
//!
//! The pod name IS the
//! broker identity — no registration protocol needed. Pure code, no
//! kube deps.
//!
//! Strimzi-pattern DNS: each broker pod is reachable at
//! `<pod>.<headless-svc>.<namespace>.svc.<cluster-domain>`. The
//! StatefulSet's headless `Service` is what synthesises the A
//! record — no per-broker `Service` object needed, no ArgoCD drift
//! loop (gh #128). Both [`BrokerIdentity`] (self FQDN) and
//! [`crate::endpoints::BrokerRegistry`] (peer FQDNs from
//! `EndpointSlice` events) share the [`DnsConfig`] so the two paths
//! agree byte-for-byte.

use std::env;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("MY_POD_NAME env var not set")]
    MissingPodName,
    #[error("failed to parse ordinal from {pod_name:?}: {msg}")]
    ParseOrdinal { pod_name: String, msg: String },
}

/// Per-cluster DNS knobs used to build per-broker FQDNs. Computed
/// once at startup from env vars (which the Helm chart fills from
/// `values.yaml`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsConfig {
    pub namespace: String,
    /// StatefulSet `serviceName` — e.g. `"skafka"`.
    pub headless_service: String,
    /// `format!`-style with `{ordinal}` substitution, e.g.
    /// `"skafka-{ordinal}"`.
    pub pod_name_pattern: String,
    /// E.g. `"cluster.local"` (default for >99% of K8s distros).
    pub cluster_domain: String,
}

impl DnsConfig {
    /// Build the FQDN for `ordinal` under the StatefulSet's
    /// headless service. Shape:
    /// `{pod}.{headless}.{namespace}.svc.{cluster_domain}`.
    ///
    /// Accepts both placeholder dialects: the native
    /// `{ordinal}` AND printf-style `%d` — the Helm chart emits
    /// `SKAFKA_POD_NAME_PATTERN=<name>-%d`, the pattern v0.1
    /// deployments were configured with. A `%d`
    /// passing through unreplaced put `skafka-%d.…` hostnames into
    /// live Metadata responses (clients died on DNS).
    pub fn fqdn(&self, ordinal: i32) -> String {
        let ord = ordinal.to_string();
        let pod = self
            .pod_name_pattern
            .replace("{ordinal}", &ord)
            .replace("%d", &ord);
        format!(
            "{pod}.{}.{}.svc.{}",
            self.headless_service, self.namespace, self.cluster_domain
        )
    }
}

/// Identity of this broker pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerIdentity {
    /// E.g. `"skafka-0"`.
    pub pod_name: String,
    /// StatefulSet ordinal — e.g. `2` for `"skafka-2"`.
    pub ordinal: i32,
    pub namespace: String,
    /// Self FQDN, built from [`DnsConfig::fqdn`].
    pub host: String,
    pub port: i32,
    /// Shared with the [`crate::endpoints::BrokerRegistry`] so peer
    /// hosts use the same shape.
    pub dns: DnsConfig,
}

impl BrokerIdentity {
    /// Read `MY_POD_NAME` from the environment and derive the
    /// identity. Env overrides for the DNS knobs:
    ///
    /// - `SKAFKA_NAMESPACE` (default `"default"`)
    /// - `SKAFKA_HEADLESS_SVC` (default `"skafka-headless"`)
    /// - `SKAFKA_POD_NAME_PATTERN` (default `"skafka-{ordinal}"`)
    /// - `SKAFKA_CLUSTER_DOMAIN` (default `"cluster.local"`)
    pub fn from_env(namespace: &str, headless_svc: &str, port: i32) -> Result<Self, IdentityError> {
        let pod_name = env::var("MY_POD_NAME").map_err(|_| IdentityError::MissingPodName)?;
        if pod_name.is_empty() {
            return Err(IdentityError::MissingPodName);
        }
        let namespace = if namespace.is_empty() {
            env_or("SKAFKA_NAMESPACE", "default")
        } else {
            namespace.to_owned()
        };
        let headless_service = if headless_svc.is_empty() {
            env_or("SKAFKA_HEADLESS_SVC", "skafka-headless")
        } else {
            headless_svc.to_owned()
        };
        let pod_name_pattern = env_or("SKAFKA_POD_NAME_PATTERN", "skafka-{ordinal}");
        let cluster_domain = env_or("SKAFKA_CLUSTER_DOMAIN", "cluster.local");

        let ordinal = parse_ordinal(&pod_name).ok_or_else(|| IdentityError::ParseOrdinal {
            pod_name: pod_name.clone(),
            msg: "no trailing integer".to_owned(),
        })?;

        let dns = DnsConfig {
            namespace: namespace.clone(),
            headless_service,
            pod_name_pattern,
            cluster_domain,
        };
        let host = dns.fqdn(ordinal);
        Ok(Self {
            pod_name,
            ordinal,
            namespace,
            host,
            port,
            dns,
        })
    }
}

/// Extract the trailing integer from a hyphen-separated name — used
/// by both [`BrokerIdentity::from_env`] and consumers that need to
/// translate `"skafka-2"` back to `2`. Returns `None` on parse
/// failure (the v0.1 implementation returned `-1` from its public
/// wrapper; we use `Option<i32>` for
/// honesty).
pub fn parse_ordinal(name: &str) -> Option<i32> {
    let last = name.rsplit('-').next()?;
    last.parse::<i32>().ok()
}

fn env_or(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dns(namespace: &str) -> DnsConfig {
        DnsConfig {
            namespace: namespace.to_owned(),
            headless_service: "skafka-headless".to_owned(),
            pod_name_pattern: "skafka-{ordinal}".to_owned(),
            cluster_domain: "cluster.local".to_owned(),
        }
    }

    #[test]
    fn fqdn_shape_matches_strimzi() {
        let d = dns("skafka");
        assert_eq!(
            d.fqdn(2),
            "skafka-2.skafka-headless.skafka.svc.cluster.local"
        );
        assert_eq!(
            d.fqdn(0),
            "skafka-0.skafka-headless.skafka.svc.cluster.local"
        );
    }

    #[test]
    fn parse_ordinal_extracts_trailing_int() {
        assert_eq!(parse_ordinal("skafka-0"), Some(0));
        assert_eq!(parse_ordinal("skafka-2"), Some(2));
        assert_eq!(parse_ordinal("skafka-broker-2"), Some(2));
        assert_eq!(parse_ordinal("skafka-broker-2-foo"), None);
        assert_eq!(parse_ordinal(""), None);
        assert_eq!(parse_ordinal("nopodname"), None);
    }
}
