//! `KafkaCluster` — top-level cluster CR.
//!
//! Drives the operator's external-listener plumbing: cert-manager
//! `Certificate`, per-broker `Service`, and Gateway-API `TLSRoute`.
//! The broker StatefulSet itself is owned by the Helm chart — this
//! CR carries runtime-decided shape (replica count, hostname pattern,
//! certificate issuer ref).
//!
//! **`spec.replicas` is templated by the chart, not user-authored.**
//! `deploy/helm/kaas/templates/kafkacluster.yaml` sets it from
//! `.Values.broker.replicaCount`; day-2 replica changes flow via
//! `helm upgrade`, not `kubectl edit kafkacluster`. The reconciler
//! reads but never writes this field.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::condition::Condition;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[kube(
    group = "kaas.rs",
    version = "v1alpha1",
    kind = "KafkaCluster",
    plural = "kafkaclusters",
    singular = "kafkacluster",
    namespaced,
    status = "KafkaClusterStatus",
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"External","type":"boolean","jsonPath":".spec.listeners.external.enabled"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterSpec {
    #[schemars(range(min = 1))]
    pub replicas: i32,

    #[serde(default)]
    pub storage: KafkaClusterStorage,

    #[serde(default)]
    pub listeners: KafkaClusterListeners,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterStorage {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub class_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub size: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterListeners {
    #[serde(default)]
    pub internal: InternalListener,
    #[serde(default)]
    pub external: ExternalListener,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InternalListener {
    /// The committed CRD ships `default: 9092` (verifiable at
    /// `deploy/crds/kaas.rs_kafkaclusters.yaml:127`). Apiserver
    /// defaulting fills this when the chart omits it. Schemars
    /// renders the helper via `#[schemars(default = ...)]`.
    #[serde(default = "default_internal_port")]
    #[schemars(default = "default_internal_port")]
    pub port: i32,
}

impl Default for InternalListener {
    fn default() -> Self {
        Self {
            port: default_internal_port(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalListener {
    pub enabled: bool,

    /// `default: 9093` in the committed CRD (`...kafkaclusters.yaml:88`).
    #[serde(default = "default_external_port")]
    #[schemars(default = "default_external_port")]
    pub port: i32,

    /// printf-style `%d` for the broker ordinal, e.g.
    /// `broker-%d.kafka.example.com`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hostname_pattern: String,

    /// Optional convenience hostname added to the certificate SANs.
    /// Not required for operation.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub bootstrap_hostname: String,

    #[serde(default)]
    pub tls: TlsConfig,

    #[serde(default)]
    pub gateway: GatewayConfig,

    #[serde(default)]
    pub service: ServiceConfig,
}

impl Default for ExternalListener {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_external_port(),
            hostname_pattern: String::new(),
            bootstrap_hostname: String::new(),
            tls: TlsConfig::default(),
            gateway: GatewayConfig::default(),
            service: ServiceConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    #[serde(default)]
    pub cert_manager: CertManagerConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CertManagerConfig {
    pub enabled: bool,
    #[serde(default)]
    pub issuer_ref: IssuerRef,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IssuerRef {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,

    /// `ClusterIssuer` or `Issuer`. Empty defaults at reconcile time
    /// (operator side; we don't apiserver-default it — gh #137).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[schemars(regex(pattern = r"^(ClusterIssuer|Issuer)?$"))]
    pub kind: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayConfig {
    pub enabled: bool,
    #[serde(default)]
    pub gateway_ref: GatewayRef,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRef {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceConfig {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub annotations: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bootstrap_servers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

fn default_internal_port() -> i32 {
    9092
}

fn default_external_port() -> i32 {
    9093
}
