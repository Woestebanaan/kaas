//! Kubernetes-style status `Condition` shim.
//!
//! `k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition` does
//! not derive `schemars::JsonSchema` under the workspace's
//! `k8s-openapi` feature set, so the kube-derive macro can't embed it
//! in a CRD `Status`. Mirror the apimachinery shape locally instead —
//! same field names, same JSON tags, same validation regexes per
//! https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Condition.
//!
//! The on-wire JSON shape is byte-identical to the apimachinery type
//! the controller-gen output produces, so the CRD YAML diff at
//! workstream E stays clean.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One status condition. Field shapes mirror apimachinery's
/// `metav1.Condition` exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Condition {
    /// CamelCase or `foo.example.com/CamelCase`. Required.
    #[serde(rename = "type")]
    #[schemars(
        length(max = 316),
        regex(
            pattern = r"^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$"
        )
    )]
    pub type_: String,

    /// `True`, `False`, or `Unknown`. Required.
    #[schemars(regex(pattern = r"^(True|False|Unknown)$"))]
    pub status: String,

    /// `metadata.generation` observed at transition. Optional;
    /// must be ≥ 0 when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub observed_generation: Option<i64>,

    /// RFC 3339 timestamp. Required.
    pub last_transition_time: String,

    /// Programmatic identifier. Required, CamelCase, 1..=1024 chars.
    #[schemars(
        length(min = 1, max = 1024),
        regex(pattern = r"^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$")
    )]
    pub reason: String,

    /// Human-readable. Required, up to 32768 chars.
    #[schemars(length(max = 32768))]
    pub message: String,
}

impl Condition {
    /// `True` is the apimachinery convention for the affirmative state.
    pub const STATUS_TRUE: &'static str = "True";
    /// `False` is the apimachinery convention for the negative state.
    pub const STATUS_FALSE: &'static str = "False";
    /// `Unknown` is the apimachinery convention when neither holds.
    pub const STATUS_UNKNOWN: &'static str = "Unknown";
}
