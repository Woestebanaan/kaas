//! `KafkaClusterAssignments` — read-only debug mirror of
//! `/data/__cluster/assignment.json`.
//!
//! Port of
//! `archive/operator/api/v1alpha1/kafkaclusterassignments_types.go`.
//! The controller-broker writes `Status` fire-and-forget after every
//! `assignment.json` rewrite; brokers never read this CR. One CR per
//! `KafkaCluster`, sharing the parent's name and namespace.
//!
//! `Spec` is intentionally empty — all state lives in `Status`.
//! Modifying `Spec` has no effect on cluster behaviour.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[kube(
    group = "skafka.io",
    version = "v1alpha1",
    kind = "KafkaClusterAssignments",
    plural = "kafkaclusterassignments",
    singular = "kafkaclusterassignments",
    namespaced,
    status = "KafkaClusterAssignmentsStatus",
    printcolumn = r#"{"name":"Controller","type":"string","jsonPath":".status.controller"}"#,
    printcolumn = r#"{"name":"Epoch","type":"integer","jsonPath":".status.controllerEpoch"}"#,
    printcolumn = r#"{"name":"Version","type":"integer","jsonPath":".status.assignmentVersion"}"#,
    printcolumn = r#"{"name":"Truncated","type":"boolean","jsonPath":".status.truncated"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterAssignmentsSpec {}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterAssignmentsStatus {
    /// `leaseTransitions` of the `skafka-controller` Lease at write
    /// time. Brokers reject `assignment.json` files with stale epochs.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub controller_epoch: i64,

    /// Controller-local monotonic counter, bumped on every write
    /// within a single controller's tenure.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub assignment_version: i64,

    /// RFC 3339 wall-clock time the controller produced this
    /// assignment.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub generated_at: String,

    /// Broker ID currently holding the controller Lease.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub controller: String,

    /// `true` when the partition list was clipped to fit under the
    /// 1MB K8s object size limit. Inspect the file on the PVC for
    /// the full list when set.
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<MirroredBroker>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<MirroredPartition>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consumer_groups: Vec<MirroredConsumerGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MirroredBroker {
    pub id: String,

    /// `alive`, `draining`, or `dead`.
    #[schemars(regex(pattern = r"^(alive|draining|dead)$"))]
    pub health: BrokerHealth,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_seen: String,
}

/// String-typed enum mirror so the on-wire JSON value is a literal
/// (`"alive"` / `"draining"` / `"dead"`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BrokerHealth {
    Alive,
    Draining,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MirroredPartition {
    pub topic: String,
    pub partition: i32,
    pub broker: String,
    pub epoch: i64,

    /// `leader` (only role today; reserved for future).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[schemars(regex(pattern = r"^(leader)?$"))]
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MirroredConsumerGroup {
    pub group_id: String,
    pub broker: String,
    pub epoch: i64,
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}
