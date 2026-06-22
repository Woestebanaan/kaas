//! sk-broker — broker glue: [`Broker`], [`TopicRegistry`],
//! [`LocalLeaseManager`], env-var parser ([`cli`]).
//!
//! Phase 3 ships the narrow shape every handler reads from. Phase 5
//! grows this with the real `Coordinator` (assignment.json watcher),
//! takeover driver, heartbeat client. The `heartbeatpb` module is
//! generated at build time by `tonic-build` from
//! `proto/heartbeat.proto` and is Phase-5-consumer-only for now.

pub mod broker;
pub mod cli;
pub mod handlers;
pub mod local_lease;
pub mod topic_registry;

pub use broker::Broker;
pub use cli::{Cli, ListenerEntry};
pub use handlers::{
    ApiVersionsHandler, FetchHandler, InitProducerIdHandler, ListOffsetsHandler, MetadataHandler,
    ProduceHandler,
};
pub use local_lease::LocalLeaseManager;
pub use topic_registry::{ConfigError as TopicConfigError, TopicMeta, TopicRegistry};

pub mod heartbeatpb {
    // tonic-build emits `as i32`, large match arms, and similar patterns that
    // trip the workspace clippy gate. Generated code is not subject to the
    // hand-written style rules.
    #![allow(
        clippy::all,
        clippy::pedantic,
        clippy::nursery,
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        missing_debug_implementations
    )]
    tonic::include_proto!("skafka.heartbeat.v1");
}
