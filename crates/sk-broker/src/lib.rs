//! sk-broker — broker glue: Coordinator, takeover, heartbeat client.
//!
//! Populated in Phase 5 of the rewrite. The `heartbeatpb` module is generated
//! at build time by `tonic-build` from `proto/heartbeat.proto`.

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
