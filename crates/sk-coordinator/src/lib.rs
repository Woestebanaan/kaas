//! sk-coordinator — consumer-group + transaction coordinator,
//! offset store.
//!
//! Populated in Phase 5 (group coordinator) + Phase 6 (transactions).

pub mod atomic_write;
pub mod errors;
pub mod group;
pub mod manager;
pub mod offset_store;

pub use errors::CoordError;
pub use group::{
    error_codes, Group, GroupSnapshot, GroupState, HeartbeatRequest, JoinOutcome, JoinRequest,
    JoinedMember, MemberSnapshot, ProtocolMetadata, SyncAssignment, SyncOutcome, SyncRequest,
};
pub use manager::{
    build_fetch_specs, build_offset_key, BrokerEndpoint, BrokerId, BrokerLookup,
    CoordinatorResolution, FnLookup, GroupAssignmentSource, LeaveOutcome, LocalGroupSource,
    LocalTxnSource, Manager, TxnAssignmentSource,
};
pub use offset_store::{offset_key, FetchSpec, OffsetStore};
