//! Broker-side API handler implementations.
//!
//! One file per API; each impl satisfies the
//! [`sk_protocol::Handler`] trait. The host (`bins/skafka/main.rs`)
//! registers them on a [`sk_protocol::Dispatcher`].

pub mod add_offsets_to_txn;
pub mod add_partitions_to_txn;
pub mod api_versions;
pub mod delete_groups;
pub mod describe_groups;
pub mod end_txn;
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod init_producer_id;
pub mod join_group;
pub mod leave_group;
pub mod list_groups;
pub mod list_offsets;
pub mod metadata;
pub mod offset_commit;
pub mod offset_delete;
pub mod offset_fetch;
pub mod produce;
pub mod sasl;
pub mod sync_group;
pub mod txn_offset_commit;

pub use add_offsets_to_txn::AddOffsetsToTxnHandler;
pub use add_partitions_to_txn::AddPartitionsToTxnHandler;
pub use api_versions::ApiVersionsHandler;
pub use delete_groups::DeleteGroupsHandler;
pub use describe_groups::DescribeGroupsHandler;
pub use end_txn::EndTxnHandler;
pub use fetch::FetchHandler;
pub use find_coordinator::FindCoordinatorHandler;
pub use heartbeat::HeartbeatHandler;
pub use init_producer_id::InitProducerIdHandler;
pub use join_group::JoinGroupHandler;
pub use leave_group::LeaveGroupHandler;
pub use list_groups::ListGroupsHandler;
pub use list_offsets::ListOffsetsHandler;
pub use metadata::MetadataHandler;
pub use offset_commit::OffsetCommitHandler;
pub use offset_delete::OffsetDeleteHandler;
pub use offset_fetch::OffsetFetchHandler;
pub use produce::ProduceHandler;
pub use sasl::{SaslAuthenticateHandler, SaslHandshakeHandler};
pub use sync_group::SyncGroupHandler;
pub use txn_offset_commit::TxnOffsetCommitHandler;
