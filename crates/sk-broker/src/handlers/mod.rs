//! Broker-side API handler implementations.
//!
//! One file per API; each impl satisfies the
//! [`sk_protocol::Handler`] trait. The host (`bins/skafka/main.rs`)
//! registers them on a [`sk_protocol::Dispatcher`].

pub mod api_versions;
pub mod fetch;
pub mod init_producer_id;
pub mod list_offsets;
pub mod metadata;
pub mod produce;
pub mod sasl;

pub use api_versions::ApiVersionsHandler;
pub use fetch::FetchHandler;
pub use init_producer_id::InitProducerIdHandler;
pub use list_offsets::ListOffsetsHandler;
pub use metadata::MetadataHandler;
pub use produce::ProduceHandler;
pub use sasl::{SaslAuthenticateHandler, SaslHandshakeHandler};
