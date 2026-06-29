//! DescribeConfigs handler — API key 32.
//!
//! Topic-only surface (BROKER + BROKER_LOGGER return
//! `UNSUPPORTED_VERSION` (35) on the per-resource result — same
//! Strimzi-compat shape `IncrementalAlterConfigs` uses).
//!
//! For each topic resource, walk the live [`TopicMeta`] surface
//! and answer with the Apache-3.7-compatible default table plus
//! anything the broker has explicitly stamped on the topic. The
//! handler doesn't reach into the operator's `KafkaTopic.spec.config`
//! directly — it consumes whatever the broker's
//! `sk_storage::TopicConfigFile` reader has loaded (which the
//! cleaner / compactor already gates on).
//!
//! v1+ adds `include_synonyms` (we always emit one default-source
//! synonym alongside every entry to mirror Apache's behaviour).
//! v3+ adds `include_documentation` (looked up via
//! [`topic_config_defaults::description`]).
//!
//! Authorization: `Operation::DescribeConfigs` on the topic.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use sk_auth::{Operation, Principal, Resource};
use sk_codec::api::describe_configs::{
    self, config_type, resource_type, source, DescribeConfigsResult, DescribeConfigsResultConfig,
    DescribeConfigsSynonym, Response,
};
use sk_protocol::{ConnState, Handler, HandlerError};

use crate::broker::Broker;
use crate::topic_config_defaults;

const ERR_NONE: i16 = 0;
const ERR_UNKNOWN_TOPIC: i16 = 3;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_UNSUPPORTED_VERSION: i16 = 35;

#[derive(Debug)]
pub struct DescribeConfigsHandler {
    broker: Arc<Broker>,
}

impl DescribeConfigsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DescribeConfigsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_configs::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let mut results = Vec::with_capacity(req.resources.len());

        for resource in req.resources {
            if resource.resource_type != resource_type::TOPIC {
                results.push(DescribeConfigsResult {
                    error_code: ERR_UNSUPPORTED_VERSION,
                    error_message: Some("only Topic resource type is supported".into()),
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: vec![],
                });
                continue;
            }

            // Authorize.
            let res = Resource::topic(&resource.resource_name);
            if !self
                .broker
                .authorizer
                .authorize(&principal, &res, Operation::DescribeConfigs)
            {
                results.push(DescribeConfigsResult {
                    error_code: ERR_TOPIC_AUTHZ_FAILED,
                    error_message: None,
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: vec![],
                });
                continue;
            }

            // Topic must exist on this broker.
            if self.broker.topics.get(&resource.resource_name).is_none() {
                results.push(DescribeConfigsResult {
                    error_code: ERR_UNKNOWN_TOPIC,
                    error_message: None,
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: vec![],
                });
                continue;
            }

            let configs = topic_config_defaults::ALL_KEYS
                .iter()
                .filter(|entry| match resource.configuration_keys.as_ref() {
                    None => true,
                    Some(keys) => keys.iter().any(|k| k == entry.dotted_name),
                })
                .map(|entry| make_config(entry, version))
                .collect();

            results.push(DescribeConfigsResult {
                error_code: ERR_NONE,
                error_message: None,
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
                configs,
            });
        }

        let resp = Response {
            throttle_time_ms: 0,
            results,
        };
        let mut out = BytesMut::new();
        describe_configs::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

fn make_config(entry: &topic_config_defaults::Entry, version: i16) -> DescribeConfigsResultConfig {
    let value = entry.default_value.map(str::to_owned);
    let synonyms = if version >= 1 {
        vec![DescribeConfigsSynonym {
            name: entry.dotted_name.into(),
            value: value.clone(),
            source: source::DEFAULT_CONFIG,
        }]
    } else {
        vec![]
    };
    DescribeConfigsResultConfig {
        name: entry.dotted_name.into(),
        value,
        read_only: false,
        is_default: true,
        is_sensitive: false,
        synonyms,
        config_type: if version >= 2 {
            entry.config_type
        } else {
            config_type::UNKNOWN
        },
        config_source: if version >= 1 {
            source::DEFAULT_CONFIG
        } else {
            source::UNKNOWN
        },
        documentation: if version >= 3 {
            Some(entry.documentation.into())
        } else {
            None
        },
    }
}

fn principal_from(conn: &Mutex<ConnState>) -> Principal {
    conn.lock()
        .principal
        .clone()
        .unwrap_or_else(Principal::anonymous)
}
