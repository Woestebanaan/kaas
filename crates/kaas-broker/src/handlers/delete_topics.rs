//! DeleteTopics handler (key 20).
//!
//! Per topic, delete the `KafkaTopic` CR via
//! the installed [`TopicCRWriter`] (the operator's reconciler tears
//! down the partition dirs; the topic-watcher fires Deleted on every
//! broker so open handles close first), then drop the topic from the
//! in-memory registry. Without a CR writer (dev mode, unit tests)
//! only the registry removal runs.
//!
//! [`TopicCRWriter`]: crate::topic_cr_writer::TopicCRWriter

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use kaas_codec::api::delete_topics;
use kaas_protocol::{ConnState, Handler, HandlerError};

use crate::broker::Broker;
use crate::topic_cr_writer::TopicWriteError;

const ERR_NONE: i16 = 0;
const ERR_UNKNOWN_TOPIC: i16 = 3;
const ERR_INVALID_REQUEST: i16 = 42;

#[derive(Debug)]
pub struct DeleteTopicsHandler {
    broker: Arc<Broker>,
}

impl DeleteTopicsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DeleteTopicsHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = delete_topics::decode_request(&mut body, version)?;

        let writer = self.broker.cr_writer();
        let mut responses = Vec::with_capacity(req.topic_names.len());
        for name in &req.topic_names {
            let mut result = delete_topics::DeletableTopicResult {
                name: name.clone(),
                error_code: ERR_NONE,
                error_message: None,
            };
            if let Some(w) = writer.as_ref() {
                match w.delete_topic(name).await {
                    Ok(()) => {}
                    Err(TopicWriteError::NotFound(_)) => {
                        result.error_code = ERR_UNKNOWN_TOPIC;
                    }
                    Err(e) => {
                        result.error_code = ERR_INVALID_REQUEST;
                        result.error_message = Some(e.to_string());
                    }
                }
                if result.error_code != ERR_NONE {
                    responses.push(result);
                    continue;
                }
            }
            self.broker.topics.remove(name);
            responses.push(result);
        }

        let resp = delete_topics::Response {
            throttle_time_ms: 0,
            responses,
        };
        let mut out = BytesMut::new();
        delete_topics::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
