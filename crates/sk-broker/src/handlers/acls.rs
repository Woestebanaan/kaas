//! ACL admin handlers — DescribeAcls (29), CreateAcls (30),
//! DeleteAcls (31).
//!
//! The gh #107 ACL admin handlers. The wire shape
//! (int8 enum codes) is translated to the CR-side string shape and
//! delegated to the installed [`AclCRWriter`], which patches
//! `KafkaUser.spec.authorization.acls` in place; the operator's ACL
//! reconcile rebuilds `/data/__cluster/acls.json` and every broker
//! hot-reloads.
//!
//! Without a writer wired (kafka-compat tests, dev mode without an
//! apiserver) the handlers degrade to the pre-gh #107 stubs: empty
//! describe results, per-entry no-op create/delete.
//!
//! [`AclCRWriter`]: crate::acl_cr_writer::AclCRWriter

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use sk_codec::api::acl_types::{
    self, operation_from_cr, operation_to_cr, pattern_type, pattern_type_from_cr,
    pattern_type_to_cr, permission_from_cr, permission_to_cr, resource_type_from_cr,
    resource_type_to_cr,
};
use sk_codec::api::{create_acls, delete_acls, describe_acls};
use sk_protocol::{ConnState, Handler, HandlerError};

use crate::acl_cr_writer::{AclBinding, AclFilter, AclWriteError};
use crate::broker::Broker;

const ERR_UNKNOWN_SERVER_ERROR: i16 = -1;
const ERR_INVALID_REQUEST: i16 = 42;

/// CreateAcls wire entry → CR string binding. Rejects UNKNOWN/ANY
/// codes and unsupported resource types (DELEGATION_TOKEN, USER) —
/// CreateAcls callers must specify concrete bindings.
fn wire_binding_to_acl(b: &acl_types::AclBinding, version: i16) -> Result<AclBinding, String> {
    let rt = resource_type_to_cr(b.resource_type)
        .ok_or_else(|| format!("unsupported resource type: {}", b.resource_type))?;
    // v0 has no PatternType field; Apache's v0 semantic is "literal".
    let pattern_code = if version < 1 {
        pattern_type::LITERAL
    } else {
        b.pattern_type
    };
    let pt = pattern_type_to_cr(pattern_code)
        .ok_or_else(|| format!("unsupported pattern type: {pattern_code}"))?;
    if pt.is_empty() {
        return Err("pattern type ANY not valid for create".into());
    }
    let op = match operation_to_cr(b.operation) {
        Some(op) if !op.is_empty() => op,
        _ => return Err(format!("unsupported operation: {}", b.operation)),
    };
    let perm = match permission_to_cr(b.permission) {
        Some(p) if !p.is_empty() => p,
        _ => return Err(format!("unsupported permission: {}", b.permission)),
    };
    Ok(AclBinding {
        principal: b.principal.clone(),
        resource_type: rt.to_string(),
        resource_name: b.resource_name.clone(),
        pattern_type: pt.to_string(),
        operation: op.to_string(),
        permission: perm.to_string(),
        host: b.host.clone(),
    })
}

/// DescribeAcls/DeleteAcls wire filter → CR string filter. ANY /
/// UNKNOWN codes and null strings collapse to "" (wildcards).
fn wire_filter_to_acl(f: &acl_types::AclFilter, version: i16) -> Result<AclFilter, String> {
    let mut out = AclFilter {
        principal: f.principal_filter.clone().unwrap_or_default(),
        resource_name: f.resource_name_filter.clone().unwrap_or_default(),
        host: f.host_filter.clone().unwrap_or_default(),
        ..AclFilter::default()
    };
    if f.resource_type_filter != acl_types::resource_type::UNKNOWN
        && f.resource_type_filter != acl_types::resource_type::ANY
    {
        out.resource_type = resource_type_to_cr(f.resource_type_filter)
            .ok_or_else(|| {
                format!(
                    "unsupported resource type filter: {}",
                    f.resource_type_filter
                )
            })?
            .to_string();
    }
    // v0 has no PatternType filter; pre-KIP-290 semantics treated
    // every entry as literal, so filter on literal to avoid matching
    // prefixed entries.
    let pattern_code = if version < 1 {
        pattern_type::LITERAL
    } else {
        f.pattern_type_filter
    };
    if pattern_code != pattern_type::UNKNOWN && pattern_code != pattern_type::ANY {
        out.pattern_type = pattern_type_to_cr(pattern_code)
            .ok_or_else(|| format!("unsupported pattern type filter: {pattern_code}"))?
            .to_string();
    }
    if f.operation != acl_types::operation::UNKNOWN && f.operation != acl_types::operation::ANY {
        out.operation = match operation_to_cr(f.operation) {
            Some(op) if !op.is_empty() => op.to_string(),
            _ => return Err(format!("unsupported operation filter: {}", f.operation)),
        };
    }
    if f.permission_type != acl_types::permission::UNKNOWN
        && f.permission_type != acl_types::permission::ANY
    {
        out.permission = match permission_to_cr(f.permission_type) {
            Some(p) if !p.is_empty() => p.to_string(),
            _ => {
                return Err(format!(
                    "unsupported permission filter: {}",
                    f.permission_type
                ))
            }
        };
    }
    Ok(out)
}

/// CR string binding → wire shape for Describe / Delete responses.
fn acl_to_wire_binding(b: &AclBinding) -> acl_types::AclBinding {
    acl_types::AclBinding {
        resource_type: resource_type_from_cr(&b.resource_type),
        resource_name: b.resource_name.clone(),
        pattern_type: pattern_type_from_cr(&b.pattern_type),
        principal: b.principal.clone(),
        host: b.host.clone(),
        operation: operation_from_cr(&b.operation),
        permission: permission_from_cr(&b.permission),
    }
}

/// Fold a flat binding list into the per-resource shape Apache
/// clients expect: one resource row per (type, name, pattern) with
/// N matching-ACL rows inside.
fn group_bindings_by_resource(bindings: &[AclBinding]) -> Vec<describe_acls::DescribeAclsResource> {
    let mut out: Vec<describe_acls::DescribeAclsResource> = Vec::new();
    let mut idx: std::collections::HashMap<(String, String, String), usize> =
        std::collections::HashMap::new();
    for b in bindings {
        let key = (
            b.resource_type.clone(),
            b.resource_name.clone(),
            b.pattern_type.clone(),
        );
        let i = *idx.entry(key).or_insert_with(|| {
            out.push(describe_acls::DescribeAclsResource {
                resource_type: resource_type_from_cr(&b.resource_type),
                resource_name: b.resource_name.clone(),
                pattern_type: pattern_type_from_cr(&b.pattern_type),
                acls: Vec::new(),
            });
            out.len() - 1
        });
        out[i].acls.push(describe_acls::MatchingAcl {
            principal: b.principal.clone(),
            host: b.host.clone(),
            operation: operation_from_cr(&b.operation),
            permission: permission_from_cr(&b.permission),
        });
    }
    out
}

// ---- DescribeAcls (29) ---------------------------------------------

#[derive(Debug)]
pub struct DescribeAclsHandler {
    broker: Arc<Broker>,
}

impl DescribeAclsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DescribeAclsHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_acls::decode_request(&mut body, version)?;

        let mut resp = describe_acls::Response::default();
        if let Some(w) = self.broker.acl_cr_writer() {
            match wire_filter_to_acl(&req.filter, version) {
                Err(msg) => {
                    resp.error_code = ERR_INVALID_REQUEST;
                    resp.error_message = Some(msg);
                }
                Ok(filter) => match w.list_acls(filter).await {
                    Ok(bindings) => resp.resources = group_bindings_by_resource(&bindings),
                    Err(e) => {
                        resp.error_code = ERR_UNKNOWN_SERVER_ERROR;
                        resp.error_message = Some(e.to_string());
                    }
                },
            }
        }

        let mut out = BytesMut::new();
        describe_acls::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

// ---- CreateAcls (30) -----------------------------------------------

#[derive(Debug)]
pub struct CreateAclsHandler {
    broker: Arc<Broker>,
}

impl CreateAclsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for CreateAclsHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = create_acls::decode_request(&mut body, version)?;

        let writer = self.broker.acl_cr_writer();
        let mut results = Vec::with_capacity(req.creations.len());
        for b in &req.creations {
            let mut result = create_acls::CreateAclsResult::default();
            if let Some(w) = writer.as_ref() {
                match wire_binding_to_acl(b, version) {
                    Err(msg) => {
                        result.error_code = ERR_INVALID_REQUEST;
                        result.error_message = Some(msg);
                    }
                    Ok(binding) => {
                        if let Err(e) = w.create_acl(binding).await {
                            result.error_code = match e {
                                AclWriteError::UnknownPrincipal(_)
                                | AclWriteError::InvalidPrincipal(_) => ERR_INVALID_REQUEST,
                                AclWriteError::Other(_) => ERR_UNKNOWN_SERVER_ERROR,
                            };
                            result.error_message = Some(e.to_string());
                        }
                    }
                }
            }
            results.push(result);
        }

        let resp = create_acls::Response {
            throttle_time_ms: 0,
            results,
        };
        let mut out = BytesMut::new();
        create_acls::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

// ---- DeleteAcls (31) -----------------------------------------------

#[derive(Debug)]
pub struct DeleteAclsHandler {
    broker: Arc<Broker>,
}

impl DeleteAclsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DeleteAclsHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = delete_acls::decode_request(&mut body, version)?;

        let writer = self.broker.acl_cr_writer();
        let mut filter_results = Vec::with_capacity(req.filters.len());
        for f in &req.filters {
            let mut fr = delete_acls::DeleteAclsFilterResult::default();
            if let Some(w) = writer.as_ref() {
                match wire_filter_to_acl(f, version) {
                    Err(msg) => {
                        fr.error_code = ERR_INVALID_REQUEST;
                        fr.error_message = Some(msg);
                    }
                    Ok(filter) => match w.delete_acls(filter).await {
                        Ok(matched) => {
                            fr.matching_acls = matched
                                .iter()
                                .map(|m| delete_acls::DeleteAclsMatchingAcl {
                                    error_code: 0,
                                    error_message: None,
                                    binding: acl_to_wire_binding(m),
                                })
                                .collect();
                        }
                        Err(e) => {
                            fr.error_code = ERR_UNKNOWN_SERVER_ERROR;
                            fr.error_message = Some(e.to_string());
                        }
                    },
                }
            }
            filter_results.push(fr);
        }

        let resp = delete_acls::Response {
            throttle_time_ms: 0,
            filter_results,
        };
        let mut out = BytesMut::new();
        delete_acls::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sk_codec::api::acl_types::{operation, permission, resource_type};

    #[test]
    fn wire_binding_translation_maps_enums() {
        let b = acl_types::AclBinding {
            resource_type: resource_type::TOPIC,
            resource_name: "orders".into(),
            pattern_type: pattern_type::PREFIXED,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: operation::WRITE,
            permission: permission::ALLOW,
        };
        let acl = wire_binding_to_acl(&b, 1).unwrap();
        assert_eq!(acl.resource_type, "topic");
        assert_eq!(acl.pattern_type, "prefix");
        assert_eq!(acl.operation, "Write");
        assert_eq!(acl.permission, "Allow");
    }

    #[test]
    fn wire_binding_rejects_any_codes() {
        let b = acl_types::AclBinding {
            resource_type: resource_type::ANY,
            ..acl_types::AclBinding::default()
        };
        assert!(wire_binding_to_acl(&b, 1).is_err());
    }

    #[test]
    fn v0_filter_defaults_to_literal_pattern() {
        let f = acl_types::AclFilter {
            resource_type_filter: resource_type::TOPIC,
            ..acl_types::AclFilter::default()
        };
        let filter = wire_filter_to_acl(&f, 0).unwrap();
        assert_eq!(filter.pattern_type, "literal");
        // v1+ leaves UNKNOWN=0 alone → wildcard.
        let filter = wire_filter_to_acl(&f, 1).unwrap();
        assert_eq!(filter.pattern_type, "");
    }

    #[test]
    fn group_by_resource_folds_matching_rows() {
        let mk = |op: &str| AclBinding {
            principal: "User:alice".into(),
            resource_type: "topic".into(),
            resource_name: "orders".into(),
            pattern_type: "literal".into(),
            operation: op.into(),
            permission: "Allow".into(),
            host: "*".into(),
        };
        let grouped = group_bindings_by_resource(&[mk("Read"), mk("Write")]);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].acls.len(), 2);
    }
}
