//! Shared wire types for the ACL admin APIs — DescribeAcls (29),
//! CreateAcls (30), DeleteAcls (31).
//!
//! Enum tables + CR string mapping for the ACL surface (gh #107). The int8
//! codes match `org.apache.kafka.common.{resource,acl}` constants so
//! AdminClient requests translate 1:1 to the CR-side representation
//! the operator's ACL reconcile already speaks.

use crate::api::common::{read_nullable_str, write_nullable_str};
use crate::errors::CodecError;
use crate::primitives::{read_i8, write_i8};
use crate::tagged;
use crate::Bytes;
use bytes::BytesMut;

pub mod resource_type {
    pub const UNKNOWN: i8 = 0;
    pub const ANY: i8 = 1;
    pub const TOPIC: i8 = 2;
    pub const GROUP: i8 = 3;
    pub const CLUSTER: i8 = 4;
    pub const TRANSACTIONAL_ID: i8 = 5;
    pub const DELEGATION_TOKEN: i8 = 6;
    pub const USER: i8 = 7;
}

pub mod pattern_type {
    pub const UNKNOWN: i8 = 0;
    pub const ANY: i8 = 1;
    pub const MATCH: i8 = 2;
    pub const LITERAL: i8 = 3;
    pub const PREFIXED: i8 = 4;
}

pub mod operation {
    pub const UNKNOWN: i8 = 0;
    pub const ANY: i8 = 1;
    pub const ALL: i8 = 2;
    pub const READ: i8 = 3;
    pub const WRITE: i8 = 4;
    pub const CREATE: i8 = 5;
    pub const DELETE: i8 = 6;
    pub const ALTER: i8 = 7;
    pub const DESCRIBE: i8 = 8;
    pub const CLUSTER_ACTION: i8 = 9;
    pub const DESCRIBE_CONFIGS: i8 = 10;
    pub const ALTER_CONFIGS: i8 = 11;
    pub const IDEMPOTENT_WRITE: i8 = 12;
}

pub mod permission {
    pub const UNKNOWN: i8 = 0;
    pub const ANY: i8 = 1;
    pub const DENY: i8 = 2;
    pub const ALLOW: i8 = 3;
}

/// Wire → CR string for resource types. `None` for UNKNOWN/ANY (filter
/// wildcards; CreateAcls callers must pass a concrete type) and for
/// DELEGATION_TOKEN / USER (accepted on the wire but kaas has no
/// CR-side enum — the handler surfaces INVALID_REQUEST rather than
/// silently dropping).
pub fn resource_type_to_cr(t: i8) -> Option<&'static str> {
    match t {
        resource_type::TOPIC => Some("topic"),
        resource_type::GROUP => Some("group"),
        resource_type::CLUSTER => Some("cluster"),
        resource_type::TRANSACTIONAL_ID => Some("transactionalId"),
        _ => None,
    }
}

/// CR string → wire, for DescribeAcls / DeleteAcls responses. UNKNOWN
/// for unrecognised values so the encoded response stays well-formed.
pub fn resource_type_from_cr(s: &str) -> i8 {
    match s {
        "topic" => resource_type::TOPIC,
        "group" => resource_type::GROUP,
        "cluster" => resource_type::CLUSTER,
        "transactionalId" => resource_type::TRANSACTIONAL_ID,
        _ => resource_type::UNKNOWN,
    }
}

/// Wire → CR string for pattern types. `Some("")` for ANY (filter
/// wildcard); `None` for UNKNOWN. MATCH is stored as "match" — the
/// filter side treats it as literal+prefix per KIP-290.
pub fn pattern_type_to_cr(t: i8) -> Option<&'static str> {
    match t {
        pattern_type::LITERAL => Some("literal"),
        pattern_type::PREFIXED => Some("prefix"),
        pattern_type::MATCH => Some("match"),
        pattern_type::ANY => Some(""),
        _ => None,
    }
}

/// CR string → wire. LITERAL is the default, matching the operator-side
/// entry defaulting.
pub fn pattern_type_from_cr(s: &str) -> i8 {
    match s {
        "literal" | "" => pattern_type::LITERAL,
        "prefix" => pattern_type::PREFIXED,
        "match" => pattern_type::MATCH,
        _ => pattern_type::UNKNOWN,
    }
}

/// Wire → capitalised operation string used in `KafkaUserAcl.operations`
/// and the on-disk `acls.json` entries. `Some("")` for ANY (filter
/// wildcard); `None` for UNKNOWN.
pub fn operation_to_cr(op: i8) -> Option<&'static str> {
    match op {
        operation::ALL => Some("All"),
        operation::READ => Some("Read"),
        operation::WRITE => Some("Write"),
        operation::CREATE => Some("Create"),
        operation::DELETE => Some("Delete"),
        operation::ALTER => Some("Alter"),
        operation::DESCRIBE => Some("Describe"),
        operation::CLUSTER_ACTION => Some("ClusterAction"),
        operation::DESCRIBE_CONFIGS => Some("DescribeConfigs"),
        operation::ALTER_CONFIGS => Some("AlterConfigs"),
        operation::IDEMPOTENT_WRITE => Some("IdempotentWrite"),
        operation::ANY => Some(""),
        _ => None,
    }
}

/// CR string → wire — case-sensitive to match the on-disk Apache-Kafka
/// casing. UNKNOWN for unrecognised strings.
pub fn operation_from_cr(s: &str) -> i8 {
    match s {
        "All" => operation::ALL,
        "Read" => operation::READ,
        "Write" => operation::WRITE,
        "Create" => operation::CREATE,
        "Delete" => operation::DELETE,
        "Alter" => operation::ALTER,
        "Describe" => operation::DESCRIBE,
        "ClusterAction" => operation::CLUSTER_ACTION,
        "DescribeConfigs" => operation::DESCRIBE_CONFIGS,
        "AlterConfigs" => operation::ALTER_CONFIGS,
        "IdempotentWrite" => operation::IDEMPOTENT_WRITE,
        _ => operation::UNKNOWN,
    }
}

/// Wire → on-disk capitalised permission string. `Some("")` for ANY
/// (filter wildcard); `None` for UNKNOWN.
pub fn permission_to_cr(p: i8) -> Option<&'static str> {
    match p {
        permission::ALLOW => Some("Allow"),
        permission::DENY => Some("Deny"),
        permission::ANY => Some(""),
        _ => None,
    }
}

/// CR string → wire (accepts both casings the CR schema allows).
pub fn permission_from_cr(s: &str) -> i8 {
    match s {
        "Allow" | "allow" => permission::ALLOW,
        "Deny" | "deny" => permission::DENY,
        _ => permission::UNKNOWN,
    }
}

/// Filter shape used by DescribeAcls (whole request) and DeleteAcls
/// (one per filter entry). Nullable strings arrive as `None` ↔ wire
/// null; the handlers collapse those to "any along this axis".
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AclFilter {
    pub resource_type_filter: i8,
    pub resource_name_filter: Option<String>,
    /// v1+; the decoder leaves 0 (UNKNOWN) at v0 and the handler
    /// applies pre-KIP-290 literal semantics.
    pub pattern_type_filter: i8,
    pub principal_filter: Option<String>,
    pub host_filter: Option<String>,
    pub operation: i8,
    pub permission_type: i8,
}

/// One concrete ACL row (CreateAcls request entries; DescribeAcls /
/// DeleteAcls response entries).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AclBinding {
    pub resource_type: i8,
    pub resource_name: String,
    /// v1+.
    pub pattern_type: i8,
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission: i8,
}

/// Read one filter body (shared by DescribeAcls request and DeleteAcls
/// filter entries). The caller reads any surrounding array framing.
pub fn read_filter(buf: &mut Bytes, version: i16, flexible: bool) -> Result<AclFilter, CodecError> {
    let resource_type_filter = read_i8(buf)?;
    let resource_name_filter = read_nullable_str(buf, flexible)?;
    let pattern_type_filter = if version >= 1 { read_i8(buf)? } else { 0 };
    let principal_filter = read_nullable_str(buf, flexible)?;
    let host_filter = read_nullable_str(buf, flexible)?;
    let operation = read_i8(buf)?;
    let permission_type = read_i8(buf)?;
    Ok(AclFilter {
        resource_type_filter,
        resource_name_filter,
        pattern_type_filter,
        principal_filter,
        host_filter,
        operation,
        permission_type,
    })
}

/// Write one filter body — the encode mirror of [`read_filter`], used
/// by request-encode paths (tests, future admin client).
pub fn write_filter(
    buf: &mut BytesMut,
    f: &AclFilter,
    version: i16,
    flexible: bool,
) -> Result<(), CodecError> {
    write_i8(buf, f.resource_type_filter);
    write_nullable_str(buf, f.resource_name_filter.as_deref(), flexible)?;
    if version >= 1 {
        write_i8(buf, f.pattern_type_filter);
    }
    write_nullable_str(buf, f.principal_filter.as_deref(), flexible)?;
    write_nullable_str(buf, f.host_filter.as_deref(), flexible)?;
    write_i8(buf, f.operation);
    write_i8(buf, f.permission_type);
    Ok(())
}

/// Read a tagged-fields block when flexible — tiny helper the three
/// ACL modules share for per-entry framing.
pub fn read_entry_tags(buf: &mut Bytes, flexible: bool) -> Result<(), CodecError> {
    if flexible {
        tagged::read(buf)?;
    }
    Ok(())
}

/// Write an empty tagged-fields block when flexible.
pub fn write_entry_tags(buf: &mut BytesMut, flexible: bool) {
    if flexible {
        tagged::write_empty(buf);
    }
}
