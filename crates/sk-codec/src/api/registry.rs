//! Single declarative table of every API key the broker exposes.
//!
//! Drives the ApiVersions response in [`crate::api::api_versions`] and the
//! header-version lookup in [`crate::headers`]. The full 40-key table is
//! filled in over the course of Phase 1 — each per-API module's commit
//! adds its row here.

use crate::headers::HeaderVersion;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiSpec {
    pub key: i16,
    pub min_version: i16,
    pub max_version: i16,
    /// `Some(v)` if the API is flexible (KIP-482) from version `v` onward;
    /// `None` if all supported versions are still legacy.
    pub min_flexible: Option<i16>,
    pub request_hdr: fn(i16) -> HeaderVersion,
    pub response_hdr: fn(i16) -> HeaderVersion,
}

impl ApiSpec {
    /// True if `version` is in the flexible range for this API.
    pub fn is_flexible(&self, version: i16) -> bool {
        matches!(self.min_flexible, Some(min) if version >= min)
    }
}

/// Every API key the Go broker registers in
/// `archive/internal/broker/broker.go:555-891`. Phase 1 seeds with one
/// entry; the rest land as their per-API modules are ported.
pub const ALL: &[ApiSpec] = &[
    crate::api::produce::SPEC,
    crate::api::fetch::SPEC,
    crate::api::list_offsets::SPEC,
    crate::api::metadata::SPEC,
    crate::api::offset_commit::SPEC,
    crate::api::offset_fetch::SPEC,
    crate::api::find_coordinator::SPEC,
    crate::api::join_group::SPEC,
    crate::api::heartbeat::SPEC,
    crate::api::leave_group::SPEC,
    crate::api::sync_group::SPEC,
    crate::api::describe_groups::SPEC,
    crate::api::list_groups::SPEC,
    crate::api::sasl_handshake::SPEC,
    crate::api::init_producer_id::SPEC,
    crate::api::sasl_authenticate::SPEC,
    crate::api::api_versions::SPEC,
    crate::api::delete_groups::SPEC,
    crate::api::offset_delete::SPEC,
];

/// Look up the [`ApiSpec`] for a given API key, if registered.
pub fn lookup(key: i16) -> Option<&'static ApiSpec> {
    ALL.iter().find(|s| s.key == key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_returns_known_key() {
        let spec = lookup(18).expect("ApiVersions seeded in ALL");
        assert_eq!(spec.key, 18);
        assert_eq!(spec.min_version, 0);
        assert_eq!(spec.max_version, 4);
        assert_eq!(spec.min_flexible, Some(3));
    }

    #[test]
    fn lookup_returns_none_for_unknown_key() {
        assert!(lookup(999).is_none());
    }

    #[test]
    fn flex_predicate() {
        let spec = lookup(18).expect("seeded");
        assert!(!spec.is_flexible(2));
        assert!(spec.is_flexible(3));
        assert!(spec.is_flexible(4));
    }

    /// Phase 5 exit criterion §A — registry pins to 19 entries:
    /// keys 0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 22,
    /// 36, 42, 47. Bump this number when a new module lands.
    #[test]
    fn registry_size_phase5() {
        assert_eq!(ALL.len(), 19);
        let mut keys: Vec<i16> = ALL.iter().map(|s| s.key).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 22, 36, 42, 47]
        );
    }
}
