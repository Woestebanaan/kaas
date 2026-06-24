//! `Authorizer` — cluster-wide authorization decision surface.
//!
//! Three impls ship in Phase 4:
//!
//! - [`AllowAllAuthorizer`] — every check returns `true`. Default
//!   when `authorization` is unset (Strimzi "no authorization
//!   property = no restrictions" semantic).
//! - [`SuperUserAuthorizer`] — wraps an inner authorizer with an
//!   early-allow check for principals matching a configured
//!   superUser set.
//! - [`crate::AclEngine`] — implements `Authorizer` via the
//!   deny-overrides-allow ACL rule set loaded from `acls.json`.

use std::collections::HashSet;
use std::sync::Arc;

use crate::types::{Operation, Principal, Resource};

pub trait Authorizer: Send + Sync + std::fmt::Debug + 'static {
    fn authorize(&self, principal: &Principal, resource: &Resource, op: Operation) -> bool;
}

#[derive(Debug, Default)]
pub struct AllowAllAuthorizer;

impl Authorizer for AllowAllAuthorizer {
    fn authorize(&self, _p: &Principal, _r: &Resource, _op: Operation) -> bool {
        true
    }
}

/// Wraps an inner Authorizer with a superUsers early-allow check.
/// Principals matching one of the configured `User:foo`-shape strings
/// bypass ACL evaluation entirely (Strimzi `authorization.superUsers`
/// semantic).
///
/// Match is against the principal-string render (`User:alice`,
/// `ServiceAccount:default/app`). Operators configure with the same
/// shape they'd write in an ACL rule.
#[derive(Debug)]
pub struct SuperUserAuthorizer {
    supers: HashSet<String>,
    inner: Arc<dyn Authorizer>,
}

impl SuperUserAuthorizer {
    pub fn new(supers: Vec<String>, inner: Arc<dyn Authorizer>) -> Self {
        Self {
            supers: supers.into_iter().collect(),
            inner,
        }
    }
}

impl Authorizer for SuperUserAuthorizer {
    fn authorize(&self, principal: &Principal, resource: &Resource, op: Operation) -> bool {
        if self.supers.contains(&principal.as_principal_string()) {
            return true;
        }
        self.inner.authorize(principal, resource, op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PrincipalKind, ResourceKind};

    #[derive(Debug, Default)]
    struct DenyAll;
    impl Authorizer for DenyAll {
        fn authorize(&self, _p: &Principal, _r: &Resource, _op: Operation) -> bool {
            false
        }
    }

    fn p_alice() -> Principal {
        Principal {
            name: "alice".to_owned(),
            kind: PrincipalKind::User,
        }
    }

    fn r_foo() -> Resource {
        Resource::topic("foo")
    }

    #[test]
    fn allow_all_permits_everything() {
        let a = AllowAllAuthorizer;
        assert!(a.authorize(&Principal::anonymous(), &r_foo(), Operation::Read));
        assert!(a.authorize(&p_alice(), &r_foo(), Operation::Write));
    }

    #[test]
    fn super_user_short_circuits_inner() {
        let inner: Arc<dyn Authorizer> = Arc::new(DenyAll);
        let su = SuperUserAuthorizer::new(vec!["User:alice".to_owned()], inner);
        assert!(su.authorize(&p_alice(), &r_foo(), Operation::Write));
    }

    #[test]
    fn super_user_falls_through_to_inner() {
        let inner: Arc<dyn Authorizer> = Arc::new(DenyAll);
        let su = SuperUserAuthorizer::new(vec!["User:root".to_owned()], inner);
        assert!(!su.authorize(&p_alice(), &r_foo(), Operation::Write));
    }

    #[test]
    fn anonymous_principal_renders_for_super_user_match() {
        let inner: Arc<dyn Authorizer> = Arc::new(DenyAll);
        let su = SuperUserAuthorizer::new(vec!["User:ANONYMOUS".to_owned()], inner);
        let res = Resource {
            kind: ResourceKind::Topic,
            name: "anything".to_owned(),
            pattern: crate::PatternType::Literal,
        };
        assert!(su.authorize(&Principal::anonymous(), &res, Operation::Read));
    }
}
