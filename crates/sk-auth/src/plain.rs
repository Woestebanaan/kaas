//! SASL PLAIN — static-credential path.
//!
//! Phase 4 ships the static-credential flavor: the SASL PLAIN message
//! carries `NUL || authzid || NUL || authcid || NUL || password` and
//! the broker compares `password` against the user's `plainPassword`
//! field in `credentials.json`. Production operators do not write
//! `plainPassword`; the static path exists for tests and dev-mode
//! opt-in.
//!
//! The K8s ServiceAccount JWT flavor is deferred — an open
//! follow-up alongside the rest of the K8s plumbing.

use std::sync::Arc;

use subtle::ConstantTimeEq;

use crate::credentials::CredentialStore;
use crate::engine::{record_sasl_outcome, SaslExchange};
use crate::errors::AuthError;
use crate::types::{Principal, PrincipalKind};

#[derive(Debug)]
pub struct PlainExchange {
    store: Arc<dyn CredentialStore>,
    principal: Option<Principal>,
}

impl PlainExchange {
    pub fn new(store: Arc<dyn CredentialStore>) -> Self {
        Self {
            store,
            principal: None,
        }
    }
}

impl SaslExchange for PlainExchange {
    fn step(&mut self, client_msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError> {
        let outcome = self.step_inner(client_msg);
        record_sasl_outcome("PLAIN", &outcome);
        outcome
    }

    fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }
}

impl PlainExchange {
    fn step_inner(&mut self, client_msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError> {
        let mut parts = client_msg.splitn(3, |b| *b == 0);
        let _authzid = parts.next().ok_or(AuthError::MalformedSaslMessage)?;
        let authcid = parts.next().ok_or(AuthError::MalformedSaslMessage)?;
        let password = parts.next().ok_or(AuthError::MalformedSaslMessage)?;
        if authcid.is_empty() || password.is_empty() {
            return Err(AuthError::MalformedSaslMessage);
        }
        let username = std::str::from_utf8(authcid).map_err(|_| AuthError::MalformedSaslMessage)?;
        let expected = self
            .store
            .lookup_plain_password(username)
            .ok_or(AuthError::BadCredentials)?;
        // Constant-time compare to avoid leaking password-length info
        // via SASL response timing.
        if password.ct_eq(expected.as_bytes()).unwrap_u8() != 1 {
            return Err(AuthError::BadCredentials);
        }
        self.principal = Some(Principal {
            name: username.to_owned(),
            kind: PrincipalKind::User,
        });
        // SASL PLAIN has no server-side challenge — `done = true` on
        // the first round trip.
        Ok((Vec::new(), true))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::TestCred;
    use crate::CredentialLoader;

    fn loader(username: &str, password: &str) -> Arc<CredentialLoader> {
        let loader = CredentialLoader::new("/tmp/sk-auth-plain-test");
        loader.install_for_test(vec![TestCred {
            username: username.to_owned(),
            auth_type: "plain".to_owned(),
            plain_password: Some(password.to_owned()),
            ..TestCred::default()
        }]);
        Arc::new(loader)
    }

    fn plain_msg(authzid: &str, authcid: &str, password: &str) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(authzid.as_bytes());
        v.push(0);
        v.extend_from_slice(authcid.as_bytes());
        v.push(0);
        v.extend_from_slice(password.as_bytes());
        v
    }

    #[test]
    fn happy_path() {
        let mut ex = PlainExchange::new(loader("svc", "hunter2"));
        let (resp, done) = ex.step(&plain_msg("", "svc", "hunter2")).unwrap();
        assert!(done);
        assert!(resp.is_empty());
        assert_eq!(ex.principal().unwrap().name, "svc");
    }

    #[test]
    fn wrong_password_rejected() {
        let mut ex = PlainExchange::new(loader("svc", "right"));
        let err = ex.step(&plain_msg("", "svc", "wrong")).unwrap_err();
        assert!(matches!(err, AuthError::BadCredentials));
    }

    #[test]
    fn unknown_user_rejected() {
        let mut ex = PlainExchange::new(loader("svc", "hunter2"));
        let err = ex.step(&plain_msg("", "ghost", "anything")).unwrap_err();
        assert!(matches!(err, AuthError::BadCredentials));
    }

    #[test]
    fn empty_password_rejected_as_malformed() {
        let mut ex = PlainExchange::new(loader("svc", "hunter2"));
        let err = ex.step(&plain_msg("", "svc", "")).unwrap_err();
        assert!(matches!(err, AuthError::MalformedSaslMessage));
    }

    #[test]
    fn missing_nul_rejected() {
        let mut ex = PlainExchange::new(loader("svc", "hunter2"));
        let err = ex.step(b"no-separators-here").unwrap_err();
        assert!(matches!(err, AuthError::MalformedSaslMessage));
    }
}
