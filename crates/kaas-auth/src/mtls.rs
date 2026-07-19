//! mTLS — peer-cert principal extraction.
//!
//! After the TLS handshake, the broker inspects the peer's leaf
//! certificate, applies the `ssl.principal.mapping.rules` mapper
//! (gh #43) to the rendered subject DN, and asks the
//! [`crate::AuthEngine`] to resolve the mapped CN to a registered
//! principal.
//!
//! The DN rendering uses `x509_parser` — `X509Certificate::subject().to_string()`
//! — which produces the RFC 2253 form with `,` separators (no spaces)
//! and components in the order they appear in the cert. The v0.1 implementation
//! rendered DNs via its x509 library, which uses the
//! same RFC 2253 form. Differences in attribute name normalisation
//! (e.g. `CN` vs `commonName`) bubble out as principal-mapping
//! regex misses; the safe fall-back is `DEFAULT` which returns the
//! parsed CN unchanged.

use x509_parser::prelude::*;

use crate::engine::AuthEngine;
use crate::errors::AuthError;
use crate::principal_mapping::PrincipalMapper;
use crate::types::Principal;

/// Extract a principal from a DER-encoded peer leaf certificate.
///
/// `engine.authenticate_tls(mapped_cn)` resolves the (post-mapper)
/// CN against the credential store. Returns
/// [`AuthError::BadCertificate`] for parse errors and for principals
/// the engine doesn't know about.
pub fn extract_principal(
    peer_cert_der: &[u8],
    mapper: &PrincipalMapper,
    engine: &dyn AuthEngine,
) -> Result<Principal, AuthError> {
    let (_, cert) = X509Certificate::from_der(peer_cert_der)
        .map_err(|err| AuthError::X509(format!("parse leaf cert: {err}")))?;
    let subject = cert.subject();
    let dn = subject.to_string();
    let cn = subject
        .iter_common_name()
        .next()
        .and_then(|atv| atv.as_str().ok())
        .unwrap_or("")
        .to_owned();
    let mapped = mapper.apply(&dn, &cn);
    engine.authenticate_tls(&mapped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::TestCred;
    use crate::engine::RealAuthEngine;
    use crate::CredentialLoader;
    use std::sync::Arc;

    fn engine_with_tls_user(cn: &str, username: &str) -> RealAuthEngine {
        let loader = CredentialLoader::new("/tmp/kaas-auth-mtls-test");
        loader.install_for_test(vec![TestCred {
            username: username.to_owned(),
            auth_type: "tls".to_owned(),
            tls_cn: Some(cn.to_owned()),
            ..TestCred::default()
        }]);
        RealAuthEngine::new(Arc::new(loader), Arc::new(PrincipalMapper::default()))
    }

    fn issue_cert(common_name: &str) -> Vec<u8> {
        // rcgen 0.13 issues a self-signed cert with the given CN; we
        // only need the DER bytes for the parser.
        let mut params = rcgen::CertificateParams::new(vec![]).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        let kp = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&kp).unwrap();
        cert.der().to_vec()
    }

    #[test]
    fn extracts_principal_for_registered_cn() {
        let der = issue_cert("alice");
        let engine = engine_with_tls_user("alice", "alice");
        let mapper = PrincipalMapper::default();
        let p = extract_principal(&der, &mapper, &engine).unwrap();
        assert_eq!(p.name, "alice");
    }

    #[test]
    fn unknown_cn_returns_bad_certificate() {
        let der = issue_cert("eve");
        let engine = engine_with_tls_user("alice", "alice");
        let mapper = PrincipalMapper::default();
        let err = extract_principal(&der, &mapper, &engine).unwrap_err();
        assert!(matches!(err, AuthError::BadCertificate));
    }

    #[test]
    fn garbage_der_returns_x509_error() {
        let engine = engine_with_tls_user("alice", "alice");
        let mapper = PrincipalMapper::default();
        let err = extract_principal(&[0x00, 0x01, 0x02], &mapper, &engine).unwrap_err();
        assert!(matches!(err, AuthError::X509(_)));
    }
}
