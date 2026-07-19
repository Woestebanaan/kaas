//! Per-listener `AuthEngine` lookup.
//!
//! A kaas broker can host several listeners with different auth
//! policies side by side (anonymous + SCRAM, mTLS-external + SCRAM-
//! internal, …). The selector keeps that decision out of the
//! handlers — they just call
//! `selector.for_listener(conn.listener_name).new_sasl_exchange(...)`
//! and the right engine services the SASL handshake.
//!
//! gh #126: post-split, the selector is consulted ONLY for the
//! authentication-side concerns (SASL handshake, mTLS principal
//! extraction, `requires_pre_auth`). Authorization moves to a
//! cluster-wide [`crate::Authorizer`] that runs on every connection
//! regardless of listener.

use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::AuthEngine;

pub trait AuthEngineSelector: Send + Sync + std::fmt::Debug + 'static {
    /// Return the engine for the listener. Implementations must
    /// always return a non-nil engine; an unknown listener falls
    /// back to a sensible default (typically `AllowAllAuthEngine`).
    fn for_listener(&self, listener: &str) -> Arc<dyn AuthEngine>;
}

/// Wraps a single `AuthEngine` and ignores the listener argument.
/// Preserves the pre-per-listener-auth behaviour for tests and the
/// dev-mode boot path.
#[derive(Debug)]
pub struct SingleAuthEngine {
    inner: Arc<dyn AuthEngine>,
}

impl SingleAuthEngine {
    pub fn new(engine: Arc<dyn AuthEngine>) -> Self {
        Self { inner: engine }
    }
}

impl AuthEngineSelector for SingleAuthEngine {
    fn for_listener(&self, _listener: &str) -> Arc<dyn AuthEngine> {
        self.inner.clone()
    }
}

/// Maps a listener name to its assigned engine. An entry keyed by `""`
/// acts as the default for any listener not found in the map — wire
/// it to `AllowAllAuthEngine` when the broker hosts any anonymous
/// listener so unknown / untagged connections don't surprise-deny.
#[derive(Debug)]
pub struct PerListenerAuthEngine {
    map: HashMap<String, Arc<dyn AuthEngine>>,
    default: Arc<dyn AuthEngine>,
}

impl PerListenerAuthEngine {
    pub fn new(default: Arc<dyn AuthEngine>) -> Self {
        Self {
            map: HashMap::new(),
            default,
        }
    }

    pub fn insert(&mut self, listener: impl Into<String>, engine: Arc<dyn AuthEngine>) {
        self.map.insert(listener.into(), engine);
    }
}

impl AuthEngineSelector for PerListenerAuthEngine {
    fn for_listener(&self, listener: &str) -> Arc<dyn AuthEngine> {
        self.map
            .get(listener)
            .cloned()
            .unwrap_or_else(|| self.default.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::AllowAllAuthEngine;

    #[test]
    fn single_engine_always_returns_wrapped() {
        let engine: Arc<dyn AuthEngine> = Arc::new(AllowAllAuthEngine);
        let sel = SingleAuthEngine::new(engine.clone());
        for listener in ["", "internal", "external"] {
            assert!(Arc::ptr_eq(&sel.for_listener(listener), &engine));
        }
    }

    #[test]
    fn per_listener_falls_back_to_default() {
        let default: Arc<dyn AuthEngine> = Arc::new(AllowAllAuthEngine);
        let mut sel = PerListenerAuthEngine::new(default.clone());
        let named: Arc<dyn AuthEngine> = Arc::new(AllowAllAuthEngine);
        sel.insert("known", named.clone());
        assert!(Arc::ptr_eq(&sel.for_listener("known"), &named));
        assert!(Arc::ptr_eq(&sel.for_listener("ghost"), &default));
    }
}
