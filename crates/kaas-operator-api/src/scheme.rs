//! Scheme constants: every CR in this crate lives under
//! `apiVersion: skafka.io/v1alpha1`.
//!
//! These constants are duplicated into each CR's `#[kube(group, version)]`
//! attribute so `<T>::api_version()` / `<T>::api(client)` resolve
//! without an indirection — kube-derive's macro evaluates string
//! literals at compile time. Keep both in sync.

pub const GROUP: &str = "skafka.io";
pub const VERSION: &str = "v1alpha1";
