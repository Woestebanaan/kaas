//! `KafkaTopic` CR change watcher.
//!
//! Same two-layer
//! split as [`crate::endpoints`]: a pure-state cache that fires
//! callbacks on divergence, plus a kube-bound pump (in
//! `crate::kube_watchers`) that consumes
//! `kube::runtime::watcher::<KafkaTopic>` events.
//!
//! gh #76 quirk: `TopicDeleted` fires the instant
//! `metadata.deletionTimestamp` goes non-nil — NOT on the final
//! `Deleted` event K8s emits after finalizers drain. This is the
//! NFS silly-rename guard: the broker has to close its open
//! file-descriptors on the partition's segments BEFORE the operator's
//! finalizer can `unlinkat` the directory.

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;

type TopicEventCallback = Box<dyn Fn(&TopicEvent) + Send + Sync + 'static>;

/// One `KafkaTopic` divergence event. Fired by [`TopicWatcher`] when
/// the observed CR state differs from the cached state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicEvent {
    Added {
        name: String,
        partitions: i32,
        cleanup_policy: String,
        topic_id: String,
    },
    Modified {
        name: String,
        partitions: i32,
        old_partitions: i32,
        cleanup_policy: String,
        topic_id: String,
    },
    Deleted {
        name: String,
        last_partitions: i32,
    },
}

/// CR view sent into the watcher by the kube pump (or by tests).
/// The pump translates a `KafkaTopic` CR into one of these per
/// observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicObservation {
    pub name: String,
    pub partitions: i32,
    pub cleanup_policy: String,
    /// `Status.TopicID` from the CR — empty for legacy CRs that
    /// never had a status populated (gh #105 / KIP-516).
    pub topic_id: String,
    /// `true` once `metadata.deletionTimestamp` is non-nil. Fires
    /// a `TopicDeleted` event the moment we see this, not on the
    /// final K8s `Deleted` event.
    pub terminating: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheEntry {
    partitions: i32,
    cleanup_policy: String,
    topic_id: String,
}

/// Pure-state cache + divergence detector. The kube watcher (or
/// tests) feed observations into [`Self::apply`] / [`Self::deleted`];
/// the watcher fires registered callbacks with the divergence
/// events.
pub struct TopicWatcher {
    on_event: Mutex<Option<TopicEventCallback>>,
    state: Mutex<TopicWatcherState>,
}

#[derive(Debug, Default)]
struct TopicWatcherState {
    cache: HashMap<String, CacheEntry>,
    /// gh #76: track CRs we've already fired `TopicDeleted` for
    /// while finalizers churn. Suppresses duplicates.
    terminating: HashSet<String>,
}

impl std::fmt::Debug for TopicWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        f.debug_struct("TopicWatcher")
            .field("cached_topics", &state.cache.len())
            .field("terminating", &state.terminating.len())
            .finish()
    }
}

impl Default for TopicWatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicWatcher {
    pub fn new() -> Self {
        Self {
            on_event: Mutex::new(None),
            state: Mutex::new(TopicWatcherState::default()),
        }
    }

    /// Register the event callback. Replaces any prior
    /// registration.
    pub fn on_event<F>(&self, f: F)
    where
        F: Fn(&TopicEvent) + Send + Sync + 'static,
    {
        *self.on_event.lock() = Some(Box::new(f));
    }

    /// Seed the cache so the first observation of `name` does not
    /// fire a `TopicAdded` event. Used by the broker to register
    /// topics it discovered from disk before the watcher started.
    pub fn prime(&self, name: &str, partitions: i32) {
        self.state.lock().cache.insert(
            name.to_owned(),
            CacheEntry {
                partitions,
                cleanup_policy: String::new(),
                topic_id: String::new(),
            },
        );
    }

    /// Apply one CR observation. Compares against the cache and
    /// fires `TopicAdded` / `TopicModified` / `TopicDeleted`
    /// callbacks on divergence.
    pub fn apply(&self, obs: &TopicObservation) {
        let event = {
            let mut state = self.state.lock();

            // gh #76: terminating CRs fire `TopicDeleted` even
            // before K8s emits the final `Deleted` event. Suppress
            // duplicates via the `terminating` set.
            if obs.terminating {
                if state.terminating.contains(&obs.name) {
                    return;
                }
                state.terminating.insert(obs.name.clone());
                let last_partitions = state
                    .cache
                    .get(&obs.name)
                    .map(|c| c.partitions)
                    .unwrap_or(obs.partitions);
                state.cache.remove(&obs.name);
                Some(TopicEvent::Deleted {
                    name: obs.name.clone(),
                    last_partitions,
                })
            } else if let Some(entry) = state.cache.get(&obs.name).cloned() {
                if entry.partitions == obs.partitions
                    && entry.cleanup_policy == obs.cleanup_policy
                    && entry.topic_id == obs.topic_id
                {
                    None
                } else {
                    state.cache.insert(
                        obs.name.clone(),
                        CacheEntry {
                            partitions: obs.partitions,
                            cleanup_policy: obs.cleanup_policy.clone(),
                            topic_id: obs.topic_id.clone(),
                        },
                    );
                    Some(TopicEvent::Modified {
                        name: obs.name.clone(),
                        partitions: obs.partitions,
                        old_partitions: entry.partitions,
                        cleanup_policy: obs.cleanup_policy.clone(),
                        topic_id: obs.topic_id.clone(),
                    })
                }
            } else {
                state.cache.insert(
                    obs.name.clone(),
                    CacheEntry {
                        partitions: obs.partitions,
                        cleanup_policy: obs.cleanup_policy.clone(),
                        topic_id: obs.topic_id.clone(),
                    },
                );
                Some(TopicEvent::Added {
                    name: obs.name.clone(),
                    partitions: obs.partitions,
                    cleanup_policy: obs.cleanup_policy.clone(),
                    topic_id: obs.topic_id.clone(),
                })
            }
        };
        if let Some(ev) = event {
            if let Some(cb) = self.on_event.lock().as_ref() {
                cb(&ev);
            }
        }
    }

    /// Apply a final K8s `Deleted` event. Suppressed if `apply`
    /// already fired the deletion for a terminating CR (gh #76).
    pub fn deleted(&self, name: &str) {
        let event = {
            let mut state = self.state.lock();
            state.terminating.remove(name);
            state.cache.remove(name).map(|entry| TopicEvent::Deleted {
                name: name.to_owned(),
                last_partitions: entry.partitions,
            })
        };
        if let Some(ev) = event {
            if let Some(cb) = self.on_event.lock().as_ref() {
                cb(&ev);
            }
        }
    }

    /// Snapshot of cached topic names. Diagnostic.
    pub fn cached_names(&self) -> Vec<String> {
        let mut out: Vec<String> = self.state.lock().cache.keys().cloned().collect();
        out.sort();
        out
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    fn obs(name: &str, partitions: i32) -> TopicObservation {
        TopicObservation {
            name: name.to_owned(),
            partitions,
            cleanup_policy: "delete".to_owned(),
            topic_id: "".to_owned(),
            terminating: false,
        }
    }

    fn capture() -> (
        std::sync::Arc<Mutex<Vec<TopicEvent>>>,
        impl Fn(&TopicEvent) + Send + Sync + 'static,
    ) {
        let v = std::sync::Arc::new(Mutex::new(Vec::new()));
        let vc = v.clone();
        (v, move |e: &TopicEvent| vc.lock().push(e.clone()))
    }

    #[test]
    fn first_observation_fires_added() {
        let w = TopicWatcher::new();
        let (events, cb) = capture();
        w.on_event(cb);
        w.apply(&obs("t1", 3));
        let e = events.lock();
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], TopicEvent::Added { .. }));
    }

    #[test]
    fn prime_suppresses_first_added() {
        let w = TopicWatcher::new();
        let (events, cb) = capture();
        w.on_event(cb);
        w.prime("t1", 3);
        let mut o = obs("t1", 3);
        o.cleanup_policy = String::new();
        w.apply(&o);
        assert!(events.lock().is_empty());
    }

    #[test]
    fn partition_change_fires_modified() {
        let w = TopicWatcher::new();
        let (events, cb) = capture();
        w.on_event(cb);
        w.apply(&obs("t1", 3));
        w.apply(&obs("t1", 6));
        let e = events.lock();
        assert_eq!(e.len(), 2);
        match &e[1] {
            TopicEvent::Modified {
                name,
                partitions,
                old_partitions,
                ..
            } => {
                assert_eq!(name, "t1");
                assert_eq!(*partitions, 6);
                assert_eq!(*old_partitions, 3);
            }
            other => panic!("expected Modified, got {other:?}"),
        }
    }

    #[test]
    fn cleanup_policy_change_fires_modified() {
        let w = TopicWatcher::new();
        let (events, cb) = capture();
        w.on_event(cb);
        w.apply(&obs("t1", 3));
        let mut next = obs("t1", 3);
        next.cleanup_policy = "compact".to_owned();
        w.apply(&next);
        let e = events.lock();
        assert_eq!(e.len(), 2);
        assert!(matches!(e[1], TopicEvent::Modified { .. }));
    }

    #[test]
    fn terminating_fires_delete_immediately() {
        let w = TopicWatcher::new();
        let (events, cb) = capture();
        w.on_event(cb);
        w.apply(&obs("t1", 3));
        let mut term = obs("t1", 3);
        term.terminating = true;
        w.apply(&term);
        let e = events.lock();
        assert_eq!(e.len(), 2);
        match &e[1] {
            TopicEvent::Deleted {
                name,
                last_partitions,
            } => {
                assert_eq!(name, "t1");
                assert_eq!(*last_partitions, 3);
            }
            other => panic!("expected Deleted, got {other:?}"),
        }
    }

    #[test]
    fn terminating_then_deleted_does_not_fire_twice() {
        let w = TopicWatcher::new();
        let (events, cb) = capture();
        w.on_event(cb);
        w.apply(&obs("t1", 3));
        let mut term = obs("t1", 3);
        term.terminating = true;
        w.apply(&term);
        w.deleted("t1"); // final K8s Deleted event
        let e = events.lock();
        assert_eq!(e.len(), 2, "gh #76: terminating Deleted fires once");
    }

    #[test]
    fn cached_names_are_sorted() {
        let w = TopicWatcher::new();
        w.apply(&obs("t-c", 1));
        w.apply(&obs("t-a", 1));
        w.apply(&obs("t-b", 1));
        assert_eq!(w.cached_names(), vec!["t-a", "t-b", "t-c"]);
    }
}
