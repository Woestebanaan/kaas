//! `GroupTakeoverDriver` — consumer-group analogue of
//! [`TakeoverDriver`].
//!
//! Watches
//! assignment changes and tells `coordinator::Manager` to drop
//! in-memory state for groups no longer assigned here.
//!
//! Two passes:
//!
//! 1. **prev → next diff** for the common case (state changes that
//!    touched this broker's known assignment view).
//! 2. **Orphan sweep** — anything in `Manager::local_groups()` not
//!    in `next_ours` gets relinquished. This is the gh #89 self-
//!    healing pass that closes the stale `--list` leak: a stray
//!    in-memory entry that landed during a brief "I own this"
//!    window the broker has since overwritten.
//!
//! v1 does not migrate group state across brokers — the new
//! coordinator's first JoinGroup creates the group via
//! `Manager::get_or_create`, which lazily loads persisted offsets.
//! Acceptable cost: one rebalance round-trip per coordinator
//! transition.
//!
//! [`TakeoverDriver`]: crate::takeover::TakeoverDriver

use std::collections::HashSet;
use std::sync::Arc;

use kaas_coordinator::Manager;

use crate::assignment::Assignment;

pub struct GroupTakeoverDriver {
    mgr: Arc<Manager>,
    broker_id: String,
}

impl std::fmt::Debug for GroupTakeoverDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupTakeoverDriver")
            .field("broker_id", &self.broker_id)
            .finish_non_exhaustive()
    }
}

impl GroupTakeoverDriver {
    pub fn new(mgr: Arc<Manager>, broker_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            mgr,
            broker_id: broker_id.into(),
        })
    }

    /// Build the [`AssignmentChangeHandler`] closure for this
    /// driver. Register it with `coordinator.on_assignment_change(...)`
    /// at boot.
    ///
    /// [`AssignmentChangeHandler`]: crate::assignment::AssignmentChangeHandler
    pub fn as_handler(self: &Arc<Self>) -> crate::assignment::AssignmentChangeHandler {
        let me = self.clone();
        Arc::new(move |prev, next| me.on_change(prev, next))
    }

    pub fn on_change(&self, prev: Option<&Assignment>, next: &Assignment) {
        let prev_ours = groups_owned_by(prev, &self.broker_id);
        let next_ours = groups_owned_by(Some(next), &self.broker_id);

        // Single-fire dedup: a group surfaced by both passes still
        // gets relinquished exactly once. Manager::relinquish_group
        // is idempotent, but emitting duplicate calls is wasteful.
        let mut relinquished: HashSet<String> = HashSet::new();
        let mut relinquish = |group_id: &str| {
            if relinquished.insert(group_id.to_owned()) {
                self.mgr.relinquish_group(group_id);
            }
        };

        for group_id in &prev_ours {
            if next_ours.contains(group_id) {
                continue;
            }
            relinquish(group_id);
        }

        // Orphan sweep — drop any in-memory group not in the
        // broker's current assignment view.
        for group_id in self.mgr.local_groups() {
            if next_ours.contains(&group_id) {
                continue;
            }
            relinquish(&group_id);
        }
    }
}

fn groups_owned_by(a: Option<&Assignment>, broker_id: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    let a = match a {
        None => return out,
        Some(a) => a,
    };
    for g in &a.consumer_groups {
        if g.broker == broker_id {
            out.insert(g.group_id.clone());
        }
    }
    out
}
