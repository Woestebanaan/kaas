//! Status-condition helpers.
//!
//! Mirrors the Go `setCondition` helper that wraps
//! `meta.SetStatusCondition` from apimachinery. The wrapper stamps
//! `last_transition_time` (Go: `metav1.Now()`) and dedupes by
//! `type_`, preserving the existing `last_transition_time` when the
//! incoming status matches the stored one (so a reconcile that
//! re-applies the same condition doesn't churn the timestamp and
//! trigger watchers).

use chrono::{SecondsFormat, Utc};
use sk_operator_api::Condition;

/// The standard `Ready` condition type, used on every CR's
/// `Status.Conditions`. Pulled out as a constant because every
/// reconciler in workstream C references it.
pub const READY: &str = "Ready";

/// Insert or update a condition keyed on `cond.type_`. When the
/// status matches the existing entry, the timestamp is preserved;
/// otherwise `cond.last_transition_time` is stamped to "now".
pub fn set_condition(conditions: &mut Vec<Condition>, cond: Condition) {
    set_condition_with_now(
        conditions,
        cond,
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
    );
}

/// Same as [`set_condition`] with an explicit `now`. Exists for
/// tests so the timestamp boundary is deterministic — the
/// production `metav1.Time` shape is second-precision, which makes
/// elapsed-test assertions otherwise unreliable.
pub fn set_condition_with_now(conditions: &mut Vec<Condition>, mut cond: Condition, now: String) {
    for existing in conditions.iter_mut() {
        if existing.type_ != cond.type_ {
            continue;
        }
        if existing.status == cond.status {
            // No transition; preserve the existing timestamp + observedGeneration.
            cond.last_transition_time = existing.last_transition_time.clone();
        } else {
            cond.last_transition_time = now;
        }
        *existing = cond;
        return;
    }
    cond.last_transition_time = now;
    conditions.push(cond);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cond(status: &str, reason: &str) -> Condition {
        Condition {
            type_: READY.into(),
            status: status.into(),
            observed_generation: None,
            last_transition_time: String::new(),
            reason: reason.into(),
            message: "msg".into(),
        }
    }

    // Tests inject explicit timestamps via `set_condition_with_now`
    // — the public `set_condition` wraps that with `Utc::now()`. The
    // apimachinery-equivalent `metav1.Time` shape is second-precision,
    // so naive elapsed-time tests would be flaky on fast machines.

    const T0: &str = "2026-06-29T07:00:00Z";
    const T1: &str = "2026-06-29T07:05:00Z";

    #[test]
    fn inserts_on_empty() {
        let mut v = Vec::new();
        set_condition_with_now(&mut v, cond("True", "Initial"), T0.into());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].status, "True");
        assert_eq!(v[0].last_transition_time, T0);
    }

    #[test]
    fn preserves_timestamp_on_same_status() {
        let mut v = Vec::new();
        set_condition_with_now(&mut v, cond("True", "Initial"), T0.into());

        // Same status, different reason, fresh `now` — the existing
        // timestamp must be preserved.
        set_condition_with_now(&mut v, cond("True", "DifferentReasonSameStatus"), T1.into());

        assert_eq!(v.len(), 1);
        assert_eq!(v[0].reason, "DifferentReasonSameStatus");
        assert_eq!(
            v[0].last_transition_time, T0,
            "same-status update preserves timestamp"
        );
    }

    #[test]
    fn updates_timestamp_on_status_transition() {
        let mut v = Vec::new();
        set_condition_with_now(&mut v, cond("False", "Initial"), T0.into());

        set_condition_with_now(&mut v, cond("True", "Recovered"), T1.into());

        assert_eq!(v[0].status, "True");
        assert_eq!(
            v[0].last_transition_time, T1,
            "status transition stamps the new now"
        );
    }

    #[test]
    fn distinct_types_coexist() {
        let mut v = Vec::new();
        set_condition_with_now(&mut v, cond("True", "Ready"), T0.into());
        let mut other = cond("True", "PartitionsCreated");
        other.type_ = "PartitionsReady".into();
        set_condition_with_now(&mut v, other, T1.into());
        assert_eq!(v.len(), 2);
    }
}
