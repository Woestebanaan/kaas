//! Heartbeat staleness check used by the produce hot path.
//!
//! Coordinator
//! wires this onto the Produce path so a broker that has lost
//! connectivity to the controller stops acking writes within
//! `DEFAULT_HEARTBEAT_TIMEOUT`. Combined with epoch-tagged segments,
//! this caps the takeover safety delay regardless of NFS / storage
//! weirdness.

use std::time::Duration;

use tokio::time::Instant;

/// Upper bound on staleness before this broker stops acking writes.
/// The Phase-0 plan calls 3 s "the load-bearing correctness
/// guarantee" — short enough that a controller failover doesn't
/// strand the produce path on a stale broker for long.
pub const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);

/// Is the most recent message from the controller within the
/// staleness window? `last_received == None` means "no heartbeat
/// ever observed" — return `false` so a fresh-boot broker rejects
/// writes until the heartbeat stream is alive.
pub fn is_heartbeat_fresh(last_received: Option<Instant>, timeout: Duration) -> bool {
    match last_received {
        None => false,
        Some(t) => Instant::now().duration_since(t) <= timeout,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn no_heartbeat_is_not_fresh() {
        assert!(!is_heartbeat_fresh(None, DEFAULT_HEARTBEAT_TIMEOUT));
    }

    #[tokio::test(start_paused = true)]
    async fn recent_heartbeat_is_fresh() {
        let t = Instant::now();
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(is_heartbeat_fresh(Some(t), DEFAULT_HEARTBEAT_TIMEOUT));
    }

    #[tokio::test(start_paused = true)]
    async fn stale_heartbeat_is_not_fresh() {
        let t = Instant::now();
        tokio::time::advance(DEFAULT_HEARTBEAT_TIMEOUT + Duration::from_secs(1)).await;
        assert!(!is_heartbeat_fresh(Some(t), DEFAULT_HEARTBEAT_TIMEOUT));
    }
}
