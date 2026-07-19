//! Single-broker stand-in for the cluster lease manager.
//!
//! Always reports "I lead this partition" and pins the epoch at 0.
//! Phase 5 swaps this for the real `Coordinator` that reads
//! `assignment.json` off the shared NFS volume.

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalLeaseManager;

impl LocalLeaseManager {
    pub fn leads(&self, _topic: &str, _partition: i32) -> bool {
        true
    }

    pub fn current_epoch(&self) -> u32 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_leads() {
        let m = LocalLeaseManager;
        assert!(m.leads("t", 0));
        assert!(m.leads("any", 999));
    }

    #[test]
    fn epoch_is_zero() {
        assert_eq!(LocalLeaseManager.current_epoch(), 0);
    }
}
