package broker

// Test-only exports. The `*ForTest` suffix is the documentation; Go's
// `_test.go` filename suffix would scope this file to internal tests
// only, blocking external test packages like tests/stale-controller-race/
// from importing it. Keeping it as a regular .go file with the suffix
// in the symbol name is the pragmatic compromise.

// SetControllerWatchEpochForTest pins the in-memory leaseTransitions
// value of a ControllerWatch without going through a Kubernetes informer.
// Used by tests/stale-controller-race/ to simulate "broker has observed
// the Lease at epoch N" without spinning up envtest.
func SetControllerWatchEpochForTest(w *ControllerWatch, epoch int64) {
	w.epoch.Store(epoch)
}
