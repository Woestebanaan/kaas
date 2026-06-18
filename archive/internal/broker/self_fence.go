package broker

import "time"

// DefaultHeartbeatTimeout is the upper bound on staleness before this broker
// stops acking writes. The plan calls 3s "the load-bearing correctness
// guarantee" — combined with epoch-tagged segments, it makes the takeover
// safety delay short regardless of NFS or storage weirdness.
const DefaultHeartbeatTimeout = 3 * time.Second

// IsHeartbeatFresh reports whether the most recent message from the
// controller is within the timeout window. The Coordinator wires this onto
// the produce hot path so a broker that has lost connectivity to the
// controller stops acking writes within heartbeatTimeout.
func IsHeartbeatFresh(lastReceived time.Time, timeout time.Duration) bool {
	if lastReceived.IsZero() {
		// No heartbeat ever observed. On a fresh-broker startup this is the
		// transient state until the first ControllerCommand arrives — return
		// false so the broker rejects writes until the channel is alive.
		return false
	}
	return time.Since(lastReceived) <= timeout
}

// IsHeartbeatFresh on the Coordinator uses the default timeout. Configurable
// only via the package-level constant for now; if operators need to tune
// (e.g. on slow NFS / cross-AZ heartbeats) we'll expose a Config struct.
func (c *Coordinator) IsHeartbeatFresh() bool {
	return IsHeartbeatFresh(c.LastHeartbeat(), DefaultHeartbeatTimeout)
}
