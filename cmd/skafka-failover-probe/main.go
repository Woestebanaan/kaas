// skafka-failover-probe measures the heartbeat-RTT distribution between a
// broker and the cluster controller, then reports recommended values for
// heartbeatIntervalMs / heartbeatTimeoutMs given the observed p99.9.
//
// Phase 4 implementation. The binary exists in Phase 1 only so Dockerfiles
// and CI matrices can reference it without conditional logic.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "skafka-failover-probe: not yet implemented (Phase 4)")
	os.Exit(0)
}
