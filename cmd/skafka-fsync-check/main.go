// skafka-fsync-check validates that the storage backend mounted at the given
// path provides the durability guarantees skafka requires: fsync that survives
// a node crash, atomic same-directory rename(2), and close-to-open
// consistency.
//
// Phase 3 implementation. The binary exists in Phase 1 only so Dockerfiles
// and CI matrices can reference it without conditional logic.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "skafka-fsync-check: not yet implemented (Phase 3)")
	os.Exit(0)
}
