// Package stalecontrollerrace holds tests that exercise the v3.2 epoch fence
// against stale-controller writes to /data/__cluster/assignment.json.
//
// Phase 4 work. This file exists in Phase 1 only so the package compiles and
// `go test ./...` exits clean — Phase 4 will drop the real tests in here
// (SIGSTOP/SIGCONT race against rename(2), orphan tmp cleanup, partitioned
// broker push-vs-poll behaviour).
package stalecontrollerrace

import "testing"

func TestPlaceholder(t *testing.T) {
	t.Skip("stale-controller-race tests land in Phase 4")
}
