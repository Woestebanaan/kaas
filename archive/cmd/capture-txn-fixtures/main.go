// capture-txn-fixtures writes Go-side golden JSON fixtures for the
// Rust port's read-side compatibility tests (gh #172). Exercises
// TxnStateStore + FenceLog through their public Go API, walks the
// resulting __cluster/ tree, and dumps every non-empty file under
// the requested output directory.
//
// Usage:
//
//	cd archive
//	go run ./cmd/capture-txn-fixtures <output_dir>
//
// The Rust port reads the captured files via
// crates/sk-coordinator/tests/golden_fixtures.rs and asserts each
// one round-trips byte-equal through serde_json. Catches any
// silent field-order or omitempty divergence between Go's
// encoding/json and Rust's serde_json before Phase 9 cutover.
//
// This is a port-blocking tool — not a feature; the Go tree is
// frozen otherwise.
package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/woestebanaan/skafka/internal/coordinator"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: capture-txn-fixtures <output_dir>")
		os.Exit(2)
	}
	outDir := os.Args[1]
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		die("creating output dir: %v", err)
	}

	work, err := os.MkdirTemp("", "skafka-fixture-capture-*")
	if err != nil {
		die("tempdir: %v", err)
	}
	defer os.RemoveAll(work)

	clusterDir := filepath.Join(work, "__cluster")
	if err := os.MkdirAll(clusterDir, 0o755); err != nil {
		die("creating __cluster: %v", err)
	}

	captureTxnState(clusterDir)
	captureFenceLog(clusterDir)

	// Walk every produced file under the cluster dir, copy to the
	// output directory mirroring the relative layout.
	err = filepath.Walk(clusterDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(clusterDir, path)
		if err != nil {
			return err
		}
		dst := filepath.Join(outDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.WriteFile(dst, data, 0o644); err != nil {
			return err
		}
		fmt.Printf("captured %s (%d bytes)\n", rel, len(data))
		return nil
	})
	if err != nil {
		die("walking cluster dir: %v", err)
	}
}

// captureTxnState exercises every transition the slot file can
// record so the resulting fixture covers Empty / Ongoing /
// CompleteCommit / CompleteAbort + partition + group lists +
// ongoing_since_ms + transaction_timeout_ms. Each txn_id ends up
// in whichever slot fnv1a_32(id) % 50 picks; the Rust test reads
// every non-empty slot.
func captureTxnState(clusterDir string) {
	store, err := coordinator.NewTxnStateStore(clusterDir, coordinator.DefaultNumSlots)
	if err != nil {
		die("NewTxnStateStore: %v", err)
	}

	nextPID := int64(99)
	alloc := func() int64 {
		nextPID++
		return nextPID
	}

	// --- tx-commit: rejoin → AddPartitions → AddOffsets → commit
	mustAlloc(store, "tx-commit", 60_000, alloc) // epoch 0
	mustAlloc(store, "tx-commit", 60_000, alloc) // epoch 1 (rejoin)
	mustAddPartitions(store, "tx-commit", 100, 1, []coordinator.TxnTopic{
		{Topic: "events", Partitions: []int32{0, 1, 2}},
		{Topic: "audit", Partitions: []int32{7}},
	})
	mustAddOffsets(store, "tx-commit", 100, 1, "consumer-group-a")
	mustEnd(store, "tx-commit", 100, 1, true)

	// --- tx-abort: same shape but EndTxn(abort)
	mustAlloc(store, "tx-abort", 30_000, alloc) // epoch 0
	mustAddPartitions(store, "tx-abort", 101, 0, []coordinator.TxnTopic{
		{Topic: "events", Partitions: []int32{4, 5}},
	})
	mustAddOffsets(store, "tx-abort", 101, 0, "consumer-group-b")
	mustAddOffsets(store, "tx-abort", 101, 0, "consumer-group-c")
	mustEnd(store, "tx-abort", 101, 0, false)

	// --- tx-ongoing: left Ongoing so the fixture covers the live
	//     state shape including a populated groups list — both
	//     Complete* paths above clear groups via EndTxn, so without
	//     this case the captured fixtures would never exercise the
	//     non-omitempty `groups` field.
	mustAlloc(store, "tx-ongoing", 45_000, alloc)
	mustAddPartitions(store, "tx-ongoing", 102, 0, []coordinator.TxnTopic{
		{Topic: "events", Partitions: []int32{8}},
	})
	mustAddOffsets(store, "tx-ongoing", 102, 0, "consumer-group-d")
	mustAddOffsets(store, "tx-ongoing", 102, 0, "consumer-group-e")

	// --- tx-bare: just allocated with timeout; no AddPartitions.
	//     Covers the Empty-but-timeout-recorded shape.
	mustAlloc(store, "tx-bare", 90_000, alloc)
}

func captureFenceLog(clusterDir string) {
	fenceDir := coordinator.FenceLogDir(clusterDir)
	log, err := coordinator.NewFenceLog(fenceDir, "skafka-0")
	if err != nil {
		die("NewFenceLog: %v", err)
	}
	for _, pair := range []struct {
		pid   int64
		epoch int16
	}{
		{100, 1}, {101, 1}, {102, 1}, {200, 5}, {999, 17},
	} {
		if err := log.Append(pair.pid, pair.epoch); err != nil {
			die("FenceLog.Append: %v", err)
		}
	}
	// Idempotent re-append must not change the file.
	if err := log.Append(100, 1); err != nil {
		die("FenceLog.Append (re-append): %v", err)
	}
}

func mustAlloc(s *coordinator.TxnStateStore, txnID string, timeoutMs int32, alloc func() int64) {
	if _, _, err := s.GetOrAllocateWithTimeout(txnID, timeoutMs, alloc); err != nil {
		die("GetOrAllocateWithTimeout(%s): %v", txnID, err)
	}
}

func mustAddPartitions(s *coordinator.TxnStateStore, txnID string, pid int64, epoch int16, add []coordinator.TxnTopic) {
	if err := s.AddPartitions(txnID, pid, epoch, add); err != nil {
		die("AddPartitions(%s): %v", txnID, err)
	}
}

func mustAddOffsets(s *coordinator.TxnStateStore, txnID string, pid int64, epoch int16, groupID string) {
	if err := s.AddOffsetsToTxn(txnID, pid, epoch, groupID); err != nil {
		die("AddOffsetsToTxn(%s, %s): %v", txnID, groupID, err)
	}
}

func mustEnd(s *coordinator.TxnStateStore, txnID string, pid int64, epoch int16, commit bool) {
	if err := s.EndTxn(txnID, pid, epoch, commit); err != nil {
		die("EndTxn(%s): %v", txnID, err)
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "capture-txn-fixtures: "+format+"\n", args...)
	os.Exit(1)
}

// Defensive — the Go reference rounds ongoing_since_ms via
// time.Now().UnixMilli() at AddPartitions time. We don't override
// the clock; the captured fixture's ongoing_since_ms reflects the
// capture wall clock, which Rust deserializes as an arbitrary i64
// — no semantic dependency, just a serde shape check.
var _ = time.Now
var _ = strings.Builder{}
