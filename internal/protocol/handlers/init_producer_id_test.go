package handlers

import (
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// TestInitProducerIdMonotonic guards the contract gh #12 stage A
// makes to clients: every call returns a distinct producer ID, and
// IDs are monotonic so log diffs across reconnects are sortable. A
// regression that returns a constant PID would silently break
// kafka-verifiable-producer's per-message tagging once stage B
// enforces (PID, sequence) uniqueness.
func TestInitProducerIdMonotonic(t *testing.T) {
	h := NewInitProducerIdHandler()

	pid1 := callInitPID(t, h, 0)
	pid2 := callInitPID(t, h, 0)
	pid3 := callInitPID(t, h, 4)

	if pid1 == pid2 || pid2 == pid3 {
		t.Errorf("PIDs should be distinct: %d, %d, %d", pid1, pid2, pid3)
	}
	if pid2 <= pid1 || pid3 <= pid2 {
		t.Errorf("PIDs should be monotonic: %d, %d, %d", pid1, pid2, pid3)
	}
}

// TestInitProducerIdConcurrent guards uniqueness under concurrent
// callers — the atomic counter must not hand out duplicates when
// many connections race the call (the realistic startup pattern when
// a Kafka Streams app boots a topology with N stream threads).
func TestInitProducerIdConcurrent(t *testing.T) {
	h := NewInitProducerIdHandler()
	const n = 64
	pids := make([]int64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pids[idx] = callInitPID(t, h, 4)
		}(i)
	}
	wg.Wait()

	seen := make(map[int64]struct{}, n)
	for _, p := range pids {
		if _, dup := seen[p]; dup {
			t.Errorf("duplicate PID handed out: %d", p)
		}
		seen[p] = struct{}{}
	}
}

// TestInitProducerIdEpochZero pins epoch=0 for all freshly-allocated
// PIDs. Stage A of #12 doesn't track per-PID generations; a client
// that retries InitProducerId always gets a brand-new PID, so the
// epoch starts fresh at 0. Once stage B lands and we track stored
// state, this test will need updating to reflect epoch bumps.
func TestInitProducerIdEpochZero(t *testing.T) {
	h := NewInitProducerIdHandler()

	w := codec.NewWriter()
	w.WriteCompactNullableString("", true)
	w.WriteInt32(60_000)
	w.WriteInt64(-1)
	w.WriteInt16(-1)
	w.WriteEmptyTaggedFields()

	out, err := h.Handle(&connstate.ConnState{}, 4, w.Bytes())
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	r := codec.NewReader(out)
	_, _ = r.ReadInt32() // throttle
	errCode, _ := r.ReadInt16()
	if errCode != 0 {
		t.Errorf("errCode=%d, want 0", errCode)
	}
	_, _ = r.ReadInt64() // pid
	epoch, _ := r.ReadInt16()
	if epoch != 0 {
		t.Errorf("epoch=%d, want 0 (stage A always returns fresh epoch)", epoch)
	}
}

// callInitPID is a v0/v4-aware helper that runs one InitProducerId
// call through the handler and returns the producer ID. v0 uses the
// legacy nullable-string + int32 timeout body; v4 adds the compact
// header, PID/epoch hint, and trailing tagged fields.
func callInitPID(t *testing.T, h *InitProducerIdHandler, version int16) int64 {
	t.Helper()
	w := codec.NewWriter()
	if version >= 2 {
		w.WriteCompactNullableString("", true)
		w.WriteInt32(60_000)
		if version >= 3 {
			w.WriteInt64(-1)
			w.WriteInt16(-1)
		}
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteNullableString("", true)
		w.WriteInt32(60_000)
	}
	out, err := h.Handle(&connstate.ConnState{}, version, w.Bytes())
	if err != nil {
		t.Fatalf("Handle v%d: %v", version, err)
	}
	r := codec.NewReader(out)
	if _, err = r.ReadInt32(); err != nil { // throttle
		t.Fatal(err)
	}
	if _, err = r.ReadInt16(); err != nil { // errCode — checked separately
		t.Fatal(err)
	}
	pid, err := r.ReadInt64()
	if err != nil {
		t.Fatal(err)
	}
	return pid
}
