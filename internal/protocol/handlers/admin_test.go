package handlers

import (
	"context"
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// fakeCRW is an in-memory TopicCRWriter that records every call and
// can be programmed to return ErrTopicAlreadyExists / ErrTopicNotFound
// to exercise the per-error-code branches in the handler (gh #51).
type fakeCRW struct {
	mu        sync.Mutex
	created   []string
	deleted   []string
	createErr map[string]error
	deleteErr map[string]error
}

func (f *fakeCRW) CreateTopic(_ context.Context, name string, _ int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.createErr[name]; ok {
		return err
	}
	f.created = append(f.created, name)
	return nil
}

func (f *fakeCRW) DeleteTopic(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.deleteErr[name]; ok {
		return err
	}
	f.deleted = append(f.deleted, name)
	return nil
}

// fakeRegistry is a minimal TopicWriter used by the handlers under test.
type fakeRegistry struct {
	mu      sync.Mutex
	added   []string
	removed []string
}

func (r *fakeRegistry) Add(name string, _ int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, name)
}

func (r *fakeRegistry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, name)
}

// TestCreateTopicsHandler_CRWriterIsCalled is the gh #51 contract:
// when a TopicCRWriter is wired, the handler MUST call it on Create.
// Without this the admin protocol is local-broker-only and peer
// brokers never observe the topic.
func TestCreateTopicsHandler_CRWriterIsCalled(t *testing.T) {
	reg := &fakeRegistry{}
	crw := &fakeCRW{}

	h := NewCreateTopicsHandler(reg).WithCRWriter(crw)

	body := encodeCreateRequestV6(t, "alpha", 3)
	if _, err := h.Handle(&connstate.ConnState{}, 6, body); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(crw.created) != 1 || crw.created[0] != "alpha" {
		t.Errorf("CR writer Created=%v, want [alpha]", crw.created)
	}
	if len(reg.added) != 1 || reg.added[0] != "alpha" {
		t.Errorf("registry Added=%v, want [alpha]", reg.added)
	}
}

// TestCreateTopicsHandler_AlreadyExistsSkipsRegistry guards the error
// path: when CR write fails with ErrTopicAlreadyExists, the local
// registry must NOT be updated. (Otherwise a re-create would
// silently succeed locally even though peers reject it.)
func TestCreateTopicsHandler_AlreadyExistsSkipsRegistry(t *testing.T) {
	reg := &fakeRegistry{}
	crw := &fakeCRW{
		createErr: map[string]error{"dup": wrap(ErrTopicAlreadyExists)},
	}

	h := NewCreateTopicsHandler(reg).WithCRWriter(crw)
	if _, err := h.Handle(&connstate.ConnState{}, 6, encodeCreateRequestV6(t, "dup", 1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(reg.added) != 0 {
		t.Errorf("registry Added=%v, expected empty when CR write failed", reg.added)
	}
}

// TestDeleteTopicsHandler_NotFoundSkipsRegistry mirrors the
// already-exists guard for the delete path.
func TestDeleteTopicsHandler_NotFoundSkipsRegistry(t *testing.T) {
	reg := &fakeRegistry{}
	crw := &fakeCRW{
		deleteErr: map[string]error{"missing": wrap(ErrTopicNotFound)},
	}

	h := NewDeleteTopicsHandler(reg).WithCRWriter(crw)
	if _, err := h.Handle(&connstate.ConnState{}, 5, encodeDeleteRequestV5(t, "missing")); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(reg.removed) != 0 {
		t.Errorf("registry Removed=%v, expected empty when CR delete failed", reg.removed)
	}
}

// TestCreateTopicsHandler_NoCRWriter falls back to local-only mode —
// the in-process kafka-compat test path. CR writer is nil; only the
// registry is updated. Pins backwards compatibility for tests that
// don't have a K8s apiserver.
func TestCreateTopicsHandler_NoCRWriter(t *testing.T) {
	reg := &fakeRegistry{}
	h := NewCreateTopicsHandler(reg)

	if _, err := h.Handle(&connstate.ConnState{}, 6, encodeCreateRequestV6(t, "local-only", 1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(reg.added) != 1 {
		t.Errorf("registry Added=%v, want one entry", reg.added)
	}
}

// --- helpers ---

// wrap mints a wrapped sentinel that satisfies errors.Is(err, sentinel).
func wrap(sentinel error) error {
	return wrappedErr{sentinel: sentinel}
}

type wrappedErr struct {
	sentinel error
}

func (e wrappedErr) Error() string { return "wrapped: " + e.sentinel.Error() }
func (e wrappedErr) Unwrap() error { return e.sentinel }

// encodeCreateRequestV6 hand-encodes a v6 CreateTopicsRequest body
// (the dispatcher strips the request header, so the handler sees
// just this). Format mirrors DecodeCreateTopicsRequest's reader
// expectations.
func encodeCreateRequestV6(t *testing.T, name string, partitions int32) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactArray(1, func() {
		w.WriteCompactString(name)
		w.WriteInt32(partitions)
		w.WriteInt16(1)                   // ReplicationFactor
		w.WriteCompactArray(0, func() {}) // Assignments
		w.WriteCompactArray(0, func() {}) // Configs
		w.WriteEmptyTaggedFields()
	})
	w.WriteInt32(0)             // timeout_ms
	w.WriteInt8(0)              // validate_only=false
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

// encodeDeleteRequestV5 hand-encodes a v5 DeleteTopicsRequest body
// (TopicNames-shape, before the v6 KIP-516 schema change).
func encodeDeleteRequestV5(t *testing.T, name string) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactArray(1, func() {
		w.WriteCompactString(name)
	})
	w.WriteInt32(0) // timeout_ms
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}
