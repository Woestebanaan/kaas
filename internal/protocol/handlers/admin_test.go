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
	createdConfigs []map[string]string // per-create configs, gh #33 audit
	deleted   []string
	createErr map[string]error
	deleteErr map[string]error
	expanded  map[string]int32 // gh #52: ExpandTopic record
	expandErr map[string]error
}

func (f *fakeCRW) CreateTopic(_ context.Context, name string, _ int32, configs map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.createErr[name]; ok {
		return err
	}
	f.created = append(f.created, name)
	f.createdConfigs = append(f.createdConfigs, configs)
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

func (f *fakeCRW) ExpandTopic(_ context.Context, name string, newCount int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.expandErr[name]; ok {
		return err
	}
	if f.expanded == nil {
		f.expanded = make(map[string]int32)
	}
	f.expanded[name] = newCount
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

// encodeCreateRequestV6WithConfigs is the gh #33 variant — same shape
// as encodeCreateRequestV6 but with arbitrary topic-level configs
// (cleanup.policy, segment.bytes, etc.) attached to the single
// topic in the request. Streams clients exercise this exact path
// at startup for changelog/repartition topics.
func encodeCreateRequestV6WithConfigs(t *testing.T, name string, partitions int32, configs map[string]string) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactArray(1, func() {
		w.WriteCompactString(name)
		w.WriteInt32(partitions)
		w.WriteInt16(1)                   // ReplicationFactor
		w.WriteCompactArray(0, func() {}) // Assignments
		w.WriteCompactArray(len(configs), func() {
			for k, v := range configs {
				w.WriteCompactString(k)
				w.WriteCompactNullableString(v, false)
				w.WriteEmptyTaggedFields()
			}
		})
		w.WriteEmptyTaggedFields()
	})
	w.WriteInt32(0) // timeout_ms
	w.WriteInt8(0)  // validate_only=false
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

// TestCreateTopicsHandler_ConfigsPassedThrough pins gh #33's wire
// contract: a Streams-style request with cleanup.policy=compact +
// segment.bytes=1048576 + retention.ms=-1 makes it through the
// handler to the TopicCRWriter as a flat map. Without this, the
// request appears to succeed (skafka silently ignored configs) but
// downstream DescribeConfigs returns the static default — Streams
// can't tell its compact changelog wasn't actually compact.
func TestCreateTopicsHandler_ConfigsPassedThrough(t *testing.T) {
	reg := &fakeRegistry{}
	crw := &fakeCRW{}
	h := NewCreateTopicsHandler(reg).WithCRWriter(crw)

	// Configs a real Kafka Streams hello-world sends for its
	// changelog topic.
	configs := map[string]string{
		"cleanup.policy": "compact",
		"segment.bytes":  "1048576",
		"retention.ms":   "-1",
	}
	body := encodeCreateRequestV6WithConfigs(t, "myapp-changelog-store-0", 4, configs)
	if _, err := h.Handle(&connstate.ConnState{}, 6, body); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(crw.created) != 1 || crw.created[0] != "myapp-changelog-store-0" {
		t.Fatalf("CR writer received: %v, want [myapp-changelog-store-0]", crw.created)
	}
	if len(crw.createdConfigs) != 1 {
		t.Fatalf("expected 1 createdConfigs entry, got %d", len(crw.createdConfigs))
	}
	got := crw.createdConfigs[0]
	for k, want := range configs {
		if got[k] != want {
			t.Errorf("config[%q]=%q, want %q", k, got[k], want)
		}
	}
}

// TestCreateTopicsHandler_NoConfigsEmptyMap pins the no-config case:
// AdminClient.createTopics(NewTopic(name, partitions)) with no
// configs() call sends an empty configs array. The handler must
// pass an EMPTY map (not nil) so writers don't have to
// nil-check; if it ever passed nil, the test fake's lookup
// would still work but real translateConfigs would skip its
// fast-path on an explicit length check.
func TestCreateTopicsHandler_NoConfigsEmptyMap(t *testing.T) {
	reg := &fakeRegistry{}
	crw := &fakeCRW{}
	h := NewCreateTopicsHandler(reg).WithCRWriter(crw)

	body := encodeCreateRequestV6(t, "no-configs", 1)
	if _, err := h.Handle(&connstate.ConnState{}, 6, body); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(crw.createdConfigs) != 1 {
		t.Fatalf("expected 1 createdConfigs entry, got %d", len(crw.createdConfigs))
	}
	if len(crw.createdConfigs[0]) != 0 {
		t.Errorf("expected empty config map, got %+v", crw.createdConfigs[0])
	}
}

// TestCreateTopicsHandler_DefaultsNegativePartitions pins the
// gh #33 safety net for Streams' "use broker default" idiom.
// AdminClient.createTopics with NumPartitions=-1 means "use
// what the broker defaults to". skafka has no concept of a
// broker-default partition count yet; treating -1 as 1 keeps
// the create flow alive for the Streams hello-world. Real
// apps tend to override anyway.
func TestCreateTopicsHandler_DefaultsNegativePartitions(t *testing.T) {
	reg := &fakeRegistry{}
	crw := &fakeCRW{}
	h := NewCreateTopicsHandler(reg).WithCRWriter(crw)

	body := encodeCreateRequestV6(t, "broker-default-parts", -1)
	if _, err := h.Handle(&connstate.ConnState{}, 6, body); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(crw.created) != 1 {
		t.Fatalf("CR writer not called: %+v", crw.created)
	}
	// fakeRegistry also got called with the same default.
	if len(reg.added) != 1 || reg.added[0] != "broker-default-parts" {
		t.Errorf("registry added: %+v, want [broker-default-parts]", reg.added)
	}
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
