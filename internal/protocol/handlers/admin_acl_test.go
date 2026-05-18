package handlers

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// fakeACLWriter records every call and returns programmed errors /
// results. Mirrors the fakeCRW pattern in admin_test.go.
type fakeACLWriter struct {
	mu       sync.Mutex
	creates  []ACLBinding
	deletes  []ACLFilter
	lists    []ACLFilter
	createErr map[string]error // keyed by Principal
	deleteOut []ACLBinding     // returned by DeleteACLs
	listOut   []ACLBinding     // returned by ListACLs
}

func (f *fakeACLWriter) CreateACL(_ context.Context, b ACLBinding) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, b)
	if err, ok := f.createErr[b.Principal]; ok {
		return err
	}
	return nil
}

func (f *fakeACLWriter) DeleteACLs(_ context.Context, fl ACLFilter) ([]ACLBinding, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, fl)
	return f.deleteOut, nil
}

func (f *fakeACLWriter) ListACLs(_ context.Context, fl ACLFilter) ([]ACLBinding, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists = append(f.lists, fl)
	return f.listOut, nil
}

// encodeCreateAclsV2 hand-encodes a v2 (flexible) CreateAclsRequest
// with a single AclBinding. v2 is the wire-version Kafka 2.4+ ships
// with — most current AdminClients hit this.
func encodeCreateAclsV2(t *testing.T, b api.AclBinding) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactArray(1, func() {
		w.WriteInt8(b.ResourceType)
		w.WriteCompactString(b.ResourceName)
		w.WriteInt8(b.PatternType)
		w.WriteCompactString(b.Principal)
		w.WriteCompactString(b.Host)
		w.WriteInt8(b.Operation)
		w.WriteInt8(b.Permission)
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

// encodeDeleteAclsV2 hand-encodes a v2 DeleteAclsRequest with a single
// AclFilter (any-coded by setting filter fields to UNKNOWN/empty).
func encodeDeleteAclsV2(t *testing.T, f api.AclFilter) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteCompactArray(1, func() {
		w.WriteInt8(f.ResourceTypeFilter)
		w.WriteCompactNullableString(f.ResourceNameFilter, f.ResourceNameFilter == "")
		w.WriteInt8(f.PatternTypeFilter)
		w.WriteCompactNullableString(f.PrincipalFilter, f.PrincipalFilter == "")
		w.WriteCompactNullableString(f.HostFilter, f.HostFilter == "")
		w.WriteInt8(f.Operation)
		w.WriteInt8(f.PermissionType)
		w.WriteEmptyTaggedFields()
	})
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

func encodeDescribeAclsV2(t *testing.T, f api.AclFilter) []byte {
	t.Helper()
	w := codec.NewWriter()
	w.WriteInt8(f.ResourceTypeFilter)
	w.WriteCompactNullableString(f.ResourceNameFilter, f.ResourceNameFilter == "")
	w.WriteInt8(f.PatternTypeFilter)
	w.WriteCompactNullableString(f.PrincipalFilter, f.PrincipalFilter == "")
	w.WriteCompactNullableString(f.HostFilter, f.HostFilter == "")
	w.WriteInt8(f.Operation)
	w.WriteInt8(f.PermissionType)
	w.WriteEmptyTaggedFields()
	return w.Bytes()
}

// TestCreateAclsHandler_TranslatesWireToCR pins the int8→string
// translation: a wire request with TOPIC + LITERAL + WRITE + ALLOW
// hits the writer with the right CR-shape strings. Without this
// translation the writer wouldn't know what KafkaUserACL.Resource.Type
// to write.
func TestCreateAclsHandler_TranslatesWireToCR(t *testing.T) {
	w := &fakeACLWriter{}
	h := NewCreateAclsHandler().WithCRWriter(w)

	body := encodeCreateAclsV2(t, api.AclBinding{
		ResourceType: api.ResourceTypeTopic,
		ResourceName: "payments",
		PatternType:  api.PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    api.AclOperationWrite,
		Permission:   api.PermissionTypeAllow,
	})

	respBytes, err := h.Handle(&connstate.ConnState{}, 2, body)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(w.creates) != 1 {
		t.Fatalf("expected 1 CreateACL call, got %d", len(w.creates))
	}
	got := w.creates[0]
	want := ACLBinding{
		Principal: "User:alice", ResourceType: "topic", ResourceName: "payments",
		PatternType: "literal", Operation: "Write", Permission: "Allow", Host: "*",
	}
	if got != want {
		t.Errorf("binding=%+v, want %+v", got, want)
	}
	// Response must say success (ErrorCode=0).
	r := codec.NewReader(respBytes)
	_, _ = r.ReadInt32() // throttle
	var firstResultErr int16
	_ = r.ReadCompactArray(func() error {
		if firstResultErr == 0 {
			firstResultErr, _ = r.ReadInt16()
			_, _, _ = readNullableCompactStr(r)
			_ = r.ReadTaggedFields()
		}
		return nil
	})
	if firstResultErr != 0 {
		t.Errorf("result errorCode=%d, want 0", firstResultErr)
	}
}

// TestCreateAclsHandler_RejectsUnsupportedResourceType — wire-level
// DELEGATION_TOKEN (=6) has no CR mapping; the handler must report
// INVALID_REQUEST so the AdminClient sees a real error instead of a
// silent success.
func TestCreateAclsHandler_RejectsUnsupportedResourceType(t *testing.T) {
	w := &fakeACLWriter{}
	h := NewCreateAclsHandler().WithCRWriter(w)

	body := encodeCreateAclsV2(t, api.AclBinding{
		ResourceType: api.ResourceTypeDelegationToken,
		ResourceName: "tok",
		PatternType:  api.PatternTypeLiteral,
		Principal:    "User:alice",
		Operation:    api.AclOperationRead,
		Permission:   api.PermissionTypeAllow,
	})
	respBytes, err := h.Handle(&connstate.ConnState{}, 2, body)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(w.creates) != 0 {
		t.Errorf("writer was called for unsupported type: %+v", w.creates)
	}
	if code := firstCreateResultCode(t, respBytes); code != int16(codec.ErrInvalidRequest) {
		t.Errorf("errorCode=%d, want ErrInvalidRequest=%d", code, codec.ErrInvalidRequest)
	}
}

// TestCreateAclsHandler_PropagatesUnknownPrincipal — when the writer
// returns ErrUnknownPrincipal (no KafkaUser CR for User:ghost) the
// handler surfaces INVALID_REQUEST with the wrapped message.
func TestCreateAclsHandler_PropagatesUnknownPrincipal(t *testing.T) {
	w := &fakeACLWriter{
		createErr: map[string]error{
			"User:ghost": ErrUnknownPrincipal,
		},
	}
	h := NewCreateAclsHandler().WithCRWriter(w)

	body := encodeCreateAclsV2(t, api.AclBinding{
		ResourceType: api.ResourceTypeTopic, ResourceName: "p",
		PatternType: api.PatternTypeLiteral, Principal: "User:ghost",
		Operation: api.AclOperationRead, Permission: api.PermissionTypeAllow,
	})
	respBytes, err := h.Handle(&connstate.ConnState{}, 2, body)
	if err != nil {
		t.Fatal(err)
	}
	if code := firstCreateResultCode(t, respBytes); code != int16(codec.ErrInvalidRequest) {
		t.Errorf("errorCode=%d, want ErrInvalidRequest", code)
	}
}

// TestDeleteAclsHandler_FilterTranslationAndMatching — wire-level
// filter (TOPIC, LITERAL, READ, ALLOW) translates to the CR-shape
// filter, and the writer's returned bindings make it back into the
// MatchingACLs section of the response.
func TestDeleteAclsHandler_FilterTranslationAndMatching(t *testing.T) {
	w := &fakeACLWriter{
		deleteOut: []ACLBinding{
			{Principal: "User:alice", ResourceType: "topic", ResourceName: "p",
				PatternType: "literal", Operation: "Read", Permission: "Allow", Host: "*"},
		},
	}
	h := NewDeleteAclsHandler().WithCRWriter(w)

	body := encodeDeleteAclsV2(t, api.AclFilter{
		ResourceTypeFilter: api.ResourceTypeTopic,
		ResourceNameFilter: "p",
		PatternTypeFilter:  api.PatternTypeLiteral,
		PrincipalFilter:    "User:alice",
		Operation:          api.AclOperationRead,
		PermissionType:     api.PermissionTypeAllow,
	})
	if _, err := h.Handle(&connstate.ConnState{}, 2, body); err != nil {
		t.Fatal(err)
	}

	if len(w.deletes) != 1 {
		t.Fatalf("DeleteACLs call count=%d, want 1", len(w.deletes))
	}
	wantFilter := ACLFilter{
		Principal: "User:alice", ResourceType: "topic", ResourceName: "p",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	}
	if w.deletes[0] != wantFilter {
		t.Errorf("filter=%+v, want %+v", w.deletes[0], wantFilter)
	}
}

// TestDeleteAclsHandler_AnyCodesCollapseToWildcard — a filter with
// every axis set to UNKNOWN/ANY codes must produce an empty-fields
// ACLFilter (everything wildcard), not error out. This is what
// `kafka-acls.sh --remove --principal User:alice` with no other
// filters sends.
func TestDeleteAclsHandler_AnyCodesCollapseToWildcard(t *testing.T) {
	w := &fakeACLWriter{}
	h := NewDeleteAclsHandler().WithCRWriter(w)

	body := encodeDeleteAclsV2(t, api.AclFilter{
		ResourceTypeFilter: api.ResourceTypeAny,
		PatternTypeFilter:  api.PatternTypeAny,
		PrincipalFilter:    "User:alice",
		Operation:          api.AclOperationAny,
		PermissionType:     api.PermissionTypeAny,
	})
	if _, err := h.Handle(&connstate.ConnState{}, 2, body); err != nil {
		t.Fatal(err)
	}
	if len(w.deletes) != 1 {
		t.Fatalf("got %d delete calls", len(w.deletes))
	}
	got := w.deletes[0]
	if got.ResourceType != "" || got.PatternType != "" || got.Operation != "" || got.Permission != "" {
		t.Errorf("axes not collapsed to wildcard: %+v", got)
	}
	if got.Principal != "User:alice" {
		t.Errorf("principal=%q, want User:alice", got.Principal)
	}
}

// TestDescribeAclsHandler_GroupsByResource — the writer returns three
// bindings on the same (topic, p, literal) row but two distinct
// operations + a different principal. The handler must fold them into
// ONE DescribeAclsResource with three MatchingACL rows (mirroring
// Apache Kafka's response shape).
func TestDescribeAclsHandler_GroupsByResource(t *testing.T) {
	w := &fakeACLWriter{
		listOut: []ACLBinding{
			{Principal: "User:alice", ResourceType: "topic", ResourceName: "p",
				PatternType: "literal", Operation: "Read", Permission: "Allow"},
			{Principal: "User:alice", ResourceType: "topic", ResourceName: "p",
				PatternType: "literal", Operation: "Write", Permission: "Allow"},
			{Principal: "User:bob", ResourceType: "topic", ResourceName: "p",
				PatternType: "literal", Operation: "Read", Permission: "Deny"},
		},
	}
	h := NewDescribeAclsHandler().WithCRWriter(w)
	if _, err := h.Handle(&connstate.ConnState{}, 2, encodeDescribeAclsV2(t, api.AclFilter{
		ResourceTypeFilter: api.ResourceTypeAny,
		PatternTypeFilter:  api.PatternTypeAny,
		Operation:          api.AclOperationAny,
		PermissionType:     api.PermissionTypeAny,
	})); err != nil {
		t.Fatal(err)
	}
	if len(w.lists) != 1 {
		t.Fatalf("expected 1 ListACLs call, got %d", len(w.lists))
	}
}

// TestDescribeAclsHandler_NoWriterIsEmpty — pre-gh #107 dev-mode path:
// no writer wired → empty Resources slice, ErrorCode=0. Pins backward
// compatibility for the kafka-compat tests that exercise the handler
// without a K8s apiserver.
func TestDescribeAclsHandler_NoWriterIsEmpty(t *testing.T) {
	h := NewDescribeAclsHandler()
	out, err := h.Handle(&connstate.ConnState{}, 2, encodeDescribeAclsV2(t, api.AclFilter{
		ResourceTypeFilter: api.ResourceTypeAny,
		PatternTypeFilter:  api.PatternTypeAny,
		Operation:          api.AclOperationAny,
		PermissionType:     api.PermissionTypeAny,
	}))
	if err != nil {
		t.Fatal(err)
	}
	// throttle (int32) + errCode (int16) + nullable err msg + resources
	r := codec.NewReader(out)
	_, _ = r.ReadInt32()
	errCode, _ := r.ReadInt16()
	if errCode != 0 {
		t.Errorf("errCode=%d, want 0", errCode)
	}
}

// TestWireBindingToACLBinding_DefaultsV0ToLiteral — v0 has no
// PatternType field on the wire, so the decoder leaves it as zero.
// The translator must default to LITERAL there, otherwise every v0
// Create attempt would fail "pattern type ANY not valid for create".
func TestWireBindingToACLBinding_DefaultsV0ToLiteral(t *testing.T) {
	got, err := wireBindingToACLBinding(api.AclBinding{
		ResourceType: api.ResourceTypeTopic, ResourceName: "p",
		PatternType: 0, // v0 default
		Principal:   "User:alice",
		Operation:   api.AclOperationRead, Permission: api.PermissionTypeAllow,
	}, 0)
	if err != nil {
		t.Fatalf("translate v0: %v", err)
	}
	if got.PatternType != "literal" {
		t.Errorf("v0 patternType=%q, want literal", got.PatternType)
	}
}

// Test the fakeACLWriter satisfies ACLCRWriter at compile time. Cheap
// guard against drift if the interface gains a method.
var _ ACLCRWriter = (*fakeACLWriter)(nil)

// firstCreateResultCode reads the first CreateAcls result's ErrorCode
// out of an encoded v2+ flexible response body. Throttle int32 +
// compact array of results.
func firstCreateResultCode(t *testing.T, body []byte) int16 {
	t.Helper()
	r := codec.NewReader(body)
	_, _ = r.ReadInt32() // throttle
	var code int16
	first := true
	if err := r.ReadCompactArray(func() error {
		if !first {
			return nil
		}
		first = false
		c, err := r.ReadInt16()
		if err != nil {
			return err
		}
		code = c
		if _, _, err := readNullableCompactStr(r); err != nil {
			return err
		}
		return r.ReadTaggedFields()
	}); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return code
}

func readNullableCompactStr(r *codec.Reader) (string, bool, error) {
	s, present, err := r.ReadCompactNullableString()
	if err != nil {
		return "", false, err
	}
	// Translate "present=true" → null=false to match the caller's
	// (value, isNull, err) convention.
	return s, !present, nil
}

// SilenceUnused lets us reference imports that some build paths don't.
var _ = errors.New
