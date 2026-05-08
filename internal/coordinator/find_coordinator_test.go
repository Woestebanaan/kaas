package coordinator

import (
	"context"
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// fakeGroupSrc / fakeTxnSrc are tiny stubs satisfying the two
// AssignmentSource interfaces. The dispatch test only cares which
// resolve function the FindCoordinator handler picked, so the
// stubs return distinguishable broker IDs ("group-broker" vs
// "txn-broker").
type fakeGroupSrc struct{}

func (fakeGroupSrc) OwnsGroup(string) bool                     { return true }
func (fakeGroupSrc) GroupCoordinator(string) (string, bool)    { return "group-broker", true }

type fakeTxnSrc struct{}

func (fakeTxnSrc) OwnsTxn(string) bool                  { return true }
func (fakeTxnSrc) TxnCoordinator(string) (string, bool) { return "txn-broker", true }

func newDispatchManager(t *testing.T) *Manager {
	t.Helper()
	lookup := func(brokerID string) (int32, string, int32, bool) {
		switch brokerID {
		case "group-broker":
			return 7, "group-host", 9092, true
		case "txn-broker":
			return 11, "txn-host", 9092, true
		}
		return 0, "", 0, false
	}
	m := NewManager(context.Background(), fakeGroupSrc{}, lookup, nil)
	m.SetTxnAssignmentSource(fakeTxnSrc{})
	return m
}

// TestFindCoordinatorKeyTypeGroup pins the gh #91 PR 3 dispatch:
// KeyType=0 routes through the GroupAssignmentSource. Without the
// branch every request would have gone through the same path,
// which silently worked for groups but routed transactions to the
// wrong broker.
func TestFindCoordinatorKeyTypeGroup(t *testing.T) {
	m := newDispatchManager(t)
	resp := m.FindCoordinator(&api.FindCoordinatorRequest{
		Key:     "my-group",
		KeyType: 0, // group
	})
	if resp.ErrorCode != 0 {
		t.Errorf("group dispatch errCode=%d, want 0", resp.ErrorCode)
	}
	if resp.NodeID != 7 || resp.Host != "group-host" {
		t.Errorf("group dispatch routed to (%d, %q), want (7, group-host) — txn source leaked into group path",
			resp.NodeID, resp.Host)
	}
}

// TestFindCoordinatorKeyTypeTransaction is the headline gh #91 PR 3
// behaviour: KeyType=1 routes through TxnAssignmentSource. Before
// this, every transactional producer's FindCoordinator returned the
// *group* coordinator for the txnID, which was wrong both
// semantically and routing-wise (different hash spaces — wait, they
// share the hash now, but still different sources).
func TestFindCoordinatorKeyTypeTransaction(t *testing.T) {
	m := newDispatchManager(t)
	resp := m.FindCoordinator(&api.FindCoordinatorRequest{
		Key:     "my-txn",
		KeyType: 1, // transaction
	})
	if resp.ErrorCode != 0 {
		t.Errorf("txn dispatch errCode=%d, want 0", resp.ErrorCode)
	}
	if resp.NodeID != 11 || resp.Host != "txn-host" {
		t.Errorf("txn dispatch routed to (%d, %q), want (11, txn-host) — group source leaked into txn path",
			resp.NodeID, resp.Host)
	}
}

// TestFindCoordinatorKeyTypeUnknown: anything other than 0 or 1
// returns INVALID_REQUEST (42), matching Apache Kafka. Skafka does
// not yet implement type=2/3/... and returning a coordinator for
// unknown types would mislead the client into a routing decision
// it can't act on.
func TestFindCoordinatorKeyTypeUnknown(t *testing.T) {
	m := newDispatchManager(t)
	resp := m.FindCoordinator(&api.FindCoordinatorRequest{
		Key:     "anything",
		KeyType: 99,
	})
	if resp.ErrorCode != int16(codec.ErrInvalidRequest) {
		t.Errorf("unknown KeyType errCode=%d, want %d (INVALID_REQUEST)",
			resp.ErrorCode, int16(codec.ErrInvalidRequest))
	}
}

// TestFindCoordinatorTxnSrcUnwiredReturnsRetryable pins the boot-
// window contract: before cluster_runtime calls
// SetTxnAssignmentSource the txn path returns
// COORDINATOR_NOT_AVAILABLE (15), not INVALID_REQUEST. The Java
// client treats 15 as a retry signal, so the producer's
// markCoordinatorUnknown loop unsticks itself once the source
// gets wired a few hundred ms later. Returning anything terminal
// here would hard-fail every transactional producer started during
// broker boot.
func TestFindCoordinatorTxnSrcUnwiredReturnsRetryable(t *testing.T) {
	lookup := func(string) (int32, string, int32, bool) { return 0, "", 0, false }
	m := NewManager(context.Background(), fakeGroupSrc{}, lookup, nil)
	// txnSrc intentionally NOT set — pre-cluster_runtime state.

	resp := m.FindCoordinator(&api.FindCoordinatorRequest{
		Key:     "my-txn",
		KeyType: 1,
	})
	if resp.ErrorCode != int16(codec.ErrCoordinatorNotAvailable) {
		t.Errorf("unwired-txn errCode=%d, want %d (COORDINATOR_NOT_AVAILABLE — retryable)",
			resp.ErrorCode, int16(codec.ErrCoordinatorNotAvailable))
	}
}

// TestFindCoordinatorV4ArrayHonorsKeyType: in the v4 multi-key
// shape, KeyType is a request-level field (Apache schema:
// CoordinatorKeys is an array, but KeyType is scalar). Every key
// in the array is resolved against the same source.
func TestFindCoordinatorV4ArrayHonorsKeyType(t *testing.T) {
	m := newDispatchManager(t)
	resp := m.FindCoordinator(&api.FindCoordinatorRequest{
		KeyType:         1, // transaction
		CoordinatorKeys: []string{"txn-a", "txn-b"},
	})
	if len(resp.Coordinators) != 2 {
		t.Fatalf("v4 array got %d coordinators, want 2", len(resp.Coordinators))
	}
	for i, c := range resp.Coordinators {
		if c.ErrorCode != 0 {
			t.Errorf("coord[%d] errCode=%d, want 0", i, c.ErrorCode)
		}
		if c.Host != "txn-host" {
			t.Errorf("coord[%d] host=%q, want txn-host (group source must not leak into v4 txn path)",
				i, c.Host)
		}
	}
}
