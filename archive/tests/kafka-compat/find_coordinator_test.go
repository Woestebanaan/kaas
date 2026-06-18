package kafkacompat

import (
	"context"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestFindCoordinator_GroupAndTransaction pins gh #6: FindCoordinator
// (key 10) MUST resolve both KeyType=0 (group) and KeyType=1
// (transaction). Group coordinator is the broker that handles
// JoinGroup/SyncGroup/Heartbeat/OffsetCommit for the named group;
// transaction coordinator is the broker that owns the transactional
// id's PID lease.
//
// In the kafka-compat in-process broker the group source always
// answers (LocalGroupSource) so a group key resolves to broker 0.
// No txn-assignment source is wired (gh #91 follow-up territory),
// so the txn key path returns COORDINATOR_NOT_AVAILABLE rather than
// either silently routing to the group coordinator or hard-failing
// with UNSUPPORTED_VERSION.
func TestFindCoordinator_GroupAndTransaction(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("group", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.CoordinatorType = 0 // group
		req.CoordinatorKeys = []string{"some-group-id"}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("FindCoordinator(group): %v", err)
		}
		if len(resp.Coordinators) != 1 {
			t.Fatalf("expected 1 coordinator entry, got %d", len(resp.Coordinators))
		}
		c := resp.Coordinators[0]
		if c.ErrorCode != 0 {
			t.Fatalf("group ErrorCode=%d, want 0 (group source must resolve)", c.ErrorCode)
		}
		if c.NodeID < 0 || c.Host == "" || c.Port == 0 {
			t.Errorf("group result missing fields: nodeID=%d host=%q port=%d", c.NodeID, c.Host, c.Port)
		}
	})

	t.Run("transaction", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.CoordinatorType = 1 // transaction
		req.CoordinatorKeys = []string{"some-txn-id"}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("FindCoordinator(txn): %v", err)
		}
		if len(resp.Coordinators) != 1 {
			t.Fatalf("expected 1 coordinator entry, got %d", len(resp.Coordinators))
		}
		// In the in-process broker the txn source isn't wired; the
		// handler must report a retry-friendly code rather than hard-
		// failing with UNSUPPORTED_VERSION (the pre-gh #6 stub
		// behaviour that broke Java AdminClient txn bootstrap).
		got := resp.Coordinators[0].ErrorCode
		switch got {
		case 15, // COORDINATOR_NOT_AVAILABLE
			16: // NOT_COORDINATOR
			// Both are valid: AdminClient retries either way.
		case 0:
			// A real txn source IS wired — the broker resolved. That's
			// also acceptable; mark for follow-up so the test doesn't
			// silently weaken if the cluster_runtime starts setting
			// the txn source in this harness.
			t.Logf("txn source resolved cleanly (nodeID=%d host=%s port=%d)",
				resp.Coordinators[0].NodeID, resp.Coordinators[0].Host, resp.Coordinators[0].Port)
		default:
			t.Errorf("txn ErrorCode=%d, want 15/16 (retry-friendly) or 0 (resolved)", got)
		}
	})
}

// TestFindCoordinator_InvalidKeyType pins the wire-protocol negative
// case: KeyType=2 (or anything other than 0/1) maps to
// INVALID_REQUEST. Apache returns the same code; future contributors
// adding a new key type must update both the handler enum and this
// test in lockstep.
func TestFindCoordinator_InvalidKeyType(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorType = 99 // unknown
	req.CoordinatorKeys = []string{"x"}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("FindCoordinator: %v", err)
	}
	if len(resp.Coordinators) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(resp.Coordinators))
	}
	if got := resp.Coordinators[0].ErrorCode; got != 42 {
		// 42 = INVALID_REQUEST per Apache wire codes.
		t.Errorf("invalid-key-type ErrorCode=%d, want 42 (INVALID_REQUEST)", got)
	}
}
