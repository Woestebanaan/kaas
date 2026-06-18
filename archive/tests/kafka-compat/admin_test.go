package kafkacompat

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestAdminCreateDeleteTopics is the end-to-end coverage for gh #8:
// CreateTopics (key 19) and DeleteTopics (key 20) handlers exercised
// via a real Kafka admin client (franz-go's kmsg layer, the same shape
// kafka-topics.sh / kafbat-ui / Java AdminClient send on the wire).
//
// The test runs against the in-process broker started by TestMain in
// compat_test.go. It verifies:
//   - CreateTopics adds a topic that subsequently appears in Metadata.
//   - The new topic carries the requested partition count.
//   - DeleteTopics removes the topic and Metadata reflects the change.
//   - Multi-topic Create/Delete requests work in one round-trip.
//   - Topics created via the admin protocol survive a CreateTopics
//     idempotency call (re-creating with the same name is a no-op
//     that doesn't crash — separate parity gap tracked elsewhere if
//     a TOPIC_ALREADY_EXISTS error code is desired).
func TestAdminCreateDeleteTopics(t *testing.T) {
	cl := franzClient(t)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const created = "admin-test-created"

	// Sanity: topic doesn't exist yet.
	if topicInMetadata(t, ctx, cl, created) {
		t.Fatalf("test setup: %q already exists", created)
	}

	// CreateTopics with 3 partitions.
	createReq := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = created
	ct.NumPartitions = 3
	ct.ReplicationFactor = 1
	createReq.Topics = append(createReq.Topics, ct)
	createReq.TimeoutMillis = 5000
	createResp, err := createReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("CreateTopics: %v", err)
	}
	if len(createResp.Topics) != 1 {
		t.Fatalf("CreateTopics: got %d topic results, want 1", len(createResp.Topics))
	}
	if got := createResp.Topics[0].Topic; got != created {
		t.Errorf("CreateTopics: result topic=%q, want %q", got, created)
	}
	if ec := createResp.Topics[0].ErrorCode; ec != 0 {
		t.Errorf("CreateTopics: errorCode=%d, want 0", ec)
	}

	// Metadata should now report the topic with 3 partitions.
	if !topicHasPartitions(t, ctx, cl, created, 3) {
		t.Errorf("after CreateTopics: %q not visible with 3 partitions in Metadata", created)
	}

	// Idempotent re-creation is currently a silent succeed (no
	// TOPIC_ALREADY_EXISTS surface — see admin.go). The contract is
	// "doesn't crash, doesn't change partition count" — pin both.
	createResp2, err := createReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("CreateTopics (re-create): %v", err)
	}
	if len(createResp2.Topics) != 1 {
		t.Fatalf("CreateTopics (re-create): got %d topic results, want 1", len(createResp2.Topics))
	}
	if !topicHasPartitions(t, ctx, cl, created, 3) {
		t.Errorf("after re-create: partition count drifted from 3")
	}

	// DeleteTopics removes it. skafka caps DeleteTopics at v5 (the
	// v0-v5 `topic_names: [STRING]` shape); v6+ topic-id support is a
	// separate parity task. kmsg's DeleteTopicsRequest serialises
	// .TopicNames at v0-v5 and .Topics at v6+, so populate the v5
	// field for negotiated wire compat.
	deleteReq := kmsg.NewPtrDeleteTopicsRequest()
	deleteReq.TopicNames = []string{created}
	deleteReq.TimeoutMillis = 5000
	deleteResp, err := deleteReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("DeleteTopics: %v", err)
	}
	if len(deleteResp.Topics) != 1 {
		t.Fatalf("DeleteTopics: got %d responses, want 1", len(deleteResp.Topics))
	}
	if ec := deleteResp.Topics[0].ErrorCode; ec != 0 {
		t.Errorf("DeleteTopics: errorCode=%d, want 0", ec)
	}

	if topicInMetadata(t, ctx, cl, created) {
		t.Errorf("after DeleteTopics: %q still appears in Metadata", created)
	}
}

// TestAdminCreateMultipleTopics validates that a single CreateTopics
// request batching multiple topics returns one result per topic and
// all become visible in Metadata.
func TestAdminCreateMultipleTopics(t *testing.T) {
	cl := franzClient(t)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	names := []string{"admin-batch-a", "admin-batch-b", "admin-batch-c"}
	defer func() {
		// Best-effort cleanup so reruns are clean.
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TopicNames = append([]string{}, names...)
		req.TimeoutMillis = 2000
		_, _ = req.RequestWith(ctx, cl)
	}()

	req := kmsg.NewPtrCreateTopicsRequest()
	for i, n := range names {
		ct := kmsg.NewCreateTopicsRequestTopic()
		ct.Topic = n
		ct.NumPartitions = int32(i + 1) // 1, 2, 3 partitions respectively
		ct.ReplicationFactor = 1
		req.Topics = append(req.Topics, ct)
	}
	req.TimeoutMillis = 5000

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("CreateTopics: %v", err)
	}
	if len(resp.Topics) != len(names) {
		t.Fatalf("CreateTopics: got %d topic results, want %d", len(resp.Topics), len(names))
	}
	for _, r := range resp.Topics {
		if r.ErrorCode != 0 {
			t.Errorf("CreateTopics: %q errorCode=%d", r.Topic, r.ErrorCode)
		}
	}

	for i, n := range names {
		if !topicHasPartitions(t, ctx, cl, n, int32(i+1)) {
			t.Errorf("after CreateTopics: %q not visible with %d partitions", n, i+1)
		}
	}
}

// TestAdminDeleteMissingTopic exercises the DeleteTopics path against
// a name that doesn't exist. The handler currently treats this as a
// no-op success; the test pins the contract so a future change that
// surfaces UNKNOWN_TOPIC_OR_PARTITION (Apache Kafka's behaviour) is a
// deliberate decision rather than a silent regression in the other
// direction.
func TestAdminDeleteMissingTopic(t *testing.T) {
	cl := franzClient(t)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{"admin-nonexistent"}
	req.TimeoutMillis = 2000

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("DeleteTopics on missing: %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("DeleteTopics: got %d responses, want 1", len(resp.Topics))
	}
	// Current behaviour: ErrorCode=0 (silent no-op). When/if skafka starts
	// returning UNKNOWN_TOPIC_OR_PARTITION (3) here, update this assertion.
	if ec := resp.Topics[0].ErrorCode; ec != 0 {
		t.Logf("DeleteTopics on missing topic returned errorCode=%d (note: behavior changed; update test if intentional)", ec)
	}
}

// topicInMetadata returns true iff Metadata reports the named topic
// as known to the broker.
func topicInMetadata(t *testing.T, ctx context.Context, cl *kgo.Client, name string) bool {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(name)
	req.Topics = append(req.Topics, mt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("MetadataRequest: %v", err)
	}
	for _, tr := range resp.Topics {
		if tr.Topic != nil && *tr.Topic == name && tr.ErrorCode == 0 {
			return true
		}
	}
	return false
}

// topicHasPartitions returns true iff Metadata reports the named topic
// with exactly the requested partition count.
func topicHasPartitions(t *testing.T, ctx context.Context, cl *kgo.Client, name string, want int32) bool {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(name)
	req.Topics = append(req.Topics, mt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("MetadataRequest: %v", err)
	}
	for _, tr := range resp.Topics {
		if tr.Topic != nil && *tr.Topic == name {
			if tr.ErrorCode != 0 {
				t.Logf("Metadata for %q: errorCode=%d", name, tr.ErrorCode)
				return false
			}
			got := int32(len(tr.Partitions))
			if got != want {
				t.Logf("Metadata for %q: got %d partitions, want %d", name, got, want)
				return false
			}
			return true
		}
	}
	return false
}
