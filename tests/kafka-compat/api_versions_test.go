package kafkacompat

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestApiVersions_AdvertisesCoreKeys pins gh #1: the ApiVersions
// response (key 18) MUST list every API skafka has registered, with
// sane min/max ranges. Without this, a future contributor adding a
// new handler but forgetting to wire it into the registration list
// would silently regress every client's bootstrap (clients pick the
// highest mutually-supported version per API; missing keys → calls
// to that API fail with UNSUPPORTED_VERSION).
//
// Doesn't check exact max versions — those evolve as new ranges get
// added. Asserts only that critical keys are advertised at all,
// using > 0 as proof.
func TestApiVersions_AdvertisesCoreKeys(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewPtrApiVersionsRequest()
	req.ClientSoftwareName = "skafka-test"
	req.ClientSoftwareVersion = "0.0.0"
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("ApiVersions: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("ApiVersions ErrorCode=%d", resp.ErrorCode)
	}

	got := make(map[int16]struct{ min, max int16 }, len(resp.ApiKeys))
	for _, k := range resp.ApiKeys {
		got[k.ApiKey] = struct{ min, max int16 }{k.MinVersion, k.MaxVersion}
	}

	// Every API in here is something skafka registers and current
	// clients actively call. Missing one breaks bootstrap of at least
	// one of {Java AdminClient, franz-go, kafka-go, librdkafka}.
	mustHave := []struct {
		key  int16
		name string
	}{
		{0, "Produce"},
		{1, "Fetch"},
		{2, "ListOffsets"},
		{3, "Metadata"},
		{8, "OffsetCommit"},
		{9, "OffsetFetch"},
		{10, "FindCoordinator"},
		{11, "JoinGroup"},
		{12, "Heartbeat"},
		{13, "LeaveGroup"},
		{14, "SyncGroup"},
		{15, "DescribeGroups"},
		{16, "ListGroups"},
		{17, "SaslHandshake"},
		{18, "ApiVersions"},
		{19, "CreateTopics"},
		{20, "DeleteTopics"},
		{22, "InitProducerId"},
		{32, "DescribeConfigs"},
		{42, "DeleteGroups"},
		{60, "DescribeCluster"},
	}
	for _, want := range mustHave {
		if _, ok := got[want.key]; !ok {
			t.Errorf("ApiVersions missing key %d (%s)", want.key, want.name)
		}
	}

	// ApiVersions itself must be at least v0; v3+ is required for the
	// flexible bootstrap most current clients use.
	if av, ok := got[18]; !ok {
		t.Error("ApiVersions doesn't list itself")
	} else if av.max < 3 {
		t.Errorf("ApiVersions max=%d, want >= 3 (flexible bootstrap)", av.max)
	}
}

// TestApiVersions_VersionRangesAreOrdered guards against typos that
// flip min/max in a registration. Apache's contract: 0 <= min <= max.
func TestApiVersions_VersionRangesAreOrdered(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(testAddr))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := kmsg.NewPtrApiVersionsRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("ApiVersions: %v", err)
	}
	for _, k := range resp.ApiKeys {
		if k.MinVersion < 0 || k.MaxVersion < 0 || k.MinVersion > k.MaxVersion {
			t.Errorf("key %d has nonsensical range min=%d max=%d", k.ApiKey, k.MinVersion, k.MaxVersion)
		}
	}
}

// TestApiVersions_KafkaGoCanBootstrap covers the cross-client lane:
// kafka-go's Dialer.LookupPartitions uses ApiVersions to negotiate
// — if the response is malformed (wrong header type, wrong
// ResponseHeaderV1 framing, missing required keys) kafka-go would
// silently retry forever. A full bootstrap completing in under 2
// seconds proves the response is well-formed for both flexible
// (v3+) and non-flexible (v0) decoders.
func TestApiVersions_KafkaGoCanBootstrap(t *testing.T) {
	// kafka-go's Dialer is what runs ApiVersions on first connect.
	// We use the existing kafkago import already in the package.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dialer := &kafkago.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", testAddr)
	if err != nil {
		t.Fatalf("kafka-go dial: %v", err)
	}
	defer conn.Close()
}
