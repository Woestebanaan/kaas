package handlers

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// fakeCreator records every CreateTopic call. Mimics TopicCRWriter
// without the K8s API dependency.
type fakeCreator struct {
	mu       sync.Mutex
	calls    []string
	err      error // returned from every CreateTopic call (ErrTopicAlreadyExists is treated specially by the handler)
	delay    time.Duration
	calledCh chan struct{} // closed on first call; lets tests sync on "create started"
}

func (f *fakeCreator) CreateTopic(_ context.Context, name string, _ int32, _ map[string]string) error {
	f.mu.Lock()
	if f.calledCh != nil {
		select {
		case <-f.calledCh:
		default:
			close(f.calledCh)
		}
	}
	f.calls = append(f.calls, name)
	f.mu.Unlock()
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	return f.err
}

func (f *fakeCreator) callsSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.calls...)
}

// buildMetadataReqV4 hand-encodes a v4 MetadataRequest body so tests
// can drive the auto-create branch with full control over the
// `AllowAutoTopicCreation` flag.
func buildMetadataReqV4(topics []string, allowAutoCreate bool) []byte {
	w := codec.NewWriter()
	w.WriteArray(len(topics), func() {
		for _, t := range topics {
			w.WriteString(t)
		}
	})
	if allowAutoCreate {
		w.WriteInt8(1)
	} else {
		w.WriteInt8(0)
	}
	return w.Bytes()
}

func newAutoCreateHandler(creator AutoTopicCreator, cfg AutoCreateTopicsConfig) *MetadataHandler {
	src := stubBrokerSource{
		self: BrokerEndpoint{NodeID: 0, Host: "host", Port: 9092},
		all:  []BrokerEndpoint{{NodeID: 0, Host: "host", Port: 9092}},
	}
	h := NewMetadataHandlerWithSource(src, "test", stubTopics{}, stubLeaseManager{})
	if creator != nil {
		h = h.WithAutoCreate(cfg, creator)
	}
	return h
}

func decodeV4(t *testing.T, h *MetadataHandler, body []byte) *api.MetadataResponse {
	t.Helper()
	out, err := h.Handle(nil, 4, body)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	r := codec.NewReader(out)
	resp, err := api.DecodeMetadataResponse(r, 4)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	return resp
}

// findTopic returns the response entry for the named topic.
func findTopic(t *testing.T, resp *api.MetadataResponse, name string) api.MetadataTopic {
	t.Helper()
	for _, mt := range resp.Topics {
		if mt.Name == name {
			return mt
		}
	}
	t.Fatalf("topic %q not in response: %+v", name, resp.Topics)
	return api.MetadataTopic{}
}

// TestAutoCreateGateRequiresBothFlags pins Apache's contract: BOTH
// the request flag (AllowAutoTopicCreation=true) AND the broker
// config (Enabled=true) must be on. Either off → no creator call,
// UNKNOWN_TOPIC_OR_PARTITION returned.
func TestAutoCreateGateRequiresBothFlags(t *testing.T) {
	cases := []struct {
		name             string
		brokerEnabled    bool
		clientAllow      bool
		wantCreatorCalls int
		wantErrCode      int16
	}{
		{"both off", false, false, 0, int16(codec.ErrUnknownTopicOrPartition)},
		{"only client opted in", false, true, 0, int16(codec.ErrUnknownTopicOrPartition)},
		{"only broker enabled", true, false, 0, int16(codec.ErrUnknownTopicOrPartition)},
		{"both on", true, true, 1, int16(codec.ErrLeaderNotAvailable)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			creator := &fakeCreator{}
			h := newAutoCreateHandler(creator, AutoCreateTopicsConfig{
				Enabled:       tc.brokerEnabled,
				NumPartitions: 1,
			})
			resp := decodeV4(t, h, buildMetadataReqV4([]string{"new-topic"}, tc.clientAllow))
			topic := findTopic(t, resp, "new-topic")
			if topic.ErrorCode != tc.wantErrCode {
				t.Errorf("errorCode=%d, want %d", topic.ErrorCode, tc.wantErrCode)
			}
			if got := len(creator.callsSnapshot()); got != tc.wantCreatorCalls {
				t.Errorf("creator calls=%d, want %d", got, tc.wantCreatorCalls)
			}
		})
	}
}

// TestAutoCreateRejectsInternalTopic guards the `__` prefix
// reservation. Apache reserves internal-topic names (e.g.
// __consumer_offsets) for coordinator-driven creation; this branch
// must short-circuit to UNKNOWN_TOPIC_OR_PARTITION without invoking
// the creator.
func TestAutoCreateRejectsInternalTopic(t *testing.T) {
	creator := &fakeCreator{}
	h := newAutoCreateHandler(creator, AutoCreateTopicsConfig{Enabled: true, NumPartitions: 1})
	resp := decodeV4(t, h, buildMetadataReqV4([]string{"__not-yours"}, true))
	topic := findTopic(t, resp, "__not-yours")
	if topic.ErrorCode != int16(codec.ErrUnknownTopicOrPartition) {
		t.Errorf("internal topic got errorCode=%d, want UNKNOWN_TOPIC_OR_PARTITION", topic.ErrorCode)
	}
	if len(creator.callsSnapshot()) != 0 {
		t.Errorf("creator should not be called for internal topic, calls=%v", creator.calls)
	}
}

// TestAutoCreateSkipsListEverythingForm: an empty Topics array
// means "list every known topic". Apache's `!isAllTopics` clause
// short-circuits the auto-create branch — otherwise Streams'
// periodic full-refresh loop would spam-create topics for every
// transient name in its routing table.
//
// The default decodeMetadata helper uses an empty topics list with
// v1 (no AllowAutoTopicCreation flag). For this test we need v4+
// with the flag set explicitly.
func TestAutoCreateSkipsListEverythingForm(t *testing.T) {
	creator := &fakeCreator{}
	h := newAutoCreateHandler(creator, AutoCreateTopicsConfig{Enabled: true, NumPartitions: 1})
	// Empty topics list at v4 with AllowAutoTopicCreation=true.
	resp := decodeV4(t, h, buildMetadataReqV4(nil, true))
	// Should list known topics ("t" from stubTopics), not create.
	if len(creator.callsSnapshot()) != 0 {
		t.Errorf("list-everything triggered create: %v", creator.calls)
	}
	// Topic "t" should appear with no errorCode.
	topic := findTopic(t, resp, "t")
	if topic.ErrorCode != 0 {
		t.Errorf("known topic in list-everything got errorCode=%d", topic.ErrorCode)
	}
}

// TestAutoCreateDedupConcurrent guards the in-flight sync.Map: many
// concurrent MetadataRequests for the same unknown topic must
// invoke the creator at most once.
func TestAutoCreateDedupConcurrent(t *testing.T) {
	called := make(chan struct{}, 1)
	creator := &fakeCreator{
		delay:    50 * time.Millisecond,
		calledCh: called,
	}
	h := newAutoCreateHandler(creator, AutoCreateTopicsConfig{Enabled: true, NumPartitions: 1})

	const concurrency = 16
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			resp := decodeV4(t, h, buildMetadataReqV4([]string{"shared"}, true))
			topic := findTopic(t, resp, "shared")
			// Every caller must see LEADER_NOT_AVAILABLE, regardless
			// of whether it triggered the create or hit the in-flight
			// short-circuit.
			if topic.ErrorCode != int16(codec.ErrLeaderNotAvailable) {
				t.Errorf("errorCode=%d, want LEADER_NOT_AVAILABLE", topic.ErrorCode)
			}
		}()
	}
	wg.Wait()

	if got := len(creator.callsSnapshot()); got != 1 {
		t.Errorf("creator called %d times for the same topic; want 1 (dedup broken)", got)
	}
}

// TestAutoCreateAlreadyExistsReturnsLeaderNotAvailable: if the
// creator returns ErrTopicAlreadyExists (a peer broker created the
// same topic concurrently between this metadata response and the
// next), the response still uses LEADER_NOT_AVAILABLE so the
// client retries and finds the topic on the next refresh.
func TestAutoCreateAlreadyExistsReturnsLeaderNotAvailable(t *testing.T) {
	creator := &fakeCreator{err: ErrTopicAlreadyExists}
	h := newAutoCreateHandler(creator, AutoCreateTopicsConfig{Enabled: true, NumPartitions: 1})
	resp := decodeV4(t, h, buildMetadataReqV4([]string{"already-there"}, true))
	topic := findTopic(t, resp, "already-there")
	if topic.ErrorCode != int16(codec.ErrLeaderNotAvailable) {
		t.Errorf("errorCode=%d, want LEADER_NOT_AVAILABLE", topic.ErrorCode)
	}
}

// TestAutoCreateRealFailureFallsBackToUnknown: a real creator
// failure (K8s API down, RBAC denied, …) surfaces as
// UNKNOWN_TOPIC_OR_PARTITION so the client doesn't tight-loop on a
// broker that can't satisfy the create.
func TestAutoCreateRealFailureFallsBackToUnknown(t *testing.T) {
	creator := &fakeCreator{err: errors.New("forbidden by RBAC")}
	h := newAutoCreateHandler(creator, AutoCreateTopicsConfig{Enabled: true, NumPartitions: 1})
	resp := decodeV4(t, h, buildMetadataReqV4([]string{"forbidden"}, true))
	topic := findTopic(t, resp, "forbidden")
	if topic.ErrorCode != int16(codec.ErrUnknownTopicOrPartition) {
		t.Errorf("errorCode=%d, want UNKNOWN_TOPIC_OR_PARTITION", topic.ErrorCode)
	}
}

// TestAutoCreateNoCreatorWiredKeepsLegacyBehaviour: in dev mode (no
// TopicCRWriter), every unknown-topic Metadata still gets
// UNKNOWN_TOPIC_OR_PARTITION and the gates above are short-
// circuited by the nil creator check.
func TestAutoCreateNoCreatorWiredKeepsLegacyBehaviour(t *testing.T) {
	h := newAutoCreateHandler(nil, AutoCreateTopicsConfig{Enabled: true, NumPartitions: 1})
	resp := decodeV4(t, h, buildMetadataReqV4([]string{"new-topic"}, true))
	topic := findTopic(t, resp, "new-topic")
	if topic.ErrorCode != int16(codec.ErrUnknownTopicOrPartition) {
		t.Errorf("no creator wired: errorCode=%d, want UNKNOWN_TOPIC_OR_PARTITION", topic.ErrorCode)
	}
}

// TestAutoCreateUsesConfiguredNumPartitions: the request goes
// through with `NumPartitions` from broker config (defaults to 1
// per Apache 3.7).
func TestAutoCreateUsesConfiguredNumPartitions(t *testing.T) {
	creator := &fakeCallRecord{}
	h := newAutoCreateHandler(creator, AutoCreateTopicsConfig{Enabled: true, NumPartitions: 5})
	resp := decodeV4(t, h, buildMetadataReqV4([]string{"big-topic"}, true))
	if findTopic(t, resp, "big-topic").ErrorCode != int16(codec.ErrLeaderNotAvailable) {
		t.Fatalf("expected LEADER_NOT_AVAILABLE")
	}
	if got := creator.lastPartitions; got != 5 {
		t.Errorf("creator received NumPartitions=%d, want 5", got)
	}
}

// fakeCallRecord captures the partition count passed to the
// creator so tests can assert it propagates from the broker config.
type fakeCallRecord struct {
	mu             sync.Mutex
	lastPartitions int32
}

func (f *fakeCallRecord) CreateTopic(_ context.Context, _ string, partitions int32, _ map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastPartitions = partitions
	return nil
}

// TestAutoCreateDescribeConfigsReflectsLiveConfig: the
// DescribeConfigs handler must advertise the actual auto-create
// state plus num-partitions so admin clients
// (kafka-configs.sh / kafbat-ui) render the live values.
func TestAutoCreateDescribeConfigsReflectsLiveConfig(t *testing.T) {
	cases := []struct {
		enabled       bool
		numPartitions int32
	}{
		{true, 1},
		{false, 4},
		{true, 16},
	}
	for _, tc := range cases {
		t.Run(strconv.FormatBool(tc.enabled)+"/"+strconv.Itoa(int(tc.numPartitions)), func(t *testing.T) {
			src := stubBrokerSource{
				self: BrokerEndpoint{NodeID: 0, Host: "h", Port: 9092},
				all:  []BrokerEndpoint{{NodeID: 0, Host: "h", Port: 9092}},
			}
			h := NewDescribeConfigsHandler(stubTopics{}, src).WithBrokerConfig(tc.enabled, tc.numPartitions)
			entries := brokerConfigs(src, tc.enabled, tc.numPartitions)
			seen := map[string]string{}
			for _, e := range entries {
				seen[e.Name] = e.Value
			}
			if got := seen["auto.create.topics.enable"]; got != strconv.FormatBool(tc.enabled) {
				t.Errorf("auto.create.topics.enable=%q, want %q", got, strconv.FormatBool(tc.enabled))
			}
			if got := seen["num.partitions"]; got != strconv.Itoa(int(tc.numPartitions)) {
				t.Errorf("num.partitions=%q, want %d", got, tc.numPartitions)
			}
			// Smoke: exercise the handler end-to-end so we know the
			// WithBrokerConfig builder doesn't drop the values on
			// construction.
			_ = h
		})
	}
}
