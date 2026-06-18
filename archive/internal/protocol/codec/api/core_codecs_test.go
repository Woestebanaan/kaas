package api

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// ---- Metadata ----

func metadataRoundTrip(t *testing.T, version int16) {
	t.Helper()
	resp := &MetadataResponse{
		ThrottleTimeMs: 10,
		Brokers: []MetadataBroker{
			{NodeID: 0, Host: "broker-0.skafka", Port: 9092, Rack: "zone-a"},
		},
		ClusterID:    "skafka-cluster-1",
		ControllerID: 0,
		Topics: []MetadataTopic{
			{
				ErrorCode:  0,
				Name:       "payment-events",
				IsInternal: false,
				Partitions: []MetadataPartition{
					{
						ErrorCode:       0,
						PartitionIndex:  0,
						LeaderID:        0,
						LeaderEpoch:     1,
						ReplicaNodes:    []int32{0},
						IsrNodes:        []int32{0},
						OfflineReplicas: []int32{},
					},
				},
			},
		},
	}

	w := codec.NewWriter()
	EncodeMetadataResponse(w, resp, version)

	// Decode manually at a structural level — just verify no panic and non-empty output.
	if len(w.Bytes()) == 0 {
		t.Errorf("v%d: empty response", version)
	}
}

func TestMetadataResponseV1(t *testing.T)  { metadataRoundTrip(t, 1) }
func TestMetadataResponseV5(t *testing.T)  { metadataRoundTrip(t, 5) }
func TestMetadataResponseV8(t *testing.T)  { metadataRoundTrip(t, 8) }
func TestMetadataResponseV12(t *testing.T) { metadataRoundTrip(t, 12) }

func TestMetadataRequestDecodeV1(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(2, func() {
		w.WriteString("topic-a")
		w.WriteString("topic-b")
	})

	r := codec.NewReader(w.Bytes())
	req, err := DecodeMetadataRequest(r, 1)
	if err != nil {
		t.Fatalf("v1 decode: %v", err)
	}
	if len(req.Topics) != 2 || req.Topics[0] != "topic-a" || req.Topics[1] != "topic-b" {
		t.Errorf("topics: %v", req.Topics)
	}
}

func TestMetadataRequestDecodeV4(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(1, func() { w.WriteString("my-topic") })
	w.WriteInt8(1) // allow_auto_topic_creation = true

	r := codec.NewReader(w.Bytes())
	req, err := DecodeMetadataRequest(r, 4)
	if err != nil {
		t.Fatalf("v4 decode: %v", err)
	}
	if !req.AllowAutoTopicCreation {
		t.Error("AllowAutoTopicCreation should be true")
	}
}

// ---- Produce ----

func produceRoundTrip(t *testing.T, version int16) {
	t.Helper()

	// Encode a response.
	resp := &ProduceResponse{
		ThrottleTime: 0,
		Responses: []ProduceTopicResponse{
			{
				Name: "payment-events",
				PartitionResponses: []ProducePartitionResponse{
					{
						Index:          0,
						ErrorCode:      0,
						BaseOffset:     100,
						LogAppendTime:  -1,
						LogStartOffset: 0,
					},
				},
			},
		},
	}
	w := codec.NewWriter()
	EncodeProduceResponse(w, resp, version)
	if len(w.Bytes()) == 0 {
		t.Errorf("v%d: empty response", version)
	}
}

func TestProduceResponseV3(t *testing.T) { produceRoundTrip(t, 3) }
func TestProduceResponseV5(t *testing.T) { produceRoundTrip(t, 5) }
func TestProduceResponseV8(t *testing.T) { produceRoundTrip(t, 8) }

func TestProduceRequestDecodeV3(t *testing.T) {
	w := codec.NewWriter()
	w.WriteNullableString("", true) // transactional_id = null
	w.WriteInt16(-1)                // acks = all
	w.WriteInt32(5000)              // timeout_ms
	w.WriteArray(1, func() {
		w.WriteString("my-topic")
		w.WriteArray(1, func() {
			w.WriteInt32(0)               // partition
			w.WriteNullableBytes([]byte{0x01, 0x02}) // records
		})
	})

	r := codec.NewReader(w.Bytes())
	req, err := DecodeProduceRequest(r, 3)
	if err != nil {
		t.Fatalf("v3 decode: %v", err)
	}
	if req.Acks != -1 {
		t.Errorf("acks: got %d want -1", req.Acks)
	}
	if req.TimeoutMs != 5000 {
		t.Errorf("timeout_ms: got %d want 5000", req.TimeoutMs)
	}
	if len(req.TopicData) != 1 || req.TopicData[0].Name != "my-topic" {
		t.Errorf("topics: %v", req.TopicData)
	}
	if len(req.TopicData[0].PartitionData) != 1 || req.TopicData[0].PartitionData[0].Index != 0 {
		t.Errorf("partitions: %v", req.TopicData[0].PartitionData)
	}
}

// ---- Fetch ----

func fetchRoundTrip(t *testing.T, version int16) {
	t.Helper()
	resp := &FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionID:      0,
		Responses: []FetchTopicResponse{
			{
				Name: "payment-events",
				Partitions: []FetchPartitionResponse{
					{
						PartitionIndex:       0,
						ErrorCode:            0,
						HighWatermark:        200,
						LastStableOffset:     200,
						LogStartOffset:       0,
						AbortedTransactions:  nil,
						PreferredReadReplica: -1,
						Records:              []byte{0xAB, 0xCD},
					},
				},
			},
		},
	}

	w := codec.NewWriter()
	EncodeFetchResponse(w, resp, version)
	if len(w.Bytes()) == 0 {
		t.Errorf("v%d: empty response", version)
	}
}

func TestFetchResponseV4(t *testing.T)  { fetchRoundTrip(t, 4) }
func TestFetchResponseV7(t *testing.T)  { fetchRoundTrip(t, 7) }
func TestFetchResponseV11(t *testing.T) { fetchRoundTrip(t, 11) }
func TestFetchResponseV13(t *testing.T) { fetchRoundTrip(t, 13) }

func TestFetchRequestDecodeV4(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt32(-1)   // replica_id
	w.WriteInt32(500)  // max_wait_ms
	w.WriteInt32(1)    // min_bytes
	w.WriteInt32(1<<20) // max_bytes (v3+)
	w.WriteInt8(0)     // isolation_level (v4+)
	w.WriteArray(1, func() {
		w.WriteString("events")
		w.WriteArray(1, func() {
			w.WriteInt32(0)    // partition
			w.WriteInt64(50)   // fetch_offset
			w.WriteInt32(1<<16) // partition_max_bytes
		})
	})

	r := codec.NewReader(w.Bytes())
	req, err := DecodeFetchRequest(r, 4)
	if err != nil {
		t.Fatalf("v4 decode: %v", err)
	}
	if req.MaxWaitMs != 500 {
		t.Errorf("max_wait_ms: got %d want 500", req.MaxWaitMs)
	}
	if len(req.Topics) != 1 || req.Topics[0].Name != "events" {
		t.Errorf("topics: %v", req.Topics)
	}
	if req.Topics[0].Partitions[0].FetchOffset != 50 {
		t.Errorf("fetch_offset: got %d want 50", req.Topics[0].Partitions[0].FetchOffset)
	}
}

// ---- ListOffsets ----

func listOffsetsRoundTrip(t *testing.T, version int16) {
	t.Helper()
	resp := &ListOffsetsResponse{
		ThrottleTimeMs: 0,
		Topics: []ListOffsetsTopicResponse{
			{
				Name: "payment-events",
				Partitions: []ListOffsetsPartitionResponse{
					{
						PartitionIndex: 0,
						ErrorCode:      0,
						Timestamp:      -1,
						Offset:         500,
						LeaderEpoch:    2,
					},
				},
			},
		},
	}

	w := codec.NewWriter()
	EncodeListOffsetsResponse(w, resp, version)
	if len(w.Bytes()) == 0 {
		t.Errorf("v%d: empty response", version)
	}
}

func TestListOffsetsResponseV1(t *testing.T) { listOffsetsRoundTrip(t, 1) }
func TestListOffsetsResponseV4(t *testing.T) { listOffsetsRoundTrip(t, 4) }
func TestListOffsetsResponseV7(t *testing.T) { listOffsetsRoundTrip(t, 7) }

func TestListOffsetsRequestDecodeV1(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt32(-1) // replica_id
	w.WriteArray(1, func() {
		w.WriteString("my-topic")
		w.WriteArray(1, func() {
			w.WriteInt32(0)  // partition
			w.WriteInt64(-1) // timestamp = latest
		})
	})

	r := codec.NewReader(w.Bytes())
	req, err := DecodeListOffsetsRequest(r, 1)
	if err != nil {
		t.Fatalf("v1 decode: %v", err)
	}
	if len(req.Topics) != 1 || req.Topics[0].Name != "my-topic" {
		t.Errorf("topics: %v", req.Topics)
	}
	if req.Topics[0].Partitions[0].Timestamp != -1 {
		t.Errorf("timestamp: got %d want -1", req.Topics[0].Partitions[0].Timestamp)
	}
}

func TestListOffsetsRequestDecodeV2(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt32(-1) // replica_id
	w.WriteInt8(0)   // isolation_level (v2+)
	w.WriteArray(1, func() {
		w.WriteString("events")
		w.WriteArray(1, func() {
			w.WriteInt32(0)  // partition
			w.WriteInt64(-2) // timestamp = earliest
		})
	})

	r := codec.NewReader(w.Bytes())
	req, err := DecodeListOffsetsRequest(r, 2)
	if err != nil {
		t.Fatalf("v2 decode: %v", err)
	}
	if req.IsolationLevel != 0 {
		t.Errorf("isolation_level: got %d want 0", req.IsolationLevel)
	}
	if req.Topics[0].Partitions[0].Timestamp != -2 {
		t.Errorf("timestamp: got %d want -2", req.Topics[0].Partitions[0].Timestamp)
	}
}
