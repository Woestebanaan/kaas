package api

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

// ---- DescribeConfigs ----

func TestDescribeConfigsRequestDecodeAllConfigs(t *testing.T) {
	w := codec.NewWriter()
	// Resources: 1 entry — Topic / "foo" / null ConfigNames.
	w.WriteInt32(1)
	w.WriteInt8(2) // Topic
	w.WriteString("foo")
	w.WriteInt32(-1) // ConfigNames = null
	// v1+ adds IncludeSynonyms.
	w.WriteInt8(1)
	r := codec.NewReader(w.Bytes())

	req, err := DecodeDescribeConfigsRequest(r, 1)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(req.Resources) != 1 {
		t.Fatalf("got %d resources, want 1", len(req.Resources))
	}
	res := req.Resources[0]
	if res.ResourceType != 2 || res.ResourceName != "foo" {
		t.Errorf("res=%+v", res)
	}
	if !res.ConfigNull {
		t.Error("ConfigNull=false, want true (null array)")
	}
	if !req.IncludeSynonyms {
		t.Error("IncludeSynonyms not decoded")
	}
}

func TestDescribeConfigsRequestDecodeNamedConfigs(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt32(1)
	w.WriteInt8(4) // Broker
	w.WriteString("0")
	w.WriteInt32(2) // 2 names
	w.WriteString("listeners")
	w.WriteString("broker.id")
	r := codec.NewReader(w.Bytes())

	req, err := DecodeDescribeConfigsRequest(r, 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	res := req.Resources[0]
	if res.ConfigNull {
		t.Error("ConfigNull=true on non-null array")
	}
	if len(res.ConfigNames) != 2 || res.ConfigNames[0] != "listeners" {
		t.Errorf("ConfigNames=%v", res.ConfigNames)
	}
}

func TestDescribeConfigsResponseEncodeVersions(t *testing.T) {
	resp := &DescribeConfigsResponse{
		Results: []DescribeConfigsResult{{
			ResourceType: 2,
			ResourceName: "smoke",
			Configs: []DescribeConfigsEntry{{
				Name:         "retention.ms",
				Value:        "604800000",
				ReadOnly:     true,
				IsDefault:    true,
				ConfigSource: ConfigSourceDefault,
			}},
		}},
	}
	for _, v := range []int16{0, 1, 2, 3} {
		w := codec.NewWriter()
		EncodeDescribeConfigsResponse(w, resp, v)
		if len(w.Bytes()) == 0 {
			t.Errorf("v%d: empty response", v)
		}
	}
}

// ---- SASL Handshake ----

func TestSaslHandshakeRoundTrip(t *testing.T) {
	// request
	w := codec.NewWriter()
	w.WriteString("SCRAM-SHA-512")
	r := codec.NewReader(w.Bytes())
	req, err := DecodeSaslHandshakeRequest(r, 0)
	if err != nil || req.Mechanism != "SCRAM-SHA-512" {
		t.Fatalf("decode: %v, mechanism=%q", err, req.Mechanism)
	}

	// response
	resp := &SaslHandshakeResponse{ErrorCode: 0, Mechanisms: []string{"SCRAM-SHA-512", "PLAIN"}}
	w = codec.NewWriter()
	EncodeSaslHandshakeResponse(w, resp, 0)
	if len(w.Bytes()) == 0 {
		t.Error("empty response")
	}
}

// ---- SASL Authenticate ----

func TestSaslAuthenticateV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteNullableBytes([]byte("sasl-payload"))
	r := codec.NewReader(w.Bytes())
	req, err := DecodeSaslAuthenticateRequest(r, 0)
	if err != nil || string(req.AuthBytes) != "sasl-payload" {
		t.Fatalf("v0 decode: %v %q", err, req.AuthBytes)
	}
}

func TestSaslAuthenticateV2(t *testing.T) {
	w := codec.NewWriter()
	w.WriteCompactNullableBytes([]byte("scram-challenge"))
	w.WriteEmptyTaggedFields()
	r := codec.NewReader(w.Bytes())
	req, err := DecodeSaslAuthenticateRequest(r, 2)
	if err != nil || string(req.AuthBytes) != "scram-challenge" {
		t.Fatalf("v2 decode: %v %q", err, req.AuthBytes)
	}
}

func TestSaslAuthenticateResponseVersions(t *testing.T) {
	resp := &SaslAuthenticateResponse{ErrorCode: 0, AuthBytes: []byte("srv-response"), SessionTTLMs: 3600000}
	for _, v := range []int16{0, 1, 2} {
		w := codec.NewWriter()
		EncodeSaslAuthenticateResponse(w, resp, v)
		if len(w.Bytes()) == 0 {
			t.Errorf("v%d: empty response", v)
		}
	}
}

// ---- FindCoordinator ----

func TestFindCoordinatorV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteString("my-group")
	r := codec.NewReader(w.Bytes())
	req, err := DecodeFindCoordinatorRequest(r, 0)
	if err != nil || req.Key != "my-group" {
		t.Fatalf("v0: %v %q", err, req.Key)
	}
}

func TestFindCoordinatorV1(t *testing.T) {
	w := codec.NewWriter()
	w.WriteString("my-group")
	w.WriteInt8(0) // key_type = group
	r := codec.NewReader(w.Bytes())
	req, err := DecodeFindCoordinatorRequest(r, 1)
	if err != nil || req.Key != "my-group" || req.KeyType != 0 {
		t.Fatalf("v1: %v", err)
	}
}

func TestFindCoordinatorResponseV0(t *testing.T) {
	resp := &FindCoordinatorResponse{NodeID: 1, Host: "broker-1", Port: 9092}
	w := codec.NewWriter()
	EncodeFindCoordinatorResponse(w, resp, 0)
	if len(w.Bytes()) == 0 {
		t.Error("empty response")
	}
}

func TestFindCoordinatorResponseV3(t *testing.T) {
	resp := &FindCoordinatorResponse{
		Coordinators: []CoordinatorResult{
			{Key: "my-group", NodeID: 0, Host: "broker-0", Port: 9092},
		},
	}
	w := codec.NewWriter()
	EncodeFindCoordinatorResponse(w, resp, 3)
	if len(w.Bytes()) == 0 {
		t.Error("empty response")
	}
}

// ---- JoinGroup ----

func TestJoinGroupRoundTripV2(t *testing.T) {
	w := codec.NewWriter()
	w.WriteString("my-group")
	w.WriteInt32(30000) // session_timeout_ms
	w.WriteInt32(60000) // rebalance_timeout_ms
	w.WriteString("")   // member_id (empty = new member)
	w.WriteString("consumer")
	w.WriteArray(1, func() {
		w.WriteString("range")
		w.WriteNullableBytes([]byte{0x00})
	})

	r := codec.NewReader(w.Bytes())
	req, err := DecodeJoinGroupRequest(r, 2)
	if err != nil {
		t.Fatalf("v2 decode: %v", err)
	}
	if req.GroupID != "my-group" || req.SessionTimeoutMs != 30000 {
		t.Errorf("fields mismatch: %+v", req)
	}

	resp := &JoinGroupResponse{
		ErrorCode: 0, GenerationID: 1, ProtocolName: "range",
		Leader: "member-1", MemberID: "member-1",
		Members: []JoinGroupMember{{MemberID: "member-1", Metadata: []byte{0x00}}},
	}
	w = codec.NewWriter()
	EncodeJoinGroupResponse(w, resp, 2)
	if len(w.Bytes()) == 0 {
		t.Error("empty response")
	}
}

// ---- Heartbeat ----

func TestHeartbeatRoundTripV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteString("my-group")
	w.WriteInt32(1)
	w.WriteString("member-1")
	r := codec.NewReader(w.Bytes())
	req, err := DecodeHeartbeatRequest(r, 0)
	if err != nil || req.GroupID != "my-group" || req.GenerationID != 1 {
		t.Fatalf("v0: %v %+v", err, req)
	}
	w = codec.NewWriter()
	EncodeHeartbeatResponse(w, &HeartbeatResponse{ErrorCode: 0}, 0)
	if len(w.Bytes()) == 0 {
		t.Error("empty response")
	}
}

func TestHeartbeatRoundTripV4(t *testing.T) {
	w := codec.NewWriter()
	w.WriteCompactString("my-group")
	w.WriteInt32(2)
	w.WriteCompactString("member-1")
	w.WriteCompactNullableString("", true) // group_instance_id = null
	w.WriteEmptyTaggedFields()
	r := codec.NewReader(w.Bytes())
	req, err := DecodeHeartbeatRequest(r, 4)
	if err != nil || req.GroupID != "my-group" {
		t.Fatalf("v4: %v", err)
	}
}

// ---- LeaveGroup ----

func TestLeaveGroupV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteString("my-group")
	w.WriteString("member-1")
	r := codec.NewReader(w.Bytes())
	req, err := DecodeLeaveGroupRequest(r, 0)
	if err != nil || req.GroupID != "my-group" || req.MemberID != "member-1" {
		t.Fatalf("v0: %v", err)
	}
}

func TestLeaveGroupResponseV3(t *testing.T) {
	resp := &LeaveGroupResponse{
		ErrorCode: 0,
		Members:   []LeaveMemberResponse{{MemberID: "member-1", ErrorCode: 0}},
	}
	w := codec.NewWriter()
	EncodeLeaveGroupResponse(w, resp, 3)
	if len(w.Bytes()) == 0 {
		t.Error("empty response")
	}
}

// ---- SyncGroup ----

func TestSyncGroupRoundTripV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteString("my-group")
	w.WriteInt32(1)
	w.WriteString("member-1")
	w.WriteArray(1, func() {
		w.WriteString("member-1")
		w.WriteNullableBytes([]byte{0xAB})
	})
	r := codec.NewReader(w.Bytes())
	req, err := DecodeSyncGroupRequest(r, 0)
	if err != nil || len(req.Assignments) != 1 {
		t.Fatalf("v0: %v", err)
	}

	resp := &SyncGroupResponse{ErrorCode: 0, Assignment: []byte{0xAB}}
	w = codec.NewWriter()
	EncodeSyncGroupResponse(w, resp, 0)
	if len(w.Bytes()) == 0 {
		t.Error("empty response")
	}
}

// ---- OffsetCommit ----

func TestOffsetCommitRoundTripV2(t *testing.T) {
	w := codec.NewWriter()
	w.WriteString("my-group")
	w.WriteInt32(1)     // generation_id
	w.WriteString("m1") // member_id
	w.WriteArray(1, func() {
		w.WriteString("my-topic")
		w.WriteArray(1, func() {
			w.WriteInt32(0)           // partition
			w.WriteInt64(42)          // committed_offset
			w.WriteNullableString("", true) // metadata = null
		})
	})
	r := codec.NewReader(w.Bytes())
	req, err := DecodeOffsetCommitRequest(r, 2)
	if err != nil {
		t.Fatalf("v2 decode: %v", err)
	}
	if req.GroupID != "my-group" || req.Topics[0].Partitions[0].CommittedOffset != 42 {
		t.Errorf("fields: %+v", req)
	}
}

func TestOffsetCommitResponseVersions(t *testing.T) {
	resp := &OffsetCommitResponse{
		Topics: []OffsetCommitTopicResponse{
			{Name: "t", Partitions: []OffsetCommitPartitionResponse{{PartitionIndex: 0, ErrorCode: 0}}},
		},
	}
	for _, v := range []int16{2, 5, 8} {
		w := codec.NewWriter()
		EncodeOffsetCommitResponse(w, resp, v)
		if len(w.Bytes()) == 0 {
			t.Errorf("v%d: empty", v)
		}
	}
}

// ---- OffsetFetch ----

func TestOffsetFetchRoundTripV1(t *testing.T) {
	w := codec.NewWriter()
	w.WriteString("my-group")
	w.WriteArray(1, func() {
		w.WriteString("my-topic")
		w.WriteArray(1, func() { w.WriteInt32(0) })
	})
	r := codec.NewReader(w.Bytes())
	req, err := DecodeOffsetFetchRequest(r, 1)
	if err != nil || req.GroupID != "my-group" {
		t.Fatalf("v1: %v", err)
	}
}

func TestOffsetFetchResponseVersions(t *testing.T) {
	resp := &OffsetFetchResponse{
		Topics: []OffsetFetchTopicResponse{
			{Name: "t", Partitions: []OffsetFetchPartitionResponse{
				{PartitionIndex: 0, CommittedOffset: 10, ErrorCode: 0},
			}},
		},
	}
	for _, v := range []int16{1, 3, 5, 6} {
		w := codec.NewWriter()
		EncodeOffsetFetchResponse(w, resp, v)
		if len(w.Bytes()) == 0 {
			t.Errorf("v%d: empty", v)
		}
	}
}

// ---- DescribeLogDirs ----

func TestDescribeLogDirsRequestNullTopics(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt32(-1) // Topics = null → "describe everything"
	r := codec.NewReader(w.Bytes())
	req, err := DecodeDescribeLogDirsRequest(r, 1)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !req.TopicNull {
		t.Errorf("TopicNull=false on null array")
	}
}

func TestDescribeLogDirsRequestExplicit(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteString("smoke")
		w.WriteArray(2, func() {
			w.WriteInt32(0)
			w.WriteInt32(2)
		})
	})
	r := codec.NewReader(w.Bytes())
	req, err := DecodeDescribeLogDirsRequest(r, 1)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.TopicNull || len(req.Topics) != 1 {
		t.Fatalf("got %+v", req)
	}
	if req.Topics[0].Name != "smoke" || len(req.Topics[0].Partitions) != 2 ||
		req.Topics[0].Partitions[0] != 0 || req.Topics[0].Partitions[1] != 2 {
		t.Errorf("partitions: %+v", req.Topics[0])
	}
}

func TestDescribeLogDirsResponseEncode(t *testing.T) {
	resp := &DescribeLogDirsResponse{
		Results: []DescribeLogDirsResult{{
			LogDir: "/data",
			Topics: []DescribeLogDirsResponseTopic{{
				Name: "smoke",
				Partitions: []DescribeLogDirsResponsePartition{
					{PartitionIndex: 0, PartitionSize: 1024},
				},
			}},
		}},
	}
	for _, v := range []int16{0, 1} {
		w := codec.NewWriter()
		EncodeDescribeLogDirsResponse(w, resp, v)
		if len(w.Bytes()) == 0 {
			t.Errorf("v%d: empty response", v)
		}
	}
}

// ---- DescribeGroups ----

func TestDescribeGroupsRoundTripV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(2, func() {
		w.WriteString("group-1")
		w.WriteString("group-2")
	})
	r := codec.NewReader(w.Bytes())
	req, err := DecodeDescribeGroupsRequest(r, 0)
	if err != nil || len(req.Groups) != 2 {
		t.Fatalf("v0: %v", err)
	}

	resp := &DescribeGroupsResponse{
		Groups: []DescribedGroup{{GroupID: "group-1", GroupState: "Stable"}},
	}
	w = codec.NewWriter()
	EncodeDescribeGroupsResponse(w, resp, 0)
	if len(w.Bytes()) == 0 {
		t.Error("empty response")
	}
}

// Regression: members with nil MemberMetadata/MemberAssignment must be encoded
// as zero-length bytes, NOT as the -1 (null) sentinel. The Java client throws
// "non-nullable field memberMetadata was serialized as null" on a null read
// and the AdminClient thread dies (observed against kafbat-ui v1.5.0).
func TestDescribeGroupsEmptyMemberBytesAreNonNull(t *testing.T) {
	resp := &DescribeGroupsResponse{
		Groups: []DescribedGroup{{
			GroupID:    "g",
			GroupState: "Stable",
			Members: []DescribedGroupMember{{
				MemberID:   "m",
				ClientID:   "c",
				ClientHost: "h",
				// MemberMetadata, MemberAssignment intentionally nil.
			}},
		}},
	}
	for _, v := range []int16{0, 1, 2, 3, 4} {
		w := codec.NewWriter()
		EncodeDescribeGroupsResponse(w, resp, v)
		// Look for the int32(-1) sentinel anywhere in the bytes; it would
		// indicate a null write where spec demands non-nullable BYTES.
		// (-1 = 0xFFFFFFFF; we check for that 4-byte pattern.)
		buf := w.Bytes()
		for i := 0; i+4 <= len(buf); i++ {
			if buf[i] == 0xFF && buf[i+1] == 0xFF && buf[i+2] == 0xFF && buf[i+3] == 0xFF {
				t.Errorf("v%d: response contains 0xFFFFFFFF (null length) at offset %d — non-nullable bytes written as null", v, i)
				break
			}
		}
	}
}

// ---- ListGroups ----

func TestListGroupsRoundTripV0(t *testing.T) {
	r := codec.NewReader([]byte{})
	req, err := DecodeListGroupsRequest(r, 0)
	if err != nil || len(req.StatesFilter) != 0 {
		t.Fatalf("v0: %v", err)
	}

	resp := &ListGroupsResponse{
		ErrorCode: 0,
		Groups:    []ListedGroup{{GroupID: "g1", ProtocolType: "consumer"}},
	}
	w := codec.NewWriter()
	EncodeListGroupsResponse(w, resp, 0)
	if len(w.Bytes()) == 0 {
		t.Error("empty response")
	}
}

// ---- CreateTopics ----

func TestCreateTopicsRoundTripV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteString("new-topic")
		w.WriteInt32(3)  // num_partitions
		w.WriteInt16(1)  // replication_factor
		w.WriteArray(0, func() {}) // assignments
		w.WriteArray(1, func() {
			w.WriteString("retention.ms")
			w.WriteNullableString("604800000", false)
		})
	})
	w.WriteInt32(30000) // timeout_ms

	r := codec.NewReader(w.Bytes())
	req, err := DecodeCreateTopicsRequest(r, 0)
	if err != nil {
		t.Fatalf("v0 decode: %v", err)
	}
	if len(req.Topics) != 1 || req.Topics[0].Name != "new-topic" || req.Topics[0].NumPartitions != 3 {
		t.Errorf("topic: %+v", req.Topics)
	}
}

func TestCreateTopicsResponseVersions(t *testing.T) {
	resp := &CreateTopicsResponse{
		Topics: []CreatableTopicResult{{Name: "new-topic", ErrorCode: 0}},
	}
	for _, v := range []int16{0, 2, 5} {
		w := codec.NewWriter()
		EncodeCreateTopicsResponse(w, resp, v)
		if len(w.Bytes()) == 0 {
			t.Errorf("v%d: empty", v)
		}
	}
}

// ---- DeleteTopics ----

func TestDeleteTopicsRoundTripV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(1, func() { w.WriteString("old-topic") })
	w.WriteInt32(5000)

	r := codec.NewReader(w.Bytes())
	req, err := DecodeDeleteTopicsRequest(r, 0)
	if err != nil || len(req.TopicNames) != 1 || req.TopicNames[0] != "old-topic" {
		t.Fatalf("v0: %v", err)
	}
}

func TestDeleteTopicsResponseVersions(t *testing.T) {
	resp := &DeleteTopicsResponse{
		Responses: []DeletableTopicResult{{Name: "old-topic", ErrorCode: 0}},
	}
	for _, v := range []int16{0, 1, 4} {
		w := codec.NewWriter()
		EncodeDeleteTopicsResponse(w, resp, v)
		if len(w.Bytes()) == 0 {
			t.Errorf("v%d: empty", v)
		}
	}
}

// ---- ACLs ----

func TestDescribeAclsRoundTripV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt8(2) // resource_type = topic
	w.WriteNullableString("my-topic", false)
	w.WriteNullableString("User:alice", false)
	w.WriteNullableString("*", false)
	w.WriteInt8(3) // operation = write
	w.WriteInt8(1) // permission = allow

	r := codec.NewReader(w.Bytes())
	req, err := DecodeDescribeAclsRequest(r, 0)
	if err != nil || req.ResourceTypeFilter != 2 {
		t.Fatalf("v0: %v", err)
	}

	resp := &DescribeAclsResponse{ErrorCode: 0}
	codec.NewWriter()
	w2 := codec.NewWriter()
	EncodeDescribeAclsResponse(w2, resp, 0)
	if len(w2.Bytes()) == 0 {
		t.Error("empty response")
	}
}

func TestCreateAclsRoundTripV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteInt8(2) // resource_type
		w.WriteString("my-topic")
		w.WriteString("User:alice")
		w.WriteString("*")
		w.WriteInt8(3) // operation
		w.WriteInt8(1) // permission
	})
	r := codec.NewReader(w.Bytes())
	req, err := DecodeCreateAclsRequest(r, 0)
	if err != nil || len(req.Creations) != 1 {
		t.Fatalf("v0: %v", err)
	}
}

func TestDeleteAclsRoundTripV0(t *testing.T) {
	w := codec.NewWriter()
	w.WriteArray(1, func() {
		w.WriteInt8(2)
		w.WriteNullableString("my-topic", false)
		w.WriteNullableString("User:alice", false)
		w.WriteNullableString("*", false)
		w.WriteInt8(3)
		w.WriteInt8(1)
	})
	r := codec.NewReader(w.Bytes())
	req, err := DecodeDeleteAclsRequest(r, 0)
	if err != nil || len(req.Filters) != 1 {
		t.Fatalf("v0: %v", err)
	}
}
