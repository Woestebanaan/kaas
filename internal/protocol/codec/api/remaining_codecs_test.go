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

// TestFindCoordinatorRequestV3 pins gh #91 PR 3's codec fix: at v3
// the wire shape is single Key + KeyType + flexible tagged fields
// (Apache schema: Key has versions "0-3"). Before the fix the
// decoder treated v3 like v4 and tried to read a CompactArray —
// which would fail to parse a real franz-go / Java client v3 frame.
// The compat suite never hit this because clients negotiate v4 max,
// but pinning a v3 round-trip catches a silent regression that
// would only surface against a v3-pinned client.
func TestFindCoordinatorRequestV3(t *testing.T) {
	w := codec.NewWriter()
	w.WriteCompactString("my-txn")
	w.WriteInt8(1) // KeyType = transaction
	w.WriteEmptyTaggedFields()
	r := codec.NewReader(w.Bytes())
	req, err := DecodeFindCoordinatorRequest(r, 3)
	if err != nil {
		t.Fatalf("decode v3: %v", err)
	}
	if req.Key != "my-txn" {
		t.Errorf("v3 Key=%q, want %q", req.Key, "my-txn")
	}
	if req.KeyType != 1 {
		t.Errorf("v3 KeyType=%d, want 1", req.KeyType)
	}
	if len(req.CoordinatorKeys) != 0 {
		t.Errorf("v3 unexpected CoordinatorKeys=%v (array form is v4+)", req.CoordinatorKeys)
	}
}

// TestFindCoordinatorRequestV4 pins the v4 array form: KeyType
// followed by a CompactArray of keys (no single Key field).
func TestFindCoordinatorRequestV4(t *testing.T) {
	w := codec.NewWriter()
	w.WriteInt8(0) // KeyType = group
	w.WriteCompactArray(2, func() {
		w.WriteCompactString("group-a")
		w.WriteCompactString("group-b")
	})
	w.WriteEmptyTaggedFields()
	r := codec.NewReader(w.Bytes())
	req, err := DecodeFindCoordinatorRequest(r, 4)
	if err != nil {
		t.Fatalf("decode v4: %v", err)
	}
	if req.Key != "" {
		t.Errorf("v4 Key=%q, want empty (v4 uses CoordinatorKeys)", req.Key)
	}
	if got := req.CoordinatorKeys; len(got) != 2 || got[0] != "group-a" || got[1] != "group-b" {
		t.Errorf("v4 CoordinatorKeys=%v, want [group-a group-b]", got)
	}
}

// TestFindCoordinatorResponseV3 round-trips the v3 wire format:
// single coord (NodeID/Host/Port) wrapped in flexible tagged
// fields. Encoding the array form here would be the pre-fix bug.
func TestFindCoordinatorResponseV3(t *testing.T) {
	resp := &FindCoordinatorResponse{
		ThrottleTimeMs: 0,
		NodeID:         0,
		Host:           "broker-0",
		Port:           9092,
	}
	w := codec.NewWriter()
	EncodeFindCoordinatorResponse(w, resp, 3)
	r := codec.NewReader(w.Bytes())
	if _, err := r.ReadInt32(); err != nil { // throttle
		t.Fatal(err)
	}
	if errCode, err := r.ReadInt16(); err != nil || errCode != 0 {
		t.Fatalf("v3 errCode=%d err=%v", errCode, err)
	}
	if msg, _, err := r.ReadCompactNullableString(); err != nil || msg != "" {
		t.Fatalf("v3 errMsg=%q err=%v", msg, err)
	}
	if nodeID, err := r.ReadInt32(); err != nil || nodeID != 0 {
		t.Fatalf("v3 nodeID=%d err=%v", nodeID, err)
	}
	if host, err := r.ReadCompactString(); err != nil || host != "broker-0" {
		t.Fatalf("v3 host=%q err=%v", host, err)
	}
	if port, err := r.ReadInt32(); err != nil || port != 9092 {
		t.Fatalf("v3 port=%d err=%v", port, err)
	}
	if err := r.ReadTaggedFields(); err != nil {
		t.Fatalf("v3 tagged fields: %v", err)
	}
}

// TestFindCoordinatorResponseV4 round-trips the v4 array form.
func TestFindCoordinatorResponseV4(t *testing.T) {
	resp := &FindCoordinatorResponse{
		Coordinators: []CoordinatorResult{
			{Key: "my-group", NodeID: 0, Host: "broker-0", Port: 9092},
		},
	}
	w := codec.NewWriter()
	EncodeFindCoordinatorResponse(w, resp, 4)
	if len(w.Bytes()) == 0 {
		t.Error("v4 empty response")
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

// TestSyncGroupResponseAssignmentNonNullableV3 pins the gh #96
// fix: Apache Kafka's schema declares SyncGroupResponse.Assignment
// non-nullable at every version. Pre-fix skafka encoded it via
// WriteNullableBytes, so a nil assignment (e.g. in error responses
// or transient handler paths) emitted int32(-1) on the wire and
// the Java client's generated decoder threw
// `RuntimeException: non-nullable field assignment was serialized
// as null`. After the fix, a nil assignment must encode as
// int32(0) — empty bytes, never null marker.
func TestSyncGroupResponseAssignmentNonNullableV3(t *testing.T) {
	resp := &SyncGroupResponse{ErrorCode: 27, Assignment: nil} // RebalanceInProgress
	w := codec.NewWriter()
	EncodeSyncGroupResponse(w, resp, 3)

	// v3 wire shape: ThrottleTimeMs(int32) | ErrorCode(int16) | Assignment(int32 len + bytes)
	// throttle 0, errorCode 27, assignment length 0 → 4+2+4 = 10 bytes total.
	got := w.Bytes()
	if len(got) != 10 {
		t.Fatalf("len(got)=%d, want 10. raw=%x", len(got), got)
	}
	// Last 4 bytes are the assignment length prefix.
	prefix := got[len(got)-4:]
	if prefix[0] == 0xFF && prefix[1] == 0xFF && prefix[2] == 0xFF && prefix[3] == 0xFF {
		t.Fatalf("assignment prefix is int32(-1) — null marker, the gh #96 bug. raw=%x", got)
	}
	if prefix[0] != 0 || prefix[1] != 0 || prefix[2] != 0 || prefix[3] != 0 {
		t.Errorf("assignment prefix=%x, want 00000000 (length 0). raw=%x", prefix, got)
	}
}

// TestSyncGroupResponseAssignmentNonNullableV4Flexible covers the
// flexible-version wire shape (v4+). Compact-bytes uses varint
// (length+1); empty bytes is varint(1) = 0x01. Null is varint(0)
// = 0x00 — what the pre-fix code emitted via
// WriteCompactNullableBytes(nil), tripping the same Java-client
// non-nullable decode.
func TestSyncGroupResponseAssignmentNonNullableV4Flexible(t *testing.T) {
	resp := &SyncGroupResponse{ErrorCode: 27, Assignment: nil}
	w := codec.NewWriter()
	EncodeSyncGroupResponse(w, resp, 4)

	got := w.Bytes()
	// v4 wire (no v5 fields): ThrottleTimeMs(int32) | ErrorCode(int16) |
	// Assignment(varint(len+1)) | empty tagged fields (varint 0)
	// → 4 + 2 + 1 + 1 = 8 bytes for empty assignment.
	if len(got) != 8 {
		t.Fatalf("len(got)=%d, want 8. raw=%x", len(got), got)
	}
	// Byte at offset 6 is the assignment varint prefix.
	if got[6] == 0x00 {
		t.Fatalf("assignment varint is 0 — null marker, the gh #96 bug. raw=%x", got)
	}
	if got[6] != 0x01 {
		t.Errorf("assignment varint=%x, want 0x01 (compact len 0). raw=%x", got[6], got)
	}
}

// TestSyncGroupResponseProtocolFieldsNonNullableV5 pins the gh #98 #6
// fix: SyncGroupResponse v5+ added ProtocolType + ProtocolName.
// Pre-fix skafka encoded them via WriteCompactNullableString(s, s=="")
// which collapsed empty-string into the wire null sentinel
// (varint 0). For wire-format clarity (and to finish the gh #96
// cleanup of conflating empty with null), encode non-nullable so
// "" produces varint(1) + 0 bytes — empty string, not null.
//
// Apache's Java client decodes either form, but a strict-schema
// client (e.g., librdkafka with paranoid validation) now sees what
// skafka actually means.
func TestSyncGroupResponseProtocolFieldsNonNullableV5(t *testing.T) {
	resp := &SyncGroupResponse{
		ErrorCode:    0,
		ProtocolType: "",
		ProtocolName: "",
		Assignment:   []byte{},
	}
	w := codec.NewWriter()
	EncodeSyncGroupResponse(w, resp, 5)

	got := w.Bytes()
	// v5 wire (flexible since v4+):
	//   ThrottleTimeMs(int32) = 00 00 00 00
	//   ErrorCode(int16)      = 00 00
	//   ProtocolType(varint)  = 01 (compact len=0)
	//   ProtocolName(varint)  = 01
	//   Assignment(varint)    = 01
	//   tagged fields(varint) = 00
	// → 4 + 2 + 1 + 1 + 1 + 1 = 10 bytes.
	if len(got) != 10 {
		t.Fatalf("len(got)=%d, want 10. raw=%x", len(got), got)
	}
	// Bytes at offsets 6 and 7 are the ProtocolType / ProtocolName
	// varints. Both must be 0x01 (length 0 = empty), NOT 0x00 (null).
	if got[6] == 0x00 {
		t.Fatalf("ProtocolType varint is 0 — null marker, the gh #98 #6 bug. raw=%x", got)
	}
	if got[7] == 0x00 {
		t.Fatalf("ProtocolName varint is 0 — null marker, the gh #98 #6 bug. raw=%x", got)
	}
	if got[6] != 0x01 || got[7] != 0x01 {
		t.Errorf("ProtocolType/Name varints=%x %x, want 0x01 0x01 (compact len 0). raw=%x",
			got[6], got[7], got)
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
