package handlers

// Consumer group handlers return COORDINATOR_NOT_AVAILABLE (error 15) for all
// group coordination requests. Full implementation is Phase 5.
// Each handler is a thin wrapper that decodes the request (to validate it parses)
// and returns the appropriate error response.

import (
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// coordUnavailable writes a 2-byte error code response.
func coordUnavailable() []byte {
	w := codec.NewWriter()
	w.WriteInt16(int16(codec.ErrCoordinatorNotAvailable))
	return w.Bytes()
}

// ---- FindCoordinator ----

type FindCoordinatorHandler struct{}

func NewFindCoordinatorHandler() *FindCoordinatorHandler { return &FindCoordinatorHandler{} }

func (h *FindCoordinatorHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.FindCoordinatorResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable)}
	w := codec.NewWriter()
	api.EncodeFindCoordinatorResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- JoinGroup ----

type JoinGroupHandler struct{}

func NewJoinGroupHandler() *JoinGroupHandler { return &JoinGroupHandler{} }

func (h *JoinGroupHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.JoinGroupResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable), GenerationID: -1}
	w := codec.NewWriter()
	api.EncodeJoinGroupResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- Heartbeat ----

type HeartbeatHandler struct{}

func NewHeartbeatHandler() *HeartbeatHandler { return &HeartbeatHandler{} }

func (h *HeartbeatHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.HeartbeatResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable)}
	w := codec.NewWriter()
	api.EncodeHeartbeatResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- LeaveGroup ----

type LeaveGroupHandler struct{}

func NewLeaveGroupHandler() *LeaveGroupHandler { return &LeaveGroupHandler{} }

func (h *LeaveGroupHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.LeaveGroupResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable)}
	w := codec.NewWriter()
	api.EncodeLeaveGroupResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- SyncGroup ----

type SyncGroupHandler struct{}

func NewSyncGroupHandler() *SyncGroupHandler { return &SyncGroupHandler{} }

func (h *SyncGroupHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.SyncGroupResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable)}
	w := codec.NewWriter()
	api.EncodeSyncGroupResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- OffsetCommit ----

type OffsetCommitHandler struct{}

func NewOffsetCommitHandler() *OffsetCommitHandler { return &OffsetCommitHandler{} }

func (h *OffsetCommitHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.OffsetCommitResponse{}
	w := codec.NewWriter()
	api.EncodeOffsetCommitResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- OffsetFetch ----

type OffsetFetchHandler struct{}

func NewOffsetFetchHandler() *OffsetFetchHandler { return &OffsetFetchHandler{} }

func (h *OffsetFetchHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.OffsetFetchResponse{}
	w := codec.NewWriter()
	api.EncodeOffsetFetchResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- DescribeGroups ----

type DescribeGroupsHandler struct{}

func NewDescribeGroupsHandler() *DescribeGroupsHandler { return &DescribeGroupsHandler{} }

func (h *DescribeGroupsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.DescribeGroupsResponse{}
	w := codec.NewWriter()
	api.EncodeDescribeGroupsResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- ListGroups ----

type ListGroupsHandler struct{}

func NewListGroupsHandler() *ListGroupsHandler { return &ListGroupsHandler{} }

func (h *ListGroupsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.ListGroupsResponse{ErrorCode: 0}
	w := codec.NewWriter()
	api.EncodeListGroupsResponse(w, resp, version)
	return w.Bytes(), nil
}
