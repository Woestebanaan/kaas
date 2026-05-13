package handlers

import (
	"fmt"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/coordinator"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// ---- FindCoordinator ----

type FindCoordinatorHandler struct {
	coord *coordinator.Manager
}

func NewFindCoordinatorHandler(coord *coordinator.Manager) *FindCoordinatorHandler {
	return &FindCoordinatorHandler{coord: coord}
}

func NewFindCoordinatorHandlerStub() *FindCoordinatorHandler {
	return &FindCoordinatorHandler{}
}

func (h *FindCoordinatorHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeFindCoordinatorRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("find coordinator decode: %w", err)
	}

	var resp *api.FindCoordinatorResponse
	if h.coord != nil {
		resp = h.coord.FindCoordinator(req)
	} else {
		resp = &api.FindCoordinatorResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable)}
	}

	w := codec.NewWriter()
	api.EncodeFindCoordinatorResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- JoinGroup ----

type JoinGroupHandler struct {
	coord *coordinator.Manager
}

func NewJoinGroupHandler(coord *coordinator.Manager) *JoinGroupHandler {
	return &JoinGroupHandler{coord: coord}
}

func (h *JoinGroupHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeJoinGroupRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("join group decode: %w", err)
	}

	var resp *api.JoinGroupResponse
	if h.coord != nil {
		clientID := ""
		if conn != nil {
			clientID = conn.ClientID
		}
		resp = h.coord.JoinGroup(req, version, clientID)
	} else {
		resp = &api.JoinGroupResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable), GenerationID: -1}
	}

	w := codec.NewWriter()
	api.EncodeJoinGroupResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- SyncGroup ----

type SyncGroupHandler struct {
	coord *coordinator.Manager
}

func NewSyncGroupHandler(coord *coordinator.Manager) *SyncGroupHandler {
	return &SyncGroupHandler{coord: coord}
}

func (h *SyncGroupHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeSyncGroupRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("sync group decode: %w", err)
	}

	var resp *api.SyncGroupResponse
	if h.coord != nil {
		resp = h.coord.SyncGroup(req)
	} else {
		resp = &api.SyncGroupResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable)}
	}

	w := codec.NewWriter()
	api.EncodeSyncGroupResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- Heartbeat ----

type HeartbeatHandler struct {
	coord *coordinator.Manager
}

func NewHeartbeatHandler(coord *coordinator.Manager) *HeartbeatHandler {
	return &HeartbeatHandler{coord: coord}
}

func (h *HeartbeatHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeHeartbeatRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("heartbeat decode: %w", err)
	}

	var resp *api.HeartbeatResponse
	if h.coord != nil {
		resp = h.coord.Heartbeat(req)
	} else {
		resp = &api.HeartbeatResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable)}
	}

	w := codec.NewWriter()
	api.EncodeHeartbeatResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- LeaveGroup ----

type LeaveGroupHandler struct {
	coord *coordinator.Manager
}

func NewLeaveGroupHandler(coord *coordinator.Manager) *LeaveGroupHandler {
	return &LeaveGroupHandler{coord: coord}
}

func (h *LeaveGroupHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeLeaveGroupRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("leave group decode: %w", err)
	}

	var resp *api.LeaveGroupResponse
	if h.coord != nil {
		resp = h.coord.LeaveGroup(req)
	} else {
		resp = &api.LeaveGroupResponse{ErrorCode: int16(codec.ErrCoordinatorNotAvailable)}
	}

	w := codec.NewWriter()
	api.EncodeLeaveGroupResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- OffsetCommit ----

type OffsetCommitHandler struct {
	coord *coordinator.Manager
}

func NewOffsetCommitHandler(coord *coordinator.Manager) *OffsetCommitHandler {
	return &OffsetCommitHandler{coord: coord}
}

func (h *OffsetCommitHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeOffsetCommitRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("offset commit decode: %w", err)
	}

	var resp *api.OffsetCommitResponse
	if h.coord != nil {
		resp = h.coord.OffsetCommit(req)
	} else {
		resp = buildOffsetCommitError(req, int16(codec.ErrCoordinatorNotAvailable))
	}

	w := codec.NewWriter()
	api.EncodeOffsetCommitResponse(w, resp, version)
	return w.Bytes(), nil
}

func buildOffsetCommitError(req *api.OffsetCommitRequest, errCode int16) *api.OffsetCommitResponse {
	resp := &api.OffsetCommitResponse{}
	for _, t := range req.Topics {
		tr := api.OffsetCommitTopicResponse{Name: t.Name}
		for _, p := range t.Partitions {
			tr.Partitions = append(tr.Partitions, api.OffsetCommitPartitionResponse{
				PartitionIndex: p.PartitionIndex, ErrorCode: errCode,
			})
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}

// ---- OffsetFetch ----

type OffsetFetchHandler struct {
	coord *coordinator.Manager
}

func NewOffsetFetchHandler(coord *coordinator.Manager) *OffsetFetchHandler {
	return &OffsetFetchHandler{coord: coord}
}

func (h *OffsetFetchHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeOffsetFetchRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("offset fetch decode: %w", err)
	}

	var resp *api.OffsetFetchResponse
	if h.coord != nil {
		resp = h.coord.OffsetFetch(req)
	} else {
		resp = &api.OffsetFetchResponse{}
	}

	w := codec.NewWriter()
	api.EncodeOffsetFetchResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- DescribeGroups ----

type DescribeGroupsHandler struct {
	coord *coordinator.Manager
}

func NewDescribeGroupsHandler(coord *coordinator.Manager) *DescribeGroupsHandler {
	return &DescribeGroupsHandler{coord: coord}
}

func (h *DescribeGroupsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDescribeGroupsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("describe groups decode: %w", err)
	}

	var resp *api.DescribeGroupsResponse
	if h.coord != nil {
		resp = h.coord.DescribeGroups(req)
	} else {
		resp = &api.DescribeGroupsResponse{}
	}

	w := codec.NewWriter()
	api.EncodeDescribeGroupsResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- ListGroups ----

type ListGroupsHandler struct {
	coord *coordinator.Manager
}

func NewListGroupsHandler(coord *coordinator.Manager) *ListGroupsHandler {
	return &ListGroupsHandler{coord: coord}
}

func (h *ListGroupsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeListGroupsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("list groups decode: %w", err)
	}

	var resp *api.ListGroupsResponse
	if h.coord != nil {
		resp = h.coord.ListGroups(req)
	} else {
		resp = &api.ListGroupsResponse{}
	}

	w := codec.NewWriter()
	api.EncodeListGroupsResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- DeleteGroups (gh #89) ----

type DeleteGroupsHandler struct {
	coord      *coordinator.Manager
	authorizer auth.Authorizer // gh #126: cluster-wide
}

func NewDeleteGroupsHandler(coord *coordinator.Manager, authorizer auth.Authorizer) *DeleteGroupsHandler {
	return &DeleteGroupsHandler{coord: coord, authorizer: authorizer}
}

func (h *DeleteGroupsHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDeleteGroupsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("delete groups decode: %w", err)
	}

	resp := &api.DeleteGroupsResponse{}

	// Per-group ACL gate. Producers/consumers normally hold Read on a
	// group; deleting requires Delete. AdminClient.deleteConsumerGroups
	// is the typical caller and runs under an operator principal.
	if h.authorizer != nil {
		principal := principalFrom(conn)
		var allowed []string
		for _, gid := range req.GroupNames {
			if !h.authorizer.Authorize(principal, auth.Resource{Type: "group", Name: gid, PatternType: "literal"}, auth.OpDelete) {
				resp.Results = append(resp.Results, api.DeleteGroupsResult{
					GroupID:   gid,
					ErrorCode: int16(codec.ErrGroupAuthorizationFailed),
				})
				continue
			}
			allowed = append(allowed, gid)
		}
		// Replace request groups with the allowed-only subset; the
		// coordinator only sees what passed the ACL check.
		req.GroupNames = allowed
	}

	if h.coord != nil && len(req.GroupNames) > 0 {
		coordResp := h.coord.DeleteGroups(req)
		resp.Results = append(resp.Results, coordResp.Results...)
	} else if h.coord == nil {
		// No coordinator wired (e.g. local-dev / pre-init). All
		// groups get COORDINATOR_NOT_AVAILABLE so the client retries.
		for _, gid := range req.GroupNames {
			resp.Results = append(resp.Results, api.DeleteGroupsResult{
				GroupID:   gid,
				ErrorCode: int16(codec.ErrCoordinatorNotAvailable),
			})
		}
	}

	w := codec.NewWriter()
	api.EncodeDeleteGroupsResponse(w, resp, version)
	return w.Bytes(), nil
}
