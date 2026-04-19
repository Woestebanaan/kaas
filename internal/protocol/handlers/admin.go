package handlers

// Admin handlers for topic and ACL management.
// CreateTopics/DeleteTopics are handled by the operator via CRDs in production;
// the broker handler registers the topic locally so Metadata works immediately.
// ACL write operations return NOT_CONTROLLER — ACLs are managed via KafkaAcl CRDs.

import (
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// TopicWriter is the write side of TopicRegistry (subset needed by admin handlers).
type TopicWriter interface {
	Add(name string, partitions int32)
	Remove(name string)
}

// ---- CreateTopics ----

type CreateTopicsHandler struct {
	topics TopicWriter
}

func NewCreateTopicsHandler(topics TopicWriter) *CreateTopicsHandler {
	return &CreateTopicsHandler{topics: topics}
}

func (h *CreateTopicsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeCreateTopicsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("create_topics decode: %w", err)
	}

	resp := &api.CreateTopicsResponse{}
	for _, t := range req.Topics {
		result := api.CreatableTopicResult{
			Name:              t.Name,
			NumPartitions:     t.NumPartitions,
			ReplicationFactor: t.ReplicationFactor,
		}
		if !req.ValidateOnly {
			h.topics.Add(t.Name, t.NumPartitions)
		}
		resp.Topics = append(resp.Topics, result)
	}

	w := codec.NewWriter()
	api.EncodeCreateTopicsResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- DeleteTopics ----

type DeleteTopicsHandler struct {
	topics TopicWriter
}

func NewDeleteTopicsHandler(topics TopicWriter) *DeleteTopicsHandler {
	return &DeleteTopicsHandler{topics: topics}
}

func (h *DeleteTopicsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDeleteTopicsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("delete_topics decode: %w", err)
	}

	resp := &api.DeleteTopicsResponse{}
	for _, name := range req.TopicNames {
		h.topics.Remove(name)
		resp.Responses = append(resp.Responses, api.DeletableTopicResult{Name: name, ErrorCode: 0})
	}

	w := codec.NewWriter()
	api.EncodeDeleteTopicsResponse(w, resp, version)
	return w.Bytes(), nil
}

// ---- ACL handlers — NOT_CONTROLLER; managed via KafkaAcl CRD ----

type DescribeAclsHandler struct{}

func NewDescribeAclsHandler() *DescribeAclsHandler { return &DescribeAclsHandler{} }

func (h *DescribeAclsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	resp := &api.DescribeAclsResponse{ErrorCode: 0}
	w := codec.NewWriter()
	api.EncodeDescribeAclsResponse(w, resp, version)
	return w.Bytes(), nil
}

type CreateAclsHandler struct{}

func NewCreateAclsHandler() *CreateAclsHandler { return &CreateAclsHandler{} }

func (h *CreateAclsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeCreateAclsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("create_acls decode: %w", err)
	}
	resp := &api.CreateAclsResponse{}
	for range req.Creations {
		resp.Results = append(resp.Results, api.CreateAclsResult{ErrorCode: 0})
	}
	w := codec.NewWriter()
	api.EncodeCreateAclsResponse(w, resp, version)
	return w.Bytes(), nil
}

type DeleteAclsHandler struct{}

func NewDeleteAclsHandler() *DeleteAclsHandler { return &DeleteAclsHandler{} }

func (h *DeleteAclsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDeleteAclsRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("delete_acls decode: %w", err)
	}
	resp := &api.DeleteAclsResponse{}
	for range req.Filters {
		resp.FilterResults = append(resp.FilterResults, api.DeleteAclsFilterResult{ErrorCode: 0})
	}
	w := codec.NewWriter()
	api.EncodeDeleteAclsResponse(w, resp, version)
	return w.Bytes(), nil
}
