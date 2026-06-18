package handlers

import (
	"fmt"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// ControllerIDProvider returns the broker NodeID currently holding
// the cluster-controller lease, or -1 when no controller is known
// (boot window, lease just released). Optional: when a handler is
// constructed without one, ControllerID falls back to Self().NodeID
// for parity with the existing Metadata response (gh #85).
type ControllerIDProvider interface {
	ControllerID() int32
}

// DescribeClusterHandler serves DescribeCluster (API key 60).
// Surfaces what AdminClient.describeCluster() needs:
// cluster id, controller id, broker list, and (optionally) the
// caller's authorized-operations bitmap.
type DescribeClusterHandler struct {
	brokers    BrokerSource
	clusterID  string
	controller ControllerIDProvider
}

func NewDescribeClusterHandler(brokers BrokerSource, clusterID string) *DescribeClusterHandler {
	return &DescribeClusterHandler{brokers: brokers, clusterID: clusterID}
}

// WithController wires a ControllerIDProvider. The cluster runtime
// passes the broker.Coordinator here once it's up; before that the
// handler falls back to Self().NodeID (every broker reports itself
// as controller) — the same fallback Metadata uses, and good enough
// for AdminClient.describeCluster() which only needs the field
// populated, not strictly accurate during boot.
func (h *DescribeClusterHandler) WithController(c ControllerIDProvider) *DescribeClusterHandler {
	h.controller = c
	return h
}

func (h *DescribeClusterHandler) Handle(conn *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	req, err := api.DecodeDescribeClusterRequest(r, version)
	if err != nil {
		return nil, fmt.Errorf("describe-cluster decode: %w", err)
	}

	listener := connstate.ListenerName("internal")
	if conn != nil && conn.Listener != "" {
		listener = conn.Listener
	}

	resp := &api.DescribeClusterResponse{
		EndpointType: 1, // brokers — the only type skafka serves
		ClusterID:    h.clusterID,
		ControllerID: h.controllerID(),
		// Apache default: Int32.MIN_VALUE signals "field not populated".
		// We don't compute per-caller authorized ops yet, so use the
		// sentinel rather than reporting an empty bitmap (which the
		// Java client would treat as "no operations allowed").
		ClusterAuthorizedOperations: -2147483648,
	}

	// EndpointType=2 (controllers) is the KRaft controller-quorum
	// endpoint. skafka uses K8s Leases for controller election (no
	// metadata quorum); reject explicitly so KRaft-aware clients see
	// a clean error rather than an empty broker list and assume the
	// cluster is dead.
	if req.EndpointType != 0 && req.EndpointType != 1 {
		resp.ErrorCode = int16(codec.ErrUnsupportedVersion)
		resp.ErrorMessage = "skafka does not expose a KRaft controller endpoint"
		w := codec.NewWriter()
		api.EncodeDescribeClusterResponse(w, resp, version)
		return w.Bytes(), nil
	}

	for _, b := range h.brokers.All() {
		host, port := b.addressFor(listener)
		resp.Brokers = append(resp.Brokers, api.DescribeClusterBroker{
			BrokerID: b.NodeID,
			Host:     host,
			Port:     port,
		})
	}

	w := codec.NewWriter()
	api.EncodeDescribeClusterResponse(w, resp, version)
	return w.Bytes(), nil
}

func (h *DescribeClusterHandler) controllerID() int32 {
	if h.controller != nil {
		if id := h.controller.ControllerID(); id >= 0 {
			return id
		}
	}
	return h.brokers.Self().NodeID
}
