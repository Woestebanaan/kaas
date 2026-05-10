package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// DescribeClusterRequest (API key 60, v0–v1). All versions are
// flexible (flexibleVersions=0+). Apache schema fields:
//
//	IncludeClusterAuthorizedOperations: "0+", bool
//	EndpointType:                       "1+", int8 (1=brokers, 2=controllers)
//
// skafka serves only EndpointType=1 (brokers); type=2 is the KRaft
// controller-quorum endpoint and is a non-goal.
type DescribeClusterRequest struct {
	IncludeClusterAuthorizedOperations bool
	EndpointType                       int8 // v1+, default 1=brokers
}

// DescribeClusterBroker is one broker entry in the response.
type DescribeClusterBroker struct {
	BrokerID int32
	Host     string
	Port     int32
	Rack     string // nullable
}

// DescribeClusterResponse (API key 60, v0–v1). Apache schema:
//
//	ThrottleTimeMs:               "0+"
//	ErrorCode:                    "0+"
//	ErrorMessage:                 "0+", nullable
//	EndpointType:                 "1+", default 1
//	ClusterId:                    "0+"
//	ControllerId:                 "0+", default -1
//	Brokers:                      "0+" (array)
//	ClusterAuthorizedOperations:  "0+", default Int32.MIN_VALUE (signals "not requested")
type DescribeClusterResponse struct {
	ThrottleTimeMs              int32
	ErrorCode                   int16
	ErrorMessage                string
	EndpointType                int8 // v1+
	ClusterID                   string
	ControllerID                int32
	Brokers                     []DescribeClusterBroker
	ClusterAuthorizedOperations int32
}

func DecodeDescribeClusterRequest(r *codec.Reader, version int16) (*DescribeClusterRequest, error) {
	req := &DescribeClusterRequest{EndpointType: 1}
	flag, err := r.ReadInt8()
	if err != nil {
		return nil, err
	}
	req.IncludeClusterAuthorizedOperations = flag != 0
	if version >= 1 {
		et, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.EndpointType = et
	}
	if err := r.ReadTaggedFields(); err != nil {
		return nil, err
	}
	return req, nil
}

func EncodeDescribeClusterResponse(w *codec.Writer, resp *DescribeClusterResponse, version int16) {
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteInt16(resp.ErrorCode)
	w.WriteCompactNullableString(resp.ErrorMessage, resp.ErrorMessage == "")
	if version >= 1 {
		w.WriteInt8(resp.EndpointType)
	}
	w.WriteCompactString(resp.ClusterID)
	w.WriteInt32(resp.ControllerID)
	w.WriteCompactArray(len(resp.Brokers), func() {
		for _, b := range resp.Brokers {
			w.WriteInt32(b.BrokerID)
			w.WriteCompactString(b.Host)
			w.WriteInt32(b.Port)
			w.WriteCompactNullableString(b.Rack, b.Rack == "")
			w.WriteEmptyTaggedFields()
		}
	})
	w.WriteInt32(resp.ClusterAuthorizedOperations)
	w.WriteEmptyTaggedFields()
}
