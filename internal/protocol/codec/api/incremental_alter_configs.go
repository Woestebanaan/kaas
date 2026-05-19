package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// IncrementalAlterConfigsRequest (API key 44, v0–v1). KIP-339. The
// modern replacement for AlterConfigs — supports per-key SET / DELETE /
// APPEND / SUBTRACT instead of replacing the entire config block.
// Used by `kafka-configs.sh --alter --add-config / --delete-config`
// and AdminClient.incrementalAlterConfigs().
//
// Apache schema (clients/.../IncrementalAlterConfigsRequest.json):
//   - Resources:        "0+"
//   - ValidateOnly:     "0+"
//   - flexibleVersions: "1+"
type IncrementalAlterConfigsRequest struct {
	Resources    []IncrementalAlterConfigsResource
	ValidateOnly bool
}

type IncrementalAlterConfigsResource struct {
	ResourceType int8 // matches ConfigResource{Topic=2, Broker=4, BrokerLogger=8}
	ResourceName string
	Configs      []IncrementalAlterConfigsConfig
}

// IncrementalAlterConfigsConfig is a single per-key mutation.
// ConfigOperation: 0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT.
type IncrementalAlterConfigsConfig struct {
	Name            string
	ConfigOperation int8
	Value           string // nullable when ConfigOperation == DELETE
}

const (
	IncrementalAlterConfigsOpSet      int8 = 0
	IncrementalAlterConfigsOpDelete   int8 = 1
	IncrementalAlterConfigsOpAppend   int8 = 2
	IncrementalAlterConfigsOpSubtract int8 = 3
)

// IncrementalAlterConfigsResponse (v0–v1).
type IncrementalAlterConfigsResponse struct {
	ThrottleTimeMs int32
	Responses      []IncrementalAlterConfigsResult
}

type IncrementalAlterConfigsResult struct {
	ErrorCode    int16
	ErrorMessage string // nullable
	ResourceType int8
	ResourceName string
}

func DecodeIncrementalAlterConfigsRequest(r *codec.Reader, version int16) (*IncrementalAlterConfigsRequest, error) {
	flexible := version >= 1
	req := &IncrementalAlterConfigsRequest{}

	readResource := func() error {
		var res IncrementalAlterConfigsResource
		var err error
		if res.ResourceType, err = r.ReadInt8(); err != nil {
			return err
		}
		if res.ResourceName, err = readString(r, flexible); err != nil {
			return err
		}
		readCfg := func() error {
			var c IncrementalAlterConfigsConfig
			var err error
			if c.Name, err = readString(r, flexible); err != nil {
				return err
			}
			if c.ConfigOperation, err = r.ReadInt8(); err != nil {
				return err
			}
			v, _, err := nullableString(r, flexible)
			if err != nil {
				return err
			}
			c.Value = v
			if flexible {
				if err := r.ReadTaggedFields(); err != nil {
					return err
				}
			}
			res.Configs = append(res.Configs, c)
			return nil
		}
		if flexible {
			if err := r.ReadCompactArray(readCfg); err != nil {
				return err
			}
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		} else {
			if err := r.ReadArray(readCfg); err != nil {
				return err
			}
		}
		req.Resources = append(req.Resources, res)
		return nil
	}
	var err error
	if flexible {
		err = r.ReadCompactArray(readResource)
	} else {
		err = r.ReadArray(readResource)
	}
	if err != nil {
		return nil, err
	}
	vo, err := r.ReadInt8()
	if err != nil {
		return nil, err
	}
	req.ValidateOnly = vo != 0
	if flexible {
		if err := r.ReadTaggedFields(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func EncodeIncrementalAlterConfigsResponse(w *codec.Writer, resp *IncrementalAlterConfigsResponse, version int16) {
	flexible := version >= 1
	w.WriteInt32(resp.ThrottleTimeMs)
	writeResponses := func() {
		for _, r := range resp.Responses {
			w.WriteInt16(r.ErrorCode)
			if flexible {
				w.WriteCompactNullableString(r.ErrorMessage, r.ErrorMessage == "")
			} else {
				w.WriteNullableString(r.ErrorMessage, r.ErrorMessage == "")
			}
			w.WriteInt8(r.ResourceType)
			writeString(w, r.ResourceName, flexible)
			if flexible {
				w.WriteEmptyTaggedFields()
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Responses), writeResponses)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Responses), writeResponses)
	}
}
