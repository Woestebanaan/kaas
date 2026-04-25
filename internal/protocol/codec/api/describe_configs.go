package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// DescribeConfigsRequest (key 32, v0–v3 non-flexible).
//
// We deliberately only implement v0–v3; v4 introduced flexible-version encoding
// and clients will negotiate down based on the max we advertise in ApiVersions.
type DescribeConfigsRequest struct {
	Resources              []DescribeConfigsResource
	IncludeSynonyms        bool // v1+
	IncludeDocumentation   bool // v3+
}

type DescribeConfigsResource struct {
	ResourceType int8
	ResourceName string
	ConfigNames  []string // null = "all configs"
	ConfigNull   bool     // distinguishes nil-array (all) from empty-array (none)
}

// DescribeConfigsResponse (key 32, v0–v3 non-flexible).
type DescribeConfigsResponse struct {
	ThrottleTimeMs int32
	Results        []DescribeConfigsResult
}

type DescribeConfigsResult struct {
	ErrorCode    int16
	ErrorMessage string
	ResourceType int8
	ResourceName string
	Configs      []DescribeConfigsEntry
}

// DescribeConfigsEntry covers the union of fields across v0–v3. The encoder
// emits only those relevant to the negotiated version.
type DescribeConfigsEntry struct {
	Name          string
	Value         string
	ValueNull     bool
	ReadOnly      bool
	IsDefault     bool  // v0 only
	ConfigSource  int8  // v1+; see ConfigSource* constants
	IsSensitive   bool
	Synonyms      []DescribeConfigsSynonym // v1+
	ConfigType    int8                     // v3+
	Documentation string                   // v3+
	DocumentationNull bool                 // v3+
}

type DescribeConfigsSynonym struct {
	Name   string
	Value  string
	IsNull bool
	Source int8
}

// ConfigSource values per the Kafka protocol.
const (
	ConfigSourceUnknown               int8 = 0
	ConfigSourceDynamicTopic          int8 = 1
	ConfigSourceDynamicBroker         int8 = 2
	ConfigSourceDynamicDefaultBroker  int8 = 3
	ConfigSourceStaticBroker          int8 = 4
	ConfigSourceDefault               int8 = 5
	ConfigSourceDynamicBrokerLogger   int8 = 6
)

// DescribeConfigs ResourceType values.
const (
	ConfigResourceTopic         int8 = 2
	ConfigResourceBroker        int8 = 4
	ConfigResourceBrokerLogger  int8 = 8
)

func DecodeDescribeConfigsRequest(r *codec.Reader, version int16) (*DescribeConfigsRequest, error) {
	req := &DescribeConfigsRequest{}

	readResource := func() error {
		var res DescribeConfigsResource
		t, err := r.ReadInt8()
		if err != nil {
			return err
		}
		res.ResourceType = t
		name, err := r.ReadString()
		if err != nil {
			return err
		}
		res.ResourceName = name

		// ConfigNames: nullable array of strings.
		count, err := r.ReadInt32()
		if err != nil {
			return err
		}
		if count < 0 {
			res.ConfigNull = true
		} else {
			for i := int32(0); i < count; i++ {
				s, err := r.ReadString()
				if err != nil {
					return err
				}
				res.ConfigNames = append(res.ConfigNames, s)
			}
		}
		req.Resources = append(req.Resources, res)
		return nil
	}
	if err := r.ReadArray(readResource); err != nil {
		return nil, err
	}
	if version >= 1 {
		b, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.IncludeSynonyms = b != 0
	}
	if version >= 3 {
		b, err := r.ReadInt8()
		if err != nil {
			return nil, err
		}
		req.IncludeDocumentation = b != 0
	}
	return req, nil
}

func EncodeDescribeConfigsResponse(w *codec.Writer, resp *DescribeConfigsResponse, version int16) {
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteArray(len(resp.Results), func() {
		for _, res := range resp.Results {
			w.WriteInt16(res.ErrorCode)
			w.WriteNullableString(res.ErrorMessage, res.ErrorMessage == "")
			w.WriteInt8(res.ResourceType)
			w.WriteString(res.ResourceName)
			w.WriteArray(len(res.Configs), func() {
				for _, c := range res.Configs {
					w.WriteString(c.Name)
					w.WriteNullableString(c.Value, c.ValueNull)
					writeBool(w, c.ReadOnly)
					if version == 0 {
						writeBool(w, c.IsDefault)
					} else {
						w.WriteInt8(c.ConfigSource)
					}
					writeBool(w, c.IsSensitive)
					if version >= 1 {
						w.WriteArray(len(c.Synonyms), func() {
							for _, s := range c.Synonyms {
								w.WriteString(s.Name)
								w.WriteNullableString(s.Value, s.IsNull)
								w.WriteInt8(s.Source)
							}
						})
					}
					if version >= 3 {
						w.WriteInt8(c.ConfigType)
						w.WriteNullableString(c.Documentation, c.DocumentationNull)
					}
				}
			})
		}
	})
}

func writeBool(w *codec.Writer, v bool) {
	if v {
		w.WriteInt8(1)
		return
	}
	w.WriteInt8(0)
}
