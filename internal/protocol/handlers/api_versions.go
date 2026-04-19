package handlers

import (
	"sort"

	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/protocol/codec"
	"github.com/woestebanaan/skafka/internal/protocol/codec/api"
)

// APIVersionsHandler handles API key 18.
// supportedVersions maps apiKey → {min, max} and is populated from the dispatcher.
type APIVersionsHandler struct {
	versions []api.APIVersion
}

// NewAPIVersionsHandler builds the handler from a map of apiKey → [min, max] version.
func NewAPIVersionsHandler(supported map[int16][2]int16) *APIVersionsHandler {
	vs := make([]api.APIVersion, 0, len(supported))
	for key, minMax := range supported {
		vs = append(vs, api.APIVersion{
			APIKey:     key,
			MinVersion: minMax[0],
			MaxVersion: minMax[1],
		})
	}
	// Stable order — clients may rely on deterministic output in tests.
	sort.Slice(vs, func(i, j int) bool { return vs[i].APIKey < vs[j].APIKey })
	return &APIVersionsHandler{versions: vs}
}

func (h *APIVersionsHandler) Handle(_ *connstate.ConnState, version int16, body []byte) ([]byte, error) {
	r := codec.NewReader(body)
	if _, err := api.DecodeAPIVersionsRequest(r, version); err != nil {
		// v0/v1/v2 have empty bodies — tolerate EOF on decode.
		// Any real parse error on v3+ is surfaced.
		if version >= 3 {
			return nil, err
		}
	}

	resp := &api.APIVersionsResponse{
		ErrorCode:    0,
		APIVersions:  h.versions,
		ThrottleTime: 0,
	}

	w := codec.NewWriter()
	api.EncodeAPIVersionsResponse(w, resp, version)
	return w.Bytes(), nil
}
