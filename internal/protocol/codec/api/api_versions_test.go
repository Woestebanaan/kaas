package api

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/protocol/codec"
)

var testVersions = []APIVersion{
	{APIKey: 0, MinVersion: 3, MaxVersion: 9},
	{APIKey: 1, MinVersion: 4, MaxVersion: 13},
	{APIKey: 3, MinVersion: 1, MaxVersion: 12},
	{APIKey: 18, MinVersion: 0, MaxVersion: 3},
}

func TestAPIVersionsRoundTripV0(t *testing.T) {
	resp := &APIVersionsResponse{ErrorCode: 0, APIVersions: testVersions}
	w := codec.NewWriter()
	EncodeAPIVersionsResponse(w, resp, 0)

	r := codec.NewReader(w.Bytes())
	errCode, _ := r.ReadInt16()
	if errCode != 0 {
		t.Fatalf("error_code: got %d want 0", errCode)
	}

	var got []APIVersion
	_ = r.ReadArray(func() error {
		key, _ := r.ReadInt16()
		min, _ := r.ReadInt16()
		max, _ := r.ReadInt16()
		got = append(got, APIVersion{key, min, max})
		return nil
	})

	if len(got) != len(testVersions) {
		t.Fatalf("api count: got %d want %d", len(got), len(testVersions))
	}
	for i, v := range testVersions {
		if got[i] != v {
			t.Errorf("[%d] got %+v want %+v", i, got[i], v)
		}
	}
}

func TestAPIVersionsRoundTripV1(t *testing.T) {
	resp := &APIVersionsResponse{ErrorCode: 0, APIVersions: testVersions, ThrottleTime: 5}
	w := codec.NewWriter()
	EncodeAPIVersionsResponse(w, resp, 1)
	b := w.Bytes()

	r := codec.NewReader(b)
	r.ReadInt16() // error_code
	r.ReadArray(func() error {
		r.ReadInt16()
		r.ReadInt16()
		r.ReadInt16()
		return nil
	})
	throttle, err := r.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 throttle: %v", err)
	}
	if throttle != 5 {
		t.Errorf("throttle_time: got %d want 5", throttle)
	}
}

func TestAPIVersionsRoundTripV3(t *testing.T) {
	resp := &APIVersionsResponse{ErrorCode: 0, APIVersions: testVersions, ThrottleTime: 0}
	w := codec.NewWriter()
	EncodeAPIVersionsResponse(w, resp, 3)

	r := codec.NewReader(w.Bytes())
	errCode, _ := r.ReadInt16()
	if errCode != 0 {
		t.Fatalf("error_code: got %d", errCode)
	}

	var got []APIVersion
	_ = r.ReadCompactArray(func() error {
		key, _ := r.ReadInt16()
		min, _ := r.ReadInt16()
		max, _ := r.ReadInt16()
		r.ReadTaggedFields() // per-entry tagged fields
		got = append(got, APIVersion{key, min, max})
		return nil
	})

	if len(got) != len(testVersions) {
		t.Fatalf("api count: got %d want %d", len(got), len(testVersions))
	}
	for i, v := range testVersions {
		if got[i] != v {
			t.Errorf("[%d] got %+v want %+v", i, got[i], v)
		}
	}

	r.ReadInt32()        // throttle_time
	r.ReadTaggedFields() // response tagged fields
	if r.Remaining() != 0 {
		t.Errorf("unexpected trailing bytes: %d", r.Remaining())
	}
}

func TestAPIVersionsDecodeRequestV0(t *testing.T) {
	r := codec.NewReader([]byte{})
	req, err := DecodeAPIVersionsRequest(r, 0)
	if err != nil {
		t.Fatalf("v0 decode: %v", err)
	}
	if req.ClientSoftwareName != "" {
		t.Errorf("v0 should have empty software name, got %q", req.ClientSoftwareName)
	}
}

func TestAPIVersionsDecodeRequestV3(t *testing.T) {
	w := codec.NewWriter()
	w.WriteCompactString("my-client")
	w.WriteCompactString("1.2.3")
	w.WriteEmptyTaggedFields()

	r := codec.NewReader(w.Bytes())
	req, err := DecodeAPIVersionsRequest(r, 3)
	if err != nil {
		t.Fatalf("v3 decode: %v", err)
	}
	if req.ClientSoftwareName != "my-client" {
		t.Errorf("software name: got %q want %q", req.ClientSoftwareName, "my-client")
	}
	if req.ClientSoftwareVersion != "1.2.3" {
		t.Errorf("software version: got %q want %q", req.ClientSoftwareVersion, "1.2.3")
	}
}

func TestAPIVersionsAllVersionsContainKey18(t *testing.T) {
	// ApiVersions must always advertise itself.
	resp := &APIVersionsResponse{
		APIVersions: []APIVersion{{APIKey: 18, MinVersion: 0, MaxVersion: 3}},
	}
	for _, version := range []int16{0, 1, 2, 3} {
		w := codec.NewWriter()
		EncodeAPIVersionsResponse(w, resp, version)
		if len(w.Bytes()) == 0 {
			t.Errorf("v%d: empty response", version)
		}
	}
}
