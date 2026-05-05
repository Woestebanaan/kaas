package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// RequestHeader holds the decoded Kafka request frame header.
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string // may be empty
}

// readFrame reads one complete Kafka request frame from r.
// Returns the header and the raw body bytes (everything after client_id / tagged fields).
func readFrame(r io.Reader) (RequestHeader, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return RequestHeader{}, nil, err
	}
	totalLen := int(binary.BigEndian.Uint32(lenBuf[:]))
	if totalLen < 4 {
		return RequestHeader{}, nil, fmt.Errorf("protocol: frame length %d too small", totalLen)
	}
	if totalLen > 100<<20 { // 100 MB sanity cap
		return RequestHeader{}, nil, fmt.Errorf("protocol: frame length %d exceeds limit", totalLen)
	}

	buf := make([]byte, totalLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return RequestHeader{}, nil, err
	}

	pos := 0
	apiKey := int16(binary.BigEndian.Uint16(buf[pos:]))
	pos += 2
	apiVersion := int16(binary.BigEndian.Uint16(buf[pos:]))
	pos += 2
	correlationID := int32(binary.BigEndian.Uint32(buf[pos:]))
	pos += 4

	// client_id: nullable string (int16 length)
	var clientID string
	if pos+2 > len(buf) {
		return RequestHeader{}, nil, io.ErrUnexpectedEOF
	}
	clientIDLen := int16(binary.BigEndian.Uint16(buf[pos:]))
	pos += 2
	if clientIDLen > 0 {
		if pos+int(clientIDLen) > len(buf) {
			return RequestHeader{}, nil, io.ErrUnexpectedEOF
		}
		clientID = string(buf[pos : pos+int(clientIDLen)])
		pos += int(clientIDLen)
	}

	// Flexible version APIs include tagged fields in the request header.
	if flexibleRequestHeader(apiKey, apiVersion) {
		// Read and skip header tagged fields (uvarint count, then skip each).
		n, width := decodeUvarint(buf[pos:])
		pos += width
		for i := uint64(0); i < n; i++ {
			// tag key
			_, w := decodeUvarint(buf[pos:])
			pos += w
			// tag value length
			size, w := decodeUvarint(buf[pos:])
			pos += w
			pos += int(size)
		}
	}

	hdr := RequestHeader{
		APIKey:        apiKey,
		APIVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	return hdr, buf[pos:], nil
}

// writeFrame prepends the int32 frame length and writes the complete response to w.
// responseBody must already contain [correlationID:4][tagged_fields?][body...].
func writeFrame(w io.Writer, responseBody []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(responseBody)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(responseBody)
	return err
}

// buildResponsePrefix builds the [correlationID:4][tagged_fields?] prefix for a response.
func buildResponsePrefix(correlationID int32, flexible bool) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(correlationID))
	if flexible {
		buf = append(buf, 0x00) // empty tagged fields
	}
	return buf
}

// decodeUvarint decodes a uvarint from buf and returns (value, bytesRead).
func decodeUvarint(buf []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range buf {
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
		if i == 9 {
			return 0, 0
		}
	}
	return 0, 0
}

// flexibleResponseHeader reports whether the response header for this API+version
// should include tagged fields (RESPONSE_HEADER_V1).
// ApiVersions (key 18) always uses RESPONSE_HEADER_V0 (no tagged fields) regardless
// of version — required so clients can bootstrap before knowing what's supported.
func flexibleResponseHeader(apiKey, apiVersion int16) bool {
	if apiKey == 18 {
		return false
	}
	return flexibleRequestHeader(apiKey, apiVersion)
}

// flexibleRequestHeader reports whether this API key + version uses the flexible
// request header format (REQUEST_HEADER_V2, which includes tagged fields).
// Source: https://kafka.apache.org/protocol#protocol_api_keys
func flexibleRequestHeader(apiKey, apiVersion int16) bool {
	// Map of apiKey → minimum version that is "flexible".
	// -1 means never flexible.
	flexibleMin := map[int16]int16{
		0:  9,  // Produce
		1:  12, // Fetch
		2:  6,  // ListOffsets
		3:  9,  // Metadata
		8:  8,  // OffsetCommit
		9:  8,  // OffsetFetch (v8 uses flexible)
		10: 3,  // FindCoordinator
		11: 6,  // JoinGroup
		12: 4,  // Heartbeat
		13: 4,  // LeaveGroup
		14: 4,  // SyncGroup
		15: 5,  // DescribeGroups
		16: 3,  // ListGroups
		17: 2,  // SaslHandshake — never flexible (max v1)
		18: 3,  // ApiVersions (v3+ = flexible; v4 same format)
		19: 5,  // CreateTopics
		20: 4,  // DeleteTopics
		21: 2,  // DeleteRecords
		29: 2,  // DescribeAcls
		30: 2,  // CreateAcls
		31: 2,  // DeleteAcls
		36: 2,  // SaslAuthenticate
	}
	min, ok := flexibleMin[apiKey]
	if !ok {
		return false
	}
	return apiVersion >= min
}
