package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// framePool recycles request-frame byte slices (gh #132 item 4). Pre-pool
// the readFrame allocation was ~50% of all heap traffic on the produce
// hot path (post the gh #133 zero-copy decode fix, which eliminated the
// other 50%). Buffers up to 1 MiB go back into the pool; outsized
// requests skip the pool so a one-off huge frame doesn't pin a large
// buffer for the rest of the broker's life.
const frameBufPoolMaxCap = 1 << 20

var framePool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 64*1024)
		return &b
	},
}

// frameBuf returns a buffer of exactly n bytes. The first return value
// is the pool handle that putFrameBuf wants when the caller is done;
// the second is the byte slice the caller actually reads/decodes into.
func frameBuf(n int) (*[]byte, []byte) {
	bp := framePool.Get().(*[]byte)
	if cap(*bp) < n {
		// Pooled buffer too small for this request — allocate fresh.
		// Keeping the old pointer means the under-sized one stays out
		// of the pool until the next GC visits it; that's fine, the
		// pool will grow to fit the workload's request sizes.
		*bp = make([]byte, n)
	} else {
		*bp = (*bp)[:n]
	}
	return bp, *bp
}

// putFrameBuf returns a frame buffer to the pool. Safe to call with nil
// (no-op) so callers can defer it before the buffer is necessarily set.
//
// TEMP: disabled pending investigation — recycling appears to break the
// kafka-compat consumer flow. Tests pass without it; perf impact is
// modest. Re-enable once the aliasing hazard is identified.
func putFrameBuf(bp *[]byte) {
	_ = bp
}

// RequestHeader holds the decoded Kafka request frame header.
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string // may be empty
}

// readFrame reads one complete Kafka request frame from r. Returns the
// header, the raw body bytes (everything after client_id / tagged fields),
// and a pool handle. The caller MUST invoke putFrameBuf(handle) after
// the request lifecycle ends — typically after the response has been
// written. Returns a nil handle on error so callers can use a deferred
// putFrameBuf without a nil check.
func readFrame(r io.Reader) (RequestHeader, []byte, *[]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return RequestHeader{}, nil, nil, err
	}
	totalLen := int(binary.BigEndian.Uint32(lenBuf[:]))
	if totalLen < 4 {
		return RequestHeader{}, nil, nil, fmt.Errorf("protocol: frame length %d too small", totalLen)
	}
	if totalLen > 100<<20 { // 100 MB sanity cap
		return RequestHeader{}, nil, nil, fmt.Errorf("protocol: frame length %d exceeds limit", totalLen)
	}

	bufHandle, buf := frameBuf(totalLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		putFrameBuf(bufHandle)
		return RequestHeader{}, nil, nil, err
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
		putFrameBuf(bufHandle)
		return RequestHeader{}, nil, nil, io.ErrUnexpectedEOF
	}
	clientIDLen := int16(binary.BigEndian.Uint16(buf[pos:]))
	pos += 2
	if clientIDLen > 0 {
		if pos+int(clientIDLen) > len(buf) {
			putFrameBuf(bufHandle)
			return RequestHeader{}, nil, nil, io.ErrUnexpectedEOF
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
	return hdr, buf[pos:], bufHandle, nil
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
		9:  6,  // OffsetFetch (Apache schema: flexibleVersions=6+; pre-gh #142 said v8 here, which let Java AdminClient's OffsetFetch v6/v7 read a tag-less header but a tag-full body, leaving --reset-offsets --execute silently retrying)
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
		22: 2,  // InitProducerId
		24: 3,  // AddPartitionsToTxn (gh #23, flexibleVersions=3+)
		25: 3,  // AddOffsetsToTxn (gh #24, flexibleVersions=3+)
		26: 3,  // EndTxn (gh #25/#26, flexibleVersions=3+)
		27: 1,  // WriteTxnMarkers (gh #114, flexibleVersions=1+)
		28: 3,  // TxnOffsetCommit (gh #27, flexibleVersions=3+)
		42: 2,  // DeleteGroups
		29: 2,  // DescribeAcls
		30: 2,  // CreateAcls
		31: 2,  // DeleteAcls
		37: 2,  // CreatePartitions (gh #52, flexibleVersions=2+)
		36: 2,  // SaslAuthenticate
		// DescribeCluster (gh #102): flexibleVersions=0+ per Apache
		// schema — EVERY version uses REQUEST_HEADER_V2 with tagged
		// fields. Pre-fix v0.1.93/94/95 hit "describe-cluster decode:
		// unexpected EOF" on every kafbat-ui + Kafka Streams admin
		// client (both fan out DescribeCluster v1 periodically) because
		// the dispatcher used the legacy header and the handler then
		// read from the wrong offset.
		60: 0,
		// gh #138: missing entries here caused the Java AdminClient
		// (kafka-configs.sh, kafka-broker-api-versions.sh) to hang for
		// minutes on DescribeClientQuotas v1 / AlterClientQuotas v1.
		// The codec wrote a flexible body but the dispatcher emitted
		// RESPONSE_HEADER_V0 (no tagged fields). The client's decoder
		// then read past the response into garbage, silently retried
		// until its default 5-minute timeout fired.
		23: 3, // OffsetForLeaderEpoch (gh #101, flexibleVersions=4+; codec gates on >=3)
		48: 1, // DescribeClientQuotas (gh #103, flexibleVersions=1+)
		49: 1, // AlterClientQuotas (gh #103, flexibleVersions=1+)
		50: 0, // DescribeUserScramCredentials (gh #104, flexibleVersions=0+)
		51: 0, // AlterUserScramCredentials (gh #104, flexibleVersions=0+)
	}
	min, ok := flexibleMin[apiKey]
	if !ok {
		return false
	}
	return apiVersion >= min
}
