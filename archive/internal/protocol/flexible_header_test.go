package protocol

import "testing"

// TestFlexibleRequestHeaderCoverage pins the invariant that every API
// key whose codec treats version N as "flexible" (KIP-482 tagged fields
// in the body) MUST also have an entry in flexibleRequestHeader so the
// dispatcher reads REQUEST_HEADER_V2 correctly. Forgetting this leaves
// the header's tag-buffer byte at the front of the body, which the
// codec then consumes as part of the first array length — surfaces as
// "delete_records decode: unexpected EOF" or similar (the actual bug
// that broke Kafbat's "Purge messages" in v0.1.35).
//
// This test is the codec-vs-header consistency guard. New flexible
// APIs added to a request codec without the corresponding map entry
// fail this test.
func TestFlexibleRequestHeaderCoverage(t *testing.T) {
	// (apiKey, version, expectedFlexible). Each row is an API + version
	// the broker advertises and the wire-format expectation.
	cases := []struct {
		name       string
		apiKey     int16
		apiVersion int16
		flexible   bool
	}{
		// Non-flexible APIs at their max supported version.
		{"Produce v3 not flexible", 0, 3, false},
		{"Fetch v4 not flexible", 1, 4, false},
		// Flexible at exactly the minimum.
		{"Produce v9 flexible", 0, 9, true},
		{"Fetch v12 flexible", 1, 12, true},
		{"DeleteTopics v4 flexible", 20, 4, true},
		// DeleteRecords v2 — the exact regression that broke Kafbat.
		{"DeleteRecords v0 not flexible", 21, 0, false},
		{"DeleteRecords v1 not flexible", 21, 1, false},
		{"DeleteRecords v2 flexible", 21, 2, true},
		// gh #102 / v0.1.96 regression — DescribeCluster has
		// flexibleVersions:"0+", so EVERY version is flexible. The
		// v0.1.93 ship missed the map entry; kafbat-ui + Kafka
		// Streams admin clients both fanout DescribeCluster v1 and
		// got "describe-cluster decode: unexpected EOF" until v0.1.96.
		{"DescribeCluster v0 flexible", 60, 0, true},
		{"DescribeCluster v1 flexible", 60, 1, true},
		// gh #23 / v0.1.94 — AddPartitionsToTxn flexibleVersions=3+.
		{"AddPartitionsToTxn v0 not flexible", 24, 0, false},
		{"AddPartitionsToTxn v2 not flexible", 24, 2, false},
		{"AddPartitionsToTxn v3 flexible", 24, 3, true},
		// gh #25/#26 / v0.1.95 — EndTxn flexibleVersions=3+.
		{"EndTxn v0 not flexible", 26, 0, false},
		{"EndTxn v2 not flexible", 26, 2, false},
		{"EndTxn v3 flexible", 26, 3, true},
		// gh #24 — AddOffsetsToTxn flexibleVersions=3+.
		{"AddOffsetsToTxn v0 not flexible", 25, 0, false},
		{"AddOffsetsToTxn v2 not flexible", 25, 2, false},
		{"AddOffsetsToTxn v3 flexible", 25, 3, true},
		// gh #27 — TxnOffsetCommit flexibleVersions=3+.
		{"TxnOffsetCommit v0 not flexible", 28, 0, false},
		{"TxnOffsetCommit v2 not flexible", 28, 2, false},
		{"TxnOffsetCommit v3 flexible", 28, 3, true},
		// gh #114 — WriteTxnMarkers flexibleVersions=1+.
		{"WriteTxnMarkers v0 not flexible", 27, 0, false},
		{"WriteTxnMarkers v1 flexible", 27, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := flexibleRequestHeader(tc.apiKey, tc.apiVersion)
			if got != tc.flexible {
				t.Errorf("flexibleRequestHeader(%d, v%d) = %v, want %v",
					tc.apiKey, tc.apiVersion, got, tc.flexible)
			}
		})
	}
}
