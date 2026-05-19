package protocol

import "testing"

// TestFlexibleResponseHeader_AdminAPIs pins gh #138 — the response
// header must use V1 (with tagged fields) for every API+version
// whose codec writes a flexible body. Pre-fix the dispatcher's
// flexibleMin map didn't cover keys 23/48/49/50/51 added by gh
// #101/#103/#104; the codec wrote tagged fields in the body but the
// header was V0 (no tagged fields), so Java AdminClient's decoder
// read past the response into garbage and silently retried until
// its 5-minute timeout fired (kafka-configs.sh --describe/--alter
// user quota hang).
//
// One-line per (key, version) entry — the test is a regression
// guard against a future contributor removing or mis-mapping
// flexibleMin entries.
func TestFlexibleResponseHeader_AdminAPIs(t *testing.T) {
	cases := []struct {
		name      string
		apiKey    int16
		version   int16
		flexible  bool
		issueNote string
	}{
		// gh #101 — OffsetForLeaderEpoch (flexibleVersions=4+; codec at >=3)
		{"OffsetForLeaderEpoch v2 non-flexible", 23, 2, false, "gh #101"},
		{"OffsetForLeaderEpoch v3 flexible", 23, 3, true, "gh #101"},
		{"OffsetForLeaderEpoch v4 flexible", 23, 4, true, "gh #101"},

		// gh #103 — DescribeClientQuotas / AlterClientQuotas (flex=1+)
		{"DescribeClientQuotas v0 non-flexible", 48, 0, false, "gh #103"},
		{"DescribeClientQuotas v1 flexible", 48, 1, true, "gh #103 / gh #138"},
		{"AlterClientQuotas v0 non-flexible", 49, 0, false, "gh #103"},
		{"AlterClientQuotas v1 flexible", 49, 1, true, "gh #103 / gh #138"},

		// gh #104 — Describe/AlterUserScramCredentials (flex=0+)
		{"DescribeUserScramCredentials v0 flexible", 50, 0, true, "gh #104"},
		{"AlterUserScramCredentials v0 flexible", 51, 0, true, "gh #104"},

		// Spot checks for previously-correct mappings.
		{"ApiVersions header always V0", 18, 3, false, "bootstrap contract"},
		{"DescribeAcls v2 flexible", 29, 2, true, "gh #107"},

		// gh #142 — OffsetFetch flexibleVersions=6+ (Apache schema).
		// Pre-fix map said v8; v6/v7 emitted a non-flexible header
		// with a flexible body so --reset-offsets --execute silently
		// retried (kafka-consumer-groups --describe --offsets on
		// empty groups showed no offset rows).
		{"OffsetFetch v5 non-flexible", 9, 5, false, "gh #142"},
		{"OffsetFetch v6 flexible", 9, 6, true, "gh #142"},
		{"OffsetFetch v7 flexible", 9, 7, true, "gh #142"},
		{"OffsetFetch v8 flexible", 9, 8, true, "gh #142"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := flexibleResponseHeader(tc.apiKey, tc.version)
			if got != tc.flexible {
				t.Errorf("flexibleResponseHeader(%d, v%d) = %v, want %v (%s)",
					tc.apiKey, tc.version, got, tc.flexible, tc.issueNote)
			}
		})
	}
}
