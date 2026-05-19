package handlers

import (
	"bytes"
	"testing"
)

// TestDecodeHyphenatedUUID pins gh #105: the Metadata handler's
// TopicID → raw-16-bytes converter. Failure modes (wrong length,
// missing hyphens, non-hex digits) all return nil so the encoder
// falls back to the all-zero sentinel without erroring.
func TestDecodeHyphenatedUUID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []byte
	}{
		{
			"canonical happy path",
			"00112233-4455-6677-8899-aabbccddeeff",
			[]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
		},
		{
			"uppercase hex",
			"AABBCCDD-EEFF-0011-2233-445566778899",
			[]byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99},
		},
		{"empty → nil (pre-#105 fallback)", "", nil},
		{"wrong length", "deadbeef", nil},
		{"missing hyphens", "00112233445566778899aabbccddeeff0000", nil},
		{"non-hex digit", "00112233-4455-6677-8899-aabbccddeeffG", nil},
		{"hyphen in wrong slot", "001122-334455-6677-8899-aabbccddeeff", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decodeHyphenatedUUID(tc.in)
			if !bytes.Equal(got, tc.want) {
				t.Errorf("decodeHyphenatedUUID(%q) = %x, want %x", tc.in, got, tc.want)
			}
		})
	}
}
