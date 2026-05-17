package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestCRC32CKnownVector(t *testing.T) {
	// Known-good CRC32C of the ASCII string "123456789" = 0xE3069283
	// This is the standard test vector for Castagnoli CRC32C.
	data := []byte("123456789")
	got := ComputeCRC(data)
	const want = uint32(0xE3069283)
	if got != want {
		t.Errorf("CRC32C(\"123456789\") = %08x, want %08x (wrong polynomial?)", got, want)
	}
}

func TestCRC32CNotIEEE(t *testing.T) {
	// CRC32C and CRC32-IEEE produce different results for the same input.
	// This catches the common bug of using the wrong table.
	// IEEE CRC of "123456789" = 0xCBF43926
	data := []byte("123456789")
	got := ComputeCRC(data)
	const ieee = uint32(0xCBF43926)
	if got == ieee {
		t.Error("ComputeCRC returned IEEE CRC32, not Castagnoli — wrong polynomial")
	}
}

func TestValidateCRCPass(t *testing.T) {
	data := []byte("skafka test data")
	crc := ComputeCRC(data)
	if err := ValidateCRC(data, crc); err != nil {
		t.Errorf("ValidateCRC: unexpected error: %v", err)
	}
}

func TestValidateCRCFail(t *testing.T) {
	data := []byte("skafka test data")
	crc := ComputeCRC(data)
	if err := ValidateCRC(data, crc+1); err == nil {
		t.Error("ValidateCRC: expected error for wrong CRC, got nil")
	}
}

func TestCRC32CEmptyInput(t *testing.T) {
	// CRC32C of empty input = 0x00000000
	got := ComputeCRC([]byte{})
	if got != 0 {
		t.Errorf("CRC32C(empty) = %08x, want 0x00000000", got)
	}
}

func TestCRC32CSingleByte(t *testing.T) {
	// Flipping a single bit in the input must produce a different CRC. A
	// polynomial with poor mixing (or a bug that returns the input verbatim)
	// would let neighbouring single-byte inputs collide.
	a := ComputeCRC([]byte{0xAB})
	b := ComputeCRC([]byte{0xAA})
	if a == b {
		t.Errorf("CRC32C(0xAB) == CRC32C(0xAA) = %08x; expected single-bit sensitivity", a)
	}
}

func TestCRC32CUint32Encoding(t *testing.T) {
	// Verify that encoding the CRC as big-endian int32 and reading it back
	// preserves the value — relevant for RecordBatch CRC field handling.
	data := []byte("record batch payload")
	crc := ComputeCRC(data)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], crc)
	got := binary.BigEndian.Uint32(buf[:])
	if got != crc {
		t.Errorf("CRC round-trip via int32 field: got %08x want %08x", got, crc)
	}
}

// TestCRC32CSpecVectors covers Castagnoli test vectors that show up across
// the iSCSI/SCTP/Btrfs literature. They are hard-coded against the Go std
// library implementation so that a polynomial regression here would also
// fail against any independent Castagnoli implementation.
func TestCRC32CSpecVectors(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want uint32
	}{
		// Canonical Castagnoli vector — already covered above, repeated here for grouping.
		{"123456789", []byte("123456789"), 0xE3069283},
		// 32 zero bytes — RFC 3720 (iSCSI) test vector.
		{"32 zero bytes", make([]byte, 32), 0x8A9136AA},
		// 32 0xFF bytes — RFC 3720 (iSCSI) test vector.
		{"32 0xFF bytes", bytes.Repeat([]byte{0xFF}, 32), 0x62A8AB43},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeCRC(tc.in)
			if got != tc.want {
				t.Errorf("CRC32C(%s) = %08x, want %08x", tc.name, got, tc.want)
			}
		})
	}
}

// BenchmarkCRC32C confirms hardware-accelerated CRC32C is engaged. On modern
// x86 (CLMUL) or ARM (CRC32 instruction), throughput should sit in the
// multi-GB/s range; software-only fallback is closer to 100-300 MB/s. A
// regression to the slow path is a five- to twenty-fold drop and shows up
// here before it shows up as broker-throughput collapse on the produce hot
// path.
func BenchmarkCRC32C(b *testing.B) {
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20} {
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = byte(i)
		}
		b.Run(humanSize(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ComputeCRC(buf)
			}
		})
	}
}

func humanSize(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKB", n>>10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}
