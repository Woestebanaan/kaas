package codec

import (
	"encoding/binary"
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
	// Sanity: computing twice gives the same result.
	data := []byte{0xAB}
	if ComputeCRC(data) != ComputeCRC(data) {
		t.Error("CRC32C is non-deterministic")
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
