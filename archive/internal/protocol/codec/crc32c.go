package codec

import (
	"fmt"
	"hash/crc32"
)

// castagnoliTable uses the Castagnoli polynomial — NOT IEEE. Common bug.
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func ComputeCRC(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

func ValidateCRC(data []byte, expected uint32) error {
	got := ComputeCRC(data)
	if got != expected {
		return fmt.Errorf("codec: CRC32C mismatch: got %08x want %08x", got, expected)
	}
	return nil
}
