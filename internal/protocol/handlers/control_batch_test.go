package handlers

import (
	"encoding/binary"
	"testing"
)

// TestEncodeControlBatchCommit pins the on-wire shape of the
// COMMIT marker — the bits Apache requires for the Java client's
// read-committed Fetch path to identify it as a marker. gh #114.
func TestEncodeControlBatchCommit(t *testing.T) {
	batch := encodeControlBatch(100, 5, true, 7)
	assertControlBatch(t, batch, 100, 5, true, 7)
}

// TestEncodeControlBatchAbort verifies the ABORT marker has the
// right ControlRecordType (0).
func TestEncodeControlBatchAbort(t *testing.T) {
	batch := encodeControlBatch(200, 3, false, 1)
	assertControlBatch(t, batch, 200, 3, false, 1)
}

func assertControlBatch(t *testing.T, batch []byte, wantPID int64, wantEpoch int16, wantCommit bool, wantCoordEpoch int32) {
	t.Helper()
	if len(batch) < 61 {
		t.Fatalf("batch too short: %d bytes", len(batch))
	}

	// Header layout (per Apache RecordBatch v2):
	//   [0:8]   baseOffset
	//   [8:12]  batchLength
	//   [12:16] partitionLeaderEpoch
	//   [16]    magic
	//   [17:21] crc
	//   [21:23] attributes
	//   [23:27] lastOffsetDelta
	//   [27:35] baseTimestamp
	//   [35:43] maxTimestamp
	//   [43:51] producerID
	//   [51:53] producerEpoch
	//   [53:57] baseSequence
	//   [57:61] numRecords
	if magic := batch[16]; magic != 2 {
		t.Errorf("magic=%d, want 2", magic)
	}
	attrs := int16(binary.BigEndian.Uint16(batch[21:23]))
	if attrs&(1<<4) == 0 {
		t.Errorf("attrs=%016b missing isTransactional bit (4)", attrs)
	}
	if attrs&(1<<5) == 0 {
		t.Errorf("attrs=%016b missing isControl bit (5)", attrs)
	}
	pid := int64(binary.BigEndian.Uint64(batch[43:51]))
	if pid != wantPID {
		t.Errorf("PID=%d, want %d", pid, wantPID)
	}
	epoch := int16(binary.BigEndian.Uint16(batch[51:53]))
	if epoch != wantEpoch {
		t.Errorf("epoch=%d, want %d", epoch, wantEpoch)
	}
	baseSeq := int32(binary.BigEndian.Uint32(batch[53:57]))
	if baseSeq != -1 {
		t.Errorf("baseSequence=%d, want -1 (control batches are idempotence-exempt)", baseSeq)
	}
	numRecords := int32(binary.BigEndian.Uint32(batch[57:61]))
	if numRecords != 1 {
		t.Errorf("numRecords=%d, want 1 (control batches carry one record)", numRecords)
	}

	// Walk into the single record to verify the ControlRecordType in
	// the key. The record body is varint-prefixed at offset 61.
	body := batch[61:]
	_, off := readVarInt(body) // record length (skip)
	body = body[off:]
	// record attributes (1) + timestampDelta (varlong) + offsetDelta (varint)
	body = body[1:]
	_, off = readVarLong(body)
	body = body[off:]
	_, off = readVarInt(body)
	body = body[off:]
	keyLen, off := readVarInt(body)
	body = body[off:]
	if keyLen != 4 {
		t.Fatalf("control record key length=%d, want 4 (version+type)", keyLen)
	}
	keyVersion := int16(binary.BigEndian.Uint16(body[0:2]))
	keyType := int16(binary.BigEndian.Uint16(body[2:4]))
	if keyVersion != 0 {
		t.Errorf("control key version=%d, want 0", keyVersion)
	}
	wantType := int16(0) // ABORT
	if wantCommit {
		wantType = 1 // COMMIT
	}
	if keyType != wantType {
		t.Errorf("control key type=%d, want %d", keyType, wantType)
	}
	body = body[keyLen:]

	valLen, off := readVarInt(body)
	body = body[off:]
	if valLen != 6 {
		t.Fatalf("control record value length=%d, want 6 (EndTxnMarker = version+coordEpoch)", valLen)
	}
	gotCoordEpoch := int32(binary.BigEndian.Uint32(body[2:6]))
	if gotCoordEpoch != wantCoordEpoch {
		t.Errorf("coord epoch=%d, want %d", gotCoordEpoch, wantCoordEpoch)
	}
}

func readVarInt(buf []byte) (int32, int) {
	v, n := readUvar(buf)
	return int32(int64(v>>1) ^ -int64(v&1)), n
}

func readVarLong(buf []byte) (int64, int) {
	v, n := readUvar(buf)
	return int64(v>>1) ^ -int64(v&1), n
}

func readUvar(buf []byte) (uint64, int) {
	var v uint64
	var s uint
	for i, b := range buf {
		if b < 0x80 {
			return v | uint64(b)<<s, i + 1
		}
		v |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}
