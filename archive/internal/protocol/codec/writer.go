package codec

import (
	"encoding/binary"
	"math"
)

type Writer struct {
	buf []byte
}

func NewWriter() *Writer {
	return &Writer{}
}

// NewWriterWithCap returns a Writer whose backing buffer has at least
// `cap` bytes of capacity. Callers that can estimate the encoded size
// up front (Fetch is the obvious one — sum of partition Records bytes
// + small per-partition / per-topic / per-response overhead) avoid
// the geometric growslice memmoves that dominate a 50 MB Fetch
// response. Pass a slight over-estimate; under-shooting falls back to
// append's normal doubling for the tail.
func NewWriterWithCap(cap int) *Writer {
	if cap <= 0 {
		return &Writer{}
	}
	return &Writer{buf: make([]byte, 0, cap)}
}

func (w *Writer) Bytes() []byte { return w.buf }

// WriteRaw appends bytes verbatim to the writer's buffer. No length
// prefix, no encoding. Used by the gh #135 splice-fallback path to
// merge materialised records bytes into the same buffer as the
// surrounding response header bytes, so they flush to the socket in
// a single Write call rather than one Write per partition.
func (w *Writer) WriteRaw(b []byte) {
	w.buf = append(w.buf, b...)
}

// Reset truncates the buffer to zero length while preserving capacity
// so the writer can be reused for additional encoding rounds without
// reallocating. Added for the gh #130 splice path which alternates
// between writing header bytes via codec.Writer and splicing records
// directly to the wire — the writer is reused for each per-partition
// chunk.
func (w *Writer) Reset() {
	w.buf = w.buf[:0]
}

func (w *Writer) WriteInt8(v int8) {
	w.buf = append(w.buf, byte(v))
}

func (w *Writer) WriteInt16(v int16) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v))
}

func (w *Writer) WriteInt32(v int32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
}

func (w *Writer) WriteInt64(v int64) {
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v))
}

// WriteFloat64 emits a big-endian IEEE-754 double. Used by KIP-546
// quota APIs (DescribeClientQuotas / AlterClientQuotas) which carry
// rates as float64 on the wire.
func (w *Writer) WriteFloat64(v float64) {
	w.buf = binary.BigEndian.AppendUint64(w.buf, math.Float64bits(v))
}

// WriteUvarint writes an unsigned variable-length integer.
func (w *Writer) WriteUvarint(v uint64) {
	w.buf = binary.AppendUvarint(w.buf, v)
}

// WriteVarint writes a zigzag-encoded signed variable-length integer.
func (w *Writer) WriteVarint(v int64) {
	w.buf = binary.AppendVarint(w.buf, v)
}

// WriteString writes an int16-length-prefixed string.
func (w *Writer) WriteString(s string) {
	w.WriteInt16(int16(len(s)))
	w.buf = append(w.buf, s...)
}

// WriteNullableString writes an int16-length-prefixed string, or -1 if null=true.
func (w *Writer) WriteNullableString(s string, null bool) {
	if null {
		w.WriteInt16(-1)
		return
	}
	w.WriteString(s)
}

// WriteCompactString writes a uvarint(len+1)-prefixed string (flexible version APIs).
func (w *Writer) WriteCompactString(s string) {
	w.WriteUvarint(uint64(len(s)) + 1)
	w.buf = append(w.buf, s...)
}

// WriteCompactNullableString writes a compact string, or uvarint(0) if null=true.
func (w *Writer) WriteCompactNullableString(s string, null bool) {
	if null {
		w.WriteUvarint(0)
		return
	}
	w.WriteCompactString(s)
}

// WriteBytes writes an int32-length-prefixed byte slice.
func (w *Writer) WriteBytes(b []byte) {
	w.WriteInt32(int32(len(b)))
	w.buf = append(w.buf, b...)
}

// WriteNullableBytes writes an int32-length-prefixed byte slice, or -1 if nil.
func (w *Writer) WriteNullableBytes(b []byte) {
	if b == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteBytes(b)
}

// WriteCompactBytes writes a uvarint(len+1)-prefixed byte slice (flexible version APIs).
func (w *Writer) WriteCompactBytes(b []byte) {
	w.WriteUvarint(uint64(len(b)) + 1)
	w.buf = append(w.buf, b...)
}

// WriteCompactNullableBytes writes compact bytes, or uvarint(0) if nil.
func (w *Writer) WriteCompactNullableBytes(b []byte) {
	if b == nil {
		w.WriteUvarint(0)
		return
	}
	w.WriteCompactBytes(b)
}

// WriteArray writes an int32 count followed by the output of fn for each element.
func (w *Writer) WriteArray(count int, fn func()) {
	w.WriteInt32(int32(count))
	fn()
}

// WriteCompactArray writes a uvarint(count+1) followed by the output of fn (flexible encoding).
func (w *Writer) WriteCompactArray(count int, fn func()) {
	w.WriteUvarint(uint64(count) + 1)
	fn()
}

// WriteEmptyTaggedFields writes a zero-length tagged fields section (most common case).
func (w *Writer) WriteEmptyTaggedFields() {
	w.WriteUvarint(0)
}

// WriteRawBytes appends raw bytes directly with no length prefix.
func (w *Writer) WriteRawBytes(b []byte) {
	w.buf = append(w.buf, b...)
}

// Reserve writes a placeholder int32 and returns its position so the caller
// can fix it up later (e.g. for total_length and CRC fields).
func (w *Writer) Reserve() int {
	pos := len(w.buf)
	w.buf = append(w.buf, 0, 0, 0, 0)
	return pos
}

// FixupInt32 overwrites the int32 at pos with v (used after Reserve).
func (w *Writer) FixupInt32(pos int, v int32) {
	binary.BigEndian.PutUint32(w.buf[pos:], uint32(v))
}
