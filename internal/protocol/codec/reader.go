package codec

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Reader struct {
	buf []byte
	pos int
}

func NewReader(buf []byte) *Reader {
	return &Reader{buf: buf}
}

func (r *Reader) Remaining() int { return len(r.buf) - r.pos }

// ReadRaw reads exactly n raw bytes with no length prefix (e.g. UUIDs).
func (r *Reader) ReadRaw(n int) ([]byte, error) {
	if err := r.require(n); err != nil {
		return nil, err
	}
	b := make([]byte, n)
	copy(b, r.buf[r.pos:r.pos+n])
	r.pos += n
	return b, nil
}

func (r *Reader) require(n int) error {
	if r.pos+n > len(r.buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (r *Reader) ReadInt8() (int8, error) {
	if err := r.require(1); err != nil {
		return 0, err
	}
	v := int8(r.buf[r.pos])
	r.pos++
	return v, nil
}

func (r *Reader) ReadInt16() (int16, error) {
	if err := r.require(2); err != nil {
		return 0, err
	}
	v := int16(binary.BigEndian.Uint16(r.buf[r.pos:]))
	r.pos += 2
	return v, nil
}

func (r *Reader) ReadInt32() (int32, error) {
	if err := r.require(4); err != nil {
		return 0, err
	}
	v := int32(binary.BigEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	return v, nil
}

func (r *Reader) ReadInt64() (int64, error) {
	if err := r.require(8); err != nil {
		return 0, err
	}
	v := int64(binary.BigEndian.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v, nil
}

// ReadUvarint reads an unsigned variable-length integer (protobuf encoding).
func (r *Reader) ReadUvarint() (uint64, error) {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		if err := r.require(1); err != nil {
			return 0, err
		}
		b := r.buf[r.pos]
		r.pos++
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, fmt.Errorf("codec: uvarint overflow")
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, fmt.Errorf("codec: uvarint overflow")
}

// ReadVarint reads a zigzag-encoded signed variable-length integer.
func (r *Reader) ReadVarint() (int64, error) {
	u, err := r.ReadUvarint()
	if err != nil {
		return 0, err
	}
	// zigzag decode: matches encoding (n<<1)^(n>>63)
	return int64(u>>1) ^ -(int64(u) & 1), nil
}

// ReadString reads a length-prefixed string (int16 length, -1 = error).
func (r *Reader) ReadString() (string, error) {
	length, err := r.ReadInt16()
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", fmt.Errorf("codec: unexpected null string")
	}
	if err := r.require(int(length)); err != nil {
		return "", err
	}
	s := string(r.buf[r.pos : r.pos+int(length)])
	r.pos += int(length)
	return s, nil
}

// ReadNullableString reads a length-prefixed string; returns "" and false if null (length=-1).
func (r *Reader) ReadNullableString() (string, bool, error) {
	length, err := r.ReadInt16()
	if err != nil {
		return "", false, err
	}
	if length < 0 {
		return "", false, nil
	}
	if err := r.require(int(length)); err != nil {
		return "", false, err
	}
	s := string(r.buf[r.pos : r.pos+int(length)])
	r.pos += int(length)
	return s, true, nil
}

// ReadCompactString reads a uvarint-length-prefixed string (flexible version APIs).
// Length is uvarint(actual_length + 1); 0 means null which is an error here.
func (r *Reader) ReadCompactString() (string, error) {
	u, err := r.ReadUvarint()
	if err != nil {
		return "", err
	}
	if u == 0 {
		return "", fmt.Errorf("codec: unexpected null compact string")
	}
	length := int(u - 1)
	if err := r.require(length); err != nil {
		return "", err
	}
	s := string(r.buf[r.pos : r.pos+length])
	r.pos += length
	return s, nil
}

// ReadCompactNullableString reads a compact string; returns "" and false if null (u=0).
func (r *Reader) ReadCompactNullableString() (string, bool, error) {
	u, err := r.ReadUvarint()
	if err != nil {
		return "", false, err
	}
	if u == 0 {
		return "", false, nil
	}
	length := int(u - 1)
	if err := r.require(length); err != nil {
		return "", false, err
	}
	s := string(r.buf[r.pos : r.pos+length])
	r.pos += length
	return s, true, nil
}

// ReadBytes reads a length-prefixed byte slice (int32 length, -1 = error).
func (r *Reader) ReadBytes() ([]byte, error) {
	length, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("codec: unexpected null bytes")
	}
	if err := r.require(int(length)); err != nil {
		return nil, err
	}
	b := make([]byte, length)
	copy(b, r.buf[r.pos:r.pos+int(length)])
	r.pos += int(length)
	return b, nil
}

// ReadNullableBytes reads a length-prefixed byte slice; returns nil if null (length=-1).
//
// gh #132: returns a sub-slice of the Reader's underlying frame buffer
// rather than a copy. On the Produce hot path this drops ~1 GB/s of
// allocation + memmove pressure (the records section is decoded per
// partition per request and immediately handed to storage.Append).
// Callers must finish using the slice before the frame buffer is
// recycled. Currently every caller (produce, sasl_authenticate,
// join_group, sync_group) consumes the bytes within request lifetime,
// so this is safe.
func (r *Reader) ReadNullableBytes() ([]byte, error) {
	length, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if err := r.require(int(length)); err != nil {
		return nil, err
	}
	b := r.buf[r.pos : r.pos+int(length) : r.pos+int(length)]
	r.pos += int(length)
	return b, nil
}

// ReadCompactBytes reads a uvarint-prefixed byte slice (flexible version APIs).
// gh #132: zero-copy — see ReadNullableBytes for aliasing semantics.
func (r *Reader) ReadCompactBytes() ([]byte, error) {
	u, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if u == 0 {
		return nil, fmt.Errorf("codec: unexpected null compact bytes")
	}
	length := int(u - 1)
	if err := r.require(length); err != nil {
		return nil, err
	}
	b := r.buf[r.pos : r.pos+length : r.pos+length]
	r.pos += length
	return b, nil
}

// ReadCompactNullableBytes reads compact bytes; returns nil if null (u=0).
// gh #132: zero-copy — see ReadNullableBytes for aliasing semantics.
func (r *Reader) ReadCompactNullableBytes() ([]byte, error) {
	u, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if u == 0 {
		return nil, nil
	}
	length := int(u - 1)
	if err := r.require(length); err != nil {
		return nil, err
	}
	b := r.buf[r.pos : r.pos+length : r.pos+length]
	r.pos += length
	return b, nil
}

// ReadArray calls fn exactly count times (int32-prefixed, legacy encoding).
func (r *Reader) ReadArray(fn func() error) error {
	count, err := r.ReadInt32()
	if err != nil {
		return err
	}
	if count < 0 {
		return nil // null array treated as empty
	}
	for i := int32(0); i < count; i++ {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

// ReadCompactArray calls fn exactly count times (uvarint-prefixed, flexible encoding).
// Length is uvarint(actual_count + 1); 0 means null/empty.
func (r *Reader) ReadCompactArray(fn func() error) error {
	u, err := r.ReadUvarint()
	if err != nil {
		return err
	}
	if u == 0 {
		return nil
	}
	count := int(u - 1)
	for i := 0; i < count; i++ {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

// ReadTaggedFields reads and discards all tagged fields (flexible version APIs).
// Unknown tags are skipped; callers that need specific tags must handle them before calling this.
func (r *Reader) ReadTaggedFields() error {
	numFields, err := r.ReadUvarint()
	if err != nil {
		return err
	}
	for i := uint64(0); i < numFields; i++ {
		// tag key
		if _, err := r.ReadUvarint(); err != nil {
			return err
		}
		// tag value (compact bytes)
		size, err := r.ReadUvarint()
		if err != nil {
			return err
		}
		if err := r.require(int(size)); err != nil {
			return err
		}
		r.pos += int(size)
	}
	return nil
}
