package codec

import (
	"math"
	"testing"
)

// roundTrip encodes with fn(w), then decodes with fn(r) and returns the reader.
func encode(fn func(*Writer)) []byte {
	w := NewWriter()
	fn(w)
	return w.Bytes()
}

func decode(buf []byte) *Reader {
	return NewReader(buf)
}

func TestInt8(t *testing.T) {
	for _, v := range []int8{0, 1, -1, math.MaxInt8, math.MinInt8} {
		buf := encode(func(w *Writer) { w.WriteInt8(v) })
		got, err := decode(buf).ReadInt8()
		if err != nil || got != v {
			t.Errorf("Int8(%d): got %d, err %v", v, got, err)
		}
	}
}

func TestInt16(t *testing.T) {
	for _, v := range []int16{0, 1, -1, math.MaxInt16, math.MinInt16} {
		buf := encode(func(w *Writer) { w.WriteInt16(v) })
		got, err := decode(buf).ReadInt16()
		if err != nil || got != v {
			t.Errorf("Int16(%d): got %d, err %v", v, got, err)
		}
	}
}

func TestInt32(t *testing.T) {
	for _, v := range []int32{0, 1, -1, math.MaxInt32, math.MinInt32} {
		buf := encode(func(w *Writer) { w.WriteInt32(v) })
		got, err := decode(buf).ReadInt32()
		if err != nil || got != v {
			t.Errorf("Int32(%d): got %d, err %v", v, got, err)
		}
	}
}

func TestInt64(t *testing.T) {
	for _, v := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64} {
		buf := encode(func(w *Writer) { w.WriteInt64(v) })
		got, err := decode(buf).ReadInt64()
		if err != nil || got != v {
			t.Errorf("Int64(%d): got %d, err %v", v, got, err)
		}
	}
}

func TestUvarint(t *testing.T) {
	for _, v := range []uint64{0, 1, 127, 128, 255, 300, math.MaxUint32, math.MaxUint64 / 2} {
		buf := encode(func(w *Writer) { w.WriteUvarint(v) })
		got, err := decode(buf).ReadUvarint()
		if err != nil || got != v {
			t.Errorf("Uvarint(%d): got %d, err %v", v, got, err)
		}
	}
}

func TestVarint(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 63, -64, math.MaxInt32, math.MinInt32, math.MaxInt64, math.MinInt64} {
		buf := encode(func(w *Writer) { w.WriteVarint(v) })
		got, err := decode(buf).ReadVarint()
		if err != nil || got != v {
			t.Errorf("Varint(%d): got %d, err %v", v, got, err)
		}
	}
}

func TestString(t *testing.T) {
	for _, s := range []string{"", "hello", "kafka protocol"} {
		buf := encode(func(w *Writer) { w.WriteString(s) })
		got, err := decode(buf).ReadString()
		if err != nil || got != s {
			t.Errorf("String(%q): got %q, err %v", s, got, err)
		}
	}
}

func TestNullableString(t *testing.T) {
	// non-null
	buf := encode(func(w *Writer) { w.WriteNullableString("hello", false) })
	got, ok, err := decode(buf).ReadNullableString()
	if err != nil || !ok || got != "hello" {
		t.Errorf("NullableString non-null: got %q ok=%v err=%v", got, ok, err)
	}

	// null
	buf = encode(func(w *Writer) { w.WriteNullableString("", true) })
	got, ok, err = decode(buf).ReadNullableString()
	if err != nil || ok || got != "" {
		t.Errorf("NullableString null: got %q ok=%v err=%v", got, ok, err)
	}
}

func TestCompactString(t *testing.T) {
	for _, s := range []string{"", "hello", "skafka"} {
		buf := encode(func(w *Writer) { w.WriteCompactString(s) })
		got, err := decode(buf).ReadCompactString()
		if err != nil || got != s {
			t.Errorf("CompactString(%q): got %q, err %v", s, got, err)
		}
	}
}

func TestCompactNullableString(t *testing.T) {
	// non-null
	buf := encode(func(w *Writer) { w.WriteCompactNullableString("hi", false) })
	got, ok, err := decode(buf).ReadCompactNullableString()
	if err != nil || !ok || got != "hi" {
		t.Errorf("CompactNullableString non-null: got %q ok=%v err=%v", got, ok, err)
	}

	// null
	buf = encode(func(w *Writer) { w.WriteCompactNullableString("", true) })
	got, ok, err = decode(buf).ReadCompactNullableString()
	if err != nil || ok || got != "" {
		t.Errorf("CompactNullableString null: got %q ok=%v err=%v", got, ok, err)
	}
}

func TestBytes(t *testing.T) {
	for _, b := range [][]byte{{}, {0x01, 0x02, 0x03}, make([]byte, 256)} {
		buf := encode(func(w *Writer) { w.WriteBytes(b) })
		got, err := decode(buf).ReadBytes()
		if err != nil || string(got) != string(b) {
			t.Errorf("Bytes: err %v", err)
		}
	}
}

func TestNullableBytes(t *testing.T) {
	// non-null
	buf := encode(func(w *Writer) { w.WriteNullableBytes([]byte{1, 2}) })
	got, err := decode(buf).ReadNullableBytes()
	if err != nil || string(got) != string([]byte{1, 2}) {
		t.Errorf("NullableBytes non-null: %v", err)
	}

	// null
	buf = encode(func(w *Writer) { w.WriteNullableBytes(nil) })
	got, err = decode(buf).ReadNullableBytes()
	if err != nil || got != nil {
		t.Errorf("NullableBytes null: got %v err %v", got, err)
	}
}

func TestCompactBytes(t *testing.T) {
	b := []byte("compact bytes test")
	buf := encode(func(w *Writer) { w.WriteCompactBytes(b) })
	got, err := decode(buf).ReadCompactBytes()
	if err != nil || string(got) != string(b) {
		t.Errorf("CompactBytes: got %q err %v", got, err)
	}
}

func TestCompactNullableBytes(t *testing.T) {
	// non-null
	buf := encode(func(w *Writer) { w.WriteCompactNullableBytes([]byte{9}) })
	got, err := decode(buf).ReadCompactNullableBytes()
	if err != nil || string(got) != string([]byte{9}) {
		t.Errorf("CompactNullableBytes non-null: %v", err)
	}

	// null
	buf = encode(func(w *Writer) { w.WriteCompactNullableBytes(nil) })
	got, err = decode(buf).ReadCompactNullableBytes()
	if err != nil || got != nil {
		t.Errorf("CompactNullableBytes null: got %v err %v", got, err)
	}
}

func TestArray(t *testing.T) {
	vals := []int32{1, 2, 3}
	buf := encode(func(w *Writer) {
		w.WriteArray(len(vals), func() {
			for _, v := range vals {
				w.WriteInt32(v)
			}
		})
	})

	r := decode(buf)
	var got []int32
	err := r.ReadArray(func() error {
		v, err := r.ReadInt32()
		got = append(got, v)
		return err
	})
	if err != nil || len(got) != len(vals) {
		t.Errorf("Array: got %v err %v", got, err)
	}
	for i, v := range vals {
		if got[i] != v {
			t.Errorf("Array[%d]: got %d want %d", i, got[i], v)
		}
	}
}

func TestCompactArray(t *testing.T) {
	vals := []int32{10, 20, 30}
	buf := encode(func(w *Writer) {
		w.WriteCompactArray(len(vals), func() {
			for _, v := range vals {
				w.WriteInt32(v)
			}
		})
	})

	r := decode(buf)
	var got []int32
	err := r.ReadCompactArray(func() error {
		v, err := r.ReadInt32()
		got = append(got, v)
		return err
	})
	if err != nil || len(got) != len(vals) {
		t.Errorf("CompactArray: got %v err %v", got, err)
	}
}

func TestEmptyArray(t *testing.T) {
	buf := encode(func(w *Writer) { w.WriteArray(0, func() {}) })
	r := decode(buf)
	count := 0
	err := r.ReadArray(func() error { count++; return nil })
	if err != nil || count != 0 {
		t.Errorf("EmptyArray: count=%d err=%v", count, err)
	}
}

func TestEmptyCompactArray(t *testing.T) {
	buf := encode(func(w *Writer) { w.WriteCompactArray(0, func() {}) })
	r := decode(buf)
	count := 0
	err := r.ReadCompactArray(func() error { count++; return nil })
	if err != nil || count != 0 {
		t.Errorf("EmptyCompactArray: count=%d err=%v", count, err)
	}
}

func TestTaggedFields(t *testing.T) {
	// zero fields (most common case)
	buf := encode(func(w *Writer) { w.WriteEmptyTaggedFields() })
	if err := decode(buf).ReadTaggedFields(); err != nil {
		t.Errorf("TaggedFields(0): %v", err)
	}
}

func TestReserveAndFixup(t *testing.T) {
	w := NewWriter()
	pos := w.Reserve()
	w.WriteInt32(42) // some payload after the reserved slot
	w.FixupInt32(pos, 999)

	r := NewReader(w.Bytes())
	v, _ := r.ReadInt32()
	if v != 999 {
		t.Errorf("FixupInt32: got %d want 999", v)
	}
}

func TestUnexpectedEOF(t *testing.T) {
	r := NewReader([]byte{0x00}) // only 1 byte
	if _, err := r.ReadInt32(); err == nil {
		t.Error("ReadInt32 on short buffer should fail")
	}
}
