package protocol

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"testing"
)

// fakeWriterConn is a net.Conn for testing the copySplicer path. It's
// NOT a *net.TCPConn so NewSplicerFor will pick copySplicer for it.
type fakeWriterConn struct {
	net.Conn
	out *bytes.Buffer
}

func (f *fakeWriterConn) Write(p []byte) (int, error) { return f.out.Write(p) }
func (f *fakeWriterConn) Close() error                { return nil }

// TestNewSplicerForTCP returns a tcpSplicer for a *net.TCPConn.
func TestNewSplicerForTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		c, _ := ln.Accept()
		if c != nil {
			c.Close()
		}
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	s := NewSplicerFor(conn, bufio.NewWriter(conn))
	if _, ok := s.(*tcpSplicer); !ok {
		t.Errorf("NewSplicerFor(*TCPConn) = %T; want *tcpSplicer", s)
	}
}

// TestNewSplicerForFallback returns a copySplicer for non-TCP conns.
func TestNewSplicerForFallback(t *testing.T) {
	var buf bytes.Buffer
	conn := &fakeWriterConn{out: &buf}
	s := NewSplicerFor(conn, bufio.NewWriter(&buf))
	if _, ok := s.(*copySplicer); !ok {
		t.Errorf("NewSplicerFor(fake) = %T; want *copySplicer", s)
	}
}

// TestCopySplicerWriteThenSplice exercises the full Write → Splice →
// Write sequence on the fallback path and verifies output bytes.
func TestCopySplicerWriteThenSplice(t *testing.T) {
	// Build a temp file with known content.
	f, err := os.CreateTemp(t.TempDir(), "splice-src-*")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	defer f.Close()
	content := []byte("RECORDS_BYTES_FROM_DISK")
	if _, err := f.Write(content); err != nil {
		t.Fatalf("write: %v", err)
	}

	var out bytes.Buffer
	s := &copySplicer{bw: bufio.NewWriter(&out)}

	if _, err := s.Write([]byte("PREFIX|")); err != nil {
		t.Fatalf("Write prefix: %v", err)
	}
	if err := s.Splice(f, 0, len(content)); err != nil {
		t.Fatalf("Splice: %v", err)
	}
	if _, err := s.Write([]byte("|SUFFIX")); err != nil {
		t.Fatalf("Write suffix: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	want := []byte("PREFIX|RECORDS_BYTES_FROM_DISK|SUFFIX")
	if !bytes.Equal(out.Bytes(), want) {
		t.Errorf("output = %q; want %q", out.Bytes(), want)
	}
}

// TestCopySplicerSpliceOffsetWindow splices only a middle chunk of a
// larger file. Verifies offset + length are honored.
func TestCopySplicerSpliceOffsetWindow(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "splice-window-*")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	defer f.Close()
	// "AAABBBCCCDDD" — splice the middle "BBBCCC" only.
	if _, err := f.Write([]byte("AAABBBCCCDDD")); err != nil {
		t.Fatalf("write: %v", err)
	}

	var out bytes.Buffer
	s := &copySplicer{bw: bufio.NewWriter(&out)}
	if err := s.Splice(f, 3, 6); err != nil {
		t.Fatalf("Splice: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !bytes.Equal(out.Bytes(), []byte("BBBCCC")) {
		t.Errorf("output = %q; want %q", out.Bytes(), "BBBCCC")
	}
}

// TestCopySplicerNilFile returns an error rather than panicking.
func TestCopySplicerNilFile(t *testing.T) {
	var out bytes.Buffer
	s := &copySplicer{bw: bufio.NewWriter(&out)}
	if err := s.Splice(nil, 0, 10); err == nil {
		t.Errorf("Splice(nil, ...) returned nil err; want non-nil")
	}
}

// TestCopySplicerZeroLength is a no-op.
func TestCopySplicerZeroLength(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "splice-empty-*")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("anything")); err != nil {
		t.Fatalf("write: %v", err)
	}

	var out bytes.Buffer
	s := &copySplicer{bw: bufio.NewWriter(&out)}
	if err := s.Splice(f, 0, 0); err != nil {
		t.Errorf("Splice(file, 0, 0) = %v; want nil (no-op)", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("zero-length splice wrote %d bytes; want 0", out.Len())
	}
}
