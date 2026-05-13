package protocol

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/connstate"
)

// buildRequest constructs a raw Kafka request frame for testing.
func buildRequest(apiKey, apiVersion int16, correlationID int32, clientID, body string) []byte {
	// Header body: apiKey(2) + apiVersion(2) + correlationID(4) + clientID(int16-prefixed)
	hdr := make([]byte, 0, 32)
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(apiKey))
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(apiVersion))
	hdr = binary.BigEndian.AppendUint32(hdr, uint32(correlationID))
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(clientID)))
	hdr = append(hdr, clientID...)
	hdr = append(hdr, body...)

	frame := make([]byte, 4+len(hdr))
	binary.BigEndian.PutUint32(frame, uint32(len(hdr)))
	copy(frame[4:], hdr)
	return frame
}

// readResponse reads one complete response frame from conn.
func readResponse(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		t.Fatalf("read response length: %v", err)
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return buf
}

func TestServerAcceptsConnection(t *testing.T) {
	d := NewDispatcher()
	d.Register(18, 0, 3, HandlerFunc(func(c *connstate.ConnState, version int16, body []byte) ([]byte, error) {
		// Minimal ApiVersions stub: just return no error (empty list)
		return []byte{0, 0, 0, 0, 0, 0, 0}, nil
	}))

	srv := NewServer(Config{Listeners: []ListenerConfig{{Name: "internal", Addr: "127.0.0.1:0"}}}, d)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	addr := srv.listeners[0].Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Send an ApiVersions v0 request.
	frame := buildRequest(18, 0, 42, "test-client", "")
	conn.Write(frame)

	resp := readResponse(t, conn)
	// First 4 bytes of response body = correlationID
	if len(resp) < 4 {
		t.Fatalf("response too short: %d bytes", len(resp))
	}
	gotCorr := int32(binary.BigEndian.Uint32(resp[:4]))
	if gotCorr != 42 {
		t.Errorf("correlationID: got %d want 42", gotCorr)
	}
}

func TestDispatcherUnknownAPIKey(t *testing.T) {
	d := NewDispatcher()
	hdr := RequestHeader{APIKey: 999, APIVersion: 0, CorrelationID: 1}
	resp, err := d.Dispatch(hdr, nil, &connstate.ConnState{})
	if err != nil {
		t.Fatalf("Dispatch: unexpected error: %v", err)
	}
	// Response should contain correlationID + error code 35
	if len(resp) < 6 {
		t.Fatalf("response too short")
	}
	errCode := int16(binary.BigEndian.Uint16(resp[4:6]))
	if errCode != ErrUnsupportedVersion {
		t.Errorf("error code: got %d want %d", errCode, ErrUnsupportedVersion)
	}
}

func TestDispatcherUnsupportedVersion(t *testing.T) {
	d := NewDispatcher()
	// Use api_key=3 (Metadata), not 18 — ApiVersions has special "always respond" behaviour.
	d.Register(3, 1, 12, HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
		return []byte{}, nil
	}))
	hdr := RequestHeader{APIKey: 3, APIVersion: 99, CorrelationID: 5}
	resp, err := d.Dispatch(hdr, nil, &connstate.ConnState{})
	if err != nil {
		t.Fatalf("Dispatch: unexpected error: %v", err)
	}
	errCode := int16(binary.BigEndian.Uint16(resp[4:6]))
	if errCode != ErrUnsupportedVersion {
		t.Errorf("error code: got %d want %d", errCode, ErrUnsupportedVersion)
	}
}

func TestDispatcherCallsHandler(t *testing.T) {
	called := false
	d := NewDispatcher()
	d.Register(3, 1, 12, HandlerFunc(func(c *connstate.ConnState, version int16, body []byte) ([]byte, error) {
		called = true
		return []byte{0x00, 0x00}, nil
	}))
	hdr := RequestHeader{APIKey: 3, APIVersion: 5, CorrelationID: 77}
	_, err := d.Dispatch(hdr, []byte("body"), &connstate.ConnState{})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestServerGracefulShutdown(t *testing.T) {
	d := NewDispatcher()
	srv := NewServer(Config{Listeners: []ListenerConfig{{Name: "internal", Addr: "127.0.0.1:0"}}}, d)
	ctx, cancel := context.WithCancel(context.Background())

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Cancel immediately — server should stop accepting.
	cancel()

	done := make(chan struct{})
	go func() {
		srv.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("server did not shut down within 2s")
	}
}

func TestFrameReadWrite(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		frame := buildRequest(18, 0, 99, "cli", "body")
		client.Write(frame)
	}()

	server.SetDeadline(time.Now().Add(time.Second))
	hdr, body, err := readFrame(server)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if hdr.APIKey != 18 || hdr.APIVersion != 0 || hdr.CorrelationID != 99 || hdr.ClientID != "cli" {
		t.Errorf("unexpected header: %+v", hdr)
	}
	if string(body) != "body" {
		t.Errorf("body: got %q want %q", body, "body")
	}
}

func TestConnStateClientIDSet(t *testing.T) {
	d := NewDispatcher()
	d.Register(18, 0, 3, HandlerFunc(func(c *connstate.ConnState, v int16, b []byte) ([]byte, error) {
		if c.ClientID != "my-app" {
			return nil, nil
		}
		return []byte{0, 0}, nil
	}))

	srv := NewServer(Config{Listeners: []ListenerConfig{{Name: "internal", Addr: "127.0.0.1:0"}}}, d)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv.Start(ctx)

	conn, _ := net.Dial("tcp", srv.listeners[0].Addr().String())
	defer conn.Close()

	conn.Write(buildRequest(18, 0, 1, "my-app", ""))
	resp := readResponse(t, conn)
	if len(resp) < 4 {
		t.Fatal("response too short")
	}
}
