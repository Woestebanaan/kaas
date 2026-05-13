package protocol

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/woestebanaan/skafka/internal/auth"
	"github.com/woestebanaan/skafka/internal/connstate"
	"github.com/woestebanaan/skafka/internal/observability"
)

// ListenerConfig describes one named TCP listener the Server opens on
// Start. gh #124: replaces the legacy ListenAddr/TLSListenAddr/
// AuthedListenAddr triplet, decoupling exposure (Addr), encryption
// (TLSConfig != nil), and authentication (the AuthEngine selector on
// the Dispatcher; keyed by ListenerConfig.Name) into three independent
// axes — exactly the Strimzi listener model. The Name field is the
// tag stamped onto every connstate.ConnState accepted on this
// listener; handlers and the dispatcher's pre-SASL gate route off it.
type ListenerConfig struct {
	// Name is the listener tag (e.g. "plain", "secure-internal",
	// "ext-mtls"). Must be non-empty and unique across the list. The
	// per-listener auth engine map keys on this string.
	Name string

	// Addr is the host:port the listener binds to. Ignored when
	// PreBound is non-nil (tests use that to eliminate port-allocation
	// races by allocating loopback :0 themselves and handing the
	// listener over).
	Addr string

	// PreBound, if set, takes precedence over Addr — Server wraps it
	// (with TLS when TLSConfig is non-nil) and skips net.Listen.
	PreBound net.Listener

	// TLSConfig, when non-nil, wraps the bound plaintext listener with
	// tls.NewListener. Independent of authentication: a TLS listener
	// with no SASL-required auth engine is "opportunistic TLS"
	// (encrypted but anonymous).
	TLSConfig *tls.Config
}

// Config holds TCP server configuration. gh #124: every TCP listener
// flows through Listeners. The legacy ListenAddr/TLSListenAddr/
// AuthedListenAddr / TLSConfig fields are gone — callers build
// ListenerConfig entries directly.
type Config struct {
	Listeners []ListenerConfig
}

// Server is the Kafka protocol TCP server.
type Server struct {
	cfg        Config
	dispatcher *Dispatcher
	authEngine auth.AuthEngine // optional; used for mTLS CN extraction
	listeners  []net.Listener
	// listenerTag maps each open listener to the ListenerName the
	// accept loop should stamp onto incoming connections. Lets one
	// Server host the three listener policies (anon plaintext, mTLS
	// external, SASL-required authed) without sniffing port numbers
	// or types at serve time.
	listenerTag map[net.Listener]connstate.ListenerName
	wg          sync.WaitGroup
}

func NewServer(cfg Config, d *Dispatcher) *Server {
	// Back-compat: an empty Listeners slice synthesises a single
	// anonymous plaintext listener on :9092 — preserves the default
	// behaviour for callers that just want a broker on a port.
	if len(cfg.Listeners) == 0 {
		cfg.Listeners = []ListenerConfig{{Name: "internal", Addr: ":9092"}}
	}
	return &Server{cfg: cfg, dispatcher: d, listenerTag: make(map[net.Listener]connstate.ListenerName)}
}

// SetAuthEngine registers an AuthEngine used for mTLS CN extraction on TLS connections.
func (s *Server) SetAuthEngine(e auth.AuthEngine) { s.authEngine = e }

// Start opens the listener(s) and begins accepting connections.
// It returns once all listeners are bound (or on error).
func (s *Server) Start(ctx context.Context) error {
	// gh #124: open every configured listener in order. Each entry
	// owns its own (Name, Addr, PreBound, TLSConfig) tuple. The Name
	// is stamped onto connstate.ConnState.Listener at accept time so
	// downstream auth (dispatcher gate + per-handler engine
	// selector) can route per-listener.
	for _, lc := range s.cfg.Listeners {
		if lc.Name == "" {
			s.closeAll()
			return fmt.Errorf("server: listener config missing Name")
		}
		ln, err := s.openListener(lc)
		if err != nil {
			s.closeAll()
			return err
		}
		s.listeners = append(s.listeners, ln)
		s.listenerTag[ln] = connstate.ListenerName(lc.Name)
		slog.Info("skafka listening",
			"addr", ln.Addr().String(),
			"listener", lc.Name,
			"tls", lc.TLSConfig != nil)
	}

	for _, l := range s.listeners {
		s.wg.Add(1)
		go s.acceptLoop(ctx, l)
	}

	// Close listeners when context is done.
	go func() {
		<-ctx.Done()
		for _, l := range s.listeners {
			_ = l.Close()
		}
	}()

	return nil
}

// openListener resolves a ListenerConfig into a net.Listener: PreBound
// wins over Addr, and a non-nil TLSConfig wraps the plaintext listener
// with tls.NewListener — same shape as the legacy TLSPlainListener
// path, just generalised so any listener entry can carry a TLS config.
func (s *Server) openListener(lc ListenerConfig) (net.Listener, error) {
	var plain net.Listener
	if lc.PreBound != nil {
		plain = lc.PreBound
	} else {
		var err error
		plain, err = net.Listen("tcp", lc.Addr)
		if err != nil {
			return nil, fmt.Errorf("server: listen %s (%s): %w", lc.Addr, lc.Name, err)
		}
	}
	if lc.TLSConfig != nil {
		return tls.NewListener(plain, lc.TLSConfig), nil
	}
	return plain, nil
}

func (s *Server) closeAll() {
	for _, l := range s.listeners {
		_ = l.Close()
	}
	s.listeners = nil
}

// Wait blocks until all connections and listeners have closed.
func (s *Server) Wait() { s.wg.Wait() }

// Addr returns the address the primary (non-TLS) listener is bound to.
func (s *Server) Addr() string {
	if len(s.listeners) == 0 {
		return ""
	}
	return s.listeners[0].Addr().String()
}

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("listener: accepting a new client connection failed (server backs off 10ms then retries; sustained errors typically mean the listener fd is unhealthy and the broker should be restarted)",
				"addr", ln.Addr().String(), "err", err)
			// Brief back-off to avoid spinning on persistent errors.
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		// "mode" matches the Phase 10 plan name for skafka_external_connections_total.
		// Maps the listener type onto its wire-protocol mode: the in-cluster
		// listener is plaintext; the external listener is TLS-only.
		mode := "plaintext"
		if _, isTLS := conn.(*tls.Conn); isTLS {
			mode = "tls"
		}
		modeAttr := metric.WithAttributes(attribute.String("mode", mode))
		mx := observability.Global()
		mx.Connections.Add(context.Background(), 1, modeAttr)
		mx.ConnectionsOpen.Add(context.Background(), 1, modeAttr)
		// gh #139: look up the listener's tag once per accepted conn and
		// thread it down to serveConn so the connstate label is set before
		// any handshake (the dispatcher uses it to gate pre-SASL requests
		// per-listener).
		tag := s.listenerTag[ln]
		if tag == "" {
			tag = connstate.ListenerName("internal")
		}
		s.wg.Add(1)
		go func() {
			defer mx.ConnectionsOpen.Add(context.Background(), -1, modeAttr)
			s.serveConn(ctx, conn, tag)
		}()
	}
}

func (s *Server) serveConn(ctx context.Context, c net.Conn, listenerTag connstate.ListenerName) {
	defer s.wg.Done()
	defer c.Close()

	state := &connstate.ConnState{Listener: listenerTag}
	mx := observability.Global()

	// Mark TLS connections and extract the mTLS principal if a client cert is present.
	if tlsConn, ok := c.(*tls.Conn); ok {
		state.IsTLS = true
		// gh #124: the listener tag was stamped at accept time from
		// ListenerConfig.Name. No longer overridden here — a TLS listener
		// named "ext-mtls" stays "ext-mtls" instead of being forced to
		// the predefined ListenerExternal constant. mTLS principal
		// extraction below is independent of the tag.
		if err := tlsConn.Handshake(); err != nil {
			mx.TLSHandshakes.Add(context.Background(), 1,
				metric.WithAttributes(attribute.String("result", "error")))
			slog.Warn("tls: handshake with new client failed (client cert untrusted, protocol mismatch, or non-TLS bytes hitting the TLS port; connection dropped)",
				"remote", c.RemoteAddr().String(), "err", err)
			return
		}
		mx.TLSHandshakes.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("result", "ok")))
		cs := tlsConn.ConnectionState()
		if len(cs.PeerCertificates) > 0 && s.authEngine != nil {
			cn := cs.PeerCertificates[0].Subject.CommonName
			if p, err := s.authEngine.AuthenticateTLS(cn); err == nil {
				state.Principal = &p
				state.SASLDone = true
				mx.AuthSuccess.Add(context.Background(), 1,
					metric.WithAttributes(attribute.String("mechanism", "mtls")))
			} else {
				mx.AuthFailure.Add(context.Background(), 1,
					metric.WithAttributes(
						attribute.String("mechanism", "mtls"),
						attribute.String("reason", "cert_rejected"),
					))
				slog.Warn("tls: peer presented a valid cert but no KafkaUser CR matches the CN (or the auth engine rejected the principal); connection downgrades to SASL-required",
					"cn", cn, "err", err)
			}
		}
	}

	br := bufio.NewReaderSize(c, 64*1024)
	bw := bufio.NewWriterSize(c, 64*1024)

	// Close the connection when the server context is cancelled.
	go func() {
		<-ctx.Done()
		c.Close()
	}()

	for {
		hdr, body, err := readFrame(br)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && !isEOF(err) {
				// Failed to read a request frame from the client's
				// TCP stream — typically a malformed frame (corrupted
				// length prefix, client sending a non-Kafka protocol
				// to port 9092) or a mid-frame disconnect that wasn't
				// clean enough to return EOF. Clean disconnects
				// (EOF / net.ErrClosed) are silently swallowed above.
				slog.Warn("connection: reading next request frame failed",
					"client", state.ClientID,
					"err", err)
			}
			return
		}
		slog.Debug("request", "api_key", hdr.APIKey, "version", hdr.APIVersion, "client", hdr.ClientID)

		// Keep ClientID from the first request that sets it.
		if hdr.ClientID != "" && state.ClientID == "" {
			state.ClientID = hdr.ClientID
		}

		response, err := s.dispatcher.Dispatch(hdr, body, state)
		if err != nil {
			slog.Error("dispatch error", "api_key", hdr.APIKey, "version", hdr.APIVersion,
				"client", state.ClientID, "err", err)
			return
		}

		if err := writeFrame(bw, response); err != nil {
			// The framed response couldn't be written into the
			// per-connection bufio.Writer. In practice this fires
			// when the TCP send buffer is full AND the client has
			// already dropped the connection (broken pipe / RST).
			// The request handler already ran to completion; the
			// only consequence is the client doesn't see the
			// response it was waiting for and will retry per the
			// Kafka protocol's idempotent-retry contract.
			slog.Warn("connection: writing response frame failed (client likely disconnected mid-response)",
				"client", state.ClientID,
				"api_key", hdr.APIKey,
				"api_version", hdr.APIVersion,
				"response_bytes", len(response),
				"err", err)
			return
		}
		if err := bw.Flush(); err != nil {
			// The response was queued into the bufio.Writer but
			// flushing it onto the TCP socket failed. Same root
			// cause as writeFrame above: the client disconnected
			// between request receipt and response send. Logged
			// as WARN, not ERROR, because the broker did its job —
			// nothing actionable on this side.
			slog.Warn("connection: flushing response to socket failed (client likely disconnected; response built but not delivered)",
				"client", state.ClientID,
				"api_key", hdr.APIKey,
				"api_version", hdr.APIVersion,
				"err", err)
			return
		}
	}
}

func isEOF(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return s == "EOF" || s == "io: read/write on closed pipe" ||
		errors.Is(err, net.ErrClosed)
}
