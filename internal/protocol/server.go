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

// Config holds TCP server configuration.
//
// Listener / TLSPlainListener are optional pre-bound listeners that
// take precedence over the corresponding *Addr fields when non-nil.
// Tests use them to eliminate the address-allocation race when
// multiple in-process brokers want loopback-OS-assigned ports —
// allocating with `net.Listen("tcp", "127.0.0.1:0")` and passing
// the listener directly closes the gap between port assignment and
// rebind that would otherwise let a parallel test grab the port.
// In production both are nil and Start binds via *Addr as before.
//
// TLSPlainListener is the plaintext TCP listener; Server wraps it
// with `tls.NewListener(_, TLSConfig)`. (`tls.Listen(addr, cfg)`
// would re-do the listen step we're trying to skip.)
type Config struct {
	ListenAddr        string      // default ":9092"
	Listener          net.Listener // optional pre-bound; takes precedence over ListenAddr
	TLSListenAddr     string      // optional, e.g. ":9093"; empty = disabled
	TLSPlainListener  net.Listener // optional pre-bound plaintext listener wrapped with TLSConfig
	TLSConfig         *tls.Config  // required if TLSListenAddr or TLSPlainListener is set
	// AuthedListenAddr (gh #139) is the SASL-required plaintext listener.
	// Optional, e.g. ":9095"; empty = disabled. Connections accepted on
	// this port get tagged with connstate.ListenerAuthed and the
	// dispatcher rejects pre-auth requests on them. Coexists with the
	// anonymous ListenAddr — same broker, two policies.
	AuthedListenAddr  string
	AuthedListener    net.Listener // optional pre-bound; takes precedence over AuthedListenAddr
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
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":9092"
	}
	return &Server{cfg: cfg, dispatcher: d, listenerTag: make(map[net.Listener]connstate.ListenerName)}
}

// SetAuthEngine registers an AuthEngine used for mTLS CN extraction on TLS connections.
func (s *Server) SetAuthEngine(e auth.AuthEngine) { s.authEngine = e }

// Start opens the listener(s) and begins accepting connections.
// It returns once all listeners are bound (or on error).
func (s *Server) Start(ctx context.Context) error {
	var ln net.Listener
	if s.cfg.Listener != nil {
		ln = s.cfg.Listener
	} else {
		var err error
		ln, err = net.Listen("tcp", s.cfg.ListenAddr)
		if err != nil {
			return fmt.Errorf("server: listen %s: %w", s.cfg.ListenAddr, err)
		}
	}
	s.listeners = append(s.listeners, ln)
	s.listenerTag[ln] = connstate.ListenerInternal
	slog.Info("skafka listening", "addr", ln.Addr().String(), "listener", connstate.ListenerInternal)

	if s.cfg.TLSConfig != nil && (s.cfg.TLSListenAddr != "" || s.cfg.TLSPlainListener != nil) {
		var tlsLn net.Listener
		if s.cfg.TLSPlainListener != nil {
			tlsLn = tls.NewListener(s.cfg.TLSPlainListener, s.cfg.TLSConfig)
		} else {
			var err error
			tlsLn, err = tls.Listen("tcp", s.cfg.TLSListenAddr, s.cfg.TLSConfig)
			if err != nil {
				_ = ln.Close()
				return fmt.Errorf("server: tls listen %s: %w", s.cfg.TLSListenAddr, err)
			}
		}
		s.listeners = append(s.listeners, tlsLn)
		s.listenerTag[tlsLn] = connstate.ListenerExternal
		slog.Info("skafka TLS listening", "addr", tlsLn.Addr().String(), "listener", connstate.ListenerExternal)
	}

	// gh #139: optional SASL-required plaintext listener. Connections
	// here get tagged with connstate.ListenerAuthed; the dispatcher
	// rejects pre-auth requests on this tag regardless of the global
	// RequireSASL flag, so the broker can host this alongside the
	// anonymous-OK plain listener without breaking either.
	if s.cfg.AuthedListener != nil || s.cfg.AuthedListenAddr != "" {
		var authLn net.Listener
		if s.cfg.AuthedListener != nil {
			authLn = s.cfg.AuthedListener
		} else {
			var err error
			authLn, err = net.Listen("tcp", s.cfg.AuthedListenAddr)
			if err != nil {
				for _, l := range s.listeners {
					_ = l.Close()
				}
				return fmt.Errorf("server: authed listen %s: %w", s.cfg.AuthedListenAddr, err)
			}
		}
		s.listeners = append(s.listeners, authLn)
		s.listenerTag[authLn] = connstate.ListenerAuthed
		slog.Info("skafka authed-listener listening (SASL required)", "addr", authLn.Addr().String(), "listener", connstate.ListenerAuthed)
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
			tag = connstate.ListenerInternal
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
		// TLS connections override the listener tag — the TLS listener's
		// tag is always External (mTLS principal extraction lives here).
		state.Listener = connstate.ListenerExternal
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
