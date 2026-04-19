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

	"github.com/woestebanaan/skafka/internal/connstate"
)

// Config holds TCP server configuration.
type Config struct {
	ListenAddr    string      // default ":9092"
	TLSListenAddr string      // optional, e.g. ":9093"; empty = disabled
	TLSConfig     *tls.Config // required if TLSListenAddr is set
}

// Server is the Kafka protocol TCP server.
type Server struct {
	cfg        Config
	dispatcher *Dispatcher
	listeners  []net.Listener
	wg         sync.WaitGroup
}

func NewServer(cfg Config, d *Dispatcher) *Server {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":9092"
	}
	return &Server{cfg: cfg, dispatcher: d}
}

// Start opens the listener(s) and begins accepting connections.
// It returns once all listeners are bound (or on error).
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.cfg.ListenAddr, err)
	}
	s.listeners = append(s.listeners, ln)
	slog.Info("skafka listening", "addr", s.cfg.ListenAddr)

	if s.cfg.TLSListenAddr != "" && s.cfg.TLSConfig != nil {
		tlsLn, err := tls.Listen("tcp", s.cfg.TLSListenAddr, s.cfg.TLSConfig)
		if err != nil {
			_ = ln.Close()
			return fmt.Errorf("server: tls listen %s: %w", s.cfg.TLSListenAddr, err)
		}
		s.listeners = append(s.listeners, tlsLn)
		slog.Info("skafka TLS listening", "addr", s.cfg.TLSListenAddr)
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
			slog.Error("accept error", "err", err)
			// Brief back-off to avoid spinning on persistent errors.
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		s.wg.Add(1)
		go s.serveConn(ctx, conn)
	}
}

func (s *Server) serveConn(ctx context.Context, c net.Conn) {
	defer s.wg.Done()
	defer c.Close()

	state := &connstate.ConnState{}
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
				slog.Warn("read frame error", "client", state.ClientID, "err", err)
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
			slog.Warn("write frame error", "client", state.ClientID, "err", err)
			return
		}
		if err := bw.Flush(); err != nil {
			slog.Warn("flush error", "client", state.ClientID, "err", err)
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
