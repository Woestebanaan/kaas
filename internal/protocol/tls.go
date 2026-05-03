package protocol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/woestebanaan/skafka/internal/observability"
)

// TLSOption is a functional option for WatchingCertificate. WithRequireClientCert
// is the only one defined today; future options (cipher suites, ALPN, etc.)
// can plug in here without breaking the call site.
type TLSOption func(*tls.Config) error

// WithRequireClientCert enforces mTLS: clients must present a certificate
// signed by one of the CAs in caBundleFile, and the server fails the
// handshake otherwise. Without this option, TLS is opportunistic — clients
// can connect cert-less and authenticate via SASL instead.
//
// The CA bundle is loaded once at server start. Bundle rotation is NOT
// hot-reloaded today (unlike the server cert); rotating the trust anchor
// is a much rarer operational event and is a future enhancement.
func WithRequireClientCert(caBundleFile string) TLSOption {
	return func(cfg *tls.Config) error {
		if caBundleFile == "" {
			return fmt.Errorf("tls: WithRequireClientCert: empty caBundleFile")
		}
		pem, err := os.ReadFile(caBundleFile)
		if err != nil {
			return fmt.Errorf("tls: read CA bundle %s: %w", caBundleFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("tls: no valid PEM certs in %s", caBundleFile)
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
		return nil
	}
}

// WithClientCAPool is a test hook: same as WithRequireClientCert but
// takes the cert pool directly. Lets the mTLS compat test generate CA
// certs in-memory without round-tripping through a file.
func WithClientCAPool(pool *x509.CertPool) TLSOption {
	return func(cfg *tls.Config) error {
		if pool == nil {
			return fmt.Errorf("tls: WithClientCAPool: nil pool")
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
		return nil
	}
}

// WatchingCertificate loads a TLS key pair from disk and reloads it whenever the
// cert or key file is written. Returns a *tls.Config whose GetCertificate hook
// reads the latest cert via an atomic.Pointer, so rotation is picked up on the
// next TLS handshake — no server restart required.
//
// Rotation is detected via fsnotify WRITE events. Debounced 200ms to handle
// cert-manager atomic-rename-style updates (Secret remount replaces the files).
//
// Pass WithRequireClientCert(caFile) to enforce mTLS — without it, the
// listener accepts cert-less clients and they fall through to SASL.
func WatchingCertificate(certFile, keyFile string, opts ...TLSOption) (*tls.Config, error) {
	var current atomic.Pointer[tls.Certificate]

	initial, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load initial key pair: %w", err)
	}
	current.Store(&initial)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("tls: fsnotify: %w", err)
	}

	// Watch the directories of both files — Kubernetes Secret mounts rotate via
	// a symlinked ..data directory, so watching the file path directly misses
	// many events. Watching the parent directory catches all update styles.
	dirs := uniqueDirs(certFile, keyFile)
	for _, d := range dirs {
		if err := watcher.Add(d); err != nil {
			_ = watcher.Close()
			return nil, fmt.Errorf("tls: watch %s: %w", d, err)
		}
	}

	go watchLoop(watcher, certFile, keyFile, &current)

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			c := current.Load()
			if c == nil {
				return nil, fmt.Errorf("tls: no certificate loaded")
			}
			return c, nil
		},
	}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			_ = watcher.Close()
			return nil, err
		}
	}
	return cfg, nil
}

func watchLoop(w *fsnotify.Watcher, certFile, keyFile string, current *atomic.Pointer[tls.Certificate]) {
	const debounce = 200 * time.Millisecond
	var timer *time.Timer

	reload := func() {
		kp, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			slog.Warn("tls: reload failed", "err", err)
			return
		}
		current.Store(&kp)
		observability.Global().CertReloads.Add(context.Background(), 1)
		slog.Info("tls: certificate reloaded", "cert", certFile)
	}

	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				return
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(debounce, reload)

		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			slog.Warn("tls: watcher error", "err", err)
		}
	}
}

func uniqueDirs(paths ...string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, p := range paths {
		d := dirOf(p)
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
