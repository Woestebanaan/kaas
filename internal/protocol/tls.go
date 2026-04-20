package protocol

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/woestebanaan/skafka/internal/observability"
)

// WatchingCertificate loads a TLS key pair from disk and reloads it whenever the
// cert or key file is written. Returns a *tls.Config whose GetCertificate hook
// reads the latest cert via an atomic.Pointer, so rotation is picked up on the
// next TLS handshake — no server restart required.
//
// Rotation is detected via fsnotify WRITE events. Debounced 200ms to handle
// cert-manager atomic-rename-style updates (Secret remount replaces the files).
func WatchingCertificate(certFile, keyFile string) (*tls.Config, error) {
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

	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			c := current.Load()
			if c == nil {
				return nil, fmt.Errorf("tls: no certificate loaded")
			}
			return c, nil
		},
	}, nil
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
