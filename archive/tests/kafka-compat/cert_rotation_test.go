package kafkacompat

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/woestebanaan/skafka/internal/protocol"
	"github.com/woestebanaan/skafka/tests/testutil/tlscerts"
)

// TestTLSCertHotReload exercises the live-handshake side of the
// fsnotify cert-reload path (Phase 9 Gap #4). The unit test in
// internal/protocol/tls_test.go covers the in-process tls.Config
// reload; this test closes the loop by:
//
//  1. Starting a TLS listener that uses protocol.WatchingCertificate.
//  2. Doing one TLS handshake — peer cert serial S_A.
//  3. Atomically replacing the cert + key files on disk.
//  4. Waiting past the watcher's 200ms debounce.
//  5. Doing a fresh TLS handshake — peer cert serial S_B != S_A.
//
// This is the MVP-checklist row "cert-manager Certificate rotates
// without pod restart" exercised end-to-end without needing
// cert-manager itself.
func TestTLSCertHotReload(t *testing.T) {
	bundle, err := tlscerts.NewBundle("127.0.0.1", "unused-client-cn")
	if err != nil {
		t.Fatalf("NewBundle: %v", err)
	}

	// Cert-B: a second server cert signed by the same CA, different
	// serial number, same SANs. The client trusts bundle.CAPool so
	// both certs validate.
	certB, keyB, err := bundle.IssueServerCert([]string{"127.0.0.1"})
	if err != nil {
		t.Fatalf("IssueServerCert: %v", err)
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, bundle.ServerCert, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, bundle.ServerKey, 0600); err != nil {
		t.Fatal(err)
	}

	tlsCfg, err := protocol.WatchingCertificate(certFile, keyFile)
	if err != nil {
		t.Fatalf("WatchingCertificate: %v", err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	// Accept loop: complete the handshake, then close. Production
	// code path uses a Kafka dispatcher here; for cert rotation we
	// only care that the handshake completes with whichever cert
	// is currently on disk.
	var wg sync.WaitGroup
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				tc := conn.(*tls.Conn)
				_ = tc.Handshake()
				_ = tc.Close()
			}()
		}
	}()
	defer wg.Wait()

	// Round 1: handshake against cert-A.
	peerA := dialAndPeerCert(t, addr, bundle.CAPool)

	// Atomic-rename rotation: write the new pair into sibling tmp
	// files, then rename them into place. This matches how kubelet's
	// projected-volume + cert-manager Secret remount style updates
	// look on disk — fsnotify sees Rename + Create on the parent dir.
	tmpCert := filepath.Join(dir, "tls.crt.new")
	tmpKey := filepath.Join(dir, "tls.key.new")
	if err := os.WriteFile(tmpCert, certB, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmpKey, keyB, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpCert, certFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpKey, keyFile); err != nil {
		t.Fatal(err)
	}

	// 200ms debounce in watchLoop + slack for the disk read. The
	// reload runs in a goroutine; we just need to be past the timer.
	time.Sleep(500 * time.Millisecond)

	// Round 2: handshake against cert-B.
	peerB := dialAndPeerCert(t, addr, bundle.CAPool)

	if peerA.SerialNumber.Cmp(peerB.SerialNumber) == 0 {
		t.Fatalf("cert serial unchanged after rotation — watchLoop did not pick up the new cert.\n"+
			"  serial: %v\n"+
			"  hint: confirm the parent-dir watch is still installed and the 200ms debounce hasn't been increased",
			peerA.SerialNumber)
	}

	// Sanity: the new serial must match cert-B exactly.
	wantSerial, err := serialFromPEM(certB)
	if err != nil {
		t.Fatalf("parse cert-B: %v", err)
	}
	if peerB.SerialNumber.Cmp(wantSerial) != 0 {
		t.Errorf("after rotation: peer serial=%v, want cert-B serial=%v",
			peerB.SerialNumber, wantSerial)
	}
}

func dialAndPeerCert(t *testing.T, addr string, caPool *x509.CertPool) *x509.Certificate {
	t.Helper()
	host, _, _ := net.SplitHostPort(addr)
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs:    caPool,
		ServerName: host,
		MinVersion: tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("tls.Dial(%s): %v", addr, err)
	}
	defer conn.Close()

	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		t.Fatal("no peer certificates")
	}
	return state.PeerCertificates[0]
}

func serialFromPEM(certPEM []byte) (*big.Int, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in %d bytes", len(certPEM))
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	return cert.SerialNumber, nil
}
