package protocol

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeSelfSignedCert generates a fresh ECDSA key pair + self-signed cert and writes
// them to the given directory. Returns the certificate DNS name so tests can verify
// which cert was loaded.
func writeSelfSignedCert(t *testing.T, dir, dnsName string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{dnsName},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if err := os.WriteFile(filepath.Join(dir, "tls.crt"), certPEM, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestWatchingCertificateInitialLoad(t *testing.T) {
	dir := t.TempDir()
	writeSelfSignedCert(t, dir, "initial.example.com")

	cfg, err := WatchingCertificate(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"))
	if err != nil {
		t.Fatalf("WatchingCertificate: %v", err)
	}
	if cfg.GetCertificate == nil {
		t.Fatal("GetCertificate is nil")
	}
	cert, err := cfg.GetCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := x509.ParseCertificate(cert.Certificate[0])
	if parsed.Subject.CommonName != "initial.example.com" {
		t.Errorf("initial CN=%q, want initial.example.com", parsed.Subject.CommonName)
	}
}

func TestWatchingCertificateReload(t *testing.T) {
	dir := t.TempDir()
	writeSelfSignedCert(t, dir, "v1.example.com")

	cfg, err := WatchingCertificate(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"))
	if err != nil {
		t.Fatal(err)
	}

	// Rotate: write a new cert to the same path.
	writeSelfSignedCert(t, dir, "v2.example.com")

	// Wait for debounced reload (200ms + a bit).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cert, err := cfg.GetCertificate(nil)
		if err == nil && len(cert.Certificate) > 0 {
			p, _ := x509.ParseCertificate(cert.Certificate[0])
			if p.Subject.CommonName == "v2.example.com" {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("cert was not reloaded within 3s")
}

func TestWatchingCertificateBadFile(t *testing.T) {
	_, err := WatchingCertificate("/nonexistent/cert.pem", "/nonexistent/key.pem")
	if err == nil {
		t.Error("expected error for missing cert/key files")
	}
}
