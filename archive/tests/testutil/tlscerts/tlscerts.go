// Package tlscerts generates throwaway CA + server + client certificates
// in-memory for tests that exercise TLS-shaped flows. Not for production.
//
// Lives outside internal/ so external test packages
// (tests/kafka-compat, tests/integration) can import it without forming
// a build cycle through controller/broker test helpers.
package tlscerts

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

// Bundle is a self-contained CA + server + client cert chain suitable
// for spinning up a TLS-with-mTLS broker in a test.
type Bundle struct {
	CACert     []byte // PEM-encoded CA cert
	CAPool     *x509.CertPool
	ServerCert []byte // PEM-encoded server cert (signed by CA)
	ServerKey  []byte // PEM-encoded server private key
	ClientCert []byte // PEM-encoded client cert with CN
	ClientKey  []byte // PEM-encoded client private key
	ClientCN   string // the CN baked into the client cert

	// Retained so the bundle can re-issue additional server certs
	// against the same trust anchor — see IssueServerCert.
	caCertParsed *x509.Certificate
	caKey        *ecdsa.PrivateKey
}

// IssueServerCert generates a new server cert + key pair signed by the
// bundle's CA, with the given SANs. Used for cert-rotation tests: the
// initial cert comes from NewBundle*; this method produces a follow-up
// cert with a different serial number but the same trust chain so a
// client trusting bundle.CAPool accepts both.
func (b *Bundle) IssueServerCert(serverSANs []string) (certPEM, keyPEM []byte, err error) {
	var dnsNames []string
	var ipSANs []net.IP
	for _, s := range serverSANs {
		if ip := net.ParseIP(s); ip != nil {
			ipSANs = append(ipSANs, ip)
		} else {
			dnsNames = append(dnsNames, s)
		}
	}
	return signLeaf(b.caCertParsed, b.caKey, "skafka-server", dnsNames, ipSANs, x509.ExtKeyUsageServerAuth)
}

// NewBundle builds a fresh cert bundle. serverHost is the SAN entry on
// the server cert ("127.0.0.1" for in-process tests). clientCN is the
// CN baked into the client cert; the broker's mTLS path looks this up
// against credentials.json's tlsCN map to derive the principal.
func NewBundle(serverHost, clientCN string) (*Bundle, error) {
	return NewBundleWithSANs([]string{serverHost}, clientCN)
}

// NewBundleWithSANs is like NewBundle but lets the server cert carry
// multiple SAN entries — needed for the per-broker external listener
// test where one cert serves SNI for broker-0.localhost,
// broker-1.localhost, etc. Each SAN that parses as an IP is added as
// an IP SAN; everything else becomes a DNS SAN.
func NewBundleWithSANs(serverSANs []string, clientCN string) (*Bundle, error) {
	// --- CA ---
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ca key: %w", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "skafka-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("ca cert: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("ca parse: %w", err)
	}
	caPEM := encodeCert(caDER)

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	// --- Server cert (signed by CA) ---
	var dnsNames []string
	var ipSANs []net.IP
	for _, s := range serverSANs {
		if ip := net.ParseIP(s); ip != nil {
			ipSANs = append(ipSANs, ip)
		} else {
			dnsNames = append(dnsNames, s)
		}
	}
	serverPEM, serverKeyPEM, err := signLeaf(caCert, caKey, "skafka-server", dnsNames, ipSANs, x509.ExtKeyUsageServerAuth)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}

	// --- Client cert (signed by CA) ---
	clientPEM, clientKeyPEM, err := signLeaf(caCert, caKey, clientCN, nil, nil, x509.ExtKeyUsageClientAuth)
	if err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}

	return &Bundle{
		CACert:       caPEM,
		CAPool:       pool,
		ServerCert:   serverPEM,
		ServerKey:    serverKeyPEM,
		ClientCert:   clientPEM,
		ClientKey:    clientKeyPEM,
		ClientCN:     clientCN,
		caCertParsed: caCert,
		caKey:        caKey,
	}, nil
}

// signLeaf generates a leaf cert (server or client) signed by the given CA.
func signLeaf(
	caCert *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	cn string,
	dnsNames []string,
	ips []net.IP,
	extKeyUsage x509.ExtKeyUsage,
) (certPEM, keyPEM []byte, err error) {
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("leaf key: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{extKeyUsage},
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("leaf cert: %w", err)
	}
	certPEM = encodeCert(der)

	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return nil, nil, fmt.Errorf("leaf key marshal: %w", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

func encodeCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
