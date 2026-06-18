package auth

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"strings"
	"testing"

	"golang.org/x/crypto/pbkdf2"
)

// staticStore is a CredentialStore backed by a fixed set of credentials for testing.
type staticStore struct {
	users map[string]staticCred
}

type staticCred struct {
	storedKey  []byte
	serverKey  []byte
	salt       []byte
	iterations int
}

func buildStaticStore(password string) *staticStore {
	salt := make([]byte, 16)
	// Use a fixed salt for deterministic tests.
	for i := range salt {
		salt[i] = byte(i + 1)
	}
	const iter = 4096
	saltedPw := pbkdf2.Key([]byte(password), salt, iter, 64, sha512.New)
	mac1 := hmac.New(sha512.New, saltedPw)
	mac1.Write([]byte("Client Key"))
	clientKey := mac1.Sum(nil)
	h := sha512.Sum512(clientKey)
	storedKey := h[:]
	mac2 := hmac.New(sha512.New, saltedPw)
	mac2.Write([]byte("Server Key"))
	serverKey := mac2.Sum(nil)
	return &staticStore{users: map[string]staticCred{
		"alice": {storedKey: storedKey, serverKey: serverKey, salt: salt, iterations: iter},
	}}
}

func (s *staticStore) LookupSCRAM(u string) ([]byte, []byte, []byte, int, bool) {
	c, ok := s.users[u]
	if !ok {
		return nil, nil, nil, 0, false
	}
	return c.storedKey, c.serverKey, c.salt, c.iterations, true
}
func (s *staticStore) LookupTLS(cn string) (string, bool)       { return cn, true }
func (s *staticStore) LookupSA(ns, name string) bool            { return true }
func (s *staticStore) LookupQuotas(_ string) *Quotas            { return nil }

func runSCRAMExchange(t *testing.T, password, username string) error {
	return runSCRAMExchangeWithServerPassword(t, password, password, username)
}

// runSCRAMExchangeWithServerPassword lets the server be set up with one password
// while the client sends a proof derived from another (for wrong-password tests).
func runSCRAMExchangeWithServerPassword(t *testing.T, serverPassword, clientPassword, username string) error {
	t.Helper()
	store := buildStaticStore(serverPassword)
	exch := NewScramExchange(store)
	password := clientPassword

	// Step 0: client-first
	clientFirst := []byte("n,,n=" + username + ",r=clientnonce123")
	serverFirstBytes, done, err := exch.Step(clientFirst)
	if err != nil {
		return err
	}
	if done {
		t.Error("expected done=false after client-first")
	}

	// Parse server-first to get nonce and salt.
	serverFirst := string(serverFirstBytes)
	attrs := parseAttrs(strings.TrimPrefix(serverFirst, "n,,"))
	// Actually serverFirst is already parsed form "r=...,s=...,i=..."
	atMap := make(map[string]string)
	for _, part := range strings.Split(serverFirst, ",") {
		if len(part) >= 2 && part[1] == '=' {
			atMap[string(part[0])] = part[2:]
		}
	}
	fullNonce := atMap["r"]
	saltB64 := atMap["s"]
	iterStr := atMap["i"]
	_ = attrs

	salt, _ := base64.StdEncoding.DecodeString(saltB64)
	iter := iterationsFromString(iterStr)

	// Client computes the proof.
	saltedPw := pbkdf2.Key([]byte(password), salt, iter, 64, sha512.New)
	mac1 := hmac.New(sha512.New, saltedPw)
	mac1.Write([]byte("Client Key"))
	clientKey := mac1.Sum(nil)
	h := sha512.Sum512(clientKey)
	storedKey := h[:]

	clientFirstBare := "n=" + username + ",r=clientnonce123"
	clientFinalWithoutProof := "c=biws,r=" + fullNonce
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof

	clientSig := hmacSHA512(storedKey, []byte(authMessage))
	clientProof := make([]byte, len(clientKey))
	for i := range clientKey {
		clientProof[i] = clientKey[i] ^ clientSig[i]
	}

	clientFinal := clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(clientProof)

	// Step 1: client-final
	serverFinalBytes, done, err := exch.Step([]byte(clientFinal))
	if err != nil {
		return err
	}
	if !done {
		t.Error("expected done=true after client-final")
	}

	// Verify server-final contains "v=..."
	serverFinal := string(serverFinalBytes)
	if !strings.HasPrefix(serverFinal, "v=") {
		t.Errorf("server-final should start with v=, got: %s", serverFinal)
	}

	p := exch.Principal()
	if p.Name != username {
		t.Errorf("principal name=%q, want %q", p.Name, username)
	}
	return nil
}

func TestSCRAMHappyPath(t *testing.T) {
	if err := runSCRAMExchange(t, "correct-password", "alice"); err != nil {
		t.Fatalf("SCRAM exchange failed: %v", err)
	}
}

func TestSCRAMWrongPassword(t *testing.T) {
	err := runSCRAMExchangeWithServerPassword(t, "server-password", "client-wrong-password", "alice")
	if err == nil {
		t.Error("expected error for wrong password, got nil")
	}
}

func TestSCRAMUnknownUser(t *testing.T) {
	store := buildStaticStore("pw")
	exch := NewScramExchange(store)
	_, _, err := exch.Step([]byte("n,,n=unknown,r=nonce"))
	if err == nil {
		t.Error("expected error for unknown user")
	}
}

func TestSCRAMMissingGS2Header(t *testing.T) {
	store := buildStaticStore("pw")
	exch := NewScramExchange(store)
	_, _, err := exch.Step([]byte("invalid-no-gs2-header"))
	if err == nil {
		t.Error("expected error for missing GS2 header")
	}
}

func TestSCRAMNonceMismatch(t *testing.T) {
	store := buildStaticStore("correct-password")
	exch := NewScramExchange(store)

	_, _, _ = exch.Step([]byte("n,,n=alice,r=clientnonce"))

	// Send client-final with wrong nonce.
	_, _, err := exch.Step([]byte("c=biws,r=WRONG-NONCE,p=AAAA"))
	if err == nil {
		t.Error("expected error for nonce mismatch")
	}
}
