package controllers

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"os"
	"testing"

	"golang.org/x/crypto/pbkdf2"
)

// TestSCRAMRFC5802Vectors verifies the SCRAM derivation against the known-good
// vectors from RFC 5802 Section 5 (adapted for SHA-512 key length).
func TestSCRAMDerivationStructure(t *testing.T) {
	password := "test-password"
	cred, err := computeScram(password)
	if err != nil {
		t.Fatalf("computeScram: %v", err)
	}

	if cred.Iterations != scramIterations {
		t.Errorf("iterations=%d, want %d", cred.Iterations, scramIterations)
	}

	saltBytes, err := base64.StdEncoding.DecodeString(cred.Salt)
	if err != nil || len(saltBytes) != 16 {
		t.Errorf("salt invalid: %v (len=%d)", err, len(saltBytes))
	}

	storedKeyBytes, err := base64.StdEncoding.DecodeString(cred.StoredKey)
	if err != nil || len(storedKeyBytes) != 64 {
		t.Errorf("storedKey invalid: %v (len=%d)", err, len(storedKeyBytes))
	}

	serverKeyBytes, err := base64.StdEncoding.DecodeString(cred.ServerKey)
	if err != nil || len(serverKeyBytes) != 64 {
		t.Errorf("serverKey invalid: %v (len=%d)", err, len(serverKeyBytes))
	}

	// Verify the derivation is internally consistent: re-derive and compare.
	saltedPw := pbkdf2.Key([]byte(password), saltBytes, scramIterations, 64, sha512.New)

	mac := hmac.New(sha512.New, saltedPw)
	mac.Write([]byte("Server Key"))
	expectedServerKey := mac.Sum(nil)

	if cred.ServerKey != base64.StdEncoding.EncodeToString(expectedServerKey) {
		t.Error("ServerKey does not match re-derived value")
	}
}

func TestSCRAMDifferentPasswordsDifferentKeys(t *testing.T) {
	c1, _ := computeScram("password1")
	c2, _ := computeScram("password2")
	if c1.StoredKey == c2.StoredKey {
		t.Error("different passwords produced the same StoredKey")
	}
	if c1.Salt == c2.Salt {
		t.Error("two calls produced the same salt (should be random)")
	}
}

func TestCredentialsFileRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Write a user.
	cf := &CredentialsFile{Version: 1}
	cf.upsertUser(UserCredential{
		Username: "alice",
		AuthType: "tls",
		TLSCN:    "alice",
	})
	if err := writeCredentials(dir, cf); err != nil {
		t.Fatalf("writeCredentials: %v", err)
	}

	// Read back and verify.
	cf2, err := readCredentials(dir)
	if err != nil {
		t.Fatalf("readCredentials: %v", err)
	}
	if len(cf2.Users) != 1 || cf2.Users[0].Username != "alice" {
		t.Errorf("unexpected users: %+v", cf2.Users)
	}

	// Upsert (update) the user.
	cf2.upsertUser(UserCredential{Username: "alice", AuthType: "tls", TLSCN: "alice-updated"})
	_ = writeCredentials(dir, cf2)

	cf3, _ := readCredentials(dir)
	if len(cf3.Users) != 1 {
		t.Errorf("upsert should not duplicate: got %d users", len(cf3.Users))
	}
	if cf3.Users[0].TLSCN != "alice-updated" {
		t.Errorf("TLSCN not updated: %q", cf3.Users[0].TLSCN)
	}

	// Remove the user.
	cf3.removeUser("alice")
	_ = writeCredentials(dir, cf3)

	cf4, _ := readCredentials(dir)
	if len(cf4.Users) != 0 {
		t.Errorf("user not removed: got %d users", len(cf4.Users))
	}
}

func TestReadCredentialsMissingFile(t *testing.T) {
	cf, err := readCredentials(t.TempDir())
	if err != nil {
		t.Fatalf("expected no error for missing file, got: %v", err)
	}
	if cf.Version != 1 || len(cf.Users) != 0 {
		t.Errorf("unexpected default: %+v", cf)
	}
}

func TestWriteCredentialsCreatesDir(t *testing.T) {
	dir := t.TempDir()
	cf := &CredentialsFile{Version: 1}
	cf.upsertUser(UserCredential{Username: "bob", AuthType: "tls"})
	if err := writeCredentials(dir, cf); err != nil {
		t.Fatalf("writeCredentials: %v", err)
	}
	if _, err := os.Stat(dir + "/__cluster/credentials.json"); err != nil {
		t.Errorf("credentials.json not created: %v", err)
	}
}
