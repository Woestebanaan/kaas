package auth

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCredentialLoaderSCRAM(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	cf := credFile{Version: 1, Users: []credUser{
		{
			Username: "alice",
			AuthType: "scram-sha-512",
			Scram: &scramJSON{
				Salt:       base64.StdEncoding.EncodeToString([]byte("saltsaltsaltsalt")),
				StoredKey:  base64.StdEncoding.EncodeToString(make([]byte, 64)),
				ServerKey:  base64.StdEncoding.EncodeToString(make([]byte, 64)),
				Iterations: 8192,
			},
		},
	}}
	data, _ := json.Marshal(cf)
	_ = os.WriteFile(path, data, 0644)

	l := NewCredentialLoader(path)
	if err := l.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	_, _, salt, iter, ok := l.LookupSCRAM("alice")
	if !ok {
		t.Fatal("alice not found")
	}
	if string(salt) != "saltsaltsaltsalt" || iter != 8192 {
		t.Errorf("unexpected salt/iter: %s / %d", salt, iter)
	}
	if _, _, _, _, ok := l.LookupSCRAM("missing"); ok {
		t.Error("missing user should not be found")
	}
}

func TestCredentialLoaderTLSAndSA(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	cf := credFile{Version: 1, Users: []credUser{
		{Username: "bob", AuthType: "tls", TLSCN: "bob-cn"},
		{Username: "carol-sa", AuthType: "kubernetes-serviceaccount", SA: &saJSON{Name: "carol", Namespace: "payments"}},
	}}
	data, _ := json.Marshal(cf)
	_ = os.WriteFile(path, data, 0644)

	l := NewCredentialLoader(path)
	_ = l.Reload()

	if u, ok := l.LookupTLS("bob-cn"); !ok || u != "bob" {
		t.Errorf("LookupTLS returned %q, %v", u, ok)
	}
	if !l.LookupSA("payments", "carol") {
		t.Error("SA payments/carol should be registered")
	}
	if l.LookupSA("other", "carol") {
		t.Error("SA other/carol should not be registered")
	}
}

func TestCredentialLoaderMissingFile(t *testing.T) {
	l := NewCredentialLoader(filepath.Join(t.TempDir(), "nonexistent.json"))
	if err := l.Reload(); err != nil {
		t.Errorf("Reload on missing file should not error, got: %v", err)
	}
}
