package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestEnsureDataDirPermsChmodsRoot verifies that ensureDataDirPerms always
// applies 0775 to dataDir so the broker (running under fsGroup) can mkdir
// new topic dirs at runtime, even when the partition-init initContainer
// somehow runs as a non-root user (dev mode).
func TestEnsureDataDirPermsChmodsRoot(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("seed chmod: %v", err)
	}
	if err := ensureDataDirPerms(dir, os.Geteuid(), os.Getegid()); err != nil {
		t.Fatalf("ensureDataDirPerms: %v", err)
	}
	st, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := st.Mode().Perm(); got != 0o775 {
		t.Fatalf("dataDir perms: got %o want %o", got, 0o775)
	}
}

// TestEnsureDataDirPermsNonRootSkipsChown verifies that when not running as
// root, ensureDataDirPerms doesn't error even though chown is a no-op.
// The function is exercised in dev mode (go test runs as a normal user)
// and on production clusters (init container runs as root).
func TestEnsureDataDirPermsNonRootSkipsChown(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("test exercises the non-root branch")
	}
	dir := t.TempDir()
	sub := filepath.Join(dir, "topic-a", "0")
	if err := os.MkdirAll(sub, 0o775); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	// Should succeed without error even though we can't chown.
	if err := ensureDataDirPerms(dir, 65532, 65532); err != nil {
		t.Fatalf("ensureDataDirPerms: %v", err)
	}
}

// TestEnsureDataDirPermsMissingDir surfaces a clear error rather than
// silently succeeding when SKAFKA_DATA_DIR points to a non-existent
// path — runInit's MkdirAll precedes ensureDataDirPerms so this is
// belt-and-braces, but worth pinning.
func TestEnsureDataDirPermsMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	err := ensureDataDirPerms(missing, os.Geteuid(), os.Getegid())
	if err == nil {
		t.Fatal("expected error for missing dir, got nil")
	}
}
