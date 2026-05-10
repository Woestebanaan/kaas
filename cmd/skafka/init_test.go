package main

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// TestEnsureDataDirPermsChmodsRoot verifies that ensureDataDirPerms always
// applies 0o775 to dataDir so the broker (running under fsGroup) can mkdir
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

// TestLayerCStorageEngineUsesGroupWriteMkdir is the gh #110 layer-C
// invariant: every runtime MkdirAll under /data uses 0o775 so a missing
// init container can't cause a future cross-pod-write failure. Pinned
// here as a static check — guards against future "I'll just use 0o755"
// PRs that would silently re-introduce the original bug. Layers A
// (kubelet fsGroup), B (init container chown + chmod), C (every
// MkdirAll uses group-write) form the defence-in-depth stack.
func TestLayerCStorageEngineUsesGroupWriteMkdir(t *testing.T) {
	files := []string{
		"../../internal/storage/engine.go",
		"../../internal/storage/manifest.go",
		"../../internal/storage/producer_snapshot.go",
		"../../internal/storage/topicconfig.go",
		"../../internal/coordinator/txn_state.go",
		"../../internal/coordinator/offsets.go",
		"../../internal/coordinator/fence_log.go",
		"../../internal/assignment/store.go",
		"../../internal/fsutil/filewatch.go",
		"../../operator/controllers/acls.go",
		"../../operator/controllers/credentials.go",
		"../../operator/controllers/kafkatopic_controller.go",
		"main.go",
	}
	// Modes WITHOUT group-write that would re-introduce gh #110:
	// 0o755 (rwxr-xr-x), 0o750 (rwxr-x---), 0o700 (rwx------).
	// Match Go's two octal forms: `0755` (legacy) and `0o755`.
	bad := regexp.MustCompile(`MkdirAll\([^,]+,\s*(0o?755|0o?750|0o?700)\b`)
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if matches := bad.FindAllString(string(data), -1); len(matches) > 0 {
			t.Errorf("%s contains %d no-group-write MkdirAll callsite(s) — would re-introduce gh #110 layer-C breakage:\n  %v",
				f, len(matches), matches)
		}
	}
}
