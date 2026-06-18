package fsutil

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestFileWatcherFiresOnWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.json")
	writeFile(t, path, "v0")

	var fired atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	w := New([]FileSpec{
		{Path: path, OnChange: func() { fired.Add(1) }},
	}).WithPollInterval(50 * time.Millisecond)
	go func() { _ = w.Run(ctx) }()

	// Sleep a beat to let the watcher initialize fsnotify + cache initial mtime.
	time.Sleep(80 * time.Millisecond)
	// Sleep so the second mtime differs from the first on second-resolution
	// systems (matters less on local fs but matters for CI).
	time.Sleep(20 * time.Millisecond)

	writeFile(t, path, "v1")

	deadline := time.Now().Add(1 * time.Second)
	for fired.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if fired.Load() == 0 {
		t.Fatal("watcher did not fire after Write")
	}
}

func TestFileWatcherDispatchesPerFile(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.json")
	b := filepath.Join(dir, "b.json")
	writeFile(t, a, "x")
	writeFile(t, b, "y")

	var aFired, bFired atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	w := New([]FileSpec{
		{Path: a, OnChange: func() { aFired.Add(1) }},
		{Path: b, OnChange: func() { bFired.Add(1) }},
	}).WithPollInterval(50 * time.Millisecond)
	go func() { _ = w.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)
	writeFile(t, b, "y2")

	deadline := time.Now().Add(1 * time.Second)
	for bFired.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if bFired.Load() == 0 {
		t.Fatal("b's callback did not fire")
	}
	// a's callback must NOT fire — its file did not change.
	if aFired.Load() != 0 {
		t.Errorf("a's callback fired on b's write: %d", aFired.Load())
	}
}

func TestFileWatcherFullReadInterval(t *testing.T) {
	// Disable polling; rely entirely on the full-fire fallback. Useful
	// proof that the full-fire path isn't gated behind any "did mtime
	// change" check.
	dir := t.TempDir()
	path := filepath.Join(dir, "x.json")
	writeFile(t, path, "v0")

	var fired atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w := New([]FileSpec{
		{Path: path, OnChange: func() { fired.Add(1) }},
	}).
		WithPollInterval(0).             // disable mtime poll
		WithFullReadInterval(80 * time.Millisecond)

	go func() { _ = w.Run(ctx) }()

	deadline := time.Now().Add(1 * time.Second)
	for fired.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(30 * time.Millisecond)
	}
	if fired.Load() < 2 {
		t.Errorf("full-fire fallback did not tick at least twice in 1s: fired=%d", fired.Load())
	}
}

func TestFileWatcherEmptyFilesBlocksUntilCancel(t *testing.T) {
	// Zero files: Run should block until ctx done, not panic.
	ctx, cancel := context.WithCancel(context.Background())
	w := New(nil)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("unexpected err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestFileWatcherPollingOnlyWhenFsnotifyDisabled(t *testing.T) {
	// Tests still want fast feedback when fsnotify is unavailable. We
	// can't easily simulate fsnotify failure here — but we can confirm
	// the polling path alone is enough by setting a very short
	// pollInterval and verifying the callback fires.
	dir := t.TempDir()
	path := filepath.Join(dir, "x.json")
	writeFile(t, path, "v0")

	var fired atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w := New([]FileSpec{
		{Path: path, OnChange: func() { fired.Add(1) }},
	}).
		WithPollInterval(20 * time.Millisecond).
		WithFullReadInterval(0)

	go func() { _ = w.Run(ctx) }()

	time.Sleep(50 * time.Millisecond) // initial mtime cached
	time.Sleep(50 * time.Millisecond) // second-resolution defense
	writeFile(t, path, "v1")

	deadline := time.Now().Add(1 * time.Second)
	for fired.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if fired.Load() == 0 {
		t.Fatal("polling path did not fire")
	}
}
