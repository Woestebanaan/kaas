package storage

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestParseSegmentStem(t *testing.T) {
	cases := []struct {
		stem string
		want int64
		ok   bool
	}{
		{"00000000000000000000", 0, true},                   // legacy zero
		{"00000000000000000123", 123, true},                 // legacy non-zero
		{"00000005-00000000000000000123", 123, true},        // epoch-prefixed
		{"deadbeef-00000000000000999999", 999999, true},     // hex epoch
		{"GG-123", 0, false},                                // not hex (G is invalid)
		{"foo", 0, false},                                   // not a stem at all
		{"00000000-foo", 0, false},                          // good prefix, bad offset
		{"-00000000000000000000", 0, false},                 // empty epoch portion
	}
	for _, tc := range cases {
		got, ok := parseSegmentStem(tc.stem)
		if ok != tc.ok || got != tc.want {
			t.Errorf("parseSegmentStem(%q) = (%d,%v), want (%d,%v)", tc.stem, got, ok, tc.want, tc.ok)
		}
	}
}

func TestListSegmentsParsesBothFormats(t *testing.T) {
	// Mixed directory: one legacy unprefixed file + one epoch-prefixed file.
	dir := t.TempDir()
	mustTouch(t, filepath.Join(dir, "00000000000000000000.log"))
	mustTouch(t, filepath.Join(dir, "00000000000000000000.index"))
	mustTouch(t, filepath.Join(dir, "00000003-00000000000000000100.log"))
	mustTouch(t, filepath.Join(dir, "00000003-00000000000000000100.index"))

	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 2 {
		t.Fatalf("listSegments returned %d entries, want 2: %+v", len(segs), segs)
	}
	// Sorted by baseOffset; legacy at 0 comes first.
	sort.Slice(segs, func(i, j int) bool { return segs[i].baseOffset < segs[j].baseOffset })
	if segs[0].baseOffset != 0 || segs[1].baseOffset != 100 {
		t.Errorf("unexpected baseOffsets: %d, %d", segs[0].baseOffset, segs[1].baseOffset)
	}
	// indexPath must mirror logPath (same stem, .index suffix).
	for _, s := range segs {
		want := s.logPath[:len(s.logPath)-len(".log")] + ".index"
		if s.indexPath != want {
			t.Errorf("indexPath %q != %q", s.indexPath, want)
		}
	}
}

func TestListSegmentsSkipsSealedMarkers(t *testing.T) {
	dir := t.TempDir()
	mustTouch(t, filepath.Join(dir, "00000000-00000000000000000000.log"))
	mustTouch(t, filepath.Join(dir, "00000000-00000000000000000000.log.sealed"))

	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Errorf("listSegments returned %d, want 1 (sealed marker should be skipped): %+v", len(segs), segs)
	}
}

func TestSegmentLogPathFormat(t *testing.T) {
	got := segmentLogPath("/data/topic/0", 12345, 7)
	want := "/data/topic/0/00000007-00000000000000012345.log"
	if got != want {
		t.Errorf("segmentLogPath: %q, want %q", got, want)
	}
}

func TestCreateSegmentEmitsEpochPrefixedFiles(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(dir, 50, 9)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.close()

	if _, err := os.Stat(filepath.Join(dir, "00000009-00000000000000000050.log")); err != nil {
		t.Errorf("expected new-format log file, got err %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "00000009-00000000000000000050.index")); err != nil {
		t.Errorf("expected new-format index file, got err %v", err)
	}
	if seg.epoch != 9 {
		t.Errorf("activeSegment.epoch=%d, want 9", seg.epoch)
	}
}

func mustTouch(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte{}, 0644); err != nil {
		t.Fatalf("touch %s: %v", path, err)
	}
}
