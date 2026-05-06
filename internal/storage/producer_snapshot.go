package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// producerSnapshotFilename is the name of the per-partition snapshot
// file that persists the idempotent-producer state across restarts
// and leader takeover. Apache Kafka writes per-segment .snapshot
// files; for skafka stage B2 we keep one snapshot per partition
// (much smaller dataset, simpler lifecycle). Atomic tmp + rename
// in the same dir, same convention as manifest.json.
const producerSnapshotFilename = "producer-state.snapshot"

// producerSnapshot is the on-disk encoding of a partition's
// producerStates map. Versioned so future stage-B refinements
// (e.g. an expanded retry window or per-segment snapshots) can
// extend the schema without breaking the open path.
type producerSnapshot struct {
	Version  int                       `json:"version"`
	Entries  []producerSnapshotEntry   `json:"entries"`
}

type producerSnapshotEntry struct {
	ProducerID int64                   `json:"producer_id"`
	Epoch      int16                   `json:"epoch"`
	Recent     []producerSnapshotBatch `json:"recent,omitempty"`
}

type producerSnapshotBatch struct {
	FirstSeq   int32 `json:"first_seq"`
	LastSeq    int32 `json:"last_seq"`
	BaseOffset int64 `json:"base_offset"`
}

const producerSnapshotVersion = 1

// readProducerSnapshot loads the producer-state file from dir and
// decodes it into a fresh producerStates map. Returns
// (nil, fs.ErrNotExist) when the file is missing — the caller (a
// partition opener) treats that as "fresh partition or stage A
// data, no state to recover".
//
// A snapshot that was written by a future schema version returns
// nil with no error so the caller starts fresh; we'd rather lose
// the dedupe window for one restart than refuse to open the
// partition entirely.
func readProducerSnapshot(dir string) (map[int64]*producerEntry, error) {
	path := filepath.Join(dir, producerSnapshotFilename)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}

	var snap producerSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("producer snapshot: parse %s: %w", path, err)
	}
	if snap.Version != producerSnapshotVersion {
		// Future schema version — start fresh rather than misinterpret bytes.
		return nil, nil
	}

	states := make(map[int64]*producerEntry, len(snap.Entries))
	for _, e := range snap.Entries {
		recent := make([]recentBatch, len(e.Recent))
		for i, rb := range e.Recent {
			recent[i] = recentBatch{firstSeq: rb.FirstSeq, lastSeq: rb.LastSeq, baseOffset: rb.BaseOffset}
		}
		states[e.ProducerID] = &producerEntry{epoch: e.Epoch, recent: recent}
	}
	return states, nil
}

// writeProducerSnapshot atomically writes the producer state to
// dir using the same tmp + fsync + rename dance as writeManifest.
// An empty states map writes an empty snapshot (rather than
// removing the file) so a future read sees the explicit "this
// partition had producers but the state was cleared" signal.
func writeProducerSnapshot(dir string, states map[int64]*producerEntry) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	snap := producerSnapshot{Version: producerSnapshotVersion}
	for pid, entry := range states {
		e := producerSnapshotEntry{
			ProducerID: pid,
			Epoch:      entry.epoch,
			Recent:     make([]producerSnapshotBatch, len(entry.recent)),
		}
		for i, rb := range entry.recent {
			e.Recent[i] = producerSnapshotBatch{
				FirstSeq:   rb.firstSeq,
				LastSeq:    rb.lastSeq,
				BaseOffset: rb.baseOffset,
			}
		}
		snap.Entries = append(snap.Entries, e)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}

	tmpPath := filepath.Join(dir, producerSnapshotFilename+".tmp")
	finalPath := filepath.Join(dir, producerSnapshotFilename)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}
