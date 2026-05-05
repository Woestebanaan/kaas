package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// TopicConfigFile is the file the operator writes per topic so the broker
// can pick up per-topic retention / segment / compaction settings without
// going through a runtime API. The file lives at
// /data/<topic>/.config.json. Fields are pointers so "unset" is
// distinguishable from "set to 0".
type TopicConfigFile struct {
	RetentionMs        *int64 `json:"retentionMs,omitempty"`
	RetentionBytes     *int64 `json:"retentionBytes,omitempty"`
	SegmentBytes       *int64 `json:"segmentBytes,omitempty"`
	CleanupPolicy      string `json:"cleanupPolicy,omitempty"`
	MinCompactionLagMs *int64 `json:"minCompactionLagMs,omitempty"`
	DeleteRetentionMs  *int64 `json:"deleteRetentionMs,omitempty"`
}

// TopicConfigFileName is the file the operator writes inside /data/<topic>/.
const TopicConfigFileName = ".config.json"

// ReadTopicConfig loads the per-topic config file. Returns (nil, nil)
// when the file is absent — the broker should fall back to engine
// defaults in that case.
func ReadTopicConfig(topicDir string) (*TopicConfigFile, error) {
	b, err := os.ReadFile(filepath.Join(topicDir, TopicConfigFileName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read topic config: %w", err)
	}
	var c TopicConfigFile
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse topic config: %w", err)
	}
	return &c, nil
}

// WriteTopicConfig atomically writes the per-topic config file. Used by
// the operator's KafkaTopic reconciler.
func WriteTopicConfig(topicDir string, c *TopicConfigFile) error {
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(topicDir, TopicConfigFileName+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(topicDir, TopicConfigFileName))
}
