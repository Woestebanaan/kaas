package main

import (
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/woestebanaan/skafka/internal/storage"
)

// envOr returns os.Getenv(key), or def when the var is unset / empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envOrInt parses os.Getenv(key) as a base-10 int; returns def on
// unset / parse error.
func envOrInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envSecondsOr reads an env var as an integer count of seconds and
// returns it as a time.Duration. Empty / unparseable / negative
// returns def. Used for the controllerLease.* knobs — passing 0 to
// the cluster runtime falls back to controller.New's hardcoded
// defaults.
func envSecondsOr(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return time.Duration(n) * time.Second
}

// applyStorageEnv overlays storage.Config with values from environment
// variables. Recognised vars:
//
//   SKAFKA_FLUSH_INTERVAL_MESSAGES — durability/throughput dial. Mirrors
//     Apache Kafka's log.flush.interval.messages topic config (gh #83).
//     1 (default) = fsync per record; N > 1 = fsync every N records;
//     0 = no message-driven flush (only segment roll).
//
//   SKAFKA_FSYNC_MAX_LATENCY_MS — fsync watchdog deadline (gh #95).
//     30000 ms (default) = surface ErrStorageStalled if a single
//     committer fsync exceeds 30 s. 0 disables the watchdog so Sync
//     can block indefinitely (pre-#95 behaviour).
//
// Invalid values are logged and ignored so a typo doesn't crash the broker.
func applyStorageEnv(cfg storage.Config) storage.Config {
	if v := os.Getenv("SKAFKA_FLUSH_INTERVAL_MESSAGES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			cfg.FlushIntervalMessages = n
		} else {
			slog.Warn("invalid SKAFKA_FLUSH_INTERVAL_MESSAGES, keeping default",
				"value", v, "default", cfg.FlushIntervalMessages)
		}
	}
	if v := os.Getenv("SKAFKA_FSYNC_MAX_LATENCY_MS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			cfg.FsyncMaxLatency = time.Duration(n) * time.Millisecond
		} else {
			slog.Warn("invalid SKAFKA_FSYNC_MAX_LATENCY_MS, keeping default",
				"value", v, "default", cfg.FsyncMaxLatency)
		}
	}
	return cfg
}
