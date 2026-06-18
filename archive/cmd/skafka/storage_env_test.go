package main

import (
	"testing"

	"github.com/woestebanaan/skafka/internal/storage"
)

// TestApplyStorageEnv_FlushIntervalMessages locks the durability dial
// (gh #83) so a future refactor of the env-var plumbing can't silently
// regress it. Cases cover: unset → default kept, valid override, invalid
// → default kept, and the special 0 = "no message-driven flush" value.
func TestApplyStorageEnv_FlushIntervalMessages(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want int64
	}{
		{"unset keeps default 1", "", 1},
		{"valid override 100", "100", 100},
		{"valid override 0 disables flush", "0", 0},
		{"negative is rejected, default kept", "-5", 1},
		{"non-numeric is rejected, default kept", "abc", 1},
		{"empty string keeps default", "", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env == "" {
				t.Setenv("SKAFKA_FLUSH_INTERVAL_MESSAGES", "")
			} else {
				t.Setenv("SKAFKA_FLUSH_INTERVAL_MESSAGES", tc.env)
			}
			cfg := applyStorageEnv(storage.DefaultConfig())
			if cfg.FlushIntervalMessages != tc.want {
				t.Errorf("FlushIntervalMessages = %d, want %d (env=%q)",
					cfg.FlushIntervalMessages, tc.want, tc.env)
			}
		})
	}
}
