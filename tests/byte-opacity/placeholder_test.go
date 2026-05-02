// Package byteopacity holds the byte-opacity round-trip tests required by
// v3.3 plan constraint #22 (codec must not decode RecordBatch payloads):
// produce a compressed batch, fetch it, assert the response payload is
// byte-identical to the request payload; CPU/allocation profile assertions;
// tripwire metrics (skafka_codec_record_decode_total,
// skafka_codec_batch_reencode_total) flat at zero.
//
// Phase 3 work — depends on the real storage engine, the produce/fetch hot
// path, and the observability metrics being wired. This file exists in
// Phase 1 only so the package compiles and `go test ./...` exits clean.
package byteopacity

import "testing"

func TestPlaceholder(t *testing.T) {
	t.Skip("byte-opacity tests land in Phase 3")
}
