//! Byte-opacity tripwire counters.
//!
//! Port of `archive/internal/observability/byteopacity.go`. The
//! rewrite plan's load-bearing invariant is "the broker is a byte
//! mover, not a byte interpreter": no code path should decode
//! individual records or re-encode a `RecordBatch`. These counters
//! MUST stay at zero in steady state — every increment is a bug and
//! the `SkafkaByteOpacityViolated` alert fires on non-zero.
//!
//! As of v1 no skafka code path calls these. If you find yourself
//! adding the first call, stop: a use case that genuinely requires
//! record-level inspection needs a separate design discussion before
//! it lands. See [`crate::metrics::Metrics::codec_record_decode`].

use opentelemetry::KeyValue;

/// Record a byte-opacity tripwire (record decode). `site` names the
/// offending code path so the alert payload is actionable.
pub fn bump_codec_record_decode(site: &'static str) {
    let m = crate::metrics::global();
    m.codec_record_decode.add(1, &[KeyValue::new("site", site)]);
    tracing::warn!(site, "byte-opacity tripwire fired (record decode)");
}

/// Record a byte-opacity tripwire (batch re-encode). `site` names the
/// offending code path so the alert payload is actionable.
pub fn bump_codec_batch_reencode(site: &'static str) {
    let m = crate::metrics::global();
    m.codec_batch_reencode
        .add(1, &[KeyValue::new("site", site)]);
    tracing::warn!(site, "byte-opacity tripwire fired (batch reencode)");
}
