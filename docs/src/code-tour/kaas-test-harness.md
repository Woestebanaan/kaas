# kaas-test-harness

Shared test helpers — the only place in the workspace where a decoded-record representation is allowed to live.

Still essentially a stub, populated as integration tests need it. Its
charter is narrow and deliberate: test fixtures (including the
byte-opacity fixtures) and the `recordbatch` helper for *constructing*
RecordBatches in tests. Production crates must never grow a decoded-record
type — when a test needs one, it lives here, where the tripwire counters
can't be quietly bypassed ([wire protocol](../compat/wire-protocol.md)).
