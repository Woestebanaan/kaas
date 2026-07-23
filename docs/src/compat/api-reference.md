# Per-API reference

Grouped-by-domain reference for each implemented API key: versions, semantics, deviations from Apache Kafka 3.7, source paths, test coverage.

The [generated matrix](api-matrix.md) tells you *which* versions of each
API kaas serves; these pages tell you *how it behaves* — the semantics
behind each key, and every place behaviour differs from Apache Kafka
3.7, stated plainly. Every registered key is documented across seven
domain pages, grouped so related behaviour (and shared deviations) read
in one place, with a stable anchor per key that the matrix's "Reference"
column links to:

- [Produce, Fetch, ListOffsets & Metadata](api/produce-fetch.md) — the
  data plane
- [Consumer-group APIs](api/consumer-groups.md) — find/join/sync/commit
  and the admin group surface
- [Transaction APIs](api/transactions.md) — the EOS surface
- [Topic & config admin APIs](api/topics-configs.md)
- [ACL & quota admin APIs](api/acls-quotas.md)
- [SASL authentication APIs](api/auth.md)
- [Cluster & log-dir APIs](api/cluster-misc.md)

Every anchor follows the same template:

1. **Purpose** — what the API does in one or two sentences.
2. **Versions** — the supported range, matching the registry (the matrix
   is generated from it, so a mismatch here is a doc bug by definition).
3. **Handling** — how the request flows through kaas.
4. **Deviations** — where behaviour differs from Apache Kafka 3.7,
   stated plainly.
5. **Source** — handler and codec paths (contributor material, kept out
   of the narrative).
6. **Verified by** — unit/integration tests and `scripts/kafka-*.sh`
   scenarios.
