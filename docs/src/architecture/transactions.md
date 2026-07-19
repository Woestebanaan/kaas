# Transactions & idempotence

Idempotent-producer dedupe, the transaction coordinator state machine on slot-sharded JSON files, and EOS v2 end to end.

## Transaction state machine

Per-`transactional.id` state lives in `TxnEntry` records
(`crates/kaas-coordinator/src/txn_state.rs`), slot-sharded across
`/data/__cluster/txn_state/slot-N.json` (50 slots,
`fnv1a(transactional.id) % 50`). The states a transaction actually visits:

```mermaid
stateDiagram-v2
    [*] --> Empty : InitProducerId first allocation<br/>PID assigned, epoch 0
    Empty --> Ongoing : AddPartitionsToTxn /<br/>AddOffsetsToTxn<br/>stamps ongoingSinceMs
    Ongoing --> CompleteCommit : EndTxn(commit)<br/>clears partitions + groups,<br/>staged offsets committed
    Ongoing --> CompleteAbort : EndTxn(abort)<br/>staged offsets discarded
    Ongoing --> CompleteAbort : timeout reaper, 10 s sweep<br/>ongoingSinceMs + transactionTimeoutMs elapsed<br/>epoch bump, staged offsets discarded
    CompleteCommit --> Ongoing : AddPartitionsToTxn /<br/>AddOffsetsToTxn<br/>next transaction begins
    CompleteAbort --> Ongoing : AddPartitionsToTxn /<br/>AddOffsetsToTxn
```

Facts the diagram compresses (all from `txn_state.rs`):

- The `TxnState` enum also carries `PrepareCommit` / `PrepareAbort` variants for
  forward compatibility, but kaas never visits them: `end_txn` collapses
  prepare-then-complete into one atomic slot-file transition.
- `InitProducerId` on a **rejoin** does not reset the state: the entry keeps the
  same PID and bumps `epoch += 1` — fencing is purely the monotonic epoch. Only
  epoch overflow (`i16::MAX`) allocates a fresh PID and resets to `Empty`.
- A retried `EndTxn` in the matching `Complete*` state is answered idempotently
  (no second transition); a direction mismatch returns `INVALID_TXN_STATE`, and
  `EndTxn` on `Empty` is `INVALID_TXN_STATE` too. Epoch mismatches return
  `PRODUCER_FENCED` everywhere.

## EndTxn: commit flow

Since gh #175, cross-broker marker dispatch goes through a shared-PVC queue —
there is **no** WriteTxnMarkers RPC between brokers. `EndTxn` returns success as
soon as the queue entry is durably written; peer brokers apply markers
asynchronously.

```mermaid
flowchart TD
    producer["Producer: EndTxn(commit)"] --> handler["EndTxn handler on the txn coordinator broker<br/>owns_txn gate — otherwise NOT_COORDINATOR"]
    handler --> transition["TxnStateStore.end_txn<br/>Ongoing → CompleteCommit<br/>snapshot then clear partitions + groups, ongoingSinceMs = 0<br/>persist slot-N.json (tmp + fsync + rename)"]
    transition --> hook["offset hook, per recorded group<br/>commit → OffsetStore.commit_pending<br/>abort → OffsetStore.discard_pending"]
    hook --> split{"leader of each<br/>txn partition?"}
    split -- "self-led" --> local["write COMMIT control batch directly<br/>build_control_batch + engine.append, acks=-1"]
    split -- "peer-led" --> enqueue["marker queue enqueue<br/>marker_queue/to-&lt;broker&gt;/&lt;pid&gt;-&lt;epoch&gt;.json"]
    local --> respond["EndTxn response error_code=0<br/>as soon as the queue entry is written"]
    enqueue --> respond
    enqueue -.-> watcher["peer broker's MarkerWatcher<br/>polls its own to-&lt;self&gt;/ every 2 s"]
    watcher -.-> apply["applies marker as control-batch append<br/>to partitions it leads, then deletes the file"]
```

Self-led markers are written *before* the queue entries, so a coordinator crash
mid-dispatch never loses the local marker. A retried `EndTxn` overwrites the
same `{pid}-{epoch}.json` file — the queue is idempotent by naming. Consumers
in `read_committed` only see the transaction's records once these markers land
(the fetch path clamps to the last stable offset).

The transaction timeout reaper (spawned by the broker's cluster runtime) fires
every 10 s: any `Ongoing` entry past `ongoingSinceMs + transactionTimeoutMs`
transitions to `CompleteAbort` with an epoch bump, and its staged offsets are
discarded via the same offset hook.
