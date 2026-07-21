# The NFS substrate contract

kaas runs its multi-broker mode on a shared `ReadWriteMany` volume — NFSv4 or
NFS-equivalent — because that shared filesystem *is* the coordination
substrate: `assignment.json`, the txn slot files, the marker queue, and every
partition's log all live on it, read and written by different brokers. That
choice buys a great deal (no replication protocol, no consensus log — see the
[non-goals](./overview.md)), but it comes with a contract, and most of the
subtle bugs in kaas's history are what happens when code forgets it.

## What NFS gives you, exactly

The substrate provides **three** guarantees, and only three:

1. **Same-directory `rename()` is atomic.** A reader sees either the old target
   or the new one, never a torn intermediate. This is the load-bearing
   primitive.
2. **`open(O_CREAT|O_EXCL)` is atomic.** Exactly one racer wins an exclusive
   create; the rest get `EEXIST`.
3. **Close-to-open consistency.** After host A `close()`s a file, host B's next
   `open()` sees the fully-written contents. This is how one broker reads what
   another wrote (txn slots, marker queue, fences).

That is the whole toolbox. Everything you might *wish* were atomic is not:

- **`remove_dir_all` is not atomic** — it is N separate `unlink`s. It can be
  observed half-done and interrupted half-done.
- **Read-modify-write is not atomic** — two writers interleave.
- **Check-then-act is not atomic** — `if !exists { create }` is a TOCTOU race,
  and `Partition::open` (`mkdir_all` → open handles → recover) is exactly this
  shape.
- **`unlink` of an open file is not clean** — NFS silly-renames it to
  `.nfsXXXX` and `EBUSY`s the parent directory until the last FD closes
  (gh #76).

You cannot make a recursive delete atomic on NFS. So the principle is **not**
"make every operation atomic" — that's unachievable. It is the following.

## The contract

> **1. Build every durable state transition from the atomic primitives.**
> Write a temp file, `fsync`, then `rename` over the target. Never edit in
> place where another host can observe a half-state.
>
> **2. Any compound operation that cannot be a single atomic step must be
> idempotent and safe to retry** — because on a shared substrate it *will*
> race a concurrent actor or be interrupted, and its caller must be able to
> run it again to completion.
>
> **3. Coordinate all mutation through single-writer + epoch fencing**, so
> there is no concurrent writer to race in the first place.

### Rule 1 is already doctrine

`crates/kaas-storage/src/atomic_write.rs` is rule 1 in code: `manifest.json`,
`producer-state.snapshot`, and the operator's `.config.json` all go through
`tmp + fsync + rename`. `assignment.json` is written the same way. Segment
files never mutate in place — they append, and roll by creating a new
epoch-prefixed file and swapping a pointer. When you add a new persisted file,
it goes through `atomic_write`, full stop.

### Rule 2 is the one that gets forgotten

A compound operation that isn't retry-safe turns a transient hiccup into a
permanent fault. The pattern to internalize: **identify the desired end-state,
then drive toward it repeatedly until reached — never assume a single attempt
either fully succeeds or is someone else's problem.** A one-shot `warn!` on
failure is the anti-pattern.

### Rule 3 is kaas's core model

Single-writer-per-partition with epoch-prefixed filenames (`crates/kaas-storage`)
means two brokers never write the same log. The [controller](./controller.md)
fences stale writers by epoch. Where a *second* actor mutates state the
single-writer owns — the operator's `remove_dir_all` touching a directory a
broker holds open — you are outside rule 3, and that is where races live.

## The bugs are all contract violations

Reading the open issues through this lens, they collapse to one root cause:

| symptom | which rule | the fix in contract terms |
|---|---|---|
| operator `remove_dir_all` races `Partition::open`, ENOENT (gh #203) | 3 (two writers) | delete via a single atomic `rename` of the dir into a `.deleting/` staging area, then recurse *there*, where no broker will open it |
| `take_over` failure never retried → partition stuck (gh #215) | 2 (not retried) | reconcile loop: re-drive `take_over` for any assigned-but-unopened partition — `take_over` is already idempotent |
| orphan sweep aborts on first `ENOTEMPTY` (gh #205) | 2 (not resumable) | collect per-dir errors and continue; re-run periodically, since `ENOTEMPTY` clears once FDs close |
| `unlink`-while-open silly-renames (gh #76) | 3 (single-writer FD discipline) | only the partition leader holds log/index FDs; close before the leader-side unlink |

Four issues, one principle. That is the argument for writing it down: a
reviewer with this contract in hand catches the *next* one before it ships, by
asking three questions of any code that touches the shared volume —

1. Is this a single atomic primitive (`rename`, `O_EXCL` create)?
2. If not, is it idempotent and safe to retry to completion?
3. Is there exactly one writer, epoch-fenced against the rest?

If the answer to all three is no, the code has a latent NFS race, no matter how
well it works in a single-broker test on a local disk.

## Where this shows up next

Honest readiness ([gh #208](./readiness-rollout.md)) made rule 2 violations
*visible* rather than silent: a broker that can't finish takeover now correctly
reports NotReady instead of lying, so an un-retried `take_over` (rule 2) stalls
a rollout instead of silently dropping a partition. That is an improvement — a
silent fault is worse than a loud one — but it means rule 2 is now load-bearing
for availability, not just correctness. The takeover reconcile loop (gh #215)
is its first deliberate application.
