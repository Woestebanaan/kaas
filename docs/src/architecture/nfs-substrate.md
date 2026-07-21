# The NFS substrate contract

Apache Kafka gets durability and failover from **replication**. Every partition
has a leader and a set of in-sync followers, and a record isn't acknowledged
until it is on enough of them. Lose a broker and a follower is promoted — the
data was already there.

kaas makes a different bet. It runs on Kubernetes and stores every partition on
a **shared `ReadWriteMany` volume** — NFSv4 or an NFS-equivalent — with **one
writer per partition** and **no followers at all**. Durability comes from the
shared filesystem (plus whatever redundancy the storage itself provides), and
failover means a surviving broker opens the *same files* the dead one was
writing. There is no second copy to fall back on, because the shared volume
*is* the copy. (Why give up replication? See the [non-goals](./overview.md) —
in short, it trades a replication protocol and a consensus log for a much
smaller system that leans on Kubernetes and the filesystem instead.)

That bet buys a lot, but it moves the hard problems onto the filesystem. And
when the filesystem is NFS, correctness depends on respecting what NFS actually
promises — which is less than most code assumes. This page is that contract.
Most of the subtle bugs in kaas's history are what happens when a piece of code
forgets it.

## What lives on the shared volume

Everything a broker needs to serve, and everything the cluster needs to
coordinate, is a file other brokers can read:

- **Partition logs** — `…/<topic>/<partition>/` with the usual segment and
  index files, the same idea as Kafka's on-disk log. Only a partition's current
  leader has them open.
- **The assignment file** — who leads what. This is kaas's equivalent of the
  partition-to-leader map Kafka keeps in its metadata log (or, pre-KRaft, in
  ZooKeeper). One broker — the elected [controller](./controller.md) — writes
  it; every broker reads it.
- **The state Kafka keeps in internal topics** — consumer offsets, transaction
  state, producer fences — are plain files here rather than `__consumer_offsets`
  and the transaction log. A broker that becomes a coordinator reads the same
  file the previous one wrote.

Because these files are read and written across brokers, every one of them is
exposed to the guarantees — and the non-guarantees — below.

## What NFS actually guarantees

Three things, and only three:

1. **A rename within one directory is atomic.** A reader sees the old target or
   the new one, never a half-written mix. This is the load-bearing primitive.
2. **An exclusive create (`open` with `O_CREAT|O_EXCL`) is atomic.** Exactly one
   racer creates the file; the rest are told it already exists.
3. **Close-to-open consistency.** Once one host closes a file, the next host to
   open it sees the complete contents. This is how one broker reads what another
   wrote.

That is the whole toolbox. Everything you might *wish* were atomic is not:

- **Recursive delete is not atomic.** It is a sequence of unlinks that can be
  observed, and interrupted, half-done.
- **Read-modify-write is not atomic.** Two writers interleave.
- **Check-then-act is not atomic.** "If it doesn't exist, create it" is a race —
  and "open a partition: make the directory, open the files, recover the tail"
  is exactly that shape.
- **Deleting a file another host has open is not clean.** NFS renames it to a
  hidden `.nfsXXXX` file and keeps the parent directory busy until every open
  handle closes.

You cannot make a recursive delete atomic on NFS. So the goal is not "make
everything atomic" — it is the following.

## The contract

> **1. Build durable state changes out of the atomic primitives.** Write a temp
> file, flush it, then rename it over the target. Never mutate a file in place
> where another host can catch it half-written.
>
> **2. If an operation can't be a single atomic step, make it idempotent and
> drive it to completion by retry.** On a shared volume it *will* race another
> actor or get interrupted, so "try once, log on failure" is a latent stuck
> state. Name the desired end-state, then re-drive toward it until it is
> reached.
>
> **3. Give every piece of state a single writer, fenced by epoch.** If only one
> broker ever writes a partition, there is no concurrent writer to race — and an
> epoch stamp lets a new leader reject a zombie's late writes. This is Kafka's
> leader-epoch idea, applied to files.

### Rule 1 in practice

Every metadata file kaas persists — the per-partition manifest, the
producer-state snapshot, the assignment file, the operator-written topic config —
is written to a temp name, flushed, and renamed into place, so a reader sees
either the previous version or the next one and never a torn write. Segment logs
are never edited in place either: they are append-only, and a segment roll
creates a new (epoch-stamped) file and swaps a pointer. A new persisted file
goes through the same temp-then-rename path, no exceptions.

### Rule 2 is the one that gets forgotten

A multi-step operation that isn't retried turns a momentary hiccup into a
permanent fault. The mental model is **name the end-state, then converge to it** —
never assume one attempt either fully succeeds or is someone else's problem.
"Log a warning and move on" is the anti-pattern this rule exists to kill.

### Rule 3 is kaas's core model

Only a partition's leader writes its log, and segment filenames carry the
leadership epoch, so a stale leader's late write lands in a file the new leader
ignores — Kafka's zombie fencing, done through the filesystem (see
[file-handle ownership & takeover](./file-handles.md)). Where this breaks down
is when a *second* actor touches state the single writer owns — for instance the
operator deleting a topic's directory while a broker still has that partition
open. That is outside rule 3, and it is exactly where races live.

## How it bites — four failures, one root cause

Each of these was a real bug in kaas, and each is one rule ignored:

| what went wrong | rule broken | the fix |
|---|---|---|
| The component that deletes a removed topic's directory ran a recursive delete on the **live** path while a broker was concurrently opening the same partition — the two raced into "file not found." | 3 — two writers | Rename the directory aside in one atomic step, then delete the *renamed* copy, which no broker will ever open. |
| A broker being promoted to a partition's leader hit a transient "file not found" while opening the log, logged it, and never retried — so the partition stayed unopened and the broker never finished coming up. | 2 — not retried | A reconcile loop re-drives the open for any partition the broker should lead but hasn't opened yet; opening an already-open partition is a no-op, so retrying is always safe. |
| A cleanup sweep aborted on the first directory it couldn't remove — busy because a broker still held a handle — stranding every other orphan behind it. | 2 — not resumable | Collect per-directory failures and continue; re-run periodically, since the "busy" condition clears once the handles close. |
| Deleting a file a broker still had open left a `.nfsXXXX` tombstone that kept the parent directory busy and blocked cleanup. | 3 — single-writer FD discipline | Only the leader holds a partition's file handles, and it closes them before any delete; combined with the rename-aside above, a stray tombstone lands in a throwaway path instead of the live one. |

Four bugs, one principle. That is the argument for writing it down: the next
reviewer, holding this contract, can catch the *fifth* before it ships — by
asking three questions of any code that touches the shared volume:

1. Is this a single atomic primitive — a same-directory rename, or an exclusive
   create?
2. If not, is it idempotent and safe to re-drive until it completes?
3. Is there exactly one writer, fenced by epoch, against the rest?

If all three answers are "no," the code has a latent race — no matter how
cleanly it passes on a single broker backed by a local disk.

## Why this matters more for kaas than for Kafka

In Apache Kafka a broker's local disk is one replica among several: a corner
case on one node is masked by the others, and the filesystem is rarely the point
where the cluster coordinates. In kaas the shared volume is the *only* copy and
the coordination point for the whole cluster, so a filesystem race isn't masked —
it *is* the failure. This contract is what keeps "single writer on shared
storage" as safe in practice as "replicated across brokers" is in Kafka.
