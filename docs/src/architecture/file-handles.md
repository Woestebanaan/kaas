# File-handle ownership & takeover

Only a partition's current leader holds open file descriptors — the rule that makes deletes actually free disk on NFS instead of silly-renaming.

On NFS, removing a file that any client still holds open "silly-renames" it
into a `.nfsXXXX` entry that pins the parent directory until every FD is
closed — so segment cleanup would stop reclaiming disk, and directory removal
would loop on `EBUSY`. kaas's contract: followers keep segment state as
metadata only (size, base offset, epoch from the filename); FDs are opened on
`take_over` and dropped on `relinquish`/`close`.

## Topic delete: the handle-close path

```mermaid
flowchart TD
    del["kubectl delete kafkatopic T"] --> watch["broker topic watch<br/>fires the delete event"]
    watch --> notify["TopicRegistry drops T ·<br/>assignment recompute triggered<br/>(reason: TopicDeleted)"]
    notify --> ctl["controller: balancer drops T's partitions,<br/>writes new assignment.json"]
    ctl --> apply["every broker applies the new assignment"]
    apply --> reling["TakeoverDriver: T's partitions no longer owned<br/>→ engine.relinquish → Partition::close"]
    reling --> fds["persist manifest + producer snapshot,<br/>then close_handles() —<br/>log + index FDs dropped"]
    fds --> sweepstep["operator startup sweep (leader-elected):<br/>remove_dir_all(/data/T) once no CR references it"]
    sweepstep --> disk["directory unlink succeeds —<br/>no .nfsXXXX silly-rename, disk freed"]
```

The same ownership rule pays off in day-to-day operation, not just deletes:
segment retention, `DeleteRecords`, and segment-roll cleanup all unlink files
on the leader — the only broker with the FDs open — so removal genuinely frees
space instead of leaving `.nfsXXXX` ghosts. The graceful SIGTERM drain closes
every open partition the same way (relinquish, then manifest flush) so the
next leader never inherits a silly-rename fight.
