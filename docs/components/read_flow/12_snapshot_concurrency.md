# Sharded Explicit-Snapshot Design

This document is the authoritative design for explicit RocksDB snapshot
concurrency. It covers `DB::GetSnapshot()`, `DB::ReleaseSnapshot()`,
timestamped snapshots, snapshot-list readers, external file ingestion, and
the compaction work enabled when the oldest snapshot advances.

## Scope

The design separates two responsibilities:

1. Maintain the live snapshot set used for read visibility.
2. Propagate oldest-snapshot progress to compaction state.

Snapshot creation and release are common read-side operations. Oldest-snapshot
maintenance is less frequent and interacts with Version state protected by the
DB mutex. The common operations therefore use snapshot-shard locks, while
maintenance is performed by existing DB-mutex-protected paths.

The design does not change snapshot visibility, public APIs, file formats,
options, or compaction-picking policy. It also does not make the snapshot list
lock-free, introduce reader/writer locks, or tune the shard count dynamically.

## Goals

- Keep regular snapshot creation and release out of the DB-mutex domain.
- Allow independent readers to create and release snapshots concurrently.
- Preserve sequence ordering and timestamped-snapshot rules.
- Never report an oldest snapshot newer than a live snapshot.
- Eventually schedule cleanup made eligible by a snapshot release.
- Preserve external-ingestion and snapshot-support serialization.
- Keep CPU, waits, and I/O under the DB mutex tightly bounded.

## Data Model

`ShardedSnapshotList` owns 16 cache-line-aligned shards. The count is a fixed
implementation choice and must remain a power of two. Each shard contains:

- a mutex;
- a sequence-ordered `SnapshotList`;
- a mutex-protected cumulative release counter; and
- a mutex-protected snapshot-creation gate.

Each `SnapshotImpl` records the index of its creation shard and a private
creation epoch. Creation normally uses the current physical CPU ID
modulo 16. On platforms without a physical CPU ID, each thread caches a shard
selected by a relaxed round-robin counter. A coroutine may resume on a
different worker or CPU without affecting release: the snapshot, rather than
the current thread, identifies the owning shard.

`DBImpl` retains two DB-mutex-protected maintenance values:

- the largest conservative oldest-snapshot bound processed by all current
  column families; and
- the aggregate shard release count consumed with that bound.

`ShardedSnapshotList` advances the creation epoch when the raw snapshot Unix
time changes. Snapshots created during one contiguous run of the same Unix
time share an epoch; they are indistinguishable to the oldest-time property.
The common path reads the epoch through an atomic versioned state without
writing shared memory. A time transition serializes on a small internal mutex
and publishes the next epoch. This orders different Unix-time values at
insertion without changing the public timestamp, which remains the raw
configured-clock result.

Timestamped snapshots also remain in `TimestampedSnapshotList`, which is
protected by the DB mutex. A timestamped snapshot is linked into both the
sharded list and the timestamp index.

## Synchronization by Operation

| Operation | Synchronization |
|-----------|-----------------|
| Regular creation | One shard mutex |
| Regular release | The recorded shard mutex |
| Timestamped creation | DB mutex, then one shard mutex |
| `GetAll()` | One shard mutex at a time |
| Exact oldest time or sequence | All shard mutexes in index order |
| Snapshot count | All shard mutexes in index order |
| Oldest-bound maintenance | DB mutex, then one shard mutex at a time |
| Snapshot-support disable | All shard mutexes in index order |
| External-ingestion gate | DB mutex, then each shard mutex in index order |

Both creation and release mutate a list, so a reader/writer lock does not help
the common operations. A lock-free linked list would also require safe memory
reclamation for concurrently released snapshots and is outside this design.

## Snapshot Creation

Regular creation allocates the snapshot and reads the wall-clock time before
entering a shard. `TryNew()` then performs the following work under one shard
mutex:

1. Check whether external ingestion has blocked DB-mutex-free creation.
2. Check whether the configured memtable supports snapshots.
3. Read the last published sequence number.
4. Read or advance the private creation epoch for the raw Unix time.
5. Link the snapshot at the tail of the shard's ordered list.

Reading the sequence inside the shard mutex keeps each shard ordered. A later
creator in the same shard cannot append an older sequence number.

If the ingestion gate is active, a DB-mutex-free creator unlocks its shard,
acquires the DB mutex, and retries. This is the only regular-creation fallback
into the DB-mutex domain.

### Snapshot-Support Changes

The supported-to-unsupported transition locks all shards before publishing the
new state. A creator therefore either completes while support is still enabled
or observes the disabled state while holding its shard. The state can be read
with relaxed ordering because the shard lock supplies the synchronization.

### Timestamped Snapshots

Timestamped creation holds the DB mutex for the timestamp index and one shard
mutex for the sharded list. Validation against the latest timestamped snapshot
happens before insertion.

When a caller supplies an explicit sequence, the snapshot is linked before the
sequence is published, and both operations occur under the same shard mutex.
A regular creator in that shard cannot append the old published sequence after
the explicit-sequence snapshot. This keeps insertion at the tail and avoids a
linear search while the DB mutex is held.

## Snapshot Release

`ReleaseSnapshot()` uses the shard recorded in `SnapshotImpl`:

1. Lock that shard.
2. Unlink the snapshot.
3. Increment the shard's cumulative release counter.
4. Unlock the shard and destroy the snapshot.

It does not acquire the DB mutex, scan other shards, inspect column families,
or schedule compaction. The cumulative counter records that maintenance may
need to process oldest-snapshot progress later.

## Snapshot-List Readers

### `GetAll()`

`GetAll()` locks one shard at a time and copies that shard's ordered sequence
numbers. It also observes the shard head and release counter during the same
critical section. After all copies are complete, a 16-way min-heap merge:

- returns sequence numbers in ascending order;
- removes duplicate sequence numbers;
- excludes sequence numbers greater than `max_seq`; and
- reports the oldest write-conflict-boundary snapshot.

The merge is `O(N log 16)` for `N` copied snapshots and uses at most 16 heap
entries. The method does not hold all shard mutexes while copying an unbounded
number of snapshots.

The per-shard copies are not one global point-in-time observation. That is
intentional. Flush and compaction need the returned snapshot set plus a safe
published-sequence boundary, rather than an atomic snapshot of every list.

### Exact and Statistical Readers

Oldest-snapshot time and sequence properties lock all shards and compare their
heads. Equal sequence numbers are ordered by the private creation epoch.
Different Unix-time values therefore retain insertion order even when the
clock rolls back; snapshots in the same epoch expose the same Unix time.

Snapshot count locks all shards and sums their list counts, so the public
`rocksdb.num-snapshots` property observes one exact point in time.
Statistics-only property reads do not trigger maintenance.

BlobDB uses the exact all-shard oldest observation when deciding whether an
obsolete blob file remains visible.

WritePrepared holds the DB mutex while loading snapshots, preserving its
serialization with timestamped-snapshot changes. Before scanning, eviction
publishes `future_max_evicted_seq_`. A concurrent snapshot at or below that
boundary observes it, advances the published sequence, and retries until its
sequence is above the boundary. That retry protocol makes the filtered
snapshot vector complete. The conservative published-sequence bound below is
used separately for oldest-snapshot maintenance.

## Conservative Oldest-Snapshot Bound

Compaction maintenance does not require the exact oldest live snapshot. It
requires a value that is never newer than a live snapshot:

- A bound that is too old can delay cleanup and is safe.
- A bound that is too new can discard data visible to a snapshot and is unsafe.

Before scanning shards, the caller captures `GetLastPublishedSequence()` as
the snapshot-creation lower bound `P`. The scan initializes its result to `P`
and lowers it for each observed non-empty shard head.

This produces a conservative result:

- A snapshot present when its shard is scanned is protected by that shard's
  head, whose sequence is no newer than any member of the shard.
- A snapshot created after its shard was scanned reads its sequence after `P`
  was captured, so its sequence is no smaller than `P`.
- A snapshot released during the scan can only leave an unnecessarily old
  head in the result.

`GetAll()` calculates the same bound during its copy pass. The oldest bound is
independent of `max_seq`: a live snapshot newer than `max_seq` can still be the
globally oldest snapshot.

`OldestSnapshotInfo` carries the bound, the aggregate release count observed by
the scan, and `P`. Maintenance consumes the release count only if `P` still
equals the last published sequence when the DB-mutex-protected reconciliation
begins. If publication advanced, the scan may have paired a release with an
older creation bound, so the release remains pending for a later stable scan.

A release racing after its shard was scanned is absent from that scan's head
and counter observation. Its counter increment therefore remains visible to a
later scan. One-shard-at-a-time copying can delay progress, but it cannot lose
a release or produce an unsafe bound.

If the aggregate release count has not advanced, scan-bound stability is
irrelevant: there is no new release to consume. This prevents ordinary
published-sequence movement from making maintenance or `WaitForCompact()`
retry needlessly.

## Oldest-Snapshot Maintenance

`UpdateOldestSnapshotAndScheduleWork()` runs with the DB mutex held. A stable
scan is reconciled as follows:

1. If the aggregate release count has not advanced, skip the DB-wide update.
2. Consume the new release count.
3. If the conservative oldest bound has not advanced, stop.
4. Record the new DB-wide bound.
5. Reconcile each current column family's `VersionStorageInfo`.
6. Schedule background compaction if a file becomes eligible.

Ordinary writes advance the published sequence even when no snapshots exist.
Requiring a new release count prevents those writes from repeatedly turning
flush or SuperVersion installation into snapshot-maintenance work.

SuperVersion installation also reconciles the installed column family against
the last processed bound. This is needed even when there is no new release:
the newly installed Version can contain older per-Version snapshot state.

For each column family, reconciliation calls `UpdateOldestSnapshot()` only
when its per-Version oldest value advances. A new bottommost-file mark enqueues
the column family. A standalone range-tombstone file is enqueued on every later
per-Version advance at or above its threshold. Universal compaction can consume
the pending entry while deferring that file for snapshot visibility, so later
advances must make it eligible again.

Scheduling only places work on RocksDB's background queues. Compaction CPU,
compression, and file I/O do not run in the maintenance call.

### Maintenance Triggers

| Trigger | Purpose |
|---------|---------|
| SuperVersion installation | Reconcile newly installed Version state |
| Flush/compaction snapshot context | Reuse its required `GetAll()` scan |
| WritePrepared snapshot load | Reuse its DB-mutex-protected list load |
| Periodic compaction task | Provide eventual progress on an otherwise idle DB |
| `WaitForCompact()` | Schedule eligible work before reporting idle |

Recovery can load snapshots without scheduling maintenance because the DB is
not yet serving normal concurrent work.

If `WaitForCompact()` observes an unconsumed release with an unstable creation
bound, it releases the DB mutex, yields, and retries before deciding that the
DB is idle. The retry honors the API timeout. An unstable scan without a new
release does not delay the call. A pre-existing background error stops the
retry so the existing close and error-return precedence remains intact.

## External File Ingestion

Snapshot-consistent external ingestion needs to prevent a new snapshot between
its snapshot-set check and installation of the new Version. While holding the
DB mutex, ingestion increments the creation gate in every shard. A
DB-mutex-free creator that observes the gate retries under the DB mutex and
therefore waits for the serialized portion of ingestion.

`LogAndApply()` can temporarily release the DB mutex while writing the MANIFEST.
A creator that acquires the DB mutex during that interval may create a snapshot
even though the gate remains active. This preserves the established ingestion
serialization boundary. Ingestion removes the gate after installing or
aborting its Version changes.

## Lock Ordering

Shard mutexes are leaf locks relative to the DB mutex:

1. Code may hold the DB mutex and then acquire shard mutexes.
2. Code must not acquire the DB mutex while holding a shard mutex.
3. Multi-shard operations lock shards in increasing index order.
4. A Unix-time transition may acquire the epoch-state mutex while holding one
   shard; no path acquires a shard while holding the epoch-state mutex.

A creator that must fall back to the DB mutex first releases its shard. Snapshot
release destroys the object after releasing its shard and never enters the
DB-mutex domain.

## DB-Mutex Performance Review

The common regular create/release pair performs no work under the DB mutex.
Creation does allocation, wall-clock access, one shard lock, read-only atomic
epoch-state checks, and constant-time list insertion. A Unix-time transition
also takes the epoch-state mutex and updates three atomics; the steady path
does not write shared epoch state. Release takes its shard lock, performs
constant-time list removal, and increments one non-atomic release counter.
The ingestion-gated retry is exceptional and preserves required
serialization.

Maintenance adds at most 16 shard lock/unlock pairs, 16 head comparisons, and
16 counter additions to a DB-mutex critical section. If no release is pending,
it does not iterate the DB's column-family set, although a SuperVersion
installation can still reconcile the Version being installed. If the bound
advances, it scans current column families. Per-Version work is constant time
unless a marking threshold is crossed; crossing the threshold can scan that
Version's bottommost-file metadata. The scan can call the configured clock
and, for user-defined timestamps, `Comparator::CompareTimestamp()` once per
relevant bottommost file. Those calls execute under the DB mutex. The path
performs no file I/O.

`GetAll()` callers generally continue to hold the DB mutex while the 16-way
merge runs. Its 16 staging vectors can allocate independently and hold `N`
sequence numbers in aggregate; the result vector holds a second copy, so peak
staging is `O(N)`. Compared with a flat list pass, the merge adds at most one
heap pop and one heap push per copied entry, each over no more than 16 cursors.
The merge was not moved outside the DB mutex because doing so would require
pinning and revalidating the surrounding flush, compaction, and transaction
state. The extra complexity is not justified without evidence that
snapshot-list merging is a material critical-section cost.

### Performance Evaluation

The isolated benchmark used a temporary `db_bench` workload that repeatedly
called `GetSnapshot()` and `ReleaseSnapshot()` in batches of 256. It performed
no point reads or I/O. Both configurations used `DEBUG_LEVEL=0` on the same
192-vCPU AMD EPYC-Genoa host. Preliminary runs warmed each binary. Each thread
count then had five alternating four-second trials. The table reports median
aggregate throughput.

| Threads | Unsharded ops/s | Sharded ops/s | Speedup |
|--------:|-------------:|--------------:|--------:|
| 1 | 17.7M | 18.7M | 1.06x |
| 4 | 4.71M | 74.7M | 15.8x |
| 16 | 2.80M | 225M | 80.2x |
| 64 | 3.38M | 72.0M | 21.3x |
| 128 | 4.46M | 32.2M | 7.23x |

A representative `randomwithverify` run used 16 threads and 16 million total
operations per trial: 98% snapshot-backed reads, with three point Gets per
snapshot, 1% writes, and 1% deletes. Across three alternating trials, median
throughput increased from 590K to 4.88M operations per second, or 8.27x.

These results demonstrate contention removal on this host. They are not a
claim about every workload: the host was not isolated, threads were not pinned,
the shard count was not tuned, and neither benchmark measures the less frequent
DB-mutex maintenance paths.

## Functional Validation

Focused tests cover multi-shard ordering and deduplication, exact count and
oldest properties, timestamped insertion, snapshot-support transitions,
external ingestion before and during the MANIFEST unlock, conservative
oldest-bound races, and WritePrepared snapshot interaction.

Compaction coverage exercises bottommost-file marking, release overlapping
SuperVersion installation, unstable scan handling with and without a pending
release, timeout behavior, background-error precedence, and repeated
standalone range-tombstone scheduling. The release build covers the
benchmarked configuration, while the debug build covers the concurrency
assertions and race-focused tests.

## Correctness Invariants

- Every live snapshot is linked in exactly one shard.
- A snapshot is released from the shard recorded at creation.
- Creation epochs order different Unix-time values for equal-sequence
  snapshots across shards.
- Each shard remains ordered by sequence number.
- A maintenance oldest bound is never newer than a live snapshot.
- Processed oldest bounds never move backward.
- Every completed release is eventually included in an aggregate release
  count.
- A release observed with an unstable creation bound remains pending.
- `GetAll()` preserves ordering, deduplication, maximum-sequence filtering, and
  write-conflict-boundary semantics.
- Snapshot-support disabling is linearizable with creation.
- External ingestion preserves snapshot-consistency serialization.
- Statistics observe snapshot state but do not schedule background work.
- Stale maintenance state can retain extra data but cannot expose wrong data.

## Limitations

- The 16-shard count has not been tuned across platforms and workloads.
- Shards are selected by CPU ID, not by load; several CPUs can map to one
  shard.
- Platforms without a physical CPU ID use per-thread fallback assignment.
- `GetAll()` merging remains inside callers' DB-mutex critical sections.
- Periodic maintenance provides eventual, not deadline-based, cleanup.

## Source Map

- `db/snapshot_impl.{h,cc}`: shard storage, creation, release, readers, and
  conservative scans.
- `db/db_impl/db_impl.cc`: public snapshot operations, ingestion gating, and
  oldest-snapshot reconciliation.
- `db/db_impl/db_impl_compaction_flush.cc`: SuperVersion, flush/compaction,
  and `WaitForCompact()` triggers.
- `utilities/transactions/write_prepared_txn_db.cc`: WritePrepared snapshot
  loading and timestamped-snapshot serialization.
- `utilities/blob_db/blob_db_impl.cc`: exact oldest-snapshot visibility check.
- `db/db_etc2_test.cc` and `db/db_compaction_test.cc`: concurrency,
  publication, maintenance, and compaction regressions.
