# SuperVersion and Snapshot Isolation

**Files:** `db/column_family.h`, `db/column_family.cc`, `db/db_impl/db_impl.cc`, `db/snapshot_impl.h`

## SuperVersion Structure

`SuperVersion` (see `SuperVersion` in `db/column_family.h`) bundles the three data sources a reader needs into a single reference-counted object:

| Field | Type | Purpose |
|-------|------|---------|
| `mem` | `ReadOnlyMemTable*` | Current mutable memtable |
| `imm` | `MemTableListVersion*` | Snapshot of immutable memtable list |
| `current` | `Version*` | SST file set (all levels) |
| `mutable_cf_options` | `MutableCFOptions` | Column family options at time of creation |
| `version_number` | `uint64_t` | Monotonically increasing version ID |
| `write_stall_condition` | `WriteStallCondition` | Current write stall state |
| `full_history_ts_low` | `std::string` | Oldest readable user-defined timestamp |
| `seqno_to_time_mapping` | `shared_ptr<const SeqnoToTimeMapping>` | Sequence number to wall clock mapping |

## Reference Counting

SuperVersion uses an `std::atomic<uint32_t> refs` field:

- `Ref()` increments with `memory_order_relaxed` (safe because the caller already has a valid reference)
- `Unref()` decrements with `fetch_sub(1)` and returns true if this was the last reference (`previous_refs == 1`)
- When the last reference is released, `SuperVersion::Cleanup()` runs immediately (under the DB mutex): it unrefs `mem`, `imm`, and `current`, and pushes obsolete memtables to a `to_delete` vector. The SuperVersion object itself is then either deleted immediately or deferred to a background purge thread (when `background_purge_on_iterator_cleanup` or `avoid_unnecessary_blocking_io` is set)

**Key Invariant:** Every reader must acquire a SuperVersion reference before accessing any data and release it after the read completes. This prevents memtables and SST files from being deleted while in use.

### Impact of Long-Lasting Snapshot Reads

A SuperVersion reference pins `mem`, `imm`, and `current` (the Version containing SST file metadata). While this reference is held, none of those objects can be freed. This has two distinct resource implications:

**Memtable retention:** When a reader holds a SuperVersion reference to an old SuperVersion, the memtable and immutable memtables in that SuperVersion cannot be deleted — even after they have been flushed to SST files. Normally, after flush, the immutable memtable is unreferenced by installing a new SuperVersion. But if a long-running iterator still holds a ref to the old SuperVersion, the flushed memtable stays in memory until the iterator is destroyed. In write-heavy workloads, this can cause memory to grow significantly: new memtables are created for incoming writes while old ones are pinned by slow readers.

**SST file accumulation:** The `Version` pinned by a SuperVersion prevents the SST files it references from being deleted. When compaction produces new files and obsoletes old ones, the old files normally become candidates for deletion. But if a long-running read holds a reference to a Version that includes those old files, they remain on disk. Over time, this causes disk space to grow — the DB retains both the compacted output files and the pre-compaction input files. This is separate from snapshot-based key retention (described below) — even without explicit `DB::GetSnapshot()`, a slow iterator pins its Version's files.

**Snapshot-based key retention in compaction:** Explicit snapshots (via `DB::GetSnapshot()`) affect compaction differently. `CompactionIterator` receives the list of live snapshot sequence numbers. When it encounters multiple versions of a key, it cannot drop any version whose sequence number is visible to a live snapshot — specifically, versions with sequence numbers between the snapshot's sequence and the next-newer snapshot. This means long-lived snapshots prevent key garbage collection: old Put/Delete pairs that would normally be collapsed during compaction are preserved, increasing SST file sizes and the number of levels. The `oldest_snapshot_sequence` used by `CompactionIterator` (see `earliest_snapshot_` in `db/compaction/compaction_iterator.cc`) determines the oldest point beyond which all versions can be safely dropped.

The combined effect: a long-lasting snapshot read can simultaneously pin old memtables in memory, pin old SST files on disk (preventing space reclamation from compaction output), and prevent key-level garbage collection during compaction. This is why monitoring snapshot age (via `DB::GetSnapshotSequenceNumber()` and the `rocksdb.oldest-snapshot-time` DB property) is important in production.

## Thread-Local Caching

To avoid contention on the DB mutex for every read, SuperVersion pointers are cached in thread-local storage via `ThreadLocalPtr` (`local_sv_`).

**Fast path (thread-local hit):**

1. Atomically swap the thread-local slot with a `kSVInUse` sentinel
2. If the swapped-out value is a valid SuperVersion pointer (not `kSVObsolete`), use it directly -- no mutex, no atomic refcount
3. On release, swap the `kSVInUse` sentinel back with the SuperVersion pointer

**Slow path (cache miss or invalidated):**

1. If the swapped-out value is `kSVObsolete` (meaning `InstallSuperVersion` invalidated it), fall through
2. Lock the DB mutex
3. Call `Ref()` on the current `super_version_` to increment its refcount
4. Unlock the mutex
5. Store the new SuperVersion in the thread-local slot on release

## SuperVersion Installation

When the memtable or Version changes (e.g., after flush or compaction), `InstallSuperVersion()` is called:

Step 1: Create a new SuperVersion with updated mem/imm/current pointers (under DB mutex)

Step 2: Mark all thread-local copies as `kSVObsolete` via `ResetThreadLocalSuperVersions()`

Step 3: The old SuperVersion remains alive as long as any reader holds a reference to it

Step 4: When the last reader releases the old SuperVersion, `Cleanup()` runs under the DB mutex (unreffing mem/imm/current). Actual resource reclamation (deleting obsolete memtables, purging files) may happen on the calling thread or be deferred to background purge threads

## Snapshot Sequence Number Assignment

Reads must see a consistent point-in-time view. The sequence number determines what is visible:

**With explicit snapshot** (`ReadOptions::snapshot != nullptr`):
- Use the snapshot's sequence number directly
- The snapshot was created by `DB::GetSnapshot()` which captured `last_sequence_` at that moment

**Without explicit snapshot** (implicit snapshot):
- Call `GetLastPublishedSequence()` after SuperVersion acquisition
- The ordering is critical: SuperVersion acquisition BEFORE sequence number capture. If reversed, a flush between the two steps could compact away data that should be visible to the captured sequence number

## Snapshot Visibility Check

Visibility is enforced at multiple points in the read path:

| Location | Mechanism |
|----------|-----------|
| MemTable `SaveValue()` | `ReadCallback::IsVisible(seq)` or `CheckCallback(seq)` |
| `DBIter::IsVisible()` | `sequence <= sequence_` (snapshot seqno) check |
| `GetContext` | Initialized with snapshot sequence, filters entries during SST reads |

For transactions using `WriteUnpreparedTxn`, the `ReadCallback` also checks whether the writing transaction is the reader's own transaction, allowing reads of uncommitted data within the same transaction.

## Consistency Guarantee

The combination of SuperVersion refcounting and snapshot sequence numbers provides:

1. **Structural consistency** -- The SuperVersion reference prevents the data structures (memtable, SST files) from being deleted mid-read
2. **Logical consistency** -- The snapshot sequence number ensures only a consistent prefix of writes is visible, even as new writes proceed concurrently
3. **No reader-writer locks** -- Readers never block writers and vice versa. Writers install new SuperVersions atomically; readers see either the old or new version, never a partial state

**Key Invariant:** Readers see a consistent point-in-time snapshot of mem + imm + Version without holding the DB mutex during the read.
