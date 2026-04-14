# `db_stress` Expected-State Trace Logic

This note documents the trace/replay path used by `db_stress` crash-recovery
verification when it needs to tolerate lost buffered writes.

It is not a guide to RocksDB's generic tracing APIs in general. It is
specifically about the code path centered on:

- `db_stress_tool/db_stress_driver.cc`
- `db_stress_tool/db_stress_test_base.cc`
- `db_stress_tool/expected_state.{h,cc}`
- `trace_replay/trace_replay.{h,cc}`
- `utilities/trace/replayer_impl.cc`

## What problem this solves

`LATEST.state` is the normal `db_stress` oracle: it stores the latest expected
value for each logical key. That is sufficient when recovery must preserve the
latest state exactly.

It is not sufficient when the test intentionally allows loss of buffered writes
such as:

- `--sync_fault_injection`
- `--disable_wal`
- `--manual_wal_flush_one_in > 0`

In those modes, recovery is allowed to return an older prefix of recent writes.
The important property is "no hole":

- recovered writes must form a prefix of the writes that happened before crash
- it must not recover a newer write while losing an older one

The trace logic makes this check possible by snapshotting the oracle at a known
DB sequence number `N`, tracing subsequent writes, then rebuilding the oracle
for the recovered DB sequence number `M` by replaying the first `M - N` traced
write operations.

## When this path is active

History tracking only exists when `db_stress` uses the file-backed expected
state manager, which means `--expected_values_dir` is non-empty.

Tracing is started only when all of the following are true:

- the stress mode tracks expected state (`IsStateTracked()`)
- `--expected_values_dir` is non-empty
- `MightHaveUnsyncedDataLoss()` is true

As of the current code, `MightHaveUnsyncedDataLoss()` means:

- `FLAGS_sync_fault_injection`
- or `FLAGS_disable_wal`
- or `FLAGS_manual_wal_flush_one_in > 0`

This is broader than the older `--expected_values_dir` flag help text, which
still says historical values are tracked only with `--sync_fault_injection`.

## High-level lifecycle

The full flow for one `db_stress` process looks like this:

1. Open the DB.
2. If a historical snapshot/trace exists, restore `LATEST.state` to match the
   DB's recovered sequence number before any startup verification runs.
3. Run verification against the reconstructed `LATEST.state`.
4. Save a new historical baseline at the DB's current sequence number and start
   tracing new writes.
5. Run stress operations.
6. Crash or reopen without explicitly closing the trace.

The important ordering in `db_stress_driver.cc` is:

- `FinishInitDb()` runs before tracing is started for the new run.
- `TrackExpectedState()` runs after startup verification to avoid verification
  reads contending on the DB-wide trace mutex.
- fault-injection settings that simulate data loss are enabled after
  `TrackExpectedState()`.

That ordering ensures the sidecar oracle files are set up before the run starts
creating potentially losable DB writes.

## Files and invariants

The file-backed manager (`FileExpectedStateManager`) uses these files inside
`--expected_values_dir`:

| File | Meaning |
| --- | --- |
| `LATEST.state` | Current expected-value oracle used for normal verification |
| `PERSIST.seqno` | Separate persisted-sequence-number oracle metadata |
| `<N>.state` | Historical snapshot of expected values at DB sequence number `N` |
| `<N>.trace` | Trace of writes that happened after sequence number `N` |
| `.<name>.tmp` | Temporary file used for atomic replacement |

Only one historical generation matters at a time:

- `saved_seqno_` is the maximum sequence number found among `*.state` files
  other than `LATEST.state`
- older `*.state` and `*.trace` files are treated as stale and cleaned up

`Open()` also repairs one specific partial-save case:

- if `<N>.state` exists but `<N>.trace` does not, it creates an empty
  `<N>.trace`

That models the intended semantics of crashing after the baseline snapshot was
created but before tracing actually started.

## Why the oracle files live outside the fault-injected DB path

The expected-state snapshot and trace are written through `Env::Default()`,
not through the DB's fault-injected filesystem wrapper.

That is intentional. These files are part of the test oracle, not part of the
database state being validated. If they were subject to the same simulated data
loss as the DB files, the oracle would become unreliable exactly when it is
needed most.

`SaveAtAndAfter()` also disables `WritableFileWriter` buffering for the trace
file (`writable_file_max_buffer_size = 0`). This removes userspace buffering so
trace data is not stranded in an application buffer when the process is killed.

## Save/start-trace path

`StressTest::TrackExpectedState()` calls `SharedState::SaveAtAndAfter()`, which
dispatches to `FileExpectedStateManager::SaveAtAndAfter(DB*)`.

The save path does this:

1. Read the DB sequence number `N = db->GetLatestSequenceNumber()`.
2. Copy `LATEST.state` to a temp file.
3. Rename the temp file to `<N>.state`.
4. Create `<N>.trace` as an empty file.
5. Start RocksDB tracing on the DB, writing into `<N>.trace`.
6. Delete the previous historical `<old>.state` and `<old>.trace`, if any.

The state snapshot is created atomically via temp-file-plus-rename. The trace
file is created directly because an empty trace already has the desired meaning.

The trace options are important:

- reads are filtered out by setting
  `kTraceFilterGet | kTraceFilterMultiGet | kTraceFilterIteratorSeek |
  kTraceFilterIteratorSeekForPrev`
- writes are still traced
- `preserve_write_order = true`

The "filter" bits in `TraceOptions` are exclusion bits, so setting those bits
means "do not trace those read operations."

`preserve_write_order = true` is required because restore relies on prefix
semantics. It replays the first `M - N` traced write operations, so the trace
order must match the DB/WAL application order. Without preserved ordering, the
trace could contain the right writes in the wrong order and prefix replay would
be incorrect.

## Trace coverage contract

For expected-state recovery, the trace must satisfy this property:

- every write that can show up in recovered DB sequence/WAL state must already
  be present in the trace in the same prefix order
- extra write records are acceptable if they only appear beyond the recovered
  prefix

Equivalently:

- missing trace records are fatal
- extra suffix trace records are tolerated

This follows directly from how `Restore()` consumes the trace:

- `Restore()` chooses replay length from `db->GetLatestSequenceNumber()`, not
  from trace metadata or explicit commit acknowledgements
- it then replays exactly that many logical write operations from the trace

As a result, a later trace point can be strictly worse than an earlier one. If
a crash happens after WAL/sequence state is recoverable but before the sidecar
trace file gets the record, then `Restore()` will under-replay and validation
will fail.

By contrast, an earlier trace point can leave extra tail records for writes
that do not survive recovery. That is acceptable as long as those records stay
beyond the prefix implied by the recovered DB sequence number.

In short:

- `db_stress` needs a prefix-preserving superset of recoverable writes
- it does not require an exact set of writes known to have fully completed at
  the trace site

## Producer and consumer relationship

The semantics for this path are defined jointly by the trace producer and the
expected-state consumer:

1. Generic producer API
The producer uses generic `StartTrace()` / `Tracer` / `Replayer` APIs, but the
active consumer in this path is `FileExpectedStateManager::Restore()`, not
generic query replay.

2. Replay progress from DB sequence space
`Restore()` does not replay "until the trace says commit." It replays
`db->GetLatestSequenceNumber() - saved_seqno_` logical write operations.

3. Sidecar trace file
`<N>.trace` is written through `Env::Default()` and intentionally lives outside
the fault-injected DB path. There is no atomic coupling between WAL durability
and trace durability.

4. Ordered prefix semantics
For this path, `preserve_write_order` means the recovered trace prefix must
match DB/WAL application order. It does not by itself define whether the trace
contains an exact set of completed writes or a superset of recoverable writes;
that requirement comes from how `Restore()` interprets the trace.

## What actually goes into `<N>.trace`

`<N>.trace` is a normal RocksDB query trace file produced by `Tracer`.

In this `db_stress` path it contains:

- one `kTraceBegin` header record with trace magic and version metadata
- zero or more `kTraceWrite` records
- optionally one `kTraceEnd` footer record

Because the read trace types are filtered out, the practical payload is "header
plus write batches." Each `kTraceWrite` record stores:

- a timestamp
- a trace type
- a payload map
- the raw `WriteBatch::Data()` bytes

The timestamp is recorded by the generic tracing library, but the expected-state
restore path does not use timing at all. It uses `Replayer::Prepare()` and
`Replayer::Next()` only as a parser for the trace stream.

## Why truncated or footerless traces are expected

`db_stress` does not explicitly call `DB::EndTrace()` during the normal
crash/reopen loop. That means:

- the trace often has no `kTraceEnd` footer
- the last record may be partially written if the process dies mid-write

This is not an accident. The restore logic is intentionally tolerant of it.

The generic `TraceReader` returns `Status::Incomplete()` at EOF. The generic
replay stack already recognizes this as the kind of condition caused by killing
a process without `EndTrace()`. `FileExpectedStateManager::Restore()` adds the
expected-state-specific rule that EOF or tail corruption is acceptable only
after enough writes have already been recovered:

- if EOF is reached before enough writes were replayed, restore fails
- if EOF is reached after enough writes were replayed, restore succeeds
- if a corruption is encountered on the tail record after enough writes were
  replayed, restore also succeeds

This is the core reason the trace only needs to be good up to the recovered DB
sequence number.

## Restore path

On the next run, `FinishInitDb()` checks `shared->HasHistory()`. If history is
present, it calls `shared->Restore(db_)` before normal verification and before
the compaction filter factory is attached to shared state.

`Restore(DB*)` does this:

1. Read the recovered DB sequence number `M = db->GetLatestSequenceNumber()`.
2. Require `M >= saved_seqno_`. Otherwise the DB rolled back further than the
   oldest restorable baseline and restore fails.
3. Compute `replay_write_ops = M - saved_seqno_`.
4. Copy `<saved_seqno_>.state` to a temp `LATEST.state`.
5. Open `<saved_seqno_>.trace`.
6. Build a default `Replayer`, call `Prepare()`, and repeatedly call `Next()`
   to decode trace records.
7. Feed each decoded `TraceRecord` into a custom handler that updates the temp
   expected-state file.
8. Once exactly `replay_write_ops` logical write operations have been applied,
   restore has enough information to succeed and becomes tolerant of EOF or tail
   corruption.
9. Rename the temp `LATEST.state` into place atomically.
10. Delete `<saved_seqno_>.state`.
11. Delete traces older than `<saved_seqno_>.trace`, but keep the replayed
    trace itself for debugging.
12. Clear `saved_seqno_`.

An important detail: the default `Replayer` is not used to execute traced
operations against the DB. It is only used to parse header and record formats.
`Restore()` pulls out `TraceRecord`s with `Next()` and then calls
`record->Accept(custom_handler, &result)` on its own handler.

## How replay updates the oracle

`ExpectedStateTraceRecordHandler` implements both:

- `TraceRecord::Handler`
- `WriteBatch::Handler`

The generic trace layer gives it decoded `TraceRecord`s. For write records, it
constructs a `WriteBatch` from the traced bytes and iterates the batch, letting
the handler process each individual batch entry.

Read trace types are ignored. In practice they should not appear because the
trace options filtered them out, but the handler is still tolerant if they do.

### Key decoding

The handler does not store raw RocksDB keys in the expected-state oracle. It
maps traced user keys back to `db_stress` logical integer keys.

The path is:

1. strip any user timestamp suffix from the traced key
2. parse the remaining user key with `GetIntVal()`
3. use the resulting logical key ID to mutate the expected-state array

This is why the debug logs track:

- parse failures
- raw-key to logical-key roundtrip mismatches

The roundtrip check compares the traced raw key against `Key(parsed_id)`.

### Per-operation semantics

The handler replays only the logical effect needed by the oracle:

- `PutCF` and `TimedPutCF`
  - parse the logical key
  - read `value_base` from the traced value bytes
  - call `ExpectedState::SyncPut()`

- `PutEntityCF`
  - deserialize the wide-column entity
  - verify column consistency
  - use the default wide-column value to obtain `value_base`
  - call `SyncPut()`

- `DeleteCF`
  - parse the logical key
  - call `SyncDelete()`

- `SingleDeleteCF`
  - outside prepared transactions, replay as `DeleteCF`
  - inside prepared transactions, buffer the original single-delete form until
    commit

- `DeleteRangeCF`
  - parse begin/end logical keys
  - call `SyncDeleteRange(begin, end)`
  - count it as one replayed write operation even though it can affect many
    logical keys

- `MergeCF`
  - replay as `PutCF`
  - this matches the `db_stress` merge operator, whose merged value is derived
    from the latest operand rather than from a more complex accumulation rule

- `PutBlobIndexCF`
  - blob direct-write tracing records the transformed `BlobIndex`, not the
    original user value bytes
  - the handler therefore treats it as "one more put to this logical key" and
    derives the next `value_base` from the existing expected value

### Prepared transactions

Prepared transactions need extra care because the trace may contain prepare and
commit markers instead of immediately applied writes.

The handler buffers prepared writes in memory by transaction ID:

- `MarkBeginPrepare()` starts buffering into a temporary `WriteBatch`
- write entries encountered while buffering are appended to that batch
- `MarkEndPrepare(xid)` stores the buffered batch in a map
- `MarkCommit(xid)` replays the stored batch through the same handler
- `MarkRollback(xid)` drops the stored batch without applying it

That way the expected-state oracle reflects commit semantics rather than
prepare-time visibility.

## Why replay counts write operations, not trace records

The trace stream is made of `kTraceWrite` records, but each one contains a
whole `WriteBatch`, and a batch can contain multiple individual write entries.

Restore therefore counts replay progress using the number of write operations
applied by the handler, not the number of trace records read. The target count
is:

`db->GetLatestSequenceNumber() - saved_seqno_`

Within a traced `WriteBatch`, the handler's `Continue()` method stops batch
iteration once enough write operations have been applied. The outer restore
loop still keeps reading trace records until `Next()` returns EOF, footer, or
corruption, at which point restore decides whether the trace prefix it already
consumed was sufficient.

## Debugging support

Three flags control replay debugging:

- `--expected_state_trace_debug`
- `--expected_state_trace_debug_key`
- `--expected_state_trace_debug_max_logs`

When enabled, restore prints lines prefixed with
`[expected_state_trace_debug]`, including:

- restore begin/end markers
- `Next()` failures such as EOF or corruption
- per-key or per-range replay details
- parse failures and key roundtrip mismatches
- a replay summary with counters

Useful counters in the summary include:

- `replayed_write_ops`
- `key_decode_failures`
- `key_roundtrip_mismatches`
- `focus_key_op_hits`
- `logs_emitted`
- `logs_suppressed`

`--expected_state_trace_debug_key=<k>` narrows logging to a particular logical
key where possible. This is useful when the trace is large and only one key's
history matters.

## Crash-safety rules encoded in file deletion order

Several delete orders in the code are deliberate:

- after successfully saving a new baseline, it is safe to delete the old
  historical files in any order because the new pair is already established
- after restore succeeds, the old `<N>.state` is deleted before old traces
  because deleting the trace first and then crashing would leave no way to
  replay back up to `N`

`Clean()` also removes:

- stale temp files from interrupted `Open()` or `SaveAtAndAfter()`
- stale historical state files older than `saved_seqno_`
- stale trace files older than `saved_seqno_`

## Minimal worked example

Suppose a previous run saved a baseline at sequence number `100`:

- `100.state` contains the oracle snapshot at seqno 100
- `100.trace` contains writes after seqno 100

Then the process crashes after issuing ten more write operations. The recovered
DB comes back with latest sequence number `107`.

On the next startup:

1. `Restore()` copies `100.state` to a temp `LATEST.state`.
2. It reads `100.trace`.
3. It applies the first `107 - 100 = 7` replayed write operations to the temp
   oracle.
4. It ignores any tail after those seven operations, even if the trace ends
   without a footer or the next record is truncated.
5. It renames the rebuilt temp file into `LATEST.state`.

The rebuilt oracle now matches the recovered DB and startup verification can
check for logical holes.

## Summary

The expected-state trace logic is a prefix-recovery oracle:

- `SaveAtAndAfter()` snapshots the oracle at sequence number `N` and starts a
  write-only, write-order-preserving trace
- `Restore()` learns the recovered sequence number `M`, replays the first
  `M - N` traced write operations onto the snapshot, and rebuilds
  `LATEST.state`
- truncated or footerless traces are acceptable as long as the prefix required
  by `M` is intact
- the trace must therefore be an ordered superset of writes that could survive
  recovery; exact successful-write filtering is not the right invariant here

That is the mechanism that lets `db_stress` validate "no hole in recovery"
instead of requiring exact preservation of the latest unsynced writes.
