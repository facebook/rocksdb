# Review: wal -- Claude Code

## Summary
Overall quality rating: **good**

The WAL documentation is well-structured and covers the major subsystems comprehensively: record format, writer, reader, recovery, lifecycle, sync, recycling, compression, tracking, filtering/replication, 2PC, and configuration. The record format chapter is precise and matches the source code closely. Most technical claims are verifiable and accurate.

The biggest concerns are: (1) a factually wrong description of the `kBadRecord` error code conflating it with CRC errors, (2) a misleading description of the write group size cap behavior that states the opposite of what the code does, (3) several completeness gaps around recent features and behavioral changes from 2024-2025, and (4) some under-documented complexity in the writer's metadata record handling and the reader's recycled WAL detection refinements.

## Correctness Issues

### [WRONG] kBadRecord error code includes "Invalid CRC"
- **File:** 03_reader.md, "Corruption Reporting" section, error codes table
- **Claim:** "| `kBadRecord` | Invalid CRC or zero-length record |"
- **Reality:** `kBadRecord` does NOT cover CRC errors. CRC mismatches return the separate `kBadRecordChecksum` error code. `kBadRecord` covers zero-length records (preallocated regions) and other invalid records (e.g., decompression failures, non-recyclable record after recyclable one). The same table correctly lists `kBadRecordChecksum` as a separate code for CRC mismatches, making the `kBadRecord` description contradictory.
- **Source:** `db/log_reader.h` enum values (kBadRecord at kMaxRecordType+2, kBadRecordChecksum at kMaxRecordType+6); `db/log_reader.cc` ReadPhysicalRecord where kBadRecord and kBadRecordChecksum are returned in different code paths.
- **Fix:** Change the kBadRecord description to "Zero-length record or other invalid record (not CRC)".

### [MISLEADING] Group size cap behavior is described backwards
- **File:** 06_sync_and_durability.md, "Group Commit" section
- **Claim:** "When the leader's own write exceeds 1/8 of this limit, the group size is capped."
- **Reality:** The capping happens when the leader's write is SMALL (<=1/8 of the max), not when it exceeds 1/8. The code at `db/write_thread.cc`:451-455:
  ```
  size_t max_size = max_write_batch_group_size_bytes;
  const uint64_t min_batch_size_bytes = max_write_batch_group_size_bytes / 8;
  if (size <= min_batch_size_bytes) {
      max_size = size + min_batch_size_bytes;
  }
  ```
  When the leader's batch is small (<=128KB by default), the group size is capped at `leader_size + 128KB` to prevent small writes from being delayed by accumulating a large group. When the leader's batch is large (>128KB), the full 1MB limit applies.
- **Source:** `db/write_thread.cc` lines 448-455
- **Fix:** Rewrite to: "When the leader's own write is small (at most 1/8 of the max group size), the effective group size limit is reduced to `leader_size + max/8`. This prevents small writes from being penalized by accumulating too large a group. When the leader's write exceeds 1/8, the full `max_write_batch_group_size_bytes` limit applies."

### [MISLEADING] "Two Write Queues" section conflates two features
- **File:** 06_sync_and_durability.md, "Two Write Queues" section
- **Claim:** "With `enable_pipelined_write` or `two_write_queues`, sync behavior has additional considerations"
- **Reality:** `enable_pipelined_write` and `two_write_queues` are two completely separate features in DBOptions. Pipelined write allows the WAL write and memtable write to be pipelined (one group writes WAL while the previous group writes memtable). Two write queues (`two_write_queues`) enables a separate WAL-only write queue. The section title "Two Write Queues" is misleading when it discusses `enable_pipelined_write`, which is a different mechanism.
- **Source:** `include/rocksdb/options.h` where both are separate options
- **Fix:** Either separate the two features into distinct sub-sections or rename the section to "Advanced Write Queue Modes".

### [MISLEADING] Predecessor WAL info is "not fragmented"
- **File:** 09_tracking_and_verification.md, "Write Path" section
- **Claim:** "The predecessor info is serialized via `PredecessorWALInfo::EncodeTo()` and emitted as a single physical record (not fragmented, will switch to a new block if needed)."
- **Reality:** The claim is correct that `MaybeSwitchToNewBlock()` is called before emitting. However, calling it "not fragmented" is imprecise -- the `MaybeSwitchToNewBlock()` helper ensures there is enough room by potentially switching blocks, but the record still goes through `EmitPhysicalRecord()` which is a single physical record. The same applies to user-defined timestamp size records. What's really happening is that these metadata records call `MaybeSwitchToNewBlock()` to ensure the entire record fits in one block, unlike data records that fragment across blocks.
- **Source:** `db/log_writer.cc` MaybeSwitchToNewBlock() and MaybeAddPredecessorWALInfo()
- **Fix:** Clarify: "Metadata records (predecessor WAL info and timestamp size records) call `MaybeSwitchToNewBlock()` to ensure the entire record fits in a single block, zero-padding the current block's remainder and advancing to the next block if needed."

## Completeness Gaps

### Missing: recycle_log_file_num now compatible with kPointInTimeRecovery
- **Why it matters:** PR #12403 (2024) re-enabled WAL recycling for `kPointInTimeRecovery`. The doc in chapter 7 correctly states only `kTolerateCorruptedTailRecords` and `kAbsoluteConsistency` are incompatible, but this is a significant recent change that deserves a note.
- **Where to look:** `db/db_impl/db_impl_open.cc` SanitizeOptions around line 105-122, and the code comment at lines 116-120 mentioning the fix.
- **Suggested scope:** Add a brief note to chapter 7 that `kPointInTimeRecovery` was previously incompatible but was re-enabled.

### Missing: WAL write error propagation behavior
- **Why it matters:** PR #12448 (2024) added the behavior that after a WAL write error, subsequent WAL writes are rejected without attempting to write. This is partially covered by the "Error Handling" section in chapter 2 via `MaybeHandleSeenFileWriterError()`, but the broader implications (all subsequent writes to the DB fail after a single WAL I/O error) deserve emphasis.
- **Where to look:** `db/log_writer.cc` MaybeHandleSeenFileWriterError(), `file/writable_file_writer.h` seen_error()
- **Suggested scope:** Expand the Error Handling section in chapter 2.

### Missing: Fix recycled WAL detection with wal_compression
- **Why it matters:** PR #12643 (2024) fixed recycled WAL detection when wal_compression is enabled. The interaction between recycling and compression is a subtle edge case not documented.
- **Where to look:** The fix likely involves how the reader handles compression type records in recycled WALs.
- **Suggested scope:** Add a note to chapter 7 or 8 about the interaction.

### Missing: WAL hole detection
- **Why it matters:** PR #13226 (2024) added WAL hole detection capability. This is related to the chain verification feature documented in chapter 9 but the specific hole detection mechanism may have additional behavior.
- **Where to look:** `db/log_reader.cc` MaybeVerifyPredecessorWALInfo, recent changes in db_impl_open.cc
- **Suggested scope:** Review chapter 9 for completeness against the updated detection logic.

### Missing: Forward compatibility for WAL entries
- **Why it matters:** PR #13225 (2024) improved forward compatibility for unknown WAL entry types. The doc mentions `kRecordTypeSafeIgnoreMask` but may not fully cover the behavioral implications for entries with values >= 10 where bit 1 indicates recyclability.
- **Where to look:** `db/log_format.h` comment at line 41: "For all the values >= 10, the 1 bit indicates whether it's recyclable"
- **Suggested scope:** Add detail to chapter 1 about the bit 0 recyclable convention for types >= 10.

### Missing: WriteBufferManager enforcement during recovery
- **Why it matters:** PR #14305 (2025) enforces WriteBufferManager during WAL recovery, which means recovery can now trigger flushes mid-recovery when the WBM threshold is exceeded. This is a behavioral change to the recovery flow described in chapter 4.
- **Where to look:** `db/db_impl/db_impl_open.cc` ProcessLogFile/RecoverLogFiles
- **Suggested scope:** Add a note to chapter 4 about WBM-triggered flushes during recovery.

### Missing: Writer Close() and PublishIfClosed() methods
- **Why it matters:** `Writer::Close()` and `PublishIfClosed()` are public methods that manage the lifecycle of the WAL writer. Not documenting them leaves a gap for anyone studying the writer's lifecycle.
- **Where to look:** `db/log_writer.h` and `db/log_writer.cc`
- **Suggested scope:** Brief mention in chapter 2.

### Missing: FlushWALOptions struct
- **Why it matters:** Chapter 6 mentions `DB::FlushWAL(const FlushWALOptions&)` but doesn't describe what options `FlushWALOptions` contains beyond sync.
- **Where to look:** `include/rocksdb/options.h` for FlushWALOptions definition
- **Suggested scope:** Add fields to chapter 6 or 12.

### Missing: seq_per_batch and GetUpdatesSince incompatibility
- **Why it matters:** Chapter 10 mentions this in passing but doesn't explain why `seq_per_batch` breaks `GetUpdatesSince`. Users of WritePrepared/WriteUnprepared transactions would benefit from understanding this limitation.
- **Where to look:** `db/wal_manager.cc` GetUpdatesSince
- **Suggested scope:** Brief explanation in chapter 10.

## Depth Issues

### Recovery flow needs more detail on PredecessorWALInfo propagation
- **Current:** Chapter 4 describes RecoverLogFiles at a high level but doesn't mention how PredecessorWALInfo is built and passed across WALs during recovery.
- **Missing:** During ProcessLogFiles, the recovery code tracks the observed predecessor WAL info from the previous WAL and passes it to the next WAL's reader constructor. This is the mechanism that enables chain verification.
- **Source:** `db/db_impl/db_impl_open.cc` ProcessLogFiles where PredecessorWALInfo is constructed from the completed WAL's metadata and passed to the next reader.

### SwitchWAL description could clarify the unable_to_release_oldest_log_ behavior more
- **Current:** Chapter 5 says "If the WAL still cannot be released (the prepared transaction is still uncommitted), subsequent `SwitchWAL()` calls return without action (`unable_to_release_oldest_log_` flag)"
- **Missing:** On the first encounter, the code does NOT just attempt to flush -- it actually schedules flushing but also sets `flush_wont_release_oldest_log = true` to indicate the flush won't help. The flag prevents repeated futile flush attempts. This nuance matters for understanding WAL accumulation behavior under 2PC.
- **Source:** `db/db_impl/db_impl_write.cc` SwitchWAL, lines 2017-2028

### MaybeSwitchToNewBlock helper is undocumented
- **Current:** Chapter 2 doesn't mention this helper at all.
- **Missing:** `MaybeSwitchToNewBlock()` is called before emitting metadata records (predecessor WAL info, timestamp size records) to ensure they fit in a single block. Unlike data records which fragment across blocks, metadata records must be emitted as a single physical record within one block. If there isn't enough space, the remainder of the current block is zero-padded.
- **Source:** `db/log_writer.cc` MaybeSwitchToNewBlock()

## Structure and Style Violations

### index.md line count is borderline
- **File:** index.md
- **Details:** 43 lines. Within the 40-80 line target range.

### No other style violations found
- No box-drawing characters
- No line number references
- No inline code quotes (triple-backtick fences)
- All chapters have **Files:** line
- INVARIANT used only in index.md Key Invariants section (appropriate)

## Undocumented Complexity

### Writer destructor flushes buffer on destruction
- **What it is:** The `Writer::~Writer()` destructor calls `WriteBuffer()` to flush any remaining data in the internal buffer before the writer is destroyed. This is a safety net to prevent data loss if the caller forgets to flush. The error is intentionally ignored (PermitUncheckedError).
- **Why it matters:** Users managing writer lifecycle need to know that destruction has a side effect. Also, the ThreadStatus manipulation in the destructor (saving and restoring operation type) is unusual.
- **Key source:** `db/log_writer.cc` Writer::~Writer()
- **Suggested placement:** Add to chapter 2, after the Manual Flush section.

### Recyclable type consistency enforcement in reader
- **What it is:** Once the reader encounters a recyclable record type, it sets the `recycled_` flag. From that point, if a non-recyclable data record type appears, it returns `kBadRecord`. This enforces that a recycled WAL is consistently recyclable throughout. The check is at `ReadPhysicalRecord()`: "if (first_record_read_ && !recycled_) { return kBadRecord; }".
- **Why it matters:** This consistency check prevents subtle corruption scenarios where a recycled WAL has mixed record formats. Developers modifying the reader need to understand this invariant.
- **Key source:** `db/log_reader.cc` ReadPhysicalRecord, around the `is_recyclable_type` check
- **Suggested placement:** Add detail to chapter 7 "Reader Behavior with Recycled Logs" section.

### FragmentBufferedReader handles kBadRecordChecksum differently for recycled WALs
- **What it is:** In `FragmentBufferedReader::ReadRecord()`, when encountering `kBadRecordChecksum` and `recycled_` is true, the reader immediately returns false (EOF) without reporting corruption. This silently treats a bad checksum at the end of a recycled WAL as expected behavior (the stale data may have a bad checksum naturally). The main `Reader::ReadRecord()` has similar logic but only under `kTolerateCorruptedTailRecords` recovery mode.
- **Why it matters:** The `FragmentBufferedReader` always treats recycled bad checksums as EOF regardless of recovery mode. This is important for replication users who use `FragmentBufferedReader` via `TransactionLogIterator`.
- **Key source:** `db/log_reader.cc` FragmentBufferedReader::ReadRecord(), kBadRecordChecksum case
- **Suggested placement:** Add to chapter 3, FragmentBufferedReader section or chapter 7.

### Compression type record format version hardcoded to 2
- **What it is:** Both the writer and reader hardcode `compression_format_version = 2` when creating StreamingCompress/StreamingUncompress instances. This version number is not documented or explained.
- **Why it matters:** If the compression format version ever changes, understanding the current version and its semantics is important for forward/backward compatibility analysis.
- **Key source:** `db/log_writer.cc` AddCompressionTypeRecord (line 223), `db/log_reader.cc` InitCompression (line 704)
- **Suggested placement:** Add a note to chapter 8.

### The recycled_ flag and type >= 10 recyclability convention
- **What it is:** For record types >= 10, bit 0 indicates whether the type is recyclable (comment in `db/log_format.h` line 41). This convention is separate from the kRecyclableFullType..kRecyclableLastType range (5-8) and applies to metadata record types. For example, `kUserDefinedTimestampSizeType` (10) is non-recyclable, `kRecyclableUserDefinedTimestampSizeType` (11) is recyclable. Similarly 130 vs 131 for predecessor WAL info.
- **Why it matters:** This convention determines how the reader detects recyclable records and is critical for forward compatibility when adding new metadata record types.
- **Key source:** `db/log_format.h` comment at line 41, `db/log_reader.cc` is_recyclable_type check
- **Suggested placement:** Add to chapter 1, Record Types section.

## Positive Notes

- The record format chapter (01) is precise, well-organized, and matches the source code byte-for-byte. The CRC coverage description, header layouts, and record type tables are all verifiable.
- The configuration reference (12) is a genuinely useful single-page reference. The sanitization rules table is accurate and actionable.
- The WAL lifecycle chapter (05) correctly describes the multi-CF WAL sharing constraint and the `max_total_wal_size` auto-calculation formula, which was verified against the actual code.
- The recovery modes chapter (04) accurately describes the behavior differences between all four modes, including the subtle interaction with recycled logs.
- The 2PC chapter (11) covers the prepare/commit/rollback flow clearly and correctly describes `MinLogNumberToKeep()` semantics.
- The chain verification chapter (09) comparison table is well-structured and accurate.
- The index.md is concise, within line count targets, and the key invariants listed are genuine correctness invariants.
