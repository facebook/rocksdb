# Debates: WAL Documentation Reviews

## Debate: Recycled WAL record type consistency enforcement direction

- **CC position**: "Once the reader encounters a recyclable record type, it sets the `recycled_` flag. From that point, if a non-recyclable data record type appears, it returns `kBadRecord`." (Undocumented Complexity section, describing what the doc should say)
- **Codex position**: "The implementation enforces the opposite transition: if a recyclable record appears after the reader has already established the file as non-recycled, it returns `kBadRecord`. It does not reject a later legacy record solely because `recycled_` is already true."
- **Code evidence**: In `db/log_reader.cc`, `ReadPhysicalRecord()` has the check: `if (first_record_read_ && !recycled_) { return kBadRecord; }` inside the `is_recyclable_type` branch. This only fires when a recyclable record appears after non-recyclable records. There is NO symmetric check that rejects non-recyclable records when `recycled_` is true. `TryReadFragment()` has identical logic.
- **Resolution**: Codex is correct. The enforcement is one-directional: recyclable-after-non-recyclable is rejected, but non-recyclable-after-recyclable is allowed. CC described the opposite direction, which does not exist in the code.
- **Risk level**: medium -- the doc previously stated a non-existent invariant; someone extending the reader might rely on it

## Debate: 2PC WritePrepared prepare/commit WAL/MemTable flow

- **CC position**: Called the 2PC chapter "correct" in Positive Notes: "The 2PC chapter (11) covers the prepare/commit/rollback flow clearly and correctly describes `MinLogNumberToKeep()` semantics."
- **Codex position**: "[WRONG] 2PC chapter reverses the WritePrepared WAL/memtable flow. WritePrepared prepare writes to both WAL and memtable (`!DISABLE_MEMTABLE`). The common commit path is WAL-only (`disable_memtable = !includes_data`, typically true)."
- **Code evidence**: In `utilities/transactions/write_prepared_txn.cc`, `WritePreparedTxn::PrepareInternal()` passes `!DISABLE_MEMTABLE` (i.e., `false`) as the `disable_memtable` parameter to `WriteImpl()`, meaning memtable writes ARE enabled. `WritePreparedTxn::CommitInternal()` computes `disable_memtable = !includes_data`, which is typically `true` for pure commit markers. By contrast, `WriteCommittedTxn::PrepareInternal()` in `pessimistic_transaction.cc` passes `kDisableMemtable` (true), which IS WAL-only. The doc described the WriteCommitted behavior as universal, ignoring the WritePrepared difference.
- **Resolution**: Codex is correct. The doc's "prepare=WAL only, commit=MemTable insert" description only applies to WriteCommitted. WritePrepared is the opposite: prepare=WAL+MemTable, commit=WAL only. CC missed this factual error.
- **Risk level**: high -- fundamentally wrong description of a major transaction mode

## Debate: TransactionLogIteratorImpl reader type

- **CC position**: In the Undocumented Complexity section, CC repeated the doc's claim: "replication users who use `FragmentBufferedReader` via `TransactionLogIterator`." CC did not flag the claim as incorrect and implicitly endorsed it.
- **Codex position**: "[WRONG] `TransactionLogIteratorImpl` stores `std::unique_ptr<log::Reader>` and opens a plain `log::Reader`, not `FragmentBufferedReader`."
- **Code evidence**: In `db/transaction_log_impl.h`, the member variable is `std::unique_ptr<log::Reader> current_log_reader_`. In `db/transaction_log_impl.cc`, `OpenLogReader` constructs `new log::Reader(...)`. Tailing is implemented via `IsEOF()` / `UnmarkEOF()` on the regular reader, not via `FragmentBufferedReader`.
- **Resolution**: Codex is correct. `TransactionLogIteratorImpl` uses `log::Reader`, not `FragmentBufferedReader`. CC failed to catch this incorrect claim and even reinforced it.
- **Risk level**: medium -- incorrect reader type attribution could mislead someone modifying the replication code path
