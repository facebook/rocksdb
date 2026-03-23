# Review: wide_column — Codex

## Summary
Overall quality rating: significant issues

The docs do a decent job on the core data model. The default-column bridge, the V1/V2 serialization split, and the `PinnableWideColumns` move/pinning subtleties are mostly grounded in the current code and tests. The chapter layout is also sensible.

The bigger problem is at the subsystem boundaries. The cross-CF precedence rule is wrong, the transaction chapter overstates support across pessimistic transaction modes, and the blob chapter flattens several materially different API behaviors into one "not wired yet" summary. There are also important tested behaviors missing around `allow_unprepared_value`, caller-supplied attribute-group queries, comparator/read-option preconditions, and secondary-index integration.

## Correctness Issues

### [WRONG] The coalescing precedence rule is backwards
- **File:** `docs/components/wide_column/05_attribute_groups.md`, section `CoalescingIterator`
- **Claim:** "when duplicate column names appear across CFs, keep only the first occurrence (lowest CF order wins)" and "the CF with the lower order (earlier in the input vector to `NewCoalescingIterator`) takes precedence."
- **Reality:** later column families in the input order win. The heap yields the lower-order item first, and `CoalescingIterator::Coalesce()` only emits the last duplicate for a given column name. The public DB comment and unit tests also expect the later CF's column value to override the earlier one.
- **Source:** `db/coalescing_iterator.cc`, function `CoalescingIterator::Coalesce`; `db/multi_cf_iterator_impl.h`, class `MultiCfIteratorImpl::MultiCfHeapItemComparator`; `include/rocksdb/db.h`, method `NewCoalescingIterator`; `db/multi_cf_iterator_test.cc`, test `CoalescingIteratorTest.WideColumns`
- **Fix:** Document that duplicate column names are resolved in favor of the later column family in the iterator input order. Mention that the merged default column, and therefore `value()`, follows the same rule.

### [STALE] The transaction chapter overstates support across pessimistic transaction modes
- **File:** `docs/components/wide_column/09_transaction_support.md`, sections `Overview` and `Cross-CF Iterators in Transactions`
- **Claim:** "Both optimistic and pessimistic transactions support entity operations with the same snapshot isolation and conflict detection guarantees as plain key-value operations." Also: "Transactions support cross-CF iterators for wide-column entities."
- **Reality:** write-prepared and write-unprepared transactions explicitly do not support `GetCoalescingIterator()` or `GetAttributeGroupIterator()`. Current tests only exercise these wide-column cross-CF iterator APIs for optimistic transactions and write-committed pessimistic transactions.
- **Source:** `utilities/transactions/write_prepared_txn.cc`, methods `WritePreparedTxn::GetCoalescingIterator` and `WritePreparedTxn::GetAttributeGroupIterator`; `utilities/transactions/transaction_test.cc`, tests `TransactionTest.CoalescingIteratorSanityChecks` and `TransactionTest.AttributeGroupIteratorSanityChecks`
- **Fix:** Narrow the claims to optimistic transactions and write-committed pessimistic transactions. Call out that write-prepared/write-unprepared transactions return `Status::NotSupported()` for the wide-column multi-CF iterators.

### [MISLEADING] The blob chapter collapses several different V2 read-path behaviors into "not wired yet"
- **File:** `docs/components/wide_column/07_blob_integration.md`, section `Current Status`
- **Claim:** "The blob integration code is fully implemented in the serialization layer but not yet wired into the production write or read paths."
- **Reality:** the behavior is more split than that. Plain-value APIs (`Get`/`MultiGet`) already use `GetValueOfDefaultColumn()` on `kTypeWideColumnEntity`, so a V2 entity with an inline default column can still be read through plain APIs even if non-default columns are blob references. By contrast, `GetEntity()`, `PinnableWideColumns`, `DBIter`, wide-entity merge, and compaction-filter paths still depend on `Deserialize()` and therefore reject blob-backed V2 entities.
- **Source:** `table/get_context.cc`, method `GetContext::SaveValue`; `db/memtable.cc`, `kTypeWideColumnEntity` handling in memtable lookup; `db/db_iter.cc`, method `DBIter::SetValueAndColumnsFromEntity`; `db/merge_helper.h`, overload `TimedFullMerge(..., WideBaseValueTag, const Slice& entity, ...)`; `db/compaction/compaction_iterator.cc`, method `CompactionIterator::InvokeFilterIfNeeded`
- **Fix:** Replace the single broad "not wired" statement with an API-by-API matrix: plain reads can sometimes extract only the default column from V2 entities, while entity reads, iterators, merge, and compaction-filter paths still require V1/no blob refs.

## Completeness Gaps

### `allow_unprepared_value` is missing from the multi-CF iterator docs
- **Why it matters:** it changes the correctness contract for callers. With `ReadOptions::allow_unprepared_value = true`, a valid `CoalescingIterator` or `AttributeGroupIterator` can have an empty `value()`, `columns()`, or `attribute_groups()` until `PrepareValue()` is called.
- **Where to look:** `include/rocksdb/iterator_base.h`, method `PrepareValue`; `db/multi_cf_iterator_impl.h`, method `PopulateIterator`; `db/multi_cf_iterator_test.cc`, tests `CoalescingIteratorTest.AllowUnpreparedValue`, `CoalescingIteratorTest.AllowUnpreparedValue_Corruption`, and `AttributeGroupIteratorTest.AllowUnpreparedValue`
- **Suggested scope:** add a subsection to chapter 5, plus one sentence in the transaction chapter for transaction-backed multi-CF iterators

### Cross-CF iterator preconditions and read-option semantics are undocumented
- **Why it matters:** these iterators are not generic "merge any CFs" helpers. They require a non-empty CF list, compatible comparators, and iterator-style `io_activity` settings. They also preserve the consistent cross-CF snapshot behavior of `NewIterators()`, and tests cover explicit snapshots, implicit snapshots, and lower/upper bounds.
- **Where to look:** `db/db_impl/db_impl.cc`, template `DBImpl::NewMultiCfIterator`; `utilities/transactions/transaction_base.cc`, template `TransactionBaseImpl::NewMultiCfIterator`; `db/multi_cf_iterator_test.cc`, tests `DifferentComparatorsInMultiCFs`, `LowerAndUpperBounds`, `ConsistentViewExplicitSnapshot`, and `ConsistentViewImplicitSnapshot`
- **Suggested scope:** expand chapter 5; mention the transaction restrictions in chapter 9

### The attribute-group query model is under-documented
- **Why it matters:** `GetEntity(..., PinnableAttributeGroups*)` and `MultiGetEntity(..., PinnableAttributeGroups*)` do not discover attribute groups automatically. The caller pre-populates the desired column families in the result container, and RocksDB fan-outs point lookups over that caller-supplied list.
- **Where to look:** `include/rocksdb/db.h`, attribute-group `GetEntity`/`MultiGetEntity` comments; `db/db_impl/db_impl.cc`, attribute-group `GetEntity` and `MultiGetEntity` overloads; `db/wide/db_wide_basic_test.cc`, tests `GetEntityAsPinnableAttributeGroups` and `MultiCFMultiGetEntityAsPinnableAttributeGroups`
- **Suggested scope:** add detail to chapter 5, including a short usage example

### Secondary-index integration is entirely absent
- **Why it matters:** wide-column writes are now a first-class input to the secondary-index utilities. `PutEntity()` can inspect named columns, mutate primary column values, add/remove secondary entries, and surface callback-specific failures.
- **Where to look:** `utilities/secondary_index/secondary_index_mixin.h`, methods `UpdatePrimaryColumnValues`, `RemoveSecondaryEntries`, and `PutEntityWithSecondaryIndices`; `utilities/transactions/transaction_test.cc`, tests `TransactionTest.SecondaryIndexPutDelete` and `TransactionTest.SecondaryIndexPutEntity`
- **Suggested scope:** at least a short cross-reference in the index and a paragraph in chapter 3 or chapter 9

### The docs do not cover tested sanity/error paths for entity APIs
- **Why it matters:** invalid argument behavior is part of the contract for these APIs, especially around null CF handles, null output pointers, wrong `io_activity`, and attribute-group fan-out with invalid groups.
- **Where to look:** `db/wide/db_wide_basic_test.cc`, test `DBWideBasicTest.SanityChecks`; `utilities/transactions/transaction_test.cc`, test `TransactionTest.EntityReadSanityChecks`; `utilities/transactions/optimistic_transaction_test.cc`, test `OptimisticTransactionTest.EntityReadSanityChecks`
- **Suggested scope:** brief "preconditions and error modes" subsections in chapters 4, 5, and 9

## Depth Issues

### The transaction chapter needs a per-policy support matrix
- **Current:** the chapter describes transactions as a single feature bucket with a short list of read/write APIs.
- **Missing:** support differences between optimistic, write-committed, and write-prepared/write-unprepared transactions; iterator lifetime ("valid until Commit/Rollback/RollbackToSavePoint"); `GetEntityForUpdate()` validation rules; and the timestamp-validation nuances that only exist for write-committed transactions.
- **Source:** `include/rocksdb/utilities/transaction.h`; `utilities/transactions/transaction_base.cc`, methods `GetEntityForUpdate`, `GetCoalescingIterator`, and `GetAttributeGroupIterator`; `utilities/transactions/write_prepared_txn.cc`; `utilities/transactions/write_committed_transaction_ts_test.cc`, test `WriteCommittedTxnWithTsTest.GetEntityForUpdate`

### The blob chapter should be structured by API surface, not by one summary paragraph
- **Current:** the `Current Status` section mixes writer status, iterator status, and point-read behavior in a single short list.
- **Missing:** an explicit split between plain-value APIs (`Get`/`MultiGet`), entity APIs (`GetEntity`, `PinnableWideColumns`), iterators, merge, and compaction filter behavior when V2 entities with blob refs appear.
- **Source:** `table/get_context.cc`, `db/db_iter.cc`, `db/merge_helper.h`, `db/compaction/compaction_iterator.cc`

### The attribute-group chapter skips the status-propagation rules developers will hit first
- **Current:** the chapter says per-CF lookups may succeed or fail independently.
- **Missing:** top-level validation failures can mark otherwise valid groups as `Incomplete`; `NotFound` is carried per group while the top-level single-key API still returns `Status::OK()`; and the order of returned groups follows the caller's CF order.
- **Source:** `db/db_impl/db_impl.cc`, attribute-group `GetEntity`/`MultiGetEntity`; `db/wide/db_wide_basic_test.cc`, tests `GetEntityAsPinnableAttributeGroups` and `MultiCFMultiGetEntityAsPinnableAttributeGroups`; `db/attribute_group_iterator_impl.cc`, method `AddToAttributeGroups`

## Structure and Style Violations

### `index.md` is below the required line-count range
- **File:** `docs/components/wide_column/index.md`
- **Details:** the file is 39 lines long, below the requested 40-80 line range.

### Inline code quotes are used throughout the doc set
- **File:** all files under `docs/components/wide_column/`
- **Details:** the docs rely heavily on inline backticked identifiers, paths, and API names even though the style requirements for this doc set explicitly disallow inline code quotes.

### The transaction chapter's `Files:` line omits the implementation files that back the documented behavior
- **File:** `docs/components/wide_column/09_transaction_support.md`
- **Details:** the chapter only lists public headers, while the actual wide-column transaction behavior lives in `utilities/transactions/transaction_base.cc`, `utilities/transactions/pessimistic_transaction.cc`, and `utilities/transactions/write_prepared_txn.cc`. This makes the chapter harder to verify and maintain than the other chapters.

## Undocumented Complexity

### V2 blob-backed entities fail differently depending on which API touches them
- **What it is:** plain reads can sometimes succeed by extracting only the default column; `GetEntity()` and iterators reject blob-backed V2 entities; wide-entity merge and compaction-filter code paths also deserialize V1-only; and some table-file lookups surface the failure as `Status::Corruption()` after `GetContext` maps an internal deserialization failure to `kCorrupt`.
- **Why it matters:** this is exactly the kind of behavior split that burns time during debugging or format migrations.
- **Key source:** `table/get_context.cc`, methods `SaveValue` and `MergeWithWideColumnBaseValue`; `db/version_set.cc`, switch on `GetContext::State()`; `db/db_iter.cc`, method `SetValueAndColumnsFromEntity`; `db/merge_helper.h`; `db/compaction/compaction_iterator.cc`
- **Suggested placement:** chapter 7

### Transactional entity reads go through `WriteBatchWithIndex`, not a special transaction-only path
- **What it is:** `Transaction::GetEntity()` and `Transaction::MultiGetEntity()` use `WriteBatchWithIndex::{GetEntityFromBatchAndDB,MultiGetEntityFromBatchAndDB}`. These helpers first resolve the transaction-local batch, then fall back to DB lookups, and can merge batch operands with DB state before commit.
- **Why it matters:** it explains read-your-own-writes behavior, merge visibility, and why transaction-wide-column behavior tracks WBWI behavior so closely.
- **Key source:** `utilities/transactions/transaction_base.h`, methods `GetEntityImpl` and `MultiGetEntityImpl`; `utilities/write_batch_with_index/write_batch_with_index.cc`, methods `GetEntityFromBatchAndDB` and `MultiGetEntityFromBatchAndDB`; `utilities/write_batch_with_index/write_batch_with_index_test.cc`, wide-column batch+DB tests
- **Suggested placement:** chapter 9

### Secondary-index callbacks can mutate and veto `PutEntity()`
- **What it is:** the secondary-index mixin sorts the entity, looks up the indexed named/default columns with `WideColumnsHelper::Find`, lets the secondary index rewrite the primary column value, and may fail the write with `InvalidArgument`, `NotFound`, `Corruption`, or `NotSupported` depending on callback behavior.
- **Why it matters:** a developer debugging a failing or unexpectedly rewritten `PutEntity()` needs to know the wide-column write path is no longer isolated once secondary indices are configured.
- **Key source:** `utilities/secondary_index/secondary_index_mixin.h`, methods `UpdatePrimaryColumnValues`, `RemoveSecondaryEntries`, and `PutEntityWithSecondaryIndices`; `utilities/transactions/transaction_test.cc`, test `TransactionTest.SecondaryIndexPutEntity`
- **Suggested placement:** chapter 3 or a new cross-component chapter

### Multi-CF iterators preserve CF order in different ways for coalesced columns vs. attribute groups
- **What it is:** `AttributeGroupIterator` returns groups in caller CF order, while `CoalescingIterator` uses the same per-key CF ordering to resolve duplicate column names by last-writer-wins across the input CF list.
- **Why it matters:** if a reader assumes both iterators are just two views over the same merged state, they will misread duplicate-column behavior and default-column selection.
- **Key source:** `db/multi_cf_iterator_impl.h`; `db/attribute_group_iterator_impl.cc`; `db/coalescing_iterator.cc`; `db/multi_cf_iterator_test.cc`, test `CoalescingIteratorTest.WideColumns`
- **Suggested placement:** chapter 5

## Positive Notes

- The core-data-structures chapter correctly captures the default-column bridge between plain values and entities, which is the most important conceptual hook in this subsystem.
- The serialization chapter reflects the recent V2 format work well, including the skip-info purpose and the split between public V1 writers and blob-aware V2 infrastructure.
- The `PinnableWideColumns` section is one of the stronger parts of the doc set: the move-semantics and reset-on-deserialization-failure behavior match both the implementation and the dedicated regression tests.
