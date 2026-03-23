# Review: wide_column -- Claude Code

## Summary
Overall quality: **good**

The wide_column documentation is well-structured and technically accurate on most claims. The serialization format descriptions (Ch 02) are particularly strong -- every field, section, and format detail was verified against `wide_column_serialization.h` and `wide_column_serialization.cc`. The write and read path chapters correctly describe the flow and value type handling. The main concerns are: (1) a few INVARIANT labels applied to non-invariant properties, (2) some missing coverage of edge cases in the compaction path, (3) incomplete mention of the `kTypeValuePreferredSeqno` interaction, and (4) minor structural issues with index.md being slightly short. No major factual errors were found.

## Correctness Issues

### [MISLEADING] Index.md invariant #2 is not an independently enforced invariant
- **File:** `index.md`, Key Invariants section
- **Claim:** "The default column (empty name) is always at index 0 when present, because columns are sorted"
- **Reality:** This is a derived property from invariant #1 (strict bytewise ordering), not an independently enforced invariant. Nothing crashes or corrupts if this "invariant" is violated -- it would be caught by the ordering check. Labeling it as a key invariant implies independent enforcement.
- **Source:** `db/wide/wide_column_serialization.cc` -- `ValidateColumnOrder()` enforces ordering; no separate check for default-column position
- **Fix:** Move this to "Key Characteristics" or rephrase as "Because columns are sorted (invariant #1), the default column is always at index 0 when present."

### [MISLEADING] Index.md invariant #4 is a usage requirement, not a correctness invariant
- **File:** `index.md`, Key Invariants section
- **Claim:** "V2 entities with blob columns must be resolved to V1 before consumption by V1-only APIs (e.g., PinnableWideColumns::SetWideColumnValue)"
- **Reality:** Violating this does not cause data corruption or crash. `Deserialize()` returns `Status::NotSupported` when it encounters blob columns. This is a usage requirement / API contract, not a data integrity invariant.
- **Source:** `db/wide/wide_column_serialization.cc`, `Deserialize()` lines 399-401
- **Fix:** Move to a "Usage Notes" or "Important" callout rather than labeling as an invariant.

### [MISLEADING] Chapter 07 "Key Invariant" label misused
- **File:** `07_blob_integration.md`, Detecting Blob Columns section
- **Claim:** "**Key Invariant:** Only V2 entities can have blob columns. V1 entities always return `has_blob_columns = false`."
- **Reality:** This is a design fact about the format versioning, not a correctness invariant. No data corruption or crash occurs from this property. The code simply checks `version < kVersion2` and returns false.
- **Source:** `db/wide/wide_column_serialization.cc`, `HasBlobColumns()` lines 471-474
- **Fix:** Change "Key Invariant" to "Design property" or "Important".

### [MISLEADING] Chapter 04 oversimplifies blob handling in GetEntity point lookups
- **File:** `04_read_path.md`, GetEntity section
- **Claim:** "kTypeBlobIndex: Resolves the blob reference, then treats the result as a plain value"
- **Reality:** In the GetEntity point-lookup path via `GetContext::SaveValue()`, when `kTypeBlobIndex` is encountered in the non-merge case, the blob is NOT resolved inline. Instead, `is_blob_index_` is set to true and the raw blob index is stored via `SetPlainValue()`. Resolution happens outside GetContext, later in the read pipeline. The claim is accurate for the iterator path (`DBIter::SetValueAndColumnsFromBlob`) and the merge case, but not for the primary GetEntity point-lookup path.
- **Source:** `table/get_context.cc` lines 313-351 (non-merge blob handling), `db/db_iter.cc` line 250 (iterator blob handling)
- **Fix:** Clarify that blob resolution timing differs between point lookups (deferred) and iteration (inline).

### [MISLEADING] Chapter 04 GetImpl delegation description is imprecise
- **File:** `04_read_path.md`, GetEntity section
- **Claim:** "It delegates to the same GetImpl() infrastructure as Get(), but passes a PinnableWideColumns* instead of a PinnableSlice*."
- **Reality:** Both `Get()` and `GetEntity()` call the same `GetImpl()` with the same `GetImplOptions` struct. The difference is which fields are populated: `GetEntity` sets `get_impl_options.columns` (a `PinnableWideColumns*`), while `Get` sets `get_impl_options.value` (a `PinnableSlice*`). The phrasing "instead of" suggests a parameter type substitution, when it's actually different fields of the same struct.
- **Source:** `db/db_impl/db_impl.cc` lines 2380-2384, `db/db_impl/db_impl.h` line 683 (`GetImplOptions` struct)
- **Fix:** Say "sets `GetImplOptions::columns` instead of `GetImplOptions::value`" for precision.

### [MISLEADING] Chapter 03 conflates WriteBatch::PutEntity and WriteBatchInternal::PutEntity
- **File:** `03_write_path.md`, WriteBatch::PutEntity section
- **Claim:** Lists 5 steps (timestamp check, sort, serialize, validate size, append) as a single flow in `WriteBatch::PutEntity() in db/write_batch.cc`
- **Reality:** The flow is split across two functions: `WriteBatch::PutEntity()` (public, does timestamp check at line 1143) calls `WriteBatchInternal::PutEntity()` (internal, does sort/serialize/validate/append at lines 1074-1122). The doc makes it appear as one function.
- **Source:** `db/write_batch.cc` lines 1074-1148
- **Fix:** Mention that the public `WriteBatch::PutEntity()` handles the timestamp check, then delegates to `WriteBatchInternal::PutEntity()` for sort/serialize/append. This distinction matters because `WriteBatchInternal::PutEntity()` can be called directly (e.g., by transaction code) and bypasses the timestamp check.

### [MISLEADING] Chapter 06 - FullMergeV3 default handles absent base differently than described
- **File:** `06_compaction_and_merge.md`, Default FullMergeV3 Fallback section
- **Claim:** "1. Base is plain value or absent: Falls back to FullMergeV2() directly"
- **Reality:** The code uses `std::visit` with `overload{}` pattern. Both `std::monostate` (absent) and `Slice` (plain value) are handled by the same lambda via `if constexpr`. For absent base, `existing_value` stays nullptr; for plain value, it points to the slice. The doc's phrasing is technically correct but obscures the implementation detail that absent-base passes nullptr (not empty) to FullMergeV2.
- **Source:** `db/merge_operator.cc` lines 43-65
- **Fix:** Minor: clarify that for absent base, `FullMergeV2()` receives `existing_value = nullptr` (not empty string).

## Completeness Gaps

### Missing: FilterV3 Decision enum values in Ch 06
- **Why it matters:** The FilterV3 decisions table only lists 5 of 9 values (kKeep, kRemove, kChangeValue, kChangeWideColumnEntity, kRemoveAndSkipUntil). It omits kPurge (SingleDelete-type removal), kChangeBlobIndex (stacked BlobDB), kIOError (stacked BlobDB), and kUndetermined (FilterBlobByKey). While the omitted values are internal or specialized, kPurge is a general-purpose decision that users could encounter.
- **Where to look:** `include/rocksdb/compaction_filter.h` lines 75-151
- **Suggested scope:** Add kPurge to the table with a note; mention the others as internal-only in a footnote.

### Missing: kTypeColumnFamilyWideColumnEntity (0x17) WAL-only type
- **Why it matters:** Developers debugging WAL records or working on WAL replay need to know about this type. The ValueType table in Ch 03 only lists `kTypeWideColumnEntity` (0x16) but the WAL uses `kTypeColumnFamilyWideColumnEntity` (0x17) for non-default column families.
- **Where to look:** `db/dbformat.h` line 72
- **Suggested scope:** Brief mention in the ValueType table in Ch 03, noting it's WAL-only.

### Missing: kTypeValuePreferredSeqno interaction in compaction
- **Why it matters:** The compaction iterator's clear-and-output path (single deletion) converts `kTypeValuePreferredSeqno` to `kTypeValue` alongside `kTypeBlobIndex` and `kTypeWideColumnEntity`. This interaction is undocumented.
- **Where to look:** `db/compaction/compaction_iterator.cc` lines 607-626
- **Suggested scope:** Brief mention in Ch 06 alongside the existing single deletion coverage.

### Missing: How Get() (not GetEntity) handles entities internally
- **Why it matters:** The backward compatibility chapter describes the user-visible behavior but doesn't explain the internal mechanism -- specifically how `GetContext::SaveValue()` extracts the default column when `Get()` is called on an entity key. Developers modifying the read path need this.
- **Where to look:** `table/get_context.cc` -- `SaveValue()` method, `GetValueOfDefaultColumn()` call path
- **Suggested scope:** Add a paragraph to Ch 08 explaining the internal flow.

### Missing: SstFileWriter::PutEntity implementation details
- **Why it matters:** Ch 03 only has one sentence about `SstFileWriter::PutEntity`. It doesn't mention where the implementation lives or how it differs from the WriteBatch path.
- **Where to look:** `table/sst_file_writer.cc`
- **Suggested scope:** Add 2-3 sentences: implementation file, whether it validates key ordering, and any differences from the WriteBatch path (e.g., no timestamp check since SstFileWriter doesn't use timestamps the same way).

### Missing: DB::GetEntity for PinnableAttributeGroups overload
- **Why it matters:** Ch 04 mentions `GetEntity()` with `PinnableWideColumns*` and `MultiGetEntity` but doesn't mention the `GetEntity()` overload that takes `PinnableAttributeGroups*` for single-key multi-CF reads. This is distinct from `MultiGetEntity` and is documented only in Ch 05.
- **Where to look:** `db/db_impl/db_impl.cc` line 2387 -- `DBImpl::GetEntity(const ReadOptions&, const Slice& key, PinnableAttributeGroups*)`
- **Suggested scope:** Add a cross-reference in Ch 04 pointing to Ch 05.

### Missing: Secondary/readonly/follower instance behavior
- **Why it matters:** `PutEntity` returns `Status::NotSupported` on secondary, readonly, and follower DB instances (they are read-only). However, `GetEntity`, `MultiGetEntity`, and cross-CF iterators work transparently via inheritance from `DBImpl`. Developers using these modes need to know entity reads are supported even though writes are not.
- **Where to look:** `db/db_impl/db_impl_secondary.h` lines 134-144, `db/db_impl/db_impl_readonly.h` lines 47-57
- **Suggested scope:** Brief mention in Ch 09 or a new "Deployment Modes" section.

### Missing: Transaction PutEntityUntracked
- **Why it matters:** `PutEntityUntracked()` allows entity writes without conflict tracking in pessimistic transactions. Ch 09 doesn't mention this variant.
- **Where to look:** `utilities/transactions/transaction_base.h` lines 215-221
- **Suggested scope:** Brief mention in Ch 09 alongside PutEntity.

### Missing: Error behavior differences between size validation and ordering validation
- **Why it matters:** `Serialize()` returns `Status::Corruption` for ordering violations but `Status::InvalidArgument` for size limit violations. The doc mentions Corruption for ordering but doesn't distinguish the error types for size validation.
- **Where to look:** `db/wide/wide_column_serialization.h` -- `ValidateWideColumnLimit()` returns `InvalidArgument`, `ValidateColumnOrder()` returns `Corruption`
- **Suggested scope:** Brief note in Ch 02 Serialization section.

## Depth Issues

### Chapter 04 - GetEntity internal path needs more detail
- **Current:** "It delegates to the same `GetImpl()` infrastructure as `Get()`, but passes a `PinnableWideColumns*` instead of a `PinnableSlice*`."
- **Missing:** The mechanism of how `GetImplOptions` struct carries the `columns` pointer through `GetImpl()` to `GetContext`, and how `GetContext::SaveValue()` handles the different value types when `columns` is non-null vs when `value` is non-null.
- **Source:** `db/db_impl/db_impl.cc` lines 2380-2384, `table/get_context.cc`

### Chapter 06 - Compaction filter value type conversion lacks detail on the reverse path
- **Current:** Mentions that filters can convert values to entities and vice versa
- **Missing:** When converting from plain value to entity (`kChangeWideColumnEntity`), the compaction iterator updates `ikey_.type` and `current_key_` (lines 439-442 of `compaction_iterator.cc`). The doc should mention that this type update in the internal key is automatic and transparent to the filter.
- **Source:** `db/compaction/compaction_iterator.cc` lines 439-442

### Chapter 09 - Transaction restrictions section is thin
- **Current:** Only mentions timestamp incompatibility
- **Missing:** Any discussion of conflict detection behavior with wide columns -- does `GetEntityForUpdate` lock at the key level or column level? (Answer: key level, same as `GetForUpdate`). Also missing: behavior when mixing `Put` and `PutEntity` within the same transaction for the same key.
- **Source:** `include/rocksdb/utilities/transaction.h`, `utilities/transactions/`

## Structure and Style Violations

### index.md is 39 lines (below 40-80 target)
- **File:** `index.md`
- **Details:** The file has 39 lines, slightly below the 40-line minimum. The Key Invariants section could be expanded or a "Usage Notes" section could be added to reach the target.

### Two invariants in index.md should not use INVARIANT label
- **File:** `index.md`
- **Details:** Invariants #2 and #4 (as described in Correctness Issues above) are not true correctness invariants. Invariant #2 is a derived property; #4 is a usage requirement. Only invariants #1 and #3 are true correctness invariants (violating them could lead to data corruption).

## Undocumented Complexity

### GetContext SaveValue handling for wide-column entities
- **What it is:** When `GetContext::SaveValue()` encounters a `kTypeWideColumnEntity` during a point lookup, it takes different paths depending on whether the caller requested `PinnableWideColumns*` (via `GetEntity`) or `PinnableSlice*` (via `Get`). For `Get()`, it calls `WideColumnSerialization::GetValueOfDefaultColumn()` to extract just the default column. For `GetEntity()`, it calls `PinnableWideColumns::SetWideColumnValue()`.
- **Why it matters:** Developers modifying the read path need to understand that the same `SaveValue` function serves both `Get` and `GetEntity`, with different output paths.
- **Key source:** `table/get_context.cc` -- `GetContext::SaveValue()`
- **Suggested placement:** Add to existing chapter 04 (Read Path) or chapter 08 (Backward Compatibility)

### WriteBatchInternal::PutEntity bypasses timestamp check
- **What it is:** `WriteBatchInternal::PutEntity()` does not check for timestamp-enabled column families. Only the public `WriteBatch::PutEntity()` does. Transaction code and internal callers that use `WriteBatchInternal::PutEntity()` directly bypass the timestamp check.
- **Why it matters:** If a timestamp-enabled CF is used with the internal API, the entity would be written without a timestamp, potentially causing data inconsistency.
- **Key source:** `db/write_batch.cc` -- `WriteBatchInternal::PutEntity()` (line 1074) vs `WriteBatch::PutEntity()` (line 1124)
- **Suggested placement:** Add to chapter 03 (Write Path) as an "Internal API Note"

### Compaction clear-and-output converts entity to plain value
- **What it is:** When single deletion cannot be compacted away (e.g., snapshot prevents it), the next put/entity has its data cleared. For `kTypeWideColumnEntity`, the type is converted to `kTypeValue` (along with `kTypeBlobIndex` and `kTypeValuePreferredSeqno`). The entity's data is replaced with an empty value.
- **Why it matters:** This means an entity can silently become an empty plain value during compaction if a single deletion is involved. Users relying on entity data should be aware of this edge case.
- **Key source:** `db/compaction/compaction_iterator.cc` lines 607-626
- **Suggested placement:** Expand existing single deletion coverage in chapter 06

### PinnableWideColumns move semantics re-derive column index
- **What it is:** When a `PinnableWideColumns` is moved, if the underlying buffer pointer changes, the column index must be re-derived via `CreateIndexForWideColumns()` (deserialization). This is because `WideColumns` entries contain `Slice` objects that point into the buffer. The move constructor in `wide_columns.h` (line 152-181) detects this case and re-parses.
- **Why it matters:** This has a performance implication -- moving `PinnableWideColumns` may trigger a full deserialization. Callers storing results in containers should be aware.
- **Key source:** `include/rocksdb/wide_columns.h` -- `PinnableWideColumns::Move()` lines 152-181
- **Suggested placement:** Add to chapter 01 under PinnableWideColumns Memory Management

### Flush and block-based table are fully transparent to wide columns
- **What it is:** The flush path has zero wide-column-specific code. Wide-column entities flow through `FlushJob` as regular key-value pairs with `kTypeWideColumnEntity` type. Similarly, the block-based table reader/writer has no awareness of wide columns -- serialized entities are opaque value blobs. Deserialization only happens in `DBIter` and `GetContext`.
- **Why it matters:** Developers modifying flush or table code don't need to worry about wide-column handling. This is useful architectural context.
- **Key source:** `db/flush_job.cc` (no wide column includes), `table/block_based/` (no wide column references)
- **Suggested placement:** Brief note in Ch 03 (Write Path) expanding on "Standard flush and compaction pipelines handle the entity like any other value type"

### db_stress_wide_merge_operator as FullMergeV3 reference implementation
- **What it is:** `db_stress_tool/db_stress_wide_merge_operator.h` implements a real `FullMergeV3` merge operator for stress testing wide columns. It handles all three input variants (monostate, Slice, WideColumns) and outputs `NewColumns`.
- **Why it matters:** Users implementing custom wide-column-aware merge operators could use this as a reference. The doc mentions merge operators conceptually but provides no example.
- **Key source:** `db_stress_tool/db_stress_wide_merge_operator.h`
- **Suggested placement:** Reference in Ch 06 alongside the FullMergeV3 discussion

## Positive Notes

- **Serialization format documentation is excellent.** Chapters 01 and 02 provide accurate, detailed descriptions of both V1 and V2 formats. Every field, section offset, and varint was verified against the code. The V2 skip-info explanation and design rationale are particularly well done.
- **Backward compatibility chapter is thorough.** Chapter 08 correctly covers all the interop scenarios (Put/GetEntity, PutEntity/Get, Iterator value/columns for mixed data) and the default FullMergeV3 fallback. The migration patterns section is a useful addition.
- **Compaction filter and merge operator coverage is strong.** Chapter 06 accurately describes FilterV3 decisions, value type conversion, and FullMergeV3 input/output variants. The note about automatic sorting of NewColumns is valuable.
- **No box-drawing characters, no line number references, no inline code quotes.** The documentation follows the style guidelines well.
- **Good use of tables.** The ValueType table in Ch 03, FilterV3 decisions table in Ch 06, and iterator behavior table in Ch 08 are clear and accurate.
