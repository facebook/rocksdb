# Fix Summary: wide_column

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 7 |
| Completeness | 12 |
| Structure | 4 |
| Style | 1 |

## Disagreements Found

3 disagreements documented in `wide_column_debates.md`:
1. **Coalescing precedence rule** (high risk) -- Codex was right, doc had the rule backwards
2. **Inline code quotes** (low risk) -- CC was right, backticked identifiers are standard practice
3. **Blob chapter "not wired" characterization** (medium risk) -- Codex was right, needed API-by-API matrix

## Changes Made

### index.md
- Reclassified invariants #2 (default column position) and #4 (V2 blob resolution requirement) out of Key Invariants
- Moved default column position to Key Characteristics as a derived property
- Moved blob resolution requirement to new "Important Usage Notes" section
- Added secondary-index cross-reference, secondary/readonly instance note, and timestamp restriction
- Expanded from 39 lines to 50+ lines (meets 40-80 target)

### 01_core_data_structures.md
- No changes needed

### 02_serialization_format.md
- No changes needed (serialization coverage was praised by both reviewers)

### 03_write_path.md
- Split WriteBatch::PutEntity description into public and internal (`WriteBatchInternal::PutEntity`) with delegation explanation
- Added note about internal callers bypassing timestamp check
- Added `kTypeColumnFamilyWideColumnEntity` (0x17) WAL-only type to ValueType table
- Expanded SstFileWriter::PutEntity with implementation file reference and behavioral differences
- Added note about size validation returning `Status::InvalidArgument` vs ordering returning `Status::Corruption`
- Added note about flush and block-based table being fully transparent to wide columns
- Added `table/sst_file_writer.cc` to Files line

### 04_read_path.md
- Fixed GetImpl delegation: "sets `GetImplOptions::columns` instead of `GetImplOptions::value`" (not parameter type substitution)
- Fixed blob handling: documented that blob resolution is deferred (not inline) in GetEntity point lookups via `Version::Get()`
- Added new section "How Get() Handles Entities Internally" explaining `GetContext::SaveValue()` dual path
- Added note about iterator vs point-lookup blob resolution difference
- Added cross-reference to chapter 5 for `PinnableAttributeGroups` overload
- Added `db/version_set.cc` and `table/get_context.cc` to Files line

### 05_attribute_groups.md
- **Fixed coalescing precedence rule** (was backwards): later CF wins, not earlier CF
- Added detail on attribute-group query model (caller pre-populates CFs, not auto-discovery)
- Added status propagation rules (per-CF NotFound, top-level Incomplete, CF order preserved)
- Added Preconditions section (non-empty CF list, compatible comparators, io_activity, snapshot behavior)
- Added Unprepared Value Mode section (`allow_unprepared_value` + `PrepareValue()`)
- Added note about CoalescingIterator vs AttributeGroupIterator CF ordering semantics difference

### 06_compaction_and_merge.md
- Added `kPurge` to FilterV3 decisions table with description
- Added note about internal-only filter decisions (`kChangeBlobIndex`, `kIOError`, `kUndetermined`)
- Added `kTypeValuePreferredSeqno` to clear-and-output conversion description
- Added detail about compaction filter value type conversion being automatic (ikey_.type update)
- Added FullMergeV3 absent-base detail: `existing_value = nullptr` not empty string
- Added reference to `db_stress_wide_merge_operator.h` as FullMergeV3 reference implementation

### 07_blob_integration.md
- Changed "Key Invariant" to "Design property" for V1-only blob column check
- Replaced single "not wired" summary paragraph with API-by-API status matrix table
- Added separate rows for Get/MultiGet, GetEntity, Iterator::columns(), Iterator::value(), Merge, and Compaction filter

### 08_backward_compatibility.md
- Expanded "Reading Entities with Plain APIs" with internal mechanism: `GetContext::SaveValue()` detects null `columns_`, calls `GetValueOfDefaultColumn()`
- Added `table/get_context.cc` to Files line

### 09_transaction_support.md
- Added per-policy support matrix (optimistic, write-committed, write-prepared, write-unprepared)
- Documented `Status::NotSupported` for cross-CF iterators on write-prepared/write-unprepared
- Added `PutEntityUntracked` mention
- Added `GetEntityForUpdate` key-level (not column-level) conflict detection note
- Added `WriteBatchWithIndex` read-your-own-writes mechanism explanation
- Added mixing Put/PutEntity restriction note
- Expanded Files line with implementation files (`transaction_base.cc`, `pessimistic_transaction.cc`, `write_prepared_txn.cc`)
