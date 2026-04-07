# PR #14386 Feedback
# https://github.com/facebook/rocksdb/pull/14386

## 1. clang-tidy: 6 warnings (github-actions, 2026-02-25)

### db/db_open_with_config_test.cc (2 warnings)
- Line 94: use std::make_shared instead [modernize-make-shared]
- Line 290: The value '2049' provided to the cast expression is not in the valid range of values for 'VerifyOutputFlags' [clang-analyzer-optin.core.EnumCastOutOfRange]

### db/version_edit.cc (1 warning)
- Line 87: constness of 's' prevents automatic move [performance-no-automatic-move]

### db/wide/wide_column_serialization_test.cc (3 warnings)
- Line 872: use emplace_back instead of push_back [modernize-use-emplace]
- Line 873: use emplace_back instead of push_back [modernize-use-emplace]
- Line 876: use emplace_back instead of push_back [modernize-use-emplace]

---

## 2. Claude Code Review (github-actions, 2026-04-02)

### CRITICAL (3)

**C1: PrepareOutput entity dispatch**
PrepareOutput must be extended with a kTypeWideColumnEntity branch to call
GarbageCollectEntityBlobsIfNeeded() and ExtractLargeColumnValuesIfNeeded(),
otherwise entities bypass blob extraction/GC during compaction.

**C2: entity_columns_ stale data**
Member variable persists across iterations and may not be cleared when no
compaction filter is active, risking corrupt output from stale V2 data
applied to a V1 entity.

**C3: V2 forward compatibility**
Deserialize() returns NotSupported on V2 data, which could break rolling
upgrades if V2 entities are written before all nodes are upgraded.

### HIGH (5)

**H1**: SerializeV2Impl allocates std::vector<std::string> for all columns,
not just blob columns.

**H2**: BlobGarbageMeter::ProcessEntityBlobReferences double-parses the
internal key.

**H3**: FilterV3 eager resolution may cause memory spikes with many large
blob columns.

**H4**: entity_deserialized_ flag state management needs defensive assertions.

**H5**: BlobFetcher write-path fallback ignores prefetch and bytes-read
accounting.

### MEDIUM (5)

**M1**: API proliferation (FilterV2->V3->V4) increases maintenance burden.

**M2**: "No extraction if blob columns exist" skips entities with any existing
blob column.

**M3**: resolved_cache_ uses linear search (O(n^2) for many blob columns).

**M4**: Slice lifetime chain in InvokeFilterIfNeeded is correct but fragile.

**M5**: BlobGarbageMeter refactored control flow is less obvious.

### Test Coverage Gaps
- FilterV4 with kChangeValue/kChangeWideColumnEntity/kRemoveAndSkipUntil
- Corrupt V2 entity handling, concurrent compaction stress tests
- Very large column counts, zero columns, reverse iteration with V2 entities

Full details in review-findings.md (in the PR worktree).
