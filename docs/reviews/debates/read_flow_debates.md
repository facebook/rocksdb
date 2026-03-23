# Disagreements: read_flow

## Summary

No direct factual contradictions were found between the CC and Codex reviews. Both reviewers identified overlapping issues from different angles and complementary gaps. The reviews were largely additive -- CC focused more on field-level accuracy (struct fields, enum locations, function names) while Codex focused more on semantic accuracy (option behavior, API scope, partition block caching).

## Near-Disagreement: SuperVersion Cleanup Characterization

- **CC position**: "The cleanup path involves `to_delete` autovector in the SuperVersion struct, and the actual deletion happens outside the mutex." (Depth issue, requesting more detail)
- **Codex position**: "Once `Unref()` hits zero, `SuperVersion::Cleanup()` runs immediately... Some obsolete-file purging can also run on the caller thread." (Correctness issue, saying doc is misleading to call it "deferred")
- **Code evidence**: `SuperVersion::Cleanup()` in `db/column_family.cc:523-538` runs immediately under mutex, unreffing mem/imm/current. `DBImpl::CleanupSuperVersion()` in `db/db_impl/db_impl.cc:2194-2222` either deletes the SV immediately or defers via `AddSuperVersionsToFreeQueue` + `SchedulePurge` based on `background_purge`.
- **Resolution**: Both reviewers are correct. CC wanted more detail about the mechanics, Codex wanted the word "deferred" corrected. The fix separates immediate cleanup from optional deferred purge.
- **Risk level**: medium -- the original wording could mislead developers into thinking no cleanup runs synchronously

## Near-Disagreement: kPersistedTier Scope

- **CC position**: "The skip is conditional [on has_unpersisted_data_]" (identified as undocumented complexity)
- **Codex position**: "currently supports only Get and MultiGet; iterator creation returns Status::NotSupported(). For point lookups, memtables are skipped only when has_unpersisted_data_ is true." (identified as correctness issue)
- **Code evidence**: `db/db_impl/db_impl.cc:2615-2616` confirms conditional skip. `db/db_impl/db_impl.cc:4013-4016` confirms NotSupported for iterators.
- **Resolution**: Both correct; Codex provided the more complete picture (also noting iterator incompatibility). Fix incorporates both observations.
- **Risk level**: medium -- the original "skip memtables, read only from persisted data" was a significant oversimplification
