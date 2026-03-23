# Debates: public_api_write

## Debate: Merge Operator Name Persistence

- **CC position**: The merge operator name is stored in the OPTIONS file (via `ImmutableCFOptions` serialization in `options/cf_options.cc`) and verified during `DB::Open()`. Not stored in the MANIFEST.
- **Codex position**: The merge operator name is persisted in SST table properties as `merge_operator_name` (in `block_based_table_builder.cc`), but not used as an enforced DB-open compatibility check.
- **Code evidence**: Both are correct. The name is stored in TWO places:
  1. **OPTIONS file** (`options/cf_options.cc:887-891`): Serialized as part of `cf_immutable_options_type_info`. Verified by name at DB open via `VerifyCFOptions()` with `kByNameAllowFromNull` verification type.
  2. **SST table properties** (`table/block_based/block_based_table_builder.cc:2456-2459`): Written as `rocksdb.merge.operator` property per SST file. Read back in `table/table_properties.cc:393-394`. This is informational metadata -- NOT used for DB-open consistency checks.
- **Resolution**: Both reviewers identified real persistence paths. CC was more correct about the enforcement aspect (OPTIONS file is checked at DB open). Codex was correct about per-SST persistence. The original doc's claim that "the name is not stored persistently" was wrong -- it is stored in both locations. Fix combines both findings: mention OPTIONS file verification AND per-SST informational storage.
- **Risk level**: medium -- the original doc gave the wrong impression that there is no persistence or enforcement at all.

## Debate: SingleDelete/DeleteRange Restriction

- **CC position**: Did not flag this claim.
- **Codex position**: Flagged as [UNVERIFIABLE] -- could not find a code check, header contract, or test documenting this as a supported restriction.
- **Code evidence**: The `DB::SingleDelete()` API docs in `include/rocksdb/db.h:486-488` restrict mixing with Delete and Merge but do NOT mention DeleteRange. The `DB::DeleteRange()` docs have no mention of SingleDelete. The compaction iterator's SingleDelete logic (`compaction_iterator.cc:700-708`) does skip range tombstone sentinel keys when peeking ahead, but there is no explicit enforcement against using both on overlapping ranges.
- **Resolution**: Codex was correct that this is unverifiable as a hard restriction. The interaction is undefined but not enforced. Reworded to state that it is not explicitly enforced but can lead to undefined behavior.
- **Risk level**: low -- the restriction is reasonable advice but presenting it as an enforced contract was misleading.

## Debate: WBWI RollbackToSavePoint No-Save-Point Behavior

- **CC position**: WBWI delegates to `rep->write_batch.RollbackToSavePoint()`, which returns `Status::NotFound()`. The WBWI header comment (line 358) saying "behaves the same as Clear()" contradicts the implementation (line 363 says "Status::NotFound()").
- **Codex position**: Same conclusion -- WBWI delegates to base, returns `NotFound`. Test suite asserts this behavior.
- **Code evidence**: Both correct. `write_batch_with_index.cc:1145-1155` delegates to `rep->write_batch.RollbackToSavePoint()`. `write_batch.cc:1829-1831` returns `Status::NotFound()` when no save point exists. The base class doc in `write_batch_base.h:151-153` says "behaves the same as Clear()" but this is a stale/incorrect comment -- the implementation returns NotFound.
- **Resolution**: Both reviewers agreed. The doc claim was wrong. Removed the incorrect "Note:" paragraph.
- **Risk level**: low -- both reviewers agreed, clear code evidence.
