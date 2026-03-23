# Compaction Documentation Debates

## Debate: CompactionJob pipeline scope
- **CC position**: No issue flagged. CC did not challenge the claim that "every compaction runs through CompactionJob."
- **Codex position**: Multiple non-CompactionJob paths exist: FIFO deletion, FIFO temperature-change trivial copy, and trivial moves bypass CompactionJob entirely.
- **Code evidence**: `DBImpl::BackgroundCompaction()` in `db/db_impl/db_impl_compaction_flush.cc` has a five-way if/else chain: (1) FIFO deletion calls `LogAndApply()` directly, (2) FIFO trivial copy calls `CopyFile()` then `LogAndApply()`, (3) trivial move calls `PerformTrivialMove()` then `LogAndApply()`, (4) bottom-pri forwarding, (5) full CompactionJob path. Only path 5 constructs a CompactionJob.
- **Resolution**: Codex was right. Fixed in 01_overview.md, 05_trivial_move.md, and 08_compaction_job.md.
- **Risk level**: high -- maintainers building mental models of compaction execution would be significantly misled.

## Debate: Inline code quotes as style violation
- **CC position**: "No structural or style violations found. All checks pass."
- **Codex position**: "Inline code quotes are used throughout the entire doc set." Considers backtick-formatted identifiers a systemic violation.
- **Code evidence**: N/A -- this is a style interpretation question.
- **Resolution**: CC is right. The prohibition on "inline code quotes" refers to multi-line code snippets quoted inline, not backtick formatting for function/struct names. Backticks for identifiers (e.g., `ShouldStopBefore()`) are standard documentation practice and improve readability. No changes made.
- **Risk level**: low

## Debate: CompactionService header location
- **CC position**: The doc's reference to `include/rocksdb/options.h` for CompactionService is misleading. CC claimed the class is defined in `include/rocksdb/compaction_service.h` (or similar).
- **Codex position**: Did not flag this issue.
- **Code evidence**: The `CompactionService` class with its `Schedule()`, `Wait()`, `CancelAwaitingJobs()`, and `OnInstallation()` methods IS defined in `include/rocksdb/options.h` (lines 537-567). No `compaction_service.h` file exists. The doc's reference was correct.
- **Resolution**: CC was wrong. The doc's reference to `include/rocksdb/options.h` is accurate. No changes needed.
- **Risk level**: low

## Debate: FIFO ttl default value
- **CC position**: Did not flag the documented default of 0.
- **Codex position**: The raw default is `UINT64_MAX - 1` (sentinel), sanitized to 30 days for block-based tables, 0 otherwise.
- **Code evidence**: `include/rocksdb/advanced_options.h` line 893: `uint64_t ttl = 0xfffffffffffffffe`. Sanitization in `db/column_family.cc` lines 416-425: sentinel is converted to `kAdjustedTtl` (30 days) for block-based tables, 0 otherwise.
- **Resolution**: Codex was right. Fixed in 07_fifo_compaction.md.
- **Risk level**: medium -- operators could misconfigure TTL behavior by assuming the default is 0.

## Debate: Universal periodic_compaction_seconds default
- **CC position**: Did not flag the documented default of "0 (disabled)."
- **Codex position**: For block-based universal compaction, the effective default is 30 days. Universal also folds ttl into periodic_compaction_seconds.
- **Code evidence**: `include/rocksdb/advanced_options.h` line 941: raw default is sentinel. `db/column_family.cc` lines 427-454: for universal + block-based, sanitized to 30 days (no filter requirement unlike leveled). Lines 456-465: ttl and periodic_compaction_seconds are folded with `min()`.
- **Resolution**: Codex was right. Fixed in 06_universal_compaction.md.
- **Risk level**: medium

## Debate: Dynamic level base shift threshold
- **CC position**: Did not flag the documented example "base shifts from L6 to L5 when the last level exceeds approximately 2.56 GB."
- **Codex position**: The shift happens when the last level exceeds `max_bytes_for_level_base` (256 MB), not `max_bytes_for_level_base * multiplier` (2.56 GB).
- **Code evidence**: `db/version_set.cc` `CalculateBaseBytes()`: starting from `first_non_empty_level`, the while loop `while (base_level_ > 1 && cur_level_size > base_bytes_max)` shifts the base level. For a DB with data only in L6, `cur_level_size` starts at L6_size. The shift from L6 to L5 happens when `L6_size > base_bytes_max` (256 MB).
- **Resolution**: Codex was right. The doc's 2.56 GB figure was wrong by a factor of 10. Fixed in 04_dynamic_levels.md.
- **Risk level**: medium -- incorrect threshold could mislead capacity planning.

## Debate: Unnecessary level draining
- **CC position**: Did not flag the doc's claim that levels below the base have targets of uint64_max "which means no compaction is triggered."
- **Codex position**: These levels DO get drained because `ComputeCompactionScore()` explicitly boosts scores for unnecessary levels.
- **Code evidence**: `db/version_set.cc` `ComputeCompactionScore()` lines 3935-3943: non-empty levels at or below `lowest_unnecessary_level_` get scores boosted to at least `10.0 * (1.001 + 0.001 * depth)`, well above the 1.0 threshold. The uint64_max target makes the normal score near-zero, but the boost overrides this.
- **Resolution**: Codex was right. The doc was misleading -- the targets are uint64_max but the boost ensures draining. Fixed in 04_dynamic_levels.md.
- **Risk level**: medium -- maintainers might think data gets "stuck" in unnecessary levels.

## Debate: L0 concurrency with intra-L0
- **CC position**: Did not flag the claim "only one L0 compaction runs at a time."
- **Codex position**: The restriction is more nuanced. `PickSizeBasedIntraL0Compaction()` can run while L0-to-base is active.
- **Code evidence**: `db/compaction/compaction_picker_level.cc` `PickFileToCompact()` lines 800-811: when `level0_compactions_in_progress()` is non-empty, falls back to `PickSizeBasedIntraL0Compaction()`. The fallback deliberately allows concurrent intra-L0 + L0-to-base.
- **Resolution**: Codex was right. Fixed in 01_overview.md, 02_leveled_compaction.md, and index.md.
- **Risk level**: low -- the distinction matters for understanding concurrent compaction behavior but is unlikely to cause bugs.
