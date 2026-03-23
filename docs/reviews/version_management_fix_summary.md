# Fix Summary: version_management

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness (wrong facts) | 7 |
| Completeness (missing info) | 12 |
| Misleading (imprecise descriptions) | 11 |
| Structure/style | 2 |
| **Total** | **32** |

## Disagreements Found

0 -- Both reviewers agreed on all factual claims. Where they overlapped (kSVObsolete value, kSVInUse reentrant behavior), they reached the same conclusion. The reviews were largely complementary: CC focused more on individual field-level errors while Codex focused more on control flow and recovery semantics.

## Changes Made

### 10_super_version.md
- Fixed kSVObsolete from (void*)1 to nullptr [CC + Codex]
- Removed Step 4 (reentrant kSVInUse fallback) and added INVARIANT about assertion failure [CC + Codex]
- Clarified ReturnThreadLocalSuperVersion() returns false on CAS fail; caller unrefs [CC]
- Added GetReferencedSuperVersion() extra-ref handoff description [Codex]
- Clarified Installation Step 4: Cleanup() only called if Unref() returns true [CC]
- Added cfd->UnrefAndTryDelete() to Cleanup description [CC]
- Added ResetThreadLocalSuperVersions() ordering INVARIANT [Codex]
- Added RecalculateWriteStallConditions() to installation flow [CC undocumented complexity]

### 05_version_builder.md
- Rewrote Save Points section: save point is pre-edit snapshot, not "known-good because all files present" [Codex]
- Added ValidVersionAvailable() determines usability separately [Codex]
- Expanded Incomplete Valid Versions with atomic group, non-L0, blob-linkage constraints [CC + Codex]
- Clarified GetAndClearIntermediateFiles() returns mutable ref, does not clear [CC]
- Split Consistency Validation into Apply() vs SaveTo() responsibilities [Codex]
- Noted force_consistency_checks default is true, debug builds always check [Codex]

### 06_manifest.md
- Fixed SetCurrentFile path from db/filename.cc to file/filename.cc [CC]
- Added MANIFEST rolling condition (3) triggers next write, not current [CC depth]
- Added max_manifest_space_amp_pct=0 edge case documentation [Codex]
- Added Close-Time MANIFEST Validation section for verify_manifest_content_on_close [CC + Codex]
- Added WAL deletion watermark to snapshot description [Codex]
- Added snapshot record ordering (DB ID, WAL, per-CF state) [Codex]
- Added PurgeObsoleteFiles() reference for obsolete_manifests_ [CC]

### 07_log_and_apply.md
- Fixed pre_cb timing: after leader selection, before ProcessManifestWrites [CC]
- Replaced generic skip-manifest-write description with actual SetOptions() use case [Codex]
- Added PrepareAppend() mutex held in skip-manifest-write path [Codex]
- Added batching loop detail [CC]
- Added ReactiveVersionSet note [CC]

### 08_recovery.md
- Split Step 4 into handler finalization (Step 4) and post-replay epoch recovery (Step 5) [Codex]
- Rewrote best-effort recovery flow: snapshot-then-apply order [Codex]
- Clarified missing-file detection happens during Apply(), not before [Codex]
- Fixed ManifestTailer: catch-up mode ignores new CFs, does not discover them [Codex]
- Added paranoid_checks relaxation to normal recovery [Codex]
- Fixed UDT boundary handling: smallest gets min timestamp, largest gets max for range-tombstone sentinels [Codex]
- Added intermediate file cleanup detail for ManifestTailer [Codex]

### 04_version.md
- Added AppendVersion() unrefs the previous current Version [CC]
- Qualified PrepareAppend() mutex statement with skip-manifest-write exception [Codex]

### 03_version_storage_info.md
- Fixed option name: level0_file_num_compaction_trigger (not level0_compaction_trigger) [Codex]
- Expanded L0 scoring: max of file-count and size pressure, universal sorted-run behavior [CC + Codex]
- Added NewestFirstByEpochNumber tiebreaker description [CC]
- Fixed consistency checks: ordering/overlap validation happens in SaveTo(), not Apply() [Codex]
- Added force_consistency_checks default is true [Codex]

### index.md
- Replaced checksum mechanism with TLS reference invariant in Key Invariants [Codex]
