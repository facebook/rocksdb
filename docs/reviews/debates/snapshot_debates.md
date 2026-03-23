# Snapshot Documentation Review Debates

## Debate: Inline code quotes as style violation

- **CC position**: No inline code quotes found -- PASS. Backtick-formatted identifiers (e.g., `GetSnapshot()`, `SnapshotList`) are standard markdown and acceptable.
- **Codex position**: "Inline code quoting is pervasive across the whole doc set" -- the review brief explicitly says "NO inline code quotes." All backtick formatting violates this rule.
- **Code evidence**: The review checklist says "No inline code quotes." Looking at the docs, they use backticks for formatting identifiers like function names, class names, field names, and option names. They do NOT quote actual source code lines inline. The distinction is between quoting code snippets (e.g., `if (x > 0) { return true; }`) vs. formatting identifiers with backticks for readability.
- **Resolution**: CC is correct. Backtick-formatting of identifiers is standard markdown practice for technical documentation and is not the same as "inline code quotes" (which means quoting actual lines of source code). The docs do not contain inline code quotes in the intended sense. No changes made.
- **Risk level**: low -- this is a style interpretation difference, not a factual error.

## Debate: index.md line count

- **CC position**: 40 lines -- at the lower boundary of the 40-80 range. Acceptable.
- **Codex position**: 39 lines -- below the required size floor of 40.
- **Code evidence**: Counted with Read tool: the file has 40 lines (lines 1-40). After edits for other fixes, the file now has 42 lines.
- **Resolution**: CC is correct. The original file had 40 lines, which is within the 40-80 range. After fixes, it has 42 lines.
- **Risk level**: low -- one-line counting difference.

## Debate: Compaction retention rule: seq < earliest_snapshot_ as standalone drop rule

- **CC position**: Did not flag this as incorrect. The retention rules table includes "Sequence number < earliest_snapshot_ | DROP" as a valid row.
- **Codex position**: This rule is invented. Older versions are kept when they are the first version visible in a snapshot stripe, when SingleDelete handling requires them, when transaction conflict checking needs them, or when UDT/full-history rules prevent GC. The same section's worked example keeps foo@90 even though 90 < 100.
- **Code evidence**: In db/compaction/compaction_iterator.cc, there is no standalone `seq < earliest_snapshot_` comparison that drops keys. The actual logic uses snapshot stripes (current_user_key_snapshot_ == last_snapshot for dropping shadowed versions) and DefinitelyInSnapshot() for deletion marker handling. The example in the doc correctly keeps foo@90 because it's the first version visible to snapshot 100, which contradicts the "seq < earliest_snapshot_ -> DROP" rule in the same table.
- **Resolution**: Codex is correct. The "Sequence number < earliest_snapshot_ | DROP" row in the retention table is misleading and contradicted by the doc's own example. Removed this row from the table and added clarifying text about DefinitelyInSnapshot() vs raw comparison.
- **Risk level**: medium -- this could mislead developers modifying compaction logic into thinking there's a simple cutoff rule.

## Debate: Deletion marker handling restricted to non-bottommost levels

- **CC position**: Did not flag the "non-bottommost level" qualifier as incorrect.
- **Codex position**: The obsolete-delete rule is not limited to non-bottommost compactions. The code applies it whenever conditions are met (real compaction, allow_ingest_behind false, DefinitelyInSnapshot, KeyNotExistsBeyondOutputLevel).
- **Code evidence**: In db/compaction/compaction_iterator.cc lines 898-933, the deletion drop logic (Branch A) checks `compaction_ != nullptr && !compaction_->allow_ingest_behind() && DefinitelyInSnapshot() && KeyNotExistsBeyondOutputLevel()` without any `!bottommost_level_` guard. The `bottommost_level_` check only appears in Branch B (lines 934-979) which is a separate bottommost-specific path. Branch A applies at any level.
- **Resolution**: Codex is correct. The deletion marker drop logic applies at any compaction level, not just non-bottommost. Fixed the doc to describe the actual conditions.
- **Risk level**: medium -- could cause incorrect assumptions about deletion marker preservation in bottommost compactions.
