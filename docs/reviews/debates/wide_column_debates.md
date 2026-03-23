# Reviewer Disagreements: wide_column

## Debate: Coalescing Iterator Precedence Rule

- **CC position**: Did not flag this as an issue. The existing doc claimed "lowest CF order wins" (first occurrence kept).
- **Codex position**: The precedence rule is backwards. Later column families in the input order win, not earlier ones.
- **Code evidence**: In `db/coalescing_iterator.cc`, `Coalesce()` pops from a min-heap ordered by (column_name, CF_order ascending). When duplicate column names appear, the current entry (lower order / earlier CF) is discarded without being pushed to `wide_columns_`, and replaced by `heap.top()` (higher order / later CF). Only the last entry for a given column name survives. The API comment in `include/rocksdb/db.h` at `NewCoalescingIterator` confirms this with an example: CF2's value "quux" shadows CF1's value "baz" for "col_2".
- **Resolution**: Codex was correct. The doc's claim was exactly backwards. Fixed to document that the later CF (higher index in input vector) takes precedence.
- **Risk level**: high -- this is a correctness issue that would cause users to make wrong assumptions about data precedence.

## Debate: Inline Code Quotes

- **CC position**: "No box-drawing characters, no line number references, no inline code quotes. The documentation follows the style guidelines well." (Positive note implying the docs are compliant.)
- **Codex position**: "Inline code quotes are used throughout the doc set" and flags this as a style violation.
- **Code evidence**: The docs use backticked identifiers like `WideColumn`, `PinnableWideColumns`, `kTypeWideColumnEntity` throughout all chapters. All other component docs in `docs/components/` use the same convention. The style rule "No inline code quotes" refers to not quoting actual lines of source code (e.g., `int x = 5;`), not to backticked identifiers.
- **Resolution**: CC was correct. Backticked identifiers are standard practice across all component docs and are not "inline code quotes" in the sense the style rule prohibits. The rule targets quoted source code lines, not identifier formatting.
- **Risk level**: low -- style convention interpretation, no technical impact.

## Debate: Blob Chapter "Not Wired" Characterization

- **CC position**: Did not flag the blob chapter's "not wired yet" summary as wrong, but noted the read path blob handling is more nuanced (deferred in point lookups, inline in iterators).
- **Codex position**: The summary collapses several materially different API behaviors. Plain-value APIs (`Get`/`MultiGet`) can succeed on V2 entities with blob refs by extracting only the default column, while entity APIs, iterators, merge, and compaction-filter paths reject them.
- **Code evidence**: `table/get_context.cc` `SaveValue()` uses `GetValueOfDefaultColumn()` for the `Get()` path (no `columns_` pointer), which only reads the default column and can succeed even when non-default columns are blob references. `Deserialize()` (used by `GetEntity`, `DBIter`, merge, and compaction filter) returns `Status::NotSupported` for blob-backed V2 entities.
- **Resolution**: Codex was correct that the single "not wired" statement was an oversimplification. Both reviewers identified related nuances. Fixed by replacing the summary paragraph with an API-by-API matrix showing which paths can succeed and which fail.
- **Risk level**: medium -- the oversimplification could mislead developers about which APIs work with V2 entities.
