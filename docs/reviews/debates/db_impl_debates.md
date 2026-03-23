# Debates: db_impl Documentation

## Debate: Inline code quotes

- **CC position**: "Clean style. No line number references, no box-drawing characters, no inline code quotes." — CC rated this as a positive, asserting the docs have no inline code quotes.
- **Codex position**: "Inline code quotes are pervasive across the whole component doc set" — Codex flagged backtick usage around file names, types, functions, and options as a style violation.
- **Code evidence**: The docs use markdown backticks (``) around identifiers like `mutex_`, `WriteImpl()`, `include/rocksdb/options.h` throughout all chapters. This is standard markdown formatting for technical identifiers.
- **Resolution**: CC is correct. The style rule "no inline code quotes" means no literal source code lines quoted inline (e.g., `if (x > 0) { return; }`), not that backtick formatting of identifiers is prohibited. Using backticks for function names, variable names, file paths, and option names is standard technical documentation practice and improves readability. No change needed.
- **Risk level**: low — this is a style interpretation difference, not a factual error.

## Debate: SuperVersion thread-local staleness detection mechanism

- **CC position**: CC did not flag the thread-local description as incorrect. The doc (which CC reviewed) states: "check if its `version_number` matches the current `super_version_number_`" as the staleness detection mechanism.
- **Codex position**: Codex correctly identified that "There is no `version_number` comparison on the read fast path; staleness is propagated by `ResetThreadLocalSuperVersions()` scraping thread-local entries to `kSVObsolete`."
- **Code evidence**: `ColumnFamilyData::GetThreadLocalSuperVersion()` in `db/column_family.cc` atomically swaps the thread-local pointer to `kSVInUse`. If the swapped value is a valid pointer (not `kSVObsolete`), it is used directly with no version number check. If `kSVObsolete`, the slow path acquires a fresh ref under mutex. `ResetThreadLocalSuperVersions()` scrapes all thread-local slots to `kSVObsolete` when a new SuperVersion is installed. The `version_number` field exists but is used only by external callers like `ForwardIterator`, not by the thread-local protocol.
- **Resolution**: Codex is correct. The doc's description of version_number comparison was factually wrong. Fixed to describe the actual sentinel-swap protocol.
- **Risk level**: high — this is a core concurrency mechanism and the wrong description could mislead developers reasoning about read path performance or correctness.

## Debate: Close/snapshot status handling

- **CC position**: CC did not flag the snapshot safety check or Aborted-to-Incomplete conversion.
- **Codex position**: Codex identified two issues: (1) the snapshot check is not wrapper-only behavior but happens in `DBImpl::Close()` and the destructor, and (2) the Aborted-to-Incomplete conversion in `CloseHelper()` is separate from snapshot-related Aborted status.
- **Code evidence**: `DBImpl::~DBImpl()` calls `MaybeReleaseTimestampedSnapshotsAndCheck()` before `CloseImpl()`, but its return status is discarded via `PermitUncheckedError()`. The Aborted-to-Incomplete conversion at the end of `CloseHelper()` applies to errors from the close sequence itself. There is no `DBImpl::Close()` override — the public `DB::Close()` returns `NotSupported()` by default, and the close logic runs through the destructor or `WaitForCompact(close_db=true)`.
- **Resolution**: Codex is correct that the description was inaccurate. Fixed to clarify that `MaybeReleaseTimestampedSnapshotsAndCheck()` is called from both `Close()` and the destructor, and that the Aborted-to-Incomplete conversion in `CloseHelper()` is separate.
- **Risk level**: medium — affects understanding of close error semantics.
