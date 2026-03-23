# Debates: user_defined_timestamp

## Debate: Timestamp-Sequence Ordering — Invariant vs Application Requirement

- **CC position**: The seq/ts ordering is a genuine correctness invariant. Lists it under "INVARIANT usage is appropriate" and does not flag it.
- **Codex position**: The docs present this as an engine-maintained invariant, but current write paths only enforce timestamp size and comparator compatibility, not per-key monotonic ordering. It should be rephrased as an application-level requirement.
- **Code evidence**: The write path in `db/db_impl/db_impl_write.cc` (`WriteImpl`) assigns sequence numbers sequentially and checks timestamps are set when needed (`TimestampsUpdateNeeded`), but does not compare timestamps between consecutive writes to verify monotonicity. `WriteBatch::Put` in `db/write_batch.cc` accepts timestamps directly without validating ordering relative to prior writes. Transaction tests explicitly note that the caller must keep commit timestamps ordered.
- **Resolution**: Codex is correct. The engine does not enforce the bidirectional ordering between timestamps and sequence numbers. It is an application-level requirement that is naturally satisfied when timestamps are monotonically increasing. The doc has been updated to say "application-level requirement" and "not enforced by the engine".
- **Risk level**: medium — presenting an unenforced assumption as an engine invariant could cause developers to rely on the engine catching ordering violations.

## Debate: IncreaseFullHistoryTsLow Error Behavior

- **CC position**: CC flagged the "try-again error" as wrong and said the implementation returns `Status::InvalidArgument` when the requested timestamp is lower than current.
- **Codex position**: Codex also said the "try-again behavior" does not match the code, and documented only `InvalidArgument`.
- **Code evidence**: `db/db_impl/db_impl_compaction_flush.cc`, `IncreaseFullHistoryTsLowImpl` has TWO separate checks: (1) Pre-LogAndApply check (line ~1023): returns `Status::InvalidArgument` when the requested `ts_low` is lower than the current value. (2) Post-LogAndApply check (line ~1039): returns `Status::TryAgain` when a concurrent operation raised the value beyond the request between the check and apply.
- **Resolution**: Both reviewers were partially right. The doc's "try-again error" is a real code path but occurs only in a narrow race condition, not in the simple "lower than current" case. The fix documents both error types clearly.
- **Risk level**: low — the race condition case is rare in practice, but documenting both paths prevents confusion.

## Debate: WriteBatch::Put(cf, key, value) Semantics

- **CC position**: CC flagged that both Put and Delete have both forms (with and without explicit timestamp), and the doc creates a false asymmetry by showing only one form for each.
- **Codex position**: Codex said the CF-aware overload WITHOUT an explicit timestamp does NOT require a caller-appended suffix. It appends a zero-filled placeholder timestamp internally and sets `needs_in_place_update_ts_`.
- **Code evidence**: `db/write_batch.cc`, `WriteBatch::Put(ColumnFamilyHandle*, const Slice& key, const Slice& value)` (line ~930): When `ts_sz != 0`, appends `std::string dummy_ts(ts_sz, '\0')` and sets `needs_in_place_update_ts_ = true`. The caller does NOT manually append the timestamp.
- **Resolution**: Codex's factual claim about the placeholder mechanism is correct and more precise than CC's broader statement. Both are correct that the doc was wrong — it claimed the caller must include the timestamp suffix, which is false. The fix shows both forms and documents the deferred timestamp behavior.
- **Risk level**: high — the original claim would cause developers to double-append timestamps, producing corrupted keys.
