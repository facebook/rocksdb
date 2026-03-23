# Tiered Storage Review Debates

## Debate: output_temperature_override availability in CompactRangeOptions

- **CC position**: "CompactRangeOptions also has output_temperature_override (with the same type and default). Both CompactFiles() and CompactRange() support temperature override." CC recommended changing the doc to say "Available through both CompactFiles() and CompactRange()."
- **Codex position**: Did not address this specific claim.
- **Code evidence**: `CompactRangeOptions` (in `include/rocksdb/options.h`, lines ~2526-2580) does NOT have an `output_temperature_override` field. Only `CompactionOptions` (used by `CompactFiles()`, line ~2481) has it. These are two separate structs: `CompactRangeOptions` is for `CompactRange()`, `CompactionOptions` is for `CompactFiles()`.
- **Resolution**: CC was wrong. The original documentation correctly states that `output_temperature_override` is available only through `CompactFiles()` (not `CompactRange()`). The doc was NOT changed to follow CC's incorrect suggestion.
- **Risk level**: high -- applying CC's suggestion would have introduced a factual error telling users they can use a field that does not exist on `CompactRangeOptions`.

## Debate: FIFO threshold misordering behavior

- **CC position**: Did not address this claim.
- **Codex position**: "RocksDB validates this configuration and returns Status::NotSupported during DB open or SetOptions(). This is a checked configuration error, not undefined behavior."
- **Code evidence**: `db/column_family.cc` lines 1609-1631 explicitly validates that `file_temperature_age_thresholds` elements are in ascending order by `age` and returns `Status::NotSupported("Option file_temperature_age_thresholds requires elements to be sorted in increasing order with respect to `age` field.")`. The test `db/db_options_test.cc` lines 1551-1593 confirms this validation fires during both DB open and `SetOptions()`.
- **Resolution**: Codex was right. The doc's claim of "undefined behavior" was incorrect -- this is a validated configuration error. Fixed to state that RocksDB returns `Status::NotSupported`.
- **Risk level**: medium -- users might avoid valid configurations out of fear of undefined behavior, when in reality they would get a clear error message.

## Debate: Seqno-to-time mapping storage format

- **CC position**: Did not address the encoding format.
- **Codex position**: "The encoding is [count][delta pair 1][delta pair 2]..., where every stored pair is delta-encoded relative to a running base that starts at zero. There is no separately encoded raw first pair."
- **Code evidence**: In `db/seqno_to_time_mapping.cc` `EncodeTo()`, `SeqnoTimePair base` starts at `{0, 0}` and each pair computes `val = cur.ComputeDelta(base)`. The first pair's delta from zero is numerically equal to its raw value, so the distinction is about the encoding algorithm (all-delta-from-running-base) vs. the doc's description (raw first pair + subsequent deltas). The Codex mention of a leading `[count]` field was not confirmed in `EncodeTo()` -- the code iterates pairs without writing a count prefix.
- **Resolution**: Codex was mostly right about the delta-from-zero encoding pattern, but incorrect about the count prefix. The doc was updated to describe the actual algorithm: all pairs are delta-encoded relative to a running base starting at zero, with the first pair effectively stored as its raw values since deltas from zero equal the values.
- **Risk level**: low -- the original description was functionally equivalent but algorithmically imprecise.
