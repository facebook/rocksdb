# Crash Recovery Review Debates

## Debate: Recovery Atomicity
- **CC position**: Did not flag the claim "Recovery is atomic from the user's perspective... the database directory is left unchanged." Implicitly accepted the atomicity guarantee.
- **Codex position**: Flagged as [WRONG]. Open can mutate on-disk state before returning success. The directory is not guaranteed unchanged on failure.
- **Code evidence**: `DBImpl::Open()` in `db/db_impl/db_impl_open.cc` performs multiple disk mutations before the final success return: `CreateWAL()` creates a new `.log` file, the sentinel empty WriteBatch is fsynced to the new WAL, `GetLogSizeAndMaybeTruncate()` truncates the last WAL during `RecoverLogFiles()`, `WriteLevel0TableForRecovery()` creates SST files during recovery flushes, and `LogAndApplyForRecovery()` commits edits to the MANIFEST. Each of these can happen before a later step fails.
- **Resolution**: Codex is correct. Recovery is NOT side-effect free on disk. The claim has been rewritten to describe the actual behavior: Open either returns a usable DB handle or an error, but on-disk state may be modified even on failure. Orphan files are cleaned by `DeleteObsoleteFiles()` on the next successful open.
- **Risk level**: high -- the original claim could lead someone to assume the directory is safe to inspect or copy after a failed Open without considering intermediate state.

## Debate: Cross-CF Consistency Check Scope
- **CC position**: Did not flag the claim that the check runs "in kPointInTimeRecovery or kTolerateCorruptedTailRecords."
- **Codex position**: Flagged as [WRONG]. `stop_replay_for_corruption` is set to true only for kPointInTimeRecovery in `HandleNonOkStatusOrOldLogRecord()`. The check cannot fire for kTolerateCorruptedTailRecords in practice.
- **Code evidence**: `HandleNonOkStatusOrOldLogRecord()` (lines 1669-1710) has three branches: kSkipAnyCorruptedRecords returns OK immediately; kPointInTimeRecovery sets `*stop_replay_for_corruption = true`; kTolerateCorruptedTailRecords/kAbsoluteConsistency returns the raw status without touching the flag. The guard in `MaybeHandleStopReplayForCorruptionForInconsistency()` (lines 1762-1766) does check for both modes, but the flag is only set to true for kPointInTimeRecovery.
- **Resolution**: Codex is correct on the practical behavior. The doc now scopes the discussion primarily to kPointInTimeRecovery and explains the guard covers both modes but the flag is only set for PIT.
- **Risk level**: medium -- the original claim could mislead someone into thinking kTolerateCorruptedTailRecords has the same cross-CF safety net as kPointInTimeRecovery.

## Debate: Inline Code Quotes Style
- **CC position**: Reported "Clean -- no violations" for structure and style, did not flag backtick usage.
- **Codex position**: Flagged as a systemic style violation, stating "The prompt for this doc set explicitly forbids inline code quotes."
- **Code evidence**: N/A (style interpretation, not a code question). The existing docs across the crash_recovery component and all other component docs in `docs/components/` use backticks extensively for option names, function names, type names, and file paths. This is consistent across the entire documentation set.
- **Resolution**: No change applied. The backtick convention is consistent across the entire documentation set and aids readability. Removing backticks from all option/function/type references would reduce clarity without clear benefit.
- **Risk level**: low -- purely stylistic, no correctness impact.

## Debate: "Key Invariant" Label Usage
- **CC position**: Did not flag the use of "Key Invariant" labels.
- **Codex position**: Flagged as misuse. Claims like "recovery is atomic" and "CURRENT always points to a valid MANIFEST" are labeled as invariants but are either false or are ordinary behavior summaries, not enforced corruption-prevention invariants.
- **Code evidence**: "Recovery is atomic" is factually incorrect (see Debate 1). "CURRENT always points to a valid MANIFEST" is true during normal operation but CURRENT can be missing (BER handles this). These are not invariants in the strict sense (properties that are always maintained by the system).
- **Resolution**: Codex is partially correct. The false "recovery is atomic" invariant was removed. The CURRENT file invariant was downgraded to a behavioral description. Other "Key Invariant" labels (sequence numbers never decrease, MANIFEST before WAL) are genuine invariants and were retained.
- **Risk level**: medium -- labeling false claims as invariants could mislead developers into making incorrect assumptions about system guarantees.
