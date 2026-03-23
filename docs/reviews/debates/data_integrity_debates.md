# Data Integrity Review Debates

## Debate: Inline Code Backtick Usage
- **CC position**: "No style violations. No box-drawing characters, no line number references, no inline code quotes. Clean markdown throughout."
- **Codex position**: "Inline code quoting is used throughout despite the local style rule. The local review prompt explicitly says 'NO inline code quotes', but inline backticks are used extensively for options, enums, functions, and file paths in every chapter."
- **Code evidence**: All other component docs in `docs/components/` use backticks for identifiers (option names, function names, class names, enum values). This is standard markdown practice for distinguishing code identifiers from prose.
- **Resolution**: CC is correct. The "no inline code quotes" rule refers to quoting literal lines of source code inline, not to using backticks around identifiers. Using backticks for option names like `paranoid_checks`, function names like `ComputeBuiltinChecksum()`, and type names like `ChecksumType` is standard documentation practice used consistently across all RocksDB component docs. No change needed.
- **Risk level**: low -- purely a style interpretation question

## Debate: paranoid_checks and MANIFEST Checksum Verification
- **CC position**: "When `paranoid_checks = true` (default), RocksDB verifies MANIFEST record checksums" (implies paranoid_checks controls this)
- **Codex position**: "MANIFEST replay uses `log::Reader(... checksum=true ...)` regardless of `paranoid_checks`." Also notes that the public option comment says with `paranoid_checks = false`, the DB can still open and healthy files remain accessible.
- **Code evidence**: In `db/version_set.cc`, both `VersionSet::Recover` (via `VersionEditHandler`) and `TryRecoverFromOneManifest` create `log::Reader` with `checksum=true` unconditionally. The `paranoid_checks` option (in `include/rocksdb/options.h` lines 606-628) controls whether SST file open failures are fatal, not MANIFEST CRC verification. The comment says: "When set to false, when there are files corrupted, the DB will still be opened, and the healthy ones could still be accessed."
- **Resolution**: Codex is correct. MANIFEST CRC verification is always on. `paranoid_checks` controls SST file open failure handling. Fixed the docs to separate these concerns.
- **Risk level**: medium -- a maintainer could incorrectly believe disabling `paranoid_checks` skips MANIFEST verification

## Debate: "RocksDB never silently discards data" Invariant
- **CC position**: Listed this under "Positive Notes" as an appropriate invariant. Did not flag it as incorrect.
- **Codex position**: "The chapter claims a guarantee that the recovery code does not make." Listed `kPointInTimeRecovery`, `kSkipAnyCorruptedRecords`, `best_efforts_recovery`, and `RepairDB()` as counterexamples.
- **Code evidence**: `WALRecoveryMode::kPointInTimeRecovery` stops replay before corruption (drops tail data). `kSkipAnyCorruptedRecords` skips corrupted records anywhere. `best_efforts_recovery` can recover to older state, losing recent data. `RepairDB()` salvages what it can. All are documented behaviors that intentionally trade data retention for availability.
- **Resolution**: Codex is correct. The blanket invariant is false for recovery paths. Fixed to narrow the claim to normal read/write operations and explicitly note the recovery modes that trade data for availability.
- **Risk level**: high -- a blanket "never discards data" claim could lead operators to trust recovery modes more than warranted
