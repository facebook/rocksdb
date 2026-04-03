# RocksDB Stress Testing Framework

## Overview

RocksDB's stress testing framework (`db_stress` + `db_crashtest.py`) validates correctness and stability under extreme concurrent load, randomized configurations, fault injection, and crash-recovery scenarios. Unlike deterministic unit tests, stress tests run continuously with randomized options to exercise combinations of features that are impractical to enumerate manually.

**Key source files:** `db_stress_tool/db_stress_test_base.{h,cc}`, `db_stress_tool/db_stress_driver.cc`, `db_stress_tool/db_stress_tool.cc`, `db_stress_tool/expected_state.{h,cc}`, `db_stress_tool/expected_value.{h,cc}`, `db_stress_tool/db_stress_shared_state.h`, `db_stress_tool/db_stress_gflags.cc`, `tools/db_crashtest.py`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Architecture and Execution Flow | [01_architecture.md](01_architecture.md) | Overall design, `db_stress` entry point, `db_crashtest.py` orchestration, and the end-to-end execution flow from parameter generation through verification. |
| 2. Test Modes | [02_test_modes.md](02_test_modes.md) | Four test modes: NonBatchedOps, BatchedOps, CfConsistency, and MultiOpsTxns -- their purpose, verification strategies, and configuration restrictions. |
| 3. Expected State Tracking | [03_expected_state.md](03_expected_state.md) | `ExpectedValue` bit encoding, `PendingExpectedValue` two-phase commit, `ExpectedStateManager` with file-backed and anonymous implementations, and verification logic. |
| 4. Thread Model and Synchronization | [04_thread_model.md](04_thread_model.md) | `SharedState` coordination, `ThreadState` per-thread state, key locks, thread lifecycle, and background threads for continuous verification. |
| 5. Operation Mix | [05_operation_mix.md](05_operation_mix.md) | Configurable operation percentages, probabilistic operations (`_one_in` flags), key generation, snapshot management, and the `OperateDb` main loop. |
| 6. Crash Test Orchestration | [06_crash_test.md](06_crash_test.md) | `db_crashtest.py` blackbox and whitebox modes, crash injection mechanisms, recovery verification, and kill point logic. |
| 7. Fault Injection | [07_fault_injection.md](07_fault_injection.md) | `FaultInjectionTestFS` error types, unsynced data loss simulation, error severity levels, and DB-open fault injection. |
| 8. Parameter Randomization | [08_parameter_randomization.md](08_parameter_randomization.md) | Parameter dicts, feature-specific param sets, `finalize_and_sanitize()` compatibility rules, and dynamic option changes via `SetOptions`. |
| 9. Extending the Framework | [09_extending.md](09_extending.md) | How to add new options, operations, and test modes to the stress test framework. |
| 10. Debugging Failures | [10_debugging.md](10_debugging.md) | Common failure patterns, reproduction strategies, sanitizer builds, and debugging techniques. |

## Key Characteristics

- **Randomized testing**: Each run uses randomly generated DB options, operation mixes, and key ranges to explore configuration space
- **Crash-recovery validation**: Periodically kills and restarts the DB, verifying all committed data survives
- **Expected state tracking**: Maintains an independent record of every key's value to detect data loss, corruption, or resurrection bugs
- **Two-phase commit for expected state**: Pending operations are tracked so crash mid-operation is handled correctly during verification
- **Multiple test modes**: NonBatchedOps (individual operations), BatchedOps (WriteBatch atomicity), CfConsistency (cross-CF atomicity), MultiOpsTxns (relational-style transactions)
- **Fault injection**: Injects I/O errors, metadata corruption, and unsynced data loss at configurable probabilities
- **Continuous verification**: Background thread periodically validates DB state while stress operations are running
- **Sanitizer integration**: Designed to run under ASAN, TSAN, UBSAN, and valgrind for memory and concurrency bug detection

## Key Invariants

- `max_key` and `seed` must remain fixed across crash-restart invocations for expected state verification to work
- Operation percentages (`readpercent + writepercent + delpercent + ...`) must sum to exactly 100
- Key locks must be held while both updating expected state and performing the DB operation; releasing between them allows races
- `PendingExpectedValue` requires explicit closure via `Commit()` or `Rollback()` before destruction
- After crash recovery, only synced data is guaranteed to survive; unsynced data may be lost
