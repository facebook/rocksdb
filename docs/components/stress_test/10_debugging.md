# Debugging Failures

**Files:** `db_stress_tool/db_stress_test_base.{h,cc}`, `db_stress_tool/no_batched_ops_stress.cc`, `tools/db_crashtest.py`

## Common Failure Patterns

### Data Loss

**Symptom**: Verification reports a key that should exist but returns NotFound.

```
Verification failed for key 12345 in cf 0:
  Expected: value_base=678901
  Actual: NotFound
```

**Possible causes**:
- WAL recovery bug: Write was committed but not recovered after crash
- Compaction bug: Key was incorrectly dropped during compaction despite being live
- Expected state desynchronization: Expected state was incorrectly updated

**Investigation**: Check whether the write was persisted to WAL before the crash. Examine the expected state file to verify correctness. Add logging to compaction input/output to track key lifecycle.

### Data Corruption

**Symptom**: Verification reports a key with a different value than expected.

```
Verification failed for key 67890 in cf 1:
  Expected: value_base=111213
  Actual: value_base=141516
```

**Possible causes**:
- Checksum mismatch undetected: Data corruption in SST or WAL that bypassed checksums
- Merge operator bug: Incorrect merge result
- Race condition: Two threads wrote to the same key without proper locking

**Investigation**: Enable `--paranoid_file_checks=1` for exhaustive checksum verification. Check thread sanitizer output for data races. Add logging to merge operators.

### Assertion Failure

**Symptom**: Process aborts with an assertion failure and stack trace.

**Possible causes**:
- Internal invariant violation (e.g., level number out of range)
- Incompatible option combination that bypassed sanitization
- Concurrency bug (race condition in multi-threaded code)

**Investigation**: Reproduce with the same seed and options. Run under a debugger. Check if the failure is specific to a configuration combination.

### Deadlock / Hang

**Symptom**: No progress for extended periods; all threads blocked.

**Possible causes**:
- Lock ordering violation (Thread A holds lock1, waits for lock2; Thread B holds lock2, waits for lock1)
- Condition variable bug (thread waits but no thread signals)
- Leaked lock (acquired but never released)

**Investigation**: Attach a debugger and inspect all thread stacks (`thread apply all bt`). In blackbox mode, the script kills the process after `--interval` seconds. In whitebox mode, a 15-minute timeout applies.

## Reproduction

### Using Seeds

Both the Python and C++ layers print their seeds at the start of each run. To reproduce a failure:

```bash
# Reproduce with the same C++ seed
./db_stress --seed=1234567890 --max_key=100000 [other flags from failure]

# Reproduce with the same Python seed
python3 tools/db_crashtest.py blackbox \
    --initial_random_seed_override=SEED1 \
    --per_iteration_random_seed_override=SEED2
```

The C++ seed (`--seed`) controls key selection and operation ordering per thread. The Python seed controls parameter randomization. Both are needed for exact reproduction.

### Thread-Level Reproduction

Each thread's PRNG is seeded with `1000 + tid + FLAGS_seed`, so the operation sequence for a specific thread is deterministic given the seed. To isolate a thread-specific issue, run with `--threads=1` and `--seed=1000+TID+ORIGINAL_SEED`.

## Sanitizer Builds

| Build | Command | Detects |
|-------|---------|---------|
| ASAN | `COMPILE_WITH_ASAN=1 make db_stress` | Buffer overflows, use-after-free, memory leaks |
| TSAN | `COMPILE_WITH_TSAN=1 make db_stress` | Data races, lock order violations |
| UBSAN | `COMPILE_WITH_UBSAN=1 make db_stress` | Undefined behavior (signed overflow, null dereference) |
| Valgrind | `valgrind --leak-check=full ./db_stress --ops_per_thread=1000` | Memory errors (slower than ASAN) |

Note: ASAN and TSAN cannot be used simultaneously. Run them as separate test configurations.

## Verbose Logging

```bash
# Enable verbose output
./db_stress --verbose=1 --ops_per_thread=1000 2>&1 | tee stress.log
```

## Fault Injection Diagnostics

When a crash test failure may be related to fault injection, check the fault injection log:

- Location: `$TEST_TMPDIR/fault_injection_<pid>_<timestamp>.log`
- The crash callback automatically prints recent injected errors to stderr on process crash (SIGABRT, SIGSEGV)
- The `print_and_cleanup_fault_injection_log()` function in `db_crashtest.py` prints the log after each iteration

To distinguish fault-injection-caused failures from genuine bugs:
- Check if `FaultInjectionTestFS::IsInjectedError()` returns true for the failed operation
- Run without fault injection (`--read_fault_one_in=0 --write_fault_one_in=0 --sync_fault_injection=0`) to confirm the bug exists independently

## Bisecting to Find Culprit Commits

When a stress test failure is a regression:

Step 1: Identify a known-good commit and the current bad commit. Step 2: Use binary search to narrow down the range. Step 3: At each step, build `db_stress` and run the crash test with the same configuration. Step 4: Mark commits as good or bad based on whether the crash test passes.

Note: Use the same `--seed` and option set across all bisection runs to ensure the same code paths are exercised.

## Continuous Integration

RocksDB CI runs stress tests on every pull request. The CI configurations test multiple build modes (debug, ASAN, TSAN, UBSAN) with various test types (simple, cf_consistency, txn, etc.). When a CI stress test fails:

1. Download the test output and identify the seed, options, and failure message
2. Reproduce locally with the same configuration
3. If the failure is intermittent, increase `--duration` or run multiple iterations
4. Check if the failure correlates with recent code changes
