# Extending the Framework

**Files:** `db_stress_tool/db_stress_gflags.cc`, `db_stress_tool/db_stress_test_base.{h,cc}`, `db_stress_tool/db_stress_tool.cc`, `tools/db_crashtest.py`

## Adding a New Option

To exercise a new RocksDB option in the stress test:

### Step 1: Add the Flag

Define the gflag in `db_stress_tool/db_stress_gflags.cc` using the `DEFINE_int32` macro (or the appropriate type variant). Follow the existing flag naming conventions in that file.

### Step 2: Wire the Flag to DB Options

In `StressTest::InitDb()` or `InitializeOptionsFromFlags()` (see `db_stress_tool/db_stress_test_base.cc`), map the flag to the corresponding `Options` field.

### Step 3: Add to db_crashtest.py

Add the parameter to the appropriate dict in `db_crashtest.py`. Use a lambda for random values, or a static value if the option must remain fixed across crash-restart invocations:

```python
default_params = {
    "my_new_option": lambda: random.choice([0, 1, 10, 100]),
}
```

### Step 4: Add Sanitization Rules

If the new option is incompatible with existing options, add rules to `finalize_and_sanitize()`:

```python
if dest_params.get("my_new_option") > 0:
    dest_params["incompatible_option"] = 0
```

### Step 5: Add to Dynamic Options (Optional)

If the option is dynamically changeable, add it to `BuildOptionsTable()` so `SetOptions()` can exercise it.

## Adding a New Operation

To test a new DB operation type:

### Step 1: Add Percentage or one_in Flag

For a percentage-based operation, add a new `DEFINE_int32` flag in `db_stress_tool/db_stress_gflags.cc` that participates in the 100% sum (following the pattern of `FLAGS_readpercent`, `FLAGS_writepercent`, etc.).

For a probabilistic operation, use the `_one_in` naming convention (following the pattern of `FLAGS_flush_one_in`, `FLAGS_compact_range_one_in`, etc.).

### Step 2: Implement the Operation

Add a virtual method to `StressTest` (see `db_stress_tool/db_stress_test_base.h`) and implement it in the appropriate subclass. The implementation must follow the two-phase expected state pattern:

Step 1: Acquire the key lock. Step 2: Create `PendingExpectedValue` via `PreparePut()` or `PrepareDelete()`. Step 3: Perform the DB operation. Step 4: Call `Commit()` on success or `Rollback()` on failure. Step 5: Release the key lock.

### Step 3: Add to OperateDb Selection

For percentage-based operations, add the new operation to the cumulative probability chain in `OperateDb()` (see `db_stress_tool/db_stress_test_base.cc`). Ensure the sum validation includes the new percentage.

For probabilistic operations, add a check in the probabilistic operations section of `OperateDb()`.

### Step 4: Update db_crashtest.py

Add the new flag to the appropriate parameter dicts and update `finalize_and_sanitize()` if there are compatibility constraints.

## Adding a New Test Mode

To create an entirely new stress test mode:

### Step 1: Create a Subclass

Create a new file (e.g., `db_stress_tool/my_new_stress.cc`) with a class inheriting from `StressTest`. Implement all pure virtual methods:

- `IsStateTracked()` -- Whether external expected state is maintained
- `VerifyDb()` -- Post-operation verification logic
- `ContinuouslyVerifyDb()` -- Background verification logic
- `TestGet()`, `TestMultiGet()`, `TestGetEntity()`, `TestMultiGetEntity()`, `TestPrefixScan()`, `TestPut()`, `TestDelete()`, `TestDeleteRange()`, `TestIngestExternalFile()` -- Operation implementations

### Step 2: Add Factory Function

Add a factory function (following the pattern of `CreateNonBatchedOpsStressTest()`, `CreateBatchedOpsStressTest()`, etc.) that returns a new instance of your subclass.

### Step 3: Add to Test Selection

In `db_stress_tool/db_stress_tool.cc`, add the new mode to the test selection logic (see the `if/else if` chain that selects between `FLAGS_test_cf_consistency`, `FLAGS_test_batches_snapshots`, `FLAGS_test_multi_ops_txns`, and the default).

### Step 4: Add Parameter Dict

Add a new parameter dict in `db_crashtest.py`:

```python
my_new_mode_params = {
    "test_my_new_mode": 1,
    # Disable incompatible features
    "reopen": 0,
    # Custom operation mix
    "customopspercent": 80,
    ...
}
```

### Step 5: Update Build System

Add the new `.cc` file to `Makefile`, `CMakeLists.txt`, and `src.mk`, then run `buckifier/buckify_rocksdb.py` to update the BUCK file.

## Adding a New Feature to Existing Modes

When adding a new RocksDB feature, ensure it gets stress test coverage:

1. **Option coverage**: Add the option flag and wire it to `db_crashtest.py` parameter randomization
2. **Operation coverage**: If the feature introduces new operations, add them to the operation mix
3. **Verification coverage**: Ensure `VerifyDb()` can detect bugs related to the new feature
4. **Fault injection coverage**: Verify the feature handles injected I/O errors correctly
5. **Sanitization rules**: Add any compatibility constraints to `finalize_and_sanitize()`
6. **Dynamic options**: If the option is dynamically changeable, add it to `BuildOptionsTable()`
