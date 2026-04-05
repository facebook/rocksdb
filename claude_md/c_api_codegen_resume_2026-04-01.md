# RocksDB C API Codegen Resume Note

This file captures the current state of the RocksDB C API codegen work so it
can be resumed on another host without reconstructing context from scratch.

## Goal

Long-term goal:

- make C API code generation part of the build
- keep generated artifacts checked in
- auto-detect simple public C++ APIs that can be converted mechanically
- generate missing C bindings automatically when they follow current C API
  conventions
- if a managed simple public C++ API has no generated binding and no manual C
  binding, fail the build
- avoid a manual per-function allowlist as the final design

Important intended policy:

- existing hand-written C bindings remain valid and should be preserved
- complex/manual-only families are allowed to stay hand-written
- strict auto-detection should only apply to the families the generator
  actually manages

## Current committed state

Current commit:

- `f72649bfac2d89d97989d94d2ce5fa49a522e7cd`
- commit message: `Add auto-generated RocksDB C API bindings`

## What has been completed

### 1. Build-time regeneration is wired in

Relevant files:

- `Makefile`
- `tools/c_api_gen/regen_all.py`

What changed:

- `make` regenerates checked-in C API fragments through a stamp target
- the stamp depends on:
  - `tools/c_api_gen/generate_c_api.py`
  - `tools/c_api_gen/auto_simple_bindings.py`
  - `tools/c_api_gen/regen_all.py`
  - `tools/c_api_gen/spec.json`
  - `include/rocksdb/c.h`
  - `db/c.cc`
  - `include/rocksdb/options.h`
  - `include/rocksdb/listener.h`

### 2. Checked-in generated artifacts were moved into dedicated folders

Generated headers now live in:

- `include/rocksdb/c_api_gen/`

Generated sources now live in:

- `db/c_api_gen/`

Integration points:

- `include/rocksdb/c.h`
- `db/c.cc`

Old top-level generated `.inc` files were removed.

### 3. Existing spec-driven generator remains in place

Relevant files:

- `tools/c_api_gen/generate_c_api.py`
- `tools/c_api_gen/spec.json`

This still generates the earlier migrated families, such as:

- DB simple operations
- WriteBatch
- Transaction / TransactionDB simple
- selected options families
- selected metadata families

### 4. New auto-discovery generator was added

Relevant file:

- `tools/c_api_gen/auto_simple_bindings.py`

This generator:

- parses public C++ structs using `clang++ -Xclang -ast-dump=json`
- discovers simple direct fields
- checks whether matching C bindings already exist in:
  - `include/rocksdb/c.h`
  - generated header includes referenced by `include/rocksdb/c.h`
  - `db/c.cc`
  - generated source includes referenced by `db/c.cc`
- preserves existing manual bindings
- generates only the missing simple bindings

### 5. Simple `ReadOptions` coverage was expanded

Managed by auto-discovery, with generated artifacts:

- `include/rocksdb/c_api_gen/c_generated_readoptions_auto.h.inc`
- `db/c_api_gen/c_generated_readoptions_auto.cc.inc`

Newly covered simple direct fields include:

- `rate_limiter_priority`
- `value_size_soft_limit`
- `optimize_multiget_for_io`
- `auto_prefix_mode`
- `adaptive_readahead`
- `auto_readahead_size` getter backfill
- `allow_unprepared_value`
- `auto_refresh_iterator_with_snapshot`
- `io_activity`

Still intentionally not auto-covered there:

- `snapshot`
- `timestamp`
- `iter_start_ts`
- `iterate_lower_bound`
- `iterate_upper_bound`
- `merge_operand_count_threshold`
- `table_filter`
- `table_index_factory`
- `request_id`

Those are non-simple/manual shapes.

### 6. Simple listener/job-info metadata coverage was expanded

Managed by auto-discovery, with generated artifacts:

- `include/rocksdb/c_api_gen/c_generated_jobinfo_auto.h.inc`
- `db/c_api_gen/c_generated_jobinfo_auto.cc.inc`

This adds simple direct metadata accessors for:

- `FlushJobInfo`
- `CompactionJobInfo`
- `SubcompactionJobInfo`
- `ExternalFileIngestionInfo`
- `MemTableInfo`

Examples newly covered:

- `rocksdb_flushjobinfo_cf_id`
- `rocksdb_flushjobinfo_file_number`
- `rocksdb_flushjobinfo_oldest_blob_file_number`
- `rocksdb_flushjobinfo_thread_id`
- `rocksdb_flushjobinfo_job_id`
- `rocksdb_flushjobinfo_blob_compression_type`
- `rocksdb_compactionjobinfo_cf_id`
- `rocksdb_compactionjobinfo_thread_id`
- `rocksdb_compactionjobinfo_job_id`
- `rocksdb_compactionjobinfo_num_l0_files`
- `rocksdb_compactionjobinfo_compression`
- `rocksdb_compactionjobinfo_blob_compression_type`
- `rocksdb_compactionjobinfo_aborted`
- `rocksdb_subcompactionjobinfo_cf_id`
- `rocksdb_subcompactionjobinfo_job_id`
- `rocksdb_subcompactionjobinfo_subcompaction_job_id`
- `rocksdb_subcompactionjobinfo_compression`
- `rocksdb_subcompactionjobinfo_blob_compression_type`
- `rocksdb_externalfileingestioninfo_external_file_path`
- `rocksdb_externalfileingestioninfo_global_seqno`
- `rocksdb_memtableinfo_newest_udt`

Still intentionally not auto-covered there:

- vectors
- table properties
- callback/function objects
- custom struct/vector marshalling
- status-rich or shape-divergent APIs that need hand design

### 7. Validation helper for generated-vs-handwritten equivalence exists

Relevant files:

- `tools/c_api_gen/validate_generated_equivalence.py`
- `tools/c_api_gen/equivalence_allowlist.json`

Known historical inconsistency already documented:

- `rocksdb_transactiondb_options_set_use_per_key_point_lock_mgr`
- old `db/c.cc` had it while old `include/rocksdb/c.h` did not declare it

### 8. Tests were updated and pass locally

Relevant file:

- `db/c_test.c`

Additional `ReadOptions` round-trip assertions were added for the new simple
bindings.

## What is still incomplete

### 1. The strict failure policy is not global yet

Current strictness is only practical for the simple families the new
auto-generator manages.

It does **not** yet enforce:

- every public C++ API across all public headers
- all option structs
- all listener metadata structs
- all method-based public APIs

### 2. More families still need to be brought under auto-discovery

High-value next targets:

1. remaining simple direct `Options` / `DBOptions` / `ColumnFamilyOptions`
   fields where C API naming can remain consistent
2. `WriteOptions`-adjacent families beyond what is already in the spec
3. additional listener metadata structs with only scalar/string/status fields
4. potentially `FlushWALOptions` and similar small option structs if exposed in
   public C API

### 3. Complex families remain manual

These are still outside the strict auto-generated path:

- pointer-owning factory setters
- callback-backed types
- vectors/maps requiring C marshalling
- `std::optional` / `std::function` / custom object pointer fields
- APIs intentionally shaped differently in C than in C++

The design goal is still:

- auto-detect missing simple bindings
- skip when a manual binding already exists
- fail only for missing bindings in the managed simple families

### 4. `make check` was not fully completed in this environment

Reason:

- this host blocks a `find ... -exec` step used by the test harness

Observed result:

- compile/link progressed through the large build successfully
- `c_test` was built and passed
- `make check` later failed in `gen_parallel_tests` due to environment command
  restrictions, not due to a C API compile failure

On another host without that restriction, `make check` should be rerun.

## Known implementation details

### Auto-discovery matching policy

`tools/c_api_gen/auto_simple_bindings.py` currently works like this:

1. discover fields from public C++ headers using clang AST
2. for each supported simple field shape:
   - if matching C declaration and definition already exist, do nothing
   - if missing, generate them
   - if partially present, fail regeneration
3. unsupported field shapes are currently ignored for strict failure unless the
   family is explicitly modeled there

This means the system is already strict for "supported simple direct fields",
but not yet for every public field in every public type.

### Directory layout

Checked-in generated files:

- headers: `include/rocksdb/c_api_gen/*.h.inc`
- sources: `db/c_api_gen/*.cc.inc`

Preview artifacts:

- `tools/c_api_gen/generated/c_preview.h.inc`
- `tools/c_api_gen/generated/c_preview.cc.inc`

### Important generated files currently included

Headers:

- `include/rocksdb/c_api_gen/c_generated_backup_options_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_db_simple_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_writebatch_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_block_based_options_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_jobinfo_metadata_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_jobinfo_auto.h.inc`
- `include/rocksdb/c_api_gen/c_generated_cuckoo_options_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_readoptions_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_readoptions_auto.h.inc`
- `include/rocksdb/c_api_gen/c_generated_writeoptions_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_flushoptions_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_transaction_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_transactiondb_subset.h.inc`
- `include/rocksdb/c_api_gen/c_generated_transaction_options_subset.h.inc`

Sources:

- matching `.cc.inc` files under `db/c_api_gen/`

## Recommended next steps

### Immediate next step

Run on a less restricted host:

```bash
python3 tools/c_api_gen/regen_all.py
make -j"$(sysctl -n hw.ncpu)" c_test
TEST_TMPDIR=$(mktemp -d /tmp/rocksdb-c-api-gen-test.XXXXXX) timeout 60 ./c_test
make check
```

### Next implementation step

Extend auto-discovery to the next simple public families, likely in this order:

1. `Options` simple direct fields not yet covered
2. additional small option structs
3. more listener metadata structs

### Next policy step

Once one additional family is fully modeled:

1. make that family fail regeneration when a newly added simple direct public
   field has no matching manual or generated C binding
2. repeat family by family

That is the practical path to the original goal without breaking the tree on
existing complex/manual APIs.

## Useful commands

Regenerate all checked-in generated artifacts:

```bash
python3 tools/c_api_gen/regen_all.py
```

Check auto-generated simple bindings are up to date:

```bash
python3 tools/c_api_gen/auto_simple_bindings.py --check
```

Regenerate preview artifacts:

```bash
python3 tools/c_api_gen/generate_c_api.py \
  --spec tools/c_api_gen/spec.json \
  --header-out tools/c_api_gen/generated/c_preview.h.inc \
  --source-out tools/c_api_gen/generated/c_preview.cc.inc
```

Validate generated wrappers against a handwritten reference revision:

```bash
python3 tools/c_api_gen/validate_generated_equivalence.py --ref HEAD
```

## Notes about this working tree

At the time this note was written, there were unrelated untracked files in the
worktree. They are not part of the committed C API codegen work.
