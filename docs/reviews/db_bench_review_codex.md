# Review: db_bench — Codex

## Summary
Overall quality rating: needs work.

The chapter layout is sensible, the index length is in range, and some of the core reporting details do match the code, especially the `micros/op` vs `ops/sec` distinction and the existence of `PerfContext` / `IOStatsContext` / `Statistics` integration. The problem is that several of the behavior-heavy sections were not verified against the current implementation. The biggest mismatches are in the main write path, mixed-workload concurrency, iterator/read control flow, transaction and time-series behavior, and option defaults that changed recently.

The docs also miss several stateful behaviors that matter when people actually use `db_bench`: warmups and repeats do not recreate the DB between runs, RNG seeds evolve across runs, some benchmarks silently reuse or mutate global state, and multi-DB / multi-CF interactions are more constrained than the docs imply. A maintainer trying to debug or extend `db_bench` would still have to rediscover too much from source.

## Correctness Issues

### [WRONG] `DoWrite()` is described as direct `Put()`-based when it is batch-based
- **File:** `docs/components/db_bench/02_write_benchmarks.md`, `DoWrite Flow`
- **Claim:** "If `--batch_size == 1`: call `DB::Put()` directly" and "User-defined timestamps are NOT embedded in keys. They are passed separately via timestamp-aware API overloads."
- **Reality:** In the normal RocksDB path, `DoWrite()` always builds a `WriteBatch` and commits it with `DB::Write()`, even when `entries_per_batch_ == 1`. When user timestamps are enabled, it mutates the batch with `WriteBatch::UpdateTimestamps()`. Timestamp-aware `Put()` overloads are used in other paths such as `BGWriter()`, not in the main `DoWrite()` path.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::DoWrite`
- **Fix:** Rewrite the flow as batch-oriented, mention the stacked BlobDB exception separately, and explain that timestamps are injected into the batch in this code path.

### [WRONG] Key-format documentation gives the wrong prefix derivation
- **File:** `docs/components/db_bench/02_write_benchmarks.md`, `Key Generation`
- **Claim:** "If `--keys_per_prefix > 0`: key format is `[prefix][key_number + padding]` where prefix is derived from `key_number / keys_per_prefix`"
- **Reality:** `GenerateKeyFromInt()` computes `num_prefix = num_keys / keys_per_prefix_` and `prefix = v % num_prefix`. The prefix rotates across the key space; it is not assigned by contiguous groups of `keys_per_prefix`. The key body is also binary bytes plus `'0'` padding, not a decimal string format.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::GenerateKeyFromInt`
- **Fix:** Describe the real prefix math and clarify that keys are byte-oriented, not decimal-text keys.

### [MISLEADING] `overwrite` is not guaranteed to hit preexisting keys
- **File:** `docs/components/db_bench/02_write_benchmarks.md`, `Write Benchmark Types`
- **Claim:** "`overwrite` | RANDOM | no | Random overwrites on existing DB. Same as `fillrandom` but reuses existing DB"
- **Reality:** `overwrite` just maps to `WriteRandom` without `fresh_db=true`. If it is the first benchmark and `--use_existing_db=0` (the default), the `Benchmark` constructor already destroyed and recreated the DB, so the benchmark can run against an empty DB rather than preloaded keys.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::Benchmark`, `Benchmark::Run`
- **Fix:** Say it reuses the current DB state and that users need a prior load phase or `--use_existing_db=1` to guarantee actual overwrites.

### [WRONG] Mixed-workload chapter overstates the background-thread behavior
- **File:** `docs/components/db_bench/04_mixed_benchmarks.md`, `Concurrent Read-Write Benchmarks`
- **Claim:** "These benchmarks launch N benchmark threads plus one background writer or merger thread. The extra thread has its `Stats::exclude_from_merge_` set..." and "The background thread runs until `--duration` expires or `--num` operations complete, whichever comes first."
- **Reality:** Only the `BGWriter()`-based families call `SetExcludeFromMerge()`. `readwhilescanning` uses `BGScan()`, which does not exclude its stats, so scanner ops are merged into the final report. `BGWriter()` also does not use its own `Duration`; it runs until the reader threads finish, unless `--finish_after_writes` keeps it alive until `writes_` completes.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::ReadWhileWriting`, `Benchmark::ReadWhileMerging`, `Benchmark::ReadWhileScanning`, `Benchmark::BGWriter`, `Benchmark::BGScan`
- **Fix:** Split the behavior by benchmark family. Document that `readwhilescanning` currently merges scanner stats, and explain the `--finish_after_writes` exception.

### [WRONG] `readseq` and `seekrandom` control-flow descriptions do not match the code
- **File:** `docs/components/db_bench/03_read_benchmarks.md`, `ReadSequential Flow`, `SeekRandom Flow`
- **Claim:** "`ReadSequential` ... If iterator becomes invalid, `SeekToFirst()` to restart" and "`Seek()` (or `SeekForPrev()` if `--reverse_iterator`) ... Call `FinishedOps()` with `1 + seek_nexts` as the op count"
- **Reality:** `ReadSequential()` stops when the iterator becomes invalid; it does not wrap back to the beginning. `SeekRandom()` always calls `Iterator::Seek()`, even in reverse mode, then walks with `Prev()` if requested. It records one finished op per seek, not `1 + seek_nexts`.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::ReadSequential`, `Benchmark::SeekRandom`
- **Fix:** Update both sections to reflect the actual iterator control flow and op accounting.

### [WRONG] `multiscan` is described with the wrong scan-size knob
- **File:** `docs/components/db_bench/03_read_benchmarks.md`, `Read Benchmark Types`
- **Claim:** "`multiscan` | Batched scans | Performs `--multiscan_size` sequential scans of `--batch_size` keys each..."
- **Reality:** `MultiScan()` uses `scan_size = FLAGS_seek_nexts ? FLAGS_seek_nexts : 50`. `batch_size` is not the per-scan key count in this benchmark.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::MultiScan`
- **Fix:** Replace `batch_size` with `seek_nexts` / default `50`, and document the actual role of `multiscan_size` and `multiscan_stride`.

### [WRONG] `updaterandom` is not a counter benchmark
- **File:** `docs/components/db_bench/04_mixed_benchmarks.md`, `Read-Write Mix Benchmarks`
- **Claim:** "`updaterandom` | Read-modify-write | Reads existing value, increments counter in first 8 bytes, writes back"
- **Reality:** `UpdateRandom()` reads the current value only to count hits and possible rate-limiter bytes, then writes a fresh random value from `RandomGenerator`. There is no parsing or increment of an 8-byte counter.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::UpdateRandom`
- **Fix:** Describe it as read-then-overwrite with a newly generated value, not a numeric counter update.

### [WRONG] `randomwithverify` does not maintain a truth DB
- **File:** `docs/components/db_bench/04_mixed_benchmarks.md`, `Read-Write Mix Benchmarks`
- **Claim:** "`randomwithverify` ... Maintains truth DB for verification"
- **Reality:** It stays inside the benchmark DB. `PutMany()` writes three suffixed keys, `GetMany()` reads them in one snapshot and checks they match, and `DeleteMany()` removes the three keys. The separate `--truth_db` database is only used by the standalone `verify` benchmark.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::RandomWithVerify`, `Benchmark::GetMany`, `Benchmark::PutMany`, `Benchmark::DeleteMany`, `Benchmark::VerifyDBFromDB`
- **Fix:** Replace the truth-DB description with the actual three-key consistency pattern and point readers to `verify` for `--truth_db`.

### [WRONG] `flush` does not call `FlushWAL(true)`
- **File:** `docs/components/db_bench/04_mixed_benchmarks.md`, `Compaction Benchmarks`
- **Claim:** "`flush` | Calls `FlushWAL(true)` followed by `Flush()`"
- **Reality:** `Benchmark::Flush()` only calls `DB::Flush()` with `FlushOptions{wait=true}` on the default CF or CF vector. It never calls `FlushWAL()`.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::Flush`
- **Fix:** Document it as a memtable flush benchmark only.

### [WRONG] Transaction docs overstate requirements and document an unused flag
- **File:** `docs/components/db_bench/04_mixed_benchmarks.md`, `Transaction Benchmark`
- **Claim:** "`randomtransaction` requires `--optimistic_transaction_db` or `--transaction_db`" and "Optional `--transaction_sleep` adds delay between read and write phases"
- **Reality:** `randomtransaction` still runs without either transaction-DB flag; it falls back to `RandomTransactionInserter::DBInsert()`. `RandomTransactionVerify()` is then a no-op. `FLAGS_transaction_sleep` is defined but not passed into `RandomTransactionInserter` anywhere in `db_bench`.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::RandomTransaction`, `Benchmark::RandomTransactionVerify`; `test_util/transaction_test_util.h` + `RandomTransactionInserter`
- **Fix:** Explain the transactional versus non-transactional modes, and remove or clearly flag `transaction_sleep` as unused by `db_bench`.

### [WRONG] Time-series reads are not "latest timestamp" lookups
- **File:** `docs/components/db_bench/04_mixed_benchmarks.md`, `TimeSeries Benchmark`
- **Claim:** "Readers query random IDs with latest timestamps"
- **Reality:** readers build a key prefix for the ID, zero the trailing timestamp bytes, `Seek()` to the first version, and iterate forward through all versions for that ID. In `expire_style=delete`, some reader threads become deletion threads. The writer thread is excluded from merged stats and emits its own `timeseries write` report.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::TimeSeries`, `Benchmark::TimeSeriesReadOrDelete`, `Benchmark::TimeSeriesWrite`
- **Fix:** Describe the prefix-scan behavior, the delete-mode thread split, and the separate writer-side report.

### [WRONG] MixGraph key/value semantics are described too simply
- **File:** `docs/components/db_bench/09_mixgraph.md`, `Key Distribution`, `Value Size Distribution`
- **Claim:** "When prefix distribution parameters are set ..., a `GenerateTwoTermExpKeys` object models key access..." and "Values are clamped to `[10, --mix_max_value_size]`"
- **Reality:** prefix modeling only takes effect if `keyrange_dist_*` are set and `key_dist_a` plus `key_dist_b` are both non-zero; otherwise `use_random_modeling` wins and the prefix model is ignored. Value sizes are not truly clamped: values above `mix_max_value_size` are reduced with `% value_max`, which can produce sub-10-byte results.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::MixGraph`, `GenerateTwoTermExpKeys::DistGetKeyID`
- **Fix:** Document the precedence between `key_dist_*` and `keyrange_dist_*`, and describe the real max-size handling instead of calling it a clamp.

### [WRONG] Multi-DB selection is random, not round-robin by thread ID
- **File:** `docs/components/db_bench/10_extending.md`, `Multi-DB Mode`
- **Claim:** "Benchmarks select a DB per-thread via `SelectDB()` (round-robin by thread ID)"
- **Reality:** `SelectDBWithCfh(ThreadState*)` calls `thread->rand.Next()` and uses that to choose a DB. Many benchmarks therefore pick DBs randomly per operation or per batch, not round-robin by thread ID. Sequential write has its own special per-DB progression logic in `DoWrite()`.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::SelectDBWithCfh`, `Benchmark::DoWrite`
- **Fix:** Replace the round-robin description with the actual random-per-call policy and mention the sequential-write exception.

### [WRONG] CSV reporting does not emit QPS
- **File:** `docs/components/db_bench/06_metrics_and_reporting.md`, `CSV Reporting`; `docs/components/db_bench/01_architecture.md`, `Key Classes`
- **Claim:** "`ReporterAgent` ... writes periodic throughput samples to CSV" and the CSV schema is `secs_elapsed,interval_qps`
- **Reality:** `ReporterAgent::SleepAndReport()` writes the delta in completed operations since the previous sample. It does not divide by elapsed seconds, so the second column is "ops per report interval," not QPS.
- **Source:** `tools/db_bench_tool.cc` + `ReporterAgent::SleepAndReport`
- **Fix:** Rename the field in the docs or explicitly explain that it is interval ops, not normalized QPS.

### [MISLEADING] Time-based interval reporting and CI-only mode are stronger in the docs than in the code
- **File:** `docs/components/db_bench/06_metrics_and_reporting.md`, `Time-Based Intervals`, `Confidence Interval Only Mode`
- **Claim:** "`--stats_interval_seconds` overrides operation-based intervals" and "When `--confidence_interval_only=true`, each intermediate report shows only the CI bounds"
- **Reality:** when `stats_interval_seconds > 0`, `db_bench_tool()` forces `FLAGS_stats_interval = 1000`, and `Stats::FinishedOps()` only checks the wall-clock timer at those operation boundaries. `RunBenchmark()` still prints the normal per-run benchmark line, and `CombinedStats::ReportFinal()` still prints the final AVG/MEDIAN summary even when `confidence_interval_only` is set.
- **Source:** `tools/db_bench_tool.cc` + `db_bench_tool()`, `Stats::FinishedOps`, `Benchmark::Run`
- **Fix:** Document the 1000-op polling granularity and clarify that `confidence_interval_only` only changes the intermediate aggregate lines.

### [MISLEADING] Interval DB-stats section conflates single-CF and multi-CF output
- **File:** `docs/components/db_bench/07_perf_integration.md`, `Interval Stats with DB Properties`
- **Claim:** "thread 0 prints additional DB properties: per-CF stats via `rocksdb.cfstats`, running compaction count, running flush count, optional per-level table properties"
- **Reality:** the code has two different branches. In multi-CF mode it prints `rocksdb.cfstats` per created CF and optional table properties. In single-CF mode it prints `rocksdb.stats`, `rocksdb.num-running-compactions`, `rocksdb.num-running-flushes`, and optional table properties.
- **Source:** `tools/db_bench_tool.cc` + `Stats::FinishedOps`
- **Fix:** Split the description by single-CF versus multi-CF behavior.

### [STALE] Several option defaults and accepted values are outdated
- **File:** `docs/components/db_bench/05_configuration_flags.md`
- **Claim:** "`--format_version` | 6", "`--duration` ... Only applies to random-mode benchmarks", and "`--compression_manager` | `none` | Compression manager type (`none` for built-in, `mixed` for round-robin)"
- **Reality:** `BlockBasedTableOptions::format_version` now defaults to 7. `Duration` is used by many non-write benchmarks, including `readrandomwriterandom`, `readrandommergerandom`, `multiscan`, `mixgraph`, `randomtransaction`, and `timeseries`. `compression_manager` accepts `none`, `mixed`, `costpredictor`, and `autoskip`.
- **Source:** `tools/db_bench_tool.cc` + `DEFINE_int32(format_version, ...)`, `DEFINE_int32(duration, ...)`, `Benchmark::ReadRandomWriteRandom`, `Benchmark::ReadRandomMergeRandom`, `Benchmark::MultiScan`, `Benchmark::MixGraph`, `Benchmark::RandomTransaction`, `Benchmark::TimeSeries`, `Benchmark::InitializeOptionsFromFlags`; `include/rocksdb/table.h` + `BlockBasedTableOptions::format_version`
- **Fix:** Update the default/version table and expand the supported compression-manager list.

### [UNVERIFIABLE] Quantified performance claims are stated as facts without source data
- **File:** `docs/components/db_bench/04_mixed_benchmarks.md`, note under `mergerandom`
- **Claim:** "`mergerandom` with `--merge_operator=uint64add` achieves approximately 3x higher throughput than `updaterandom`..."
- **Reality:** there is no code path, test, or checked-in benchmark result in this component that substantiates the `3x` number. The implementation only shows that `mergerandom` uses `DB::Merge()` while `updaterandom` does a read followed by `Put()`.
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::MergeRandom`, `Benchmark::UpdateRandom`
- **Fix:** Replace the numeric claim with a qualitative explanation or cite a reproducible benchmark result.

## Completeness Gaps

### Missing benchmark coverage for existing code paths
- **Why it matters:** someone extending `db_bench` needs to know the full benchmark surface, not just the popular workloads.
- **Where to look:** `tools/db_bench_tool.cc` + benchmark help string in `DEFINE_string(benchmarks, ...)`, plus `Benchmark::Run`
- **Suggested scope:** add short coverage for at least `fillanddeleteuniquerandom`, `approximatesizerandom`, `readrandomoperands`, `getmergeoperands`, `randomreplacekeys`, `newiterator`, and `fillseekseq`

### Missing option coverage for recent 2024-2026 additions
- **Why it matters:** the flag chapter is supposed to be the quick reference, but it currently misses several knobs that changed how the tool is used.
- **Where to look:** `tools/db_bench_tool.cc` + flag definitions and `Benchmark::InitializeOptionsFromFlags`; recent history in `git log --since=2024-01-01 -- tools/db_bench_tool.cc`
- **Suggested scope:** expand the flag chapter to cover at least `openandcompact_allow_resumption`, `openandcompact_test_cancel_on_odd`, `openandcompact_cancel_after_millseconds`, `use_trie_index`, `index_block_search_type=interpolation_search|auto_search`, `prepopulate_block_cache=2`, `open_files_async`, `skip_stats_update_on_db_open`, `memtable_batch_lookup_optimization`, `separate_key_value_in_data_block`, and `verify_manifest_content_on_close`

### Missing cross-component caveats for multi-CF, multi-DB, and options-file mode
- **Why it matters:** this is where `db_bench` departs most from "plain RocksDB calls" and where configuration mistakes are easiest to make.
- **Where to look:** `tools/db_bench_tool.cc` + `Benchmark::InitializeOptionsGeneral`, `Benchmark::OpenDb`, `Benchmark::MultiReadRandom`, `Benchmark::SelectDBWithCfh`
- **Suggested scope:** add focused subsections explaining options-file post-processing, multi-DB selection policy, and the fact that `multireadrandom` operates on the default CF rather than routing per-key to hot CFs

### Missing test-alignment context
- **Why it matters:** readers should know which documented behaviors are backed by dedicated tests and which are effectively "read the source" territory.
- **Where to look:** `tools/db_bench_tool_test.cc`, `test_util/transaction_test_util.cc`, `utilities/transactions/transaction_test.cc`, `utilities/transactions/optimistic_transaction_test.cc`
- **Suggested scope:** a brief note in `10_extending.md` or the index summarizing that db_bench-specific unit coverage is mostly options-file loading, while transaction semantics are exercised through shared transaction utilities and tests

### Missing `OpenAndCompact()` cancel/resume behavior
- **Why it matters:** this is one of the newest and most specialized `db_bench` flows, and the docs currently only describe the happy path.
- **Where to look:** `tools/db_bench_tool.cc` + `Benchmark::OpenAndCompact`
- **Suggested scope:** expand the existing section with cancel-on-odd-run behavior, resumption mode, output-directory cleanup rules, and the use of `secondary_path`

## Depth Issues

### Execution-flow chapter skips the `InitializeOptionsGeneral()` layer
- **Current:** `01_architecture.md` says `Open()` calls either `InitializeOptionsFromFile()` or `InitializeOptionsFromFlags()`
- **Missing:** the second pass that still mutates runtime state after that choice, including `create_if_missing`, `statistics`, shared cache wiring, bloom filter fallback, row cache creation, env priority changes, file checksum generator, `use_existing_keys`, and rate limiter setup
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::Open`, `Benchmark::InitializeOptionsGeneral`

### Metrics chapter does not warn that some benchmarks report a synthetic single operation
- **Current:** `06_metrics_and_reporting.md` presents the standard report format as if `done_` always reflects meaningful benchmark operations
- **Missing:** `Stats::Report()` forces `done_ = 1` when a benchmark never calls `FinishedOps()`. That affects benchmarks like `compact`, `flush`, `backup`, `restore`, and `openandcompact`, where `micros/op` and `ops/sec` are "per benchmark invocation" rather than per internal DB operation
- **Source:** `tools/db_bench_tool.cc` + `Stats::Report`, `Benchmark::Compact`, `Benchmark::Flush`, `Benchmark::Backup`, `Benchmark::Restore`, `Benchmark::OpenAndCompact`

### Methodology chapter omits DB-state reuse across warmups and repeats
- **Current:** `08_methodology.md` recommends `[X5]` and `[W1-X5]` without discussing state carry-over
- **Missing:** `fresh_db` destruction/reopen happens once before the warmups and repeats for that benchmark name. Repeated write benchmarks do not get a clean DB between iterations, and repeated runs also use new per-thread RNG seeds
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::Run`, `Benchmark::RunBenchmark`, `ThreadState`

### Histogram section misses the read-versus-write rate-limiter difference
- **Current:** `06_metrics_and_reporting.md` says latency "includes inter-operation overhead (key generation, rate limiting waits, etc.)"
- **Missing:** write benchmarks explicitly call `ResetLastOpTime()` after rate-limiter sleeps in `DoWrite()`, so write histograms hide that wait time while read histograms generally include it
- **Source:** `tools/db_bench_tool.cc` + `Benchmark::DoWrite`, `Stats::ResetLastOpTime`, `Stats::FinishedOps`

## Structure and Style Violations

### Inline code formatting is used throughout despite the style rule
- **File:** all files in `docs/components/db_bench/`
- **Details:** the docs rely heavily on inline backticks for benchmark names, flags, and identifiers. The review prompt explicitly calls out "NO inline code quotes."

### The index's "Key Invariants" section uses non-invariants
- **File:** `docs/components/db_bench/index.md`
- **Details:** items like "Always use release builds," "Cache must be warm for read benchmarks," and "The `--num` flag defines the key space" are usage guidance, not correctness invariants whose violation would corrupt data or crash the tool.

## Undocumented Complexity

### Warmups and repeats reuse DB state and advance RNG seeds
- **What it is:** a benchmark with `fresh_db=true` recreates the DB once before the warmup/repeat loop, not before each iteration. `total_thread_count_` also keeps increasing across `RunBenchmark()` calls, so thread RNG seeds change between runs even with a fixed `--seed`
- **Why it matters:** repeat statistics for write benchmarks are not sampling identical starting conditions, and even read benchmarks are not replaying identical random sequences
- **Key source:** `tools/db_bench_tool.cc` + `Benchmark::Run`, `Benchmark::RunBenchmark`, `ThreadState`
- **Suggested placement:** `06_metrics_and_reporting.md` and `08_methodology.md`

### `use_existing_keys` changes both key generation and `FLAGS_num`
- **What it is:** after opening the DB, `InitializeOptionsGeneral()` loads every existing key into memory and sets `FLAGS_num = keys_.size()`. `GenerateKeyFromInt()` then bypasses its formatting logic and returns those stored keys directly
- **Why it matters:** this changes key-space semantics, startup cost, memory usage, and the behavior of benchmarks that expect synthetic keys
- **Key source:** `tools/db_bench_tool.cc` + `Benchmark::InitializeOptionsGeneral`, `Benchmark::GenerateKeyFromInt`
- **Suggested placement:** `05_configuration_flags.md` and `03_read_benchmarks.md`

### `readrandomfast` intentionally issues misses when `--num` is not a power of two
- **What it is:** `ReadRandomFast()` rounds up to the next power of two, masks random values with `pot - 1`, and counts keys `>= FLAGS_num` as non-existent
- **Why it matters:** the benchmark reduces overhead by avoiding modulo, but it does not measure the same workload as `readrandom` unless `--num` is a power of two
- **Key source:** `tools/db_bench_tool.cc` + `Benchmark::ReadRandomFast`
- **Suggested placement:** `03_read_benchmarks.md`

### Time-series reporting is split between a standalone writer report and merged reader/deleter stats
- **What it is:** the writer thread is excluded from merged stats and emits `timeseries write` directly from inside `TimeSeries()`, while the normal benchmark report represents the non-writer threads
- **Why it matters:** users can misread the output as one unified metric unless the docs explain the dual-report structure
- **Key source:** `tools/db_bench_tool.cc` + `Benchmark::TimeSeries`, `Benchmark::TimeSeriesWrite`
- **Suggested placement:** `04_mixed_benchmarks.md`

### `OpenAndCompact()` has test-only cancel/resume behavior and cleanup rules
- **What it is:** odd/even runs can deliberately cancel compaction, optionally allow resumption, and conditionally clean or reuse the output directory under `secondary_path`
- **Why it matters:** this is important if someone is using `db_bench` to reproduce or debug remote compaction service behavior rather than just timing the happy path
- **Key source:** `tools/db_bench_tool.cc` + `Benchmark::OpenAndCompact`
- **Suggested placement:** `10_extending.md`

### Hot-CF rotation depends on staged creation math, not a simple "rotate every N ops" rule
- **What it is:** `DoWrite()` computes `ops_per_stage` from `num_column_families / num_hot_column_families`, `Duration::GetStage()` drives `CreateNewCf()`, and `column_family_distribution` must sum to 100 with one entry per currently hot CF
- **Why it matters:** column-family rotation is one of the least obvious cross-component behaviors in the tool, and it directly affects workload shape
- **Key source:** `tools/db_bench_tool.cc` + `Benchmark::DoWrite`, `DBWithColumnFamilies::CreateNewCf`, `DBWithColumnFamilies::GetCfh`, `Benchmark::OpenDb`
- **Suggested placement:** `02_write_benchmarks.md` and `05_configuration_flags.md`

### `multireadrandom` is not symmetric with the hot-CF write path
- **What it is:** both MultiGet code paths operate on the default column family for the selected DB; they do not route reads through `GetCfh(key_rand)` the way `readrandom` and `DoWrite()` do
- **Why it matters:** developers benchmarking multi-CF workloads could assume batched reads are exercising the same CF-selection logic as point reads and writes when they are not
- **Key source:** `tools/db_bench_tool.cc` + `Benchmark::MultiReadRandom`
- **Suggested placement:** `03_read_benchmarks.md`

## Positive Notes

- The top-level organization is good: the index is compact, every chapter has a `Files:` line, and the chapter split is easier to navigate than a single monolithic component doc.
- The explanation of `micros/op` versus `ops/sec` in `index.md`, `01_architecture.md`, and `06_metrics_and_reporting.md` matches `Stats::Report()` and is one of the better-verified parts of the set.
- The separation between `PerfContext`, `IOStatsContext`, and DB-wide `Statistics` is directionally correct and gives readers the right mental model for thread-local versus DB-global metrics.
- The options-file discussion is pointed at a real, tested part of `db_bench`; `tools/db_bench_tool_test.cc` is narrowly focused, but it does back up that section of the docs.
