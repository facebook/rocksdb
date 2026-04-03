# Mixed and Specialized Benchmarks

**Files:** `tools/db_bench_tool.cc`

## Concurrent Read-Write Benchmarks

These benchmarks launch N benchmark threads plus one background writer, merger, or scanner thread.

| Benchmark | Pattern | Background Thread | Stats Excluded |
|-----------|---------|-------------------|----------------|
| `readwhilewriting` | N readers + 1 writer | Writer does random `Put()` at `--benchmark_write_rate_limit` rate | yes |
| `readwhilemerging` | N readers + 1 merger | Merger does random `Merge()` operations | yes |
| `readwhilescanning` | N readers + 1 scanner | Scanner does continuous full-table scans | no (scanner ops are merged into the final report) |
| `newiteratorwhilewriting` | N iterator creators + 1 writer | Measures `NewIterator()` cost under write load | yes |
| `seekrandomwhilewriting` | N seekers + 1 writer | Seek performance under write load | yes |
| `seekrandomwhilemerging` | N seekers + 1 merger | Seek performance under merge load | yes |
| `multireadwhilewriting` | N MultiGet readers + 1 writer | Batched read performance under write load | yes |

In all these benchmarks, `--threads` controls the number of reader threads. The writer/merger thread is added automatically (total threads = `--threads + 1`). The background writer/merger thread (via `BGWriter()`) runs until reader threads finish, unless `--finish_after_writes` keeps it alive until `writes_` completes. The background writer creates its own private `RateLimiter` from `--benchmark_write_rate_limit`, separate from the shared `write_rate_limiter` in `RunBenchmark`.

## Read-Write Mix Benchmarks

| Benchmark | Operation | Key Flags |
|-----------|-----------|-----------|
| `readrandomwriterandom` | Each thread does both reads and writes | `--readwritepercent` (default 90): percentage of operations that are reads. E.g., 90 means 9 reads per 1 write |
| `updaterandom` | Read-then-overwrite | Reads existing value via `DB::Get()`, then writes a fresh random value via `DB::Put()`. The read is used for hit counting and rate limiter sizing, not for deriving the new value |
| `xorupdaterandom` | Read-XOR-write | Reads value via `Get()`, XORs with new value using `BytesXOROperator`, writes via `Put()` (not `Merge()`) |
| `appendrandom` | Read-append-write | Reads existing value, appends new data, writes back. Values grow over time |
| `mergerandom` | Merge operations | Calls `DB::Merge()` with `--merge_operator`. Requires a merge operator to be configured |
| `readrandommergerandom` | Mixed Get/Merge | `--mergereadpercent` (default 70) controls merge-to-read ratio. 70 means 7 merges per 3 reads |
| `randomwithverify` | Read-write with verification | Uses `--numdistinct` keys, `--readwritepercent` for R/W ratio, `--deletepercent` for deletes. Writes three suffixed keys (K+"0", K+"1", K+"2") atomically via `WriteBatch`, and verifies all three are consistent under snapshot reads. The separate `--truth_db` is only used by the `verify` benchmark |

Note: `mergerandom` with `--merge_operator=uint64add` avoids the read-before-write round trip of `updaterandom`, so it is expected to achieve higher throughput.

## Compaction Benchmarks

| Benchmark | Operation |
|-----------|-----------|
| `compact` | Calls `CompactRange()` on a randomly selected DB (if multi-DB) for the default column family |
| `compactall` | Calls `CompactRange()` on all DB instances |
| `compact0` | Compacts from L0 to the next populated level. Adapts to dynamic level bytes mode |
| `compact1` | Compacts from the first populated level above L0 to the next level |
| `waitforcompaction` | Calls `WaitForCompact()` and blocks until background compactions finish |
| `flush` | Calls `DB::Flush()` to flush memtable to SST files |
| `openandcompact` | Opens a DB and compacts all files to the bottommost level, writing to a separate output directory. Designed for remote compaction service testing |

## Utility Benchmarks

| Benchmark | Purpose |
|-----------|---------|
| `stats` | Prints `rocksdb.stats` DB property |
| `resetstats` | Resets DB statistics counters |
| `levelstats` | Prints `rocksdb.levelstats` (per-level file counts and sizes) |
| `memstats` | Prints memtable statistics (active/immutable sizes, entry counts) |
| `sstables` | Prints `rocksdb.sstables` (SST file metadata) |
| `stats_history` | Prints statistics history |
| `verify` | Verifies DB contents against a truth DB at `--truth_db` |
| `verifychecksum` | Calls `DB::VerifyChecksum()` |
| `verifyfilechecksums` | Calls `DB::VerifyFileChecksums()` |
| `approximatememtablestats` | Tests `GetApproximateMemTableStats()` accuracy |
| `block_cache_entry_stats` | Prints block cache entry statistics |
| `cache_report_problems` | Reports block cache problems |
| `fillseekseq` | Sequential fill with interleaved seeks |
| `randomreplacekeys` | Replaces random keys in-place |

## Raw Performance Benchmarks

These benchmarks measure raw CPU performance without DB operations:

| Benchmark | Operation |
|-----------|-----------|
| `crc32c` | CRC32C of `--block_size` bytes, repeated until ~5GB processed |
| `xxhash` | XXH32 hash |
| `xxhash64` | XXH64 hash |
| `xxh3` | XXH3_64bits hash |
| `compress` | Compress `--block_size` bytes using `--compression_type`, repeated until ~1GB input processed |
| `uncompress` | Decompress pre-compressed `--block_size` block, repeated until ~5GB output produced |
| `acquireload` | Atomic acquire load microbenchmark (1000 loads per "operation") |

## Transaction Benchmark

`randomtransaction` executes random transactions and optionally verifies correctness:

- Supports three modes: `--optimistic_transaction_db` (OptimisticTransactionDB), `--transaction_db` (TransactionDB), or neither (falls back to plain `WriteBatch` operations via `DBInsert()`)
- Each transaction reads and modifies `--transaction_sets` keys (max 9999)
- Optionally calls `SetSnapshot()` per transaction (`--transaction_set_snapshot`)
- Post-benchmark verification (`RandomTransactionVerify`) checks that per-key counters sum correctly; this verification is skipped when neither transaction flag is set

Note: `--transaction_sleep` is defined but not actually used by `db_bench`.

## TimeSeries Benchmark

`timeseries` simulates a time-series workload with one writer and multiple reader/deleter threads:

- Writer generates timestamped keys using `TimestampEmulator` and is excluded from merged stats (emits a separate `timeseries write` report)
- Readers seek to a random ID with zeroed timestamp bytes and iterate forward through all versions for that ID (not "latest timestamp" lookups)
- In `--expire_style=delete` mode, some reader threads become deletion threads that remove expired entries
- Optional TTL-based expiration via `CompactionFilter` (`--expire_style=compaction_filter`)
- Time range controlled by `--time_range`

## Backup and Restore

| Benchmark | Operation |
|-----------|-----------|
| `backup` | Creates a backup via `BackupEngine` to `--backup_dir`. Rate limited by `--backup_rate_limit` |
| `restore` | Restores from `--backup_dir` to `--restore_dir`. Rate limited by `--restore_rate_limit` |
