# Read Benchmarks

**Files:** `tools/db_bench_tool.cc`

## Read Benchmark Types

| Benchmark | Access Pattern | Description |
|-----------|---------------|-------------|
| `readrandom` | Random point lookups | Core read benchmark. Calls `DB::Get()` on random keys. Reports found/total ratio |
| `readseq` | Sequential forward scan | Iterator-based sequential scan via `Next()`. Tests prefetching and block cache |
| `readreverse` | Sequential reverse scan | Iterator-based reverse scan via `Prev()` |
| `readtocache` | Sequential scan | Single-threaded `readseq` used to warm the block cache before read benchmarks |
| `readtorowcache` | Sequential reads | Reads existing keys into row cache. Requires `--use_existing_keys` and `--row_cache_size` |
| `readrandomfast` | Optimized random reads | Lightweight random read loop with reduced overhead for microbenchmarking. Rounds `--num` up to the next power of two and uses bitmask instead of modulo; keys beyond `FLAGS_num` are counted as misses |
| `readmissing` | Random point lookups | Like `readrandom` but increments `key_size_` by 1, ensuring all lookups miss |
| `readrandomsmall` | Random point lookups | Like `readrandom` but divides `reads_` by 1000 |
| `multireadrandom` | Batched random reads | Calls `DB::MultiGet()` with `--batch_size` keys per call. Note: operates on the default column family only, not routing per-key via `GetCfh()` like point reads |
| `multiscan` | Batched scans | Performs `--multiscan_size` sequential scans of `--seek_nexts` keys each (default 50 if `--seek_nexts` is 0), separated by `--multiscan_stride` key gaps |
| `seekrandom` | Random seeks + Next | Random `Seek()` followed by `--seek_nexts` `Next()` calls. Only the seek itself is counted as one op |
| `deleteseq` | Sequential deletes | Sequential key deletion |
| `deleterandom` | Random deletes | Random key deletion |
| `getmergeoperands` | Merge operand reads | Tests `GetMergeOperands()` API for merge-heavy workloads |
| `newiterator` | Iterator creation | Measures `NewIterator()` creation cost without concurrent writes |
| `approximatesizerandom` | Size estimation | Tests `GetApproximateSizes()` accuracy |

## ReadRandom Flow

Step 1: Allocate key buffer and `PinnableSlice` for results.

Step 2: Main loop until `Duration::Done()`:
- Select DB and column family via `SelectDBWithCfh()`
- Generate random key via `GetRandomKey()` and `GenerateKeyFromInt()`
- Call `DB::Get()` (or `DB::GetMergeOperands()` if `--readrandomoperands` mode)
- If found: accumulate key + value bytes for throughput reporting
- Apply read rate limiting every 256 operations
- Call `FinishedOps()` with `kRead` operation type

Step 3: Report `(found of total found)` in the stats message.

### Key Distribution

Random key selection uses `GetRandomKey()` which supports two modes:

- **Uniform** (default): `thread->rand.Next() % FLAGS_num`
- **Exponential** (`--read_random_exp_range > 0`): `FLAGS_num * exp(-r)` where `r` is uniform in `[0, read_random_exp_range]`. Larger values produce more skewed (hot-key) distributions

### Strided Access

When `--multiread_stride > 0` in `readrandom`: instead of fully random keys, generates a batch of keys at stride intervals from a random starting point. This simulates spatial locality patterns.

## MultiReadRandom Flow

Step 1: Pre-allocate `entries_per_batch_` key buffers and `PinnableSlice` arrays.

Step 2: Main loop:
- Generate `entries_per_batch_` random keys (or strided keys if `--multiread_stride > 0`)
- Call either:
  - `DB::MultiGet(ReadOptions, keys, &values)` (vector interface, default)
  - `DB::MultiGet(ReadOptions, cfh, N, keys, pin_values, statuses)` (batched interface, when `--multiread_batched=true`)
- Count found keys and accumulate bytes
- Call `FinishedOps()` with `entries_per_batch_` as the op count

The batched interface (`--multiread_batched`) uses `PinnableSlice` outputs and is more efficient for large batch sizes as it avoids string copies.

## SeekRandom Flow

Step 1: Main loop:
- Generate random key
- Create iterator or reuse existing one (controlled by `--use_tailing_iterator`)
- Call `Iterator::Seek(key)` (always Seek, even in reverse mode)
- If `--reverse_iterator`: call `Prev()` for `--seek_nexts` iterations; otherwise call `Next()`
- Accumulate bytes from all visited key-value pairs
- Call `FinishedOps()` with `1` as the op count (only the seek itself is counted, not the subsequent Next/Prev calls)
- Apply read rate limiting every 256 operations

### Iterator Bounds

When `--max_scan_distance > 0`:
- Forward seeks set `iterate_upper_bound` to `key + max_scan_distance`
- Reverse seeks set `iterate_lower_bound` to `key - max_scan_distance`

This simulates bounded range scans and tests the efficiency of iterator bound checking.

## ReadSequential Flow

Iterates through the DB sequentially:

Step 1: Create iterator with `read_options_`.

Step 2: Loop starting from `SeekToFirst()`:
- Call `Next()` to advance
- If iterator becomes invalid, the loop terminates (does NOT wrap around)
- Accumulate key + value bytes

For the `readtocache` variant, `num_threads` is forced to 1 and `reads_` is set to `num_`, ensuring a single full scan to warm the cache.

## Cache Warming

Read benchmarks require a warm block cache for representative results. Two approaches:

1. **readtocache benchmark**: Run `--benchmarks=readtocache,readrandom` to warm cache with a sequential scan before the actual benchmark
2. **Repeat syntax**: Run `--benchmarks=readrandom[W1-X5]` where `W1` runs one warmup iteration (not reported), then 5 measured iterations

Important: Without cache warming, the first read benchmark run will show significantly higher latency (often 10x+) due to cold-cache I/O.
