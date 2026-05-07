---
title: Interpolation search for SST index blocks
layout: post
author: joshkang97
category: blog
---

For workloads with uniformly distributed keys, RocksDB now supports **interpolation search** for SST index blocks as an alternative to the default binary search.

## The idea

Binary search always splits the remaining range in half:

```
mid = low + (high - low) / 2
```

That's Θ(log n) probes regardless of the data. Interpolation search instead estimates where the target should land based on its value relative to the current boundaries:

```
probe = low + (target - key[low]) * (high - low) / (key[high] - key[low])
```

On uniformly distributed keys, that's expected O(log log n) probes. The canonical example: for an index block with restart keys `0, 1, 2, ..., 1023` and a seek for `900`, binary search needs about 10 hops; interpolation search lands on it in 1.

The catch is that pure interpolation search degrades to O(n) on badly skewed data.

## Turning a key into a number

The interpolation formula needs numeric values, but index keys are variable-length byte slices. RocksDB extracts a `uint64_t` per key by reading the first 8 bytes after the common prefix shared by the block's boundary keys, in big-endian, and zero-pads to the right if the remaining bytes are too short.

```cpp
inline uint64_t ReadBe64FromKey(Slice s, bool is_user_key, size_t offset) {
  // ... strip internal seq/type bytes if needed ...
  if (s.size() - offset >= 8) {
    uint64_t val;
    memcpy(&val, s.data() + offset, sizeof(val));
    return port::kLittleEndian ? EndianSwapValue(val) : val;
  }
  // pad short tails with zeros on the right (preserves bytewise order)
}
```

Big-endian + zero-pad preserves bytewise ordering, so the linear interpolation formula stays consistent with the comparator. This is also why the feature requires `BytewiseComparator`.

Two distinct keys can still collapse to the same `uint64_t` once you go past the first 8 non-shared bytes. To avoid a divide-by-zero, we simply fall back to binary search in that case.

## How to enable it

To force interpolation search on every index block:

```cpp
rocksdb::BlockBasedTableOptions table_options;
table_options.index_block_search_type =
    rocksdb::BlockBasedTableOptions::kInterpolation;
```

## kAuto: per-block selection

`kAuto` is the recommended way to use the feature. It chooses the search algorithm for each index block automatically, based on a uniformity hint written into the block footer at SST construction time:

```cpp
table_options.index_block_search_type =
    rocksdb::BlockBasedTableOptions::kAuto;
table_options.uniform_cv_threshold = 0.2;
```

When `uniform_cv_threshold >= 0`, the SST writer scans each index block's restart keys and computes the **coefficient of variation (CV)** of the gaps between consecutive numeric key values:

```
gap[i] = key_value[i + 1] - key_value[i]
CV     = stddev(gap) / mean(gap)
```

Lower CV means the gaps are more uniform — and the more likely interpolation search will outperform binary search. The CV is computed incrementally with Welford's online algorithm, so the scan is one pass over the restart points.

If `CV < uniform_cv_threshold`, RocksDB sets an `is_uniform` bit in the block footer. At read time, `kAuto` resolves to `kInterpolation` only when that bit is set *and* the comparator is bytewise; otherwise it uses `kBinary`.

### Write overhead

Computing the `is_uniform` bit is a cheap operation as it is only computed for the index blocks in a SST file. CPU profiling of `db_bench -benchmarks=fillseq,compact -compression_type=none -disable_wal=1` attributes only ~0.08% of write-path CPU to `ScanForUniformity`.

After a few more releases, we plan to enable kAuto and uniform_cv_threshold by default.

## Benchmarks

Setup — populate a DB and force a single-level shape so all reads hit the same index structure, then measure point-read throughput:

```
# Build a release binary
make clean && DEBUG_LEVEL=0 make db_bench

# Load + compact, varying the index_shortening_mode
./db_bench -benchmarks=fillrandom,compact \
           -index_shortening_mode=1

# Then point-read against the populated DB
./db_bench -use_existing_db=true -benchmarks=readrandom \
           -index_block_search_type=binary_search   # or interpolation_search / auto_search
```

`index_shortening_mode=1` (`kShortenSeparators`) keeps the file's last index key intact, which preserves a roughly uniform numeric distribution for the benchmark.

Results, averaged over multiple runs:

| Mode | ops/s | vs `binary_search` |
|---|---|---|
| `binary_search` | 335,749 | baseline |
| `interpolation_search` | 366,598 | **+9.2%** |
| `auto_search` | 366,832 | **+9.2%** |

## Compatibility

The `is_uniform` bit reuses a previously-reserved bit in the data block footer. SSTs written by older RocksDB never set it and decode as `is_uniform = false`, so they read with binary search under `kAuto`. However, after the bit is set, if read by an older RocksDB version < 11.0.0, it will read it as a corruption error.

## Future work

Some future opportunities can involve extending interpolation search to data blocks, as well as supporting other comparators such as `ReverseBytewiseComparator`.
