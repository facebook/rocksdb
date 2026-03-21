# Filter (Bloom/Ribbon) Subsystem

Filters are probabilistic data structures that reduce unnecessary disk reads for non-existent keys. When querying for a key, the filter answers either "definitely not present" (100% accurate) or "maybe present" (with a configurable false positive rate). RocksDB uses filters extensively in both SST files and memtables.

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Filter Use Cases                         │
├─────────────────────────────────────────────────────────────┤
│  SST File Filters:        Memtable Filters:                 │
│  • Whole-key filtering    • Prefix-only filtering           │
│  • Prefix filtering        • DynamicBloom                   │
│  • Full or partitioned     • Concurrent updates             │
│  • Bloom or Ribbon         • Non-persistent                 │
└─────────────────────────────────────────────────────────────┘
```

Filters avoid reading data blocks from disk when the key is not present. For example, a 10 bits-per-key Bloom filter achieves ~1% false positive rate, meaning 99% of reads for non-existent keys skip disk I/O entirely.

**⚠️ INVARIANT:** Filters never produce false negatives—if a filter says "not present," the key is guaranteed absent. False positives are configurable via bits-per-key.

## Filter Policy API

The entry point for filter configuration is `FilterPolicy` ([include/rocksdb/filter_policy.h](../../include/rocksdb/filter_policy.h:89-134)):

```cpp
class FilterPolicy {
  // Returns filter name for compatibility checking
  virtual const char* CompatibilityName() const = 0;

  // Create a FilterBitsBuilder for constructing filters during SST creation
  virtual FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext&) const = 0;

  // Create a FilterBitsReader for querying filters during reads
  virtual FilterBitsReader* GetFilterBitsReader(
      const Slice& contents) const = 0;
};
```

Users configure filters via table options:

```cpp
BlockBasedTableOptions table_options;
// Bloom filter: ~1% FP rate
table_options.filter_policy.reset(NewBloomFilterPolicy(10.0));
// Ribbon filter: ~1% FP rate, 30% less space than Bloom
table_options.filter_policy.reset(NewRibbonFilterPolicy(10.0));
```

**⚠️ INVARIANT:** All built-in filter policies (Bloom, Ribbon) can read each other's filters via shared `CompatibilityName() == "rocksdb.BuiltinBloomFilter"`. This ensures forward compatibility.

## Bloom Filter Implementations

RocksDB provides two Bloom filter implementations optimized for different trade-offs.

### FastLocalBloom (Current Default)

FastLocalBloom ([util/bloom_impl.h](../../util/bloom_impl.h:94-345)) is a cache-local Bloom filter with SIMD-optimized queries:

```
Cache Line (512 bits = 64 bytes)
┌────────────────────────────────────────────┐
│  All probes for one key happen here        │
│  Probes: bit positions 0-511               │
│  AVX2: 8 probes simultaneously checked     │
└────────────────────────────────────────────┘
```

**Key characteristics:**
- **Cache-local:** All probes for a key land in one 64-byte cache line
- **SIMD-accelerated:** AVX2 checks 8 probes in parallel ([bloom_impl.h](../../util/bloom_impl.h:234-333))
- **FastRange32:** Uses fast integer multiplication instead of modulo ([bloom_impl.h](../../util/bloom_impl.h:200-203))
- **Accuracy:** ~0.957% FP rate at 10 bits/key (vs. 0.953% theoretical)

**Add operation:**
```cpp
// Select cache line: h1 chooses which 64-byte block
uint32_t bytes_to_cache_line = FastRange32(h1, len_bytes >> 6) << 6;
// Set num_probes bits within cache line using h2
uint32_t h = h2;
for (int i = 0; i < num_probes; ++i, h *= 0x9e3779b9) {
  int bitpos = h >> (32 - 9);  // Top 9 bits = 0-511
  data[bytes_to_cache_line + (bitpos >> 3)] |= (1 << (bitpos & 7));
}
```

**Query operation (AVX2 path):** Processes 8 probes per SIMD instruction, achieving ~0.9x latency vs. legacy Bloom with 1.1x FP rate tradeoff.

**⚠️ INVARIANT:** Filter size must be aligned to 64 bytes (cache line size). Hash functions use golden ratio `0x9e3779b9` for uniform distribution.

### LegacyBloom (Deprecated)

LegacyNoLocalityBloomImpl ([bloom_impl.h](../../util/bloom_impl.h:347-386)) scatters probes across the entire filter:

```
Filter (arbitrary size in bits)
┌─────────────────────────────────────────────────────────┐
│ Probe 1    Probe 3         Probe 2      Probe N        │
│   ↓          ↓               ↓            ↓             │
│   •          •               •            •             │
└─────────────────────────────────────────────────────────┘
   Cache misses on every probe → slow
```

**⚠️ DO NOT REUSE:** No cache locality, 10x+ slower than FastLocalBloom. Kept only for reading old SST files.

## Ribbon Filter (Space-Efficient Alternative)

Ribbon filters ([util/ribbon_config.h](../../util/ribbon_config.h:18-182), [util/ribbon_impl.h](../../util/ribbon_impl.h)) save ~30% space vs. Bloom at similar FP rates, but cost 3-4x CPU during construction.

```
Bloom:  10 bits/key → 1% FP rate
Ribbon:  7 bits/key → 1% FP rate (30% space savings)
```

**Trade-offs:**
- **Space:** 30% smaller than Bloom for same FP rate
- **Construction:** 3-4x slower, 3x temporary memory usage
- **Query:** Similar speed to Bloom
- **Use case:** Lower levels of LSM (larger, longer-lived files)

**Hybrid strategy via `NewRibbonFilterPolicy`:**
```cpp
// bloom_before_level=1: Bloom for L0, Ribbon for L1+
auto policy = NewRibbonFilterPolicy(10.0, /*bloom_before_level=*/1);
```

**⚠️ INVARIANT:** Ribbon uses 128-bit coefficient rows (`ribbon128`). Construction can fail probabilistically (~1 in 20 chance per seed), requiring retry with different hash seeds.

## Full Filter vs. Partitioned Filter

Filters can be structured as a single block or partitioned into smaller sub-filters.

### Full Filter (Default)

A single filter block covering all keys in the SST file ([table/block_based/full_filter_block.h](../../table/block_based/full_filter_block.h:29-89)):

```
SST File Layout
┌──────────────────┐
│  Data Blocks     │
├──────────────────┤
│  Meta Blocks     │
├──────────────────┤
│  Full Filter ◄───┼─── Single filter for entire SST
├──────────────────┤
│  Index Block     │
├──────────────────┤
│  Footer          │
└──────────────────┘
```

**Advantages:**
- Simple: one filter lookup per query
- Efficient: no index overhead
- Cache-friendly: single block cache entry

**Disadvantages:**
- Large memory footprint when pinned
- All-or-nothing caching

### Partitioned Filter

Filter split into multiple partitions with a top-level index ([table/block_based/partitioned_filter_block.h](../../table/block_based/partitioned_filter_block.h)):

```
SST File Layout
┌──────────────────┐
│  Data Blocks     │
├──────────────────┤
│ Filter Part 1    │ ◄─┐
│ Filter Part 2    │ ◄─┼── Multiple filter partitions
│ Filter Part 3    │ ◄─┤
├──────────────────┤   │
│ Filter Index     │ ──┘ Top-level index mapping keys → partitions
├──────────────────┤
│  Index Block     │
├──────────────────┤
│  Footer          │
└──────────────────┘
```

**Advantages:**
- Smaller cache entries: only load relevant partition
- Memory efficiency: partial pinning possible
- Granular caching: LRU on partitions

**Disadvantages:**
- Extra index lookup overhead
- More complex code path

**Configuration:**
```cpp
BlockBasedTableOptions table_options;
table_options.filter_policy.reset(NewBloomFilterPolicy(10.0));
table_options.partition_filters = true;
table_options.metadata_block_size = 4096;  // Partition size target
```

**⚠️ INVARIANT:** Partitioned filters must include the prefix of the first key in the next partition to support prefix `Seek` across partition boundaries ([partitioned_filter_block.cc](../../table/block_based/partitioned_filter_block.cc:103-125)).

## Filter Construction

During SST file creation, `FilterBitsBuilder` accumulates keys and generates filter bits.

### FilterBitsBuilder Interface

Defined in [table/block_based/filter_policy_internal.h](../../table/block_based/filter_policy_internal.h:22-106):

```cpp
class FilterBitsBuilder {
  // Add a key to the filter
  virtual void AddKey(const Slice& key) = 0;

  // Add key and its prefix (e.g., whole-key + prefix filtering)
  // Deduplicates: key vs. previous key, key vs. alt, alt vs. previous alt
  virtual void AddKeyAndAlt(const Slice& key, const Slice& alt) = 0;

  // Estimate unique keys added (for TableProperties::num_filter_entries)
  virtual size_t EstimateEntriesAdded() = 0;

  // Generate filter bits, transfer ownership to buf
  virtual Slice Finish(std::unique_ptr<const char[]>* buf) = 0;

  // Overload with optional status parameter for corruption detection
  // Has default implementation that delegates to single-parameter Finish
  virtual Slice Finish(std::unique_ptr<const char[]>* buf, Status* status) {
    return Finish(buf);
  }

  // Estimate # keys that fit in target byte size
  virtual size_t ApproximateNumEntries(size_t bytes) = 0;

  // Calculate bytes needed for num_entries
  virtual size_t CalculateSpace(size_t num_entries) = 0;
};
```

### Full Filter Construction

`FullFilterBlockBuilder` ([table/block_based/full_filter_block.cc](../../table/block_based/full_filter_block.cc:20-87)) wraps `FilterBitsBuilder`:

```cpp
void FullFilterBlockBuilder::Add(const Slice& key) {
  if (prefix_extractor_ && prefix_extractor_->InDomain(key)) {
    Slice prefix = prefix_extractor_->Transform(key);
    if (whole_key_filtering_) {
      // Add both whole key and prefix
      filter_bits_builder_->AddKeyAndAlt(key, prefix);
    } else {
      // Prefix-only filtering
      filter_bits_builder_->AddKey(prefix);
    }
  } else if (whole_key_filtering_) {
    filter_bits_builder_->AddKey(key);
  }
}
```

**⚠️ INVARIANT:** Keys are added in sorted order. `AddKeyAndAlt` deduplicates between successive calls, leveraging sorted input to avoid storing duplicate hashes.

### Partitioned Filter Construction

`PartitionedFilterBlockBuilder` ([table/block_based/partitioned_filter_block.cc](../../table/block_based/partitioned_filter_block.cc:24-346)) cuts filter partitions based on key count:

```cpp
bool PartitionedFilterBlockBuilder::DecideCutAFilterBlock() {
  size_t added = filter_bits_builder_->EstimateEntriesAdded();
  return added >= keys_per_partition_;
}

void PartitionedFilterBlockBuilder::CutAFilterBlock(...) {
  // Add next partition's prefix for cross-boundary Seek support
  if (next_prefix) {
    filter_bits_builder_->AddKeyAndAlt(*next_prefix, *next_prefix);
  }

  // Generate partition filter
  Slice filter = filter_bits_builder_->Finish(&filter_data, &status);

  // Store partition with its key range
  filters_.push_back({ikey, std::move(filter_data), filter});

  // Add previous partition's prefix for SeekForPrev support
  if (next_key && prefix_extractor_) {
    filter_bits_builder_->AddKey(prefix_extractor_->Transform(prev_key));
  }
}
```

**⚠️ INVARIANT:** Each partition must include prefixes from adjacent partitions to ensure Seek/SeekForPrev correctness across partition boundaries.

## Filter Querying

During reads, `FilterBitsReader` checks if keys may be present.

### FilterBitsReader Interface

Defined in [filter_policy_internal.h](../../table/block_based/filter_policy_internal.h:108-123):

```cpp
class FilterBitsReader {
  // Single-key query
  virtual bool MayMatch(const Slice& entry) = 0;

  // Batch query (optimized for multiget)
  virtual void MayMatch(int num_keys, Slice** keys, bool* may_match);
};
```

### Full Filter Reading

`FullFilterBlockReader` ([table/block_based/full_filter_block.cc](../../table/block_based/full_filter_block.cc:89-254)) retrieves filter from block cache:

```cpp
bool FullFilterBlockReader::MayMatch(const Slice& entry, ...) const {
  // Get or load filter block from cache
  CachableEntry<ParsedFullFilterBlock> filter_block;
  Status s = GetOrReadFilterBlock(..., &filter_block, read_options);

  FilterBitsReader* filter_bits_reader =
      filter_block.GetValue()->filter_bits_reader();

  if (filter_bits_reader->MayMatch(entry)) {
    PERF_COUNTER_ADD(bloom_sst_hit_count, 1);  // False positive or true match
    return true;
  } else {
    PERF_COUNTER_ADD(bloom_sst_miss_count, 1);  // Definite miss
    return false;
  }
}
```

**MultiGet optimization:** Batches filter queries to amortize cache lookup overhead ([full_filter_block.cc](../../table/block_based/full_filter_block.cc:186-244)):

```cpp
void FullFilterBlockReader::MayMatch(MultiGetRange* range, ...) {
  // Single cache lookup for entire batch
  CachableEntry<ParsedFullFilterBlock> filter_block;
  GetOrReadFilterBlock(..., &filter_block, read_options);

  // Extract keys to array for batch query
  std::array<Slice*, MAX_BATCH_SIZE> keys;
  std::array<bool, MAX_BATCH_SIZE> may_match;
  for (auto iter = range->begin(); iter != range->end(); ++iter) {
    keys[num_keys++] = &iter->ukey_without_ts;
  }

  // Batch query avoids repeated cache line fetches
  filter_bits_reader->MayMatch(num_keys, keys.data(), may_match.data());

  // Skip keys that definitely don't match
  for (auto iter = range->begin(); iter != range->end(); ++iter) {
    if (!may_match[i++]) {
      range->SkipKey(iter);
    }
  }
}
```

### Partitioned Filter Reading

`PartitionedFilterBlockReader` ([partitioned_filter_block.cc](../../table/block_based/partitioned_filter_block.cc:348-591)) adds an index lookup:

```cpp
bool PartitionedFilterBlockReader::MayMatch(const Slice& key, ...) {
  // 1. Load top-level index
  CachableEntry<Block_kFilterPartitionIndex> filter_block;
  GetOrReadFilterBlock(..., &filter_block, read_options);

  // 2. Binary search index to find partition handle
  BlockHandle filter_handle = GetFilterPartitionHandle(filter_block, ikey);

  // 3. Load filter partition from cache
  CachableEntry<ParsedFullFilterBlock> filter_partition_block;
  GetFilterPartitionBlock(..., filter_handle, ..., &filter_partition_block);

  // 4. Query partition filter
  FullFilterBlockReader partition_reader(table_, std::move(filter_partition_block));
  return partition_reader.KeyMayMatch(key, ...);
}
```

**MultiGet optimization:** Groups keys by partition to minimize index lookups and cache misses ([partitioned_filter_block.cc](../../table/block_based/partitioned_filter_block.cc:512-561)).

## Filter Block in SST File Format

Filters are stored as meta-blocks in the block-based table format (see [docs/components/sst_table_format.md](sst_table_format.md)):

```
Block-Based Table File
┌─────────────────────────────────────────┐
│  Data Block 1                           │
│  Data Block 2                           │
│  ...                                    │
│  Data Block N                           │
├─────────────────────────────────────────┤
│  Meta Block: "filter.<policy_name>"    │ ◄─── Full filter or partition
│  Meta Block: "filter.<policy_name>.1"  │ ◄─── Additional partitions
│  Meta Block: "filter.<policy_name>.2"  │
├─────────────────────────────────────────┤
│  Metaindex Block                        │ ──► Points to meta blocks by name
├─────────────────────────────────────────┤
│  Index Block                            │
├─────────────────────────────────────────┤
│  Footer (BlockHandle to index/metaindex)│
└─────────────────────────────────────────┘
```

**Filter metadata trailer** (5 bytes, [filter_policy.cc](../../table/block_based/filter_policy.cc:43-49)):
- Byte 0: Implementation marker (e.g., `0x01` = FastLocalBloom)
- Bytes 1-4: Format-specific metadata (e.g., num_probes for Bloom, unused for Ribbon)

**⚠️ INVARIANT:** The meta-block name format is `"filter.<FilterPolicy::CompatibilityName()>"`. All built-in policies use `"filter.rocksdb.BuiltinBloomFilter"` for cross-compatibility.

## Prefix Bloom Filtering

Prefix filtering uses `SliceTransform` to extract key prefixes and filter on those.

### Configuration

```cpp
Options options;
// Extract first 8 bytes as prefix
options.prefix_extractor.reset(NewFixedPrefixTransform(8));

BlockBasedTableOptions table_options;
table_options.filter_policy.reset(NewBloomFilterPolicy(10.0));
// Enable whole-key filtering (default) OR prefix-only
table_options.whole_key_filtering = true;  // Both whole key and prefix
// table_options.whole_key_filtering = false;  // Prefix only
```

### Prefix + Whole-Key Filtering

When `whole_key_filtering=true` and `prefix_extractor` is set, both are added ([full_filter_block.cc](../../table/block_based/full_filter_block.cc:62-78)):

```cpp
void FullFilterBlockBuilder::Add(const Slice& key) {
  if (prefix_extractor_ && prefix_extractor_->InDomain(key)) {
    Slice prefix = prefix_extractor_->Transform(key);
    if (whole_key_filtering_) {
      // Add whole key as primary, prefix as alternate
      // AddKeyAndAlt deduplicates: if key == prefix, only adds once
      filter_bits_builder_->AddKeyAndAlt(key, prefix);
    } else {
      filter_bits_builder_->AddKey(prefix);
    }
  } else if (whole_key_filtering_) {
    filter_bits_builder_->AddKey(key);
  }
}
```

**Query path:**
- `Get(key)` → checks whole key via `KeyMayMatch()`
- `Seek(prefix)` → checks prefix via `PrefixMayMatch()`

**⚠️ INVARIANT:** `prefix_extractor` must be consistent across writes and reads. Changing it invalidates existing filters.

## Filter Caching

Filters are cached in the block cache to avoid repeated disk I/O.

### Full Filter Caching

Full filters are cached as single blocks ([full_filter_block.cc](../../table/block_based/full_filter_block.cc:105-130)):

```cpp
std::unique_ptr<FilterBlockReader> FullFilterBlockReader::Create(
    const BlockBasedTable* table, ..., bool pin, ...) {
  CachableEntry<ParsedFullFilterBlock> filter_block;

  if (prefetch || !use_cache) {
    // Read filter into cache
    ReadFilterBlock(table, ..., use_cache, ..., &filter_block);

    if (use_cache && !pin) {
      filter_block.Reset();  // Release handle, keep in cache
    }
  }

  // If pinned, filter_block stays in memory via CachableEntry
  return std::make_unique<FullFilterBlockReader>(table, std::move(filter_block));
}
```

**Pinning:** `pin=true` keeps the filter in memory for the table's lifetime, avoiding cache eviction. Enabled via `cache_index_and_filter_blocks_with_high_priority=true`.

### Partitioned Filter Caching

Partitions are cached individually ([partitioned_filter_block.cc](../../table/block_based/partitioned_filter_block.cc:594-684)):

```cpp
Status PartitionedFilterBlockReader::CacheDependencies(..., bool pin, ...) {
  // Load top-level index
  CachableEntry<Block_kFilterPartitionIndex> filter_block;
  GetOrReadFilterBlock(..., &filter_block, ro);

  // Prefetch all partitions in one I/O
  biter.SeekToFirst();
  uint64_t prefetch_off = biter.value().handle.offset();
  biter.SeekToLast();
  uint64_t prefetch_len = last_handle.offset() + size - prefetch_off;
  prefetch_buffer->Prefetch(..., prefetch_off, prefetch_len);

  // Load each partition into cache
  for (biter.SeekToFirst(); biter.Valid(); biter.Next()) {
    CachableEntry<ParsedFullFilterBlock> block;
    table()->MaybeReadBlockAndLoadToCache(..., &block, ...);

    if (pin && block.IsCached()) {
      filter_map_[handle.offset()] = std::move(block);  // Pin in memory
    }
  }
}
```

**⚠️ INVARIANT:** Pinned partitions are stored in `filter_map_` to prevent eviction. Unpinning on table close requires calling `EraseFromCacheBeforeDestruction()`.

## Memtable Bloom Filter (DynamicBloom)

Memtables use `DynamicBloom` ([util/dynamic_bloom.h](../../util/dynamic_bloom.h:34-215)) for prefix-based filtering, reducing lookups in large memtables.

### Characteristics

```cpp
class DynamicBloom {
  // Concurrent-safe add (uses relaxed atomics)
  void AddConcurrently(const Slice& key);

  // Thread-safe query
  bool MayContain(const Slice& key) const;

  // Batch query for MultiGet
  void MayContain(int num_keys, Slice* keys, bool* may_match) const;
};
```

**Key differences from SST filters:**
- **Concurrent writes:** Uses `std::atomic` with relaxed memory ordering
- **Non-persistent:** Only in-memory, never serialized
- **Prefix-only:** Memtables only filter by prefix, not whole key
- **Fixed size:** Allocated upfront based on memtable size

### Implementation Details

**Double-probe optimization** ([dynamic_bloom.h](../../util/dynamic_bloom.h:180-195)): Checks 2 bits per 64-bit memory access:

```cpp
bool DynamicBloom::DoubleProbe(uint32_t h32, size_t byte_offset) const {
  uint64_t h = 0x9e3779b97f4a7c13ULL * h32;  // Remix hash
  for (unsigned i = 0;; ++i) {
    // Two bit probes per uint64_t load
    uint64_t mask = ((uint64_t)1 << (h & 63)) | ((uint64_t)1 << ((h >> 6) & 63));
    uint64_t val = data_[byte_offset ^ i].LoadRelaxed();
    if ((val & mask) != mask) return false;
    if (i + 1 >= kNumDoubleProbes) return true;
    h = (h >> 12) | (h << 52);  // Re-mix for next probe pair
  }
}
```

**Concurrency:** Add uses `FetchOrRelaxed()` to avoid data races ([dynamic_bloom.h](../../util/dynamic_bloom.h:107-117)):

```cpp
void DynamicBloom::AddHashConcurrently(uint32_t hash) {
  AddHash(hash, [](RelaxedAtomic<uint64_t>* ptr, uint64_t mask) {
    if ((mask & ptr->LoadRelaxed()) != mask) {
      ptr->FetchOrRelaxed(mask);  // Atomic OR operation
    }
  });
}
```

**⚠️ INVARIANT:** Happens-before relationship between `Add` and `MayContain` is ensured by MemTable sequence number visibility, not by atomic ordering. Relaxed atomics are sufficient to prevent data races.

## Filter Configuration Best Practices

### Choosing Bloom vs. Ribbon

| Factor | Bloom | Ribbon |
|--------|-------|--------|
| **Space** | Standard | 30% smaller |
| **Construction** | Fast | 3-4x slower |
| **Query** | Fast | Similar to Bloom |
| **Best for** | L0, small files | L1+, large files |

**Recommendation:** Use `NewRibbonFilterPolicy(bits_per_key, bloom_before_level=1)` for most workloads to get Bloom in L0 (fast flushes) and Ribbon in L1+ (space savings).

### Choosing Full vs. Partitioned

| Factor | Full | Partitioned |
|--------|------|-------------|
| **Memory** | All-or-nothing | Granular |
| **Cache efficiency** | Single entry | Multiple entries |
| **Query latency** | One lookup | Two lookups (index + partition) |
| **Best for** | Small SSTs, high cache hit rate | Large SSTs, limited cache |

**Recommendation:** Use partitioned filters for large SST files (>64MB) or when cache size is limited relative to total data size.

### Bits-per-Key Selection

| Bits/Key | FP Rate | Use Case |
|----------|---------|----------|
| 5 | ~10% | Very low memory, high disk bandwidth |
| 7 | ~2% | Balanced |
| 10 | ~1% | **Recommended default** |
| 14 | ~0.1% | Low disk latency, premium on avoiding disk I/O |
| 20 | ~0.01% | Extremely latency-sensitive |

**Rule of thumb:** Choose bits-per-key such that the cost of a false positive (disk read) is ~100x the cost of a filter query.

### Whole-Key vs. Prefix Filtering

```cpp
// Whole-key only (default if no prefix_extractor)
table_options.whole_key_filtering = true;

// Prefix only (for range queries with prefix_extractor)
table_options.whole_key_filtering = false;
options.prefix_extractor.reset(NewFixedPrefixTransform(8));

// Both (point + prefix queries)
table_options.whole_key_filtering = true;
options.prefix_extractor.reset(NewFixedPrefixTransform(8));
```

**⚠️ INVARIANT:** Prefix-only filtering (`whole_key_filtering=false`) breaks point lookup correctness for keys outside `prefix_extractor->InDomain()`. Only use when all queries are prefix-based.

## Filter Metrics and Monitoring

RocksDB tracks filter performance through two systems: Statistics and PerfContext.

### Statistics Counters ([monitoring/statistics.h](../../monitoring/statistics.h))

```cpp
BLOOM_FILTER_USEFUL        // SST filters avoided disk read (true negative)
BLOOM_FILTER_FULL_POSITIVE // Filter said "present" but key not found (false positive)
MEMTABLE_HIT               // Key found in memtable
MEMTABLE_MISS              // Key not in memtable
```

### PerfContext Counters ([include/rocksdb/perf_context.h](../../include/rocksdb/perf_context.h))

```cpp
bloom_sst_hit_count          // SST filter said "maybe present"
bloom_sst_miss_count         // SST filter said "definitely not present"
bloom_memtable_hit_count     // Memtable filter said "maybe present"
bloom_memtable_miss_count    // Memtable filter said "definitely not present"
```

**Monitoring effective FP rate:**
```
Actual FP Rate = BLOOM_FILTER_FULL_POSITIVE / (BLOOM_FILTER_USEFUL + BLOOM_FILTER_FULL_POSITIVE)
Filter Efficiency = BLOOM_FILTER_USEFUL / total_Get_calls
```

## Common Pitfalls

### 1. Changing prefix_extractor

**Problem:** Filters are built with one `prefix_extractor`, reads use another.

```cpp
// Build SSTs with prefix length 8
options.prefix_extractor.reset(NewFixedPrefixTransform(8));
db->Open(...);
db->Put("key12345678", "value");
db->Flush();

// Read with prefix length 4 (WRONG!)
options.prefix_extractor.reset(NewFixedPrefixTransform(4));
db->Get("key12345678");  // Filter miss, but key exists → false negative!
```

**Solution:** Never change `prefix_extractor` on existing data. Use compaction to rebuild filters.

### 2. Disabling whole_key_filtering with point queries

**Problem:** Point lookups (`Get`) need whole-key filtering, but it's disabled.

```cpp
table_options.whole_key_filtering = false;  // Prefix-only
options.prefix_extractor.reset(NewFixedPrefixTransform(8));

db->Put("key00000000", "value");
db->Get("key00000000");  // No filter check! Always reads SST.
```

**Solution:** Keep `whole_key_filtering=true` for mixed workloads.

### 3. Over-pinning filters

**Problem:** Pinning all filters exhausts memory.

```cpp
// 1000 SST files × 10 MB filter each = 10 GB pinned!
table_options.cache_index_and_filter_blocks = false;  // Pinned in table reader
```

**Solution:** Use `cache_index_and_filter_blocks=true` and let LRU cache manage eviction, or use partitioned filters.

## Code References

| Component | File |
|-----------|------|
| **FilterPolicy API** | [include/rocksdb/filter_policy.h](../../include/rocksdb/filter_policy.h) |
| **FilterBitsBuilder** | [table/block_based/filter_policy_internal.h](../../table/block_based/filter_policy_internal.h) |
| **FastLocalBloom** | [util/bloom_impl.h](../../util/bloom_impl.h) (FastLocalBloomImpl) |
| **Ribbon** | [util/ribbon_config.h](../../util/ribbon_config.h), [util/ribbon_impl.h](../../util/ribbon_impl.h) |
| **Full filter** | [table/block_based/full_filter_block.h](../../table/block_based/full_filter_block.h), [full_filter_block.cc](../../table/block_based/full_filter_block.cc) |
| **Partitioned filter** | [table/block_based/partitioned_filter_block.h](../../table/block_based/partitioned_filter_block.h), [partitioned_filter_block.cc](../../table/block_based/partitioned_filter_block.cc) |
| **DynamicBloom** | [util/dynamic_bloom.h](../../util/dynamic_bloom.h), [util/dynamic_bloom.cc](../../util/dynamic_bloom.cc) |
| **Bloom/Ribbon impl** | [table/block_based/filter_policy.cc](../../table/block_based/filter_policy.cc) |

## Related Documentation

- [SST Table Format](sst_table_format.md) - How filters fit into block-based table structure
- [Read Path](flush_and_read_path.md) - When and how filters are consulted during reads
- [Cache](cache.md) - Filter block caching and pinning strategies
- [ARCHITECTURE.md](../../ARCHITECTURE.md) - High-level overview of RocksDB components
