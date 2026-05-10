# Retained Block Backing For Iterator Pin APIs

**Status:** implemented

**Related files:** `include/rocksdb/iterator.h`, `include/rocksdb/table.h`, `table/block_fetcher.h`, `table/block_fetcher.cc`, `table/block_based/block_based_table_reader.cc`, `table/block_based/block_based_table_iterator.cc`, `table/block_based/reader_common.h`, `table/block_based/block.h`, `table/block_based/block.cc`

## Problem Statement

The existing `Iterator::PinCurrent()` and `Iterator::AppendPinnedCurrent()` APIs support zero-copy pinning when the current row is backed by a block-cache entry. In that case, the pinning lifetime is implemented by retaining a block-cache handle.

The missing capability is zero-copy pinning for reads that do **not** use block cache:

1. Application provides a custom source of block memory.
2. SST data blocks read from disk are stored in that memory.
3. The backing storage lifetime is decoupled from iterator lifetime.
4. A pinned key/value remains valid after iterator movement or destruction.
5. The design remains generic and does not hard-code a particular backing implementation such as `IOBuf`.

## Current Behavior

### Cache-backed pinning

When the current row comes from `BlockBasedTableIterator`, `PinCurrentKeyValue()` in `table/block_based/block_based_table_iterator.cc` retains the block-cache handle and returns it through `PinnedIterKeyValue.cleanup`.

This gives:

- zero-copy key/value slices
- lifetime independent of iterator movement
- per-block dedupe in `PinnableKeyValueBatch` via `cleanup_dedupe_token`

### Non-cache reads

The block-read path already accepts `MemoryAllocator*` in `BlockFetcher`, but the allocator currently comes from block cache only:

```cpp
inline MemoryAllocator* GetMemoryAllocator(
    const BlockBasedTableOptions& table_options) {
  return table_options.block_cache.get()
             ? table_options.block_cache->memory_allocator()
             : nullptr;
}
```

Without block cache, block bytes can still be allocated and copied, but there is no general retained backing object for `Pin*` to hold onto. As a result, `DBIter::PinCurrent()` falls back to copying when true pinning is unavailable.

## Requirements

The new design should satisfy all of the following:

1. Keep the current block-cache pinning behavior unchanged.
2. Support a custom block-memory source even when `no_block_cache=true`.
3. Support zero-copy pinning from that custom backing store.
4. Keep the feature generic. RocksDB core should not depend on `IOBuf`.
5. Keep the disabled case close to the current fast path with negligible regression risk.
6. Preserve same-block multi-key behavior:
   - multiple pinned rows from one block remain valid
   - batch pinning dedupes backing retention by block

## Design Summary

The core change is to generalize the retained backing object, not to extend `MemoryAllocator` directly.

`MemoryAllocator` is an allocation interface. The new feature needs a stronger contract:

- writable contiguous memory for a loaded block
- a retained lifetime object
- a stable dedupe token for block-level sharing

That contract fits a new abstraction such as `RetainedBlockBufferProvider`.

## Proposed Abstractions

### 1. New retained block buffer provider

```cpp
class RetainedBlockBufferProvider {
 public:
  struct Lease {
    char* data = nullptr;
    size_t size = 0;
    SharedCleanablePtr cleanup;
    const void* dedupe_token = nullptr;
  };

  virtual Status Allocate(size_t size, size_t alignment, Lease* out) = 0;
};
```

Properties:

- `data` must reference writable contiguous memory
- `data` must be aligned to `alignment`
- `cleanup` must keep that memory alive until cleanup runs
- `dedupe_token` must be identical for leases that represent the same backing block

The provider is intentionally a programmatic hook rather than a `Customizable`
option. It is registered with the options framework as non-serializable so
OPTIONS files cannot accidentally require an application-specific allocator.

### 2. New `BlockBasedTableOptions` hook

Add a new optional field:

```cpp
std::shared_ptr<RetainedBlockBufferProvider> block_buffer_provider;
```

This keeps the feature table-reader scoped instead of making `MemoryAllocator` responsible for retention semantics.

### 3. Extend `BlockContents` to carry retained backing

`BlockContents` is already the transport object for block bytes between fetch, decompression, and parsed block creation. It is the natural place to carry generic lifetime retention.

Proposed extension:

```cpp
struct BlockContents {
  Slice data;
  CacheAllocationPtr allocation;

  // New generic retained owner for externally managed block memory.
  SharedCleanablePtr cleanup;
  const void* cleanup_dedupe_token = nullptr;

  bool own_bytes() const {
    return allocation.get() != nullptr || cleanup.get() != nullptr;
  }
};
```

This allows the same object to represent:

- heap-owned bytes
- cache-allocator-owned bytes
- provider-backed retained bytes

## End-to-End Flow

### Block cache path

No semantic change is needed.

1. `BlockFetcher` reads and/or decompresses block bytes.
2. Parsed block is inserted into block cache.
3. `BlockBasedTableIterator::PinCurrentKeyValue()` retains the cache handle.
4. `PinnedIterKeyValue.cleanup` owns the cache-handle release.

`BlockContents.cleanup` can remain empty in this mode.

### Provider path with block cache disabled

1. `BlockFetcher` requests a `Lease` from `block_buffer_provider`.
2. Block bytes are read into `Lease.data`.
3. `BlockContents` stores:
   - `data`
   - empty or unused `allocation`
   - `cleanup`
   - `cleanup_dedupe_token`
4. Parsed `Block` retains the moved `BlockContents`.
5. `BlockBasedTableIterator::PinCurrentKeyValue()` retains the same `cleanup`.
6. Pinned row remains valid after iterator movement or destruction.

## Critical Sample Code

### Option lookup

```cpp
inline RetainedBlockBufferProvider* GetBlockBufferProvider(
    const BlockBasedTableOptions& table_options) {
  return table_options.block_buffer_provider.get();
}
```

### `BlockFetcher` constructor extension

```cpp
class BlockFetcher {
 public:
  BlockFetcher(...,
               RetainedBlockBufferProvider* block_buffer_provider = nullptr,
               MemoryAllocator* memory_allocator = nullptr,
               MemoryAllocator* memory_allocator_compressed = nullptr,
               bool for_compaction = false);

  const SharedCleanablePtr& block_cleanup() const { return block_cleanup_; }
  const void* block_dedupe_token() const { return block_dedupe_token_; }

 private:
  RetainedBlockBufferProvider* block_buffer_provider_ = nullptr;
  SharedCleanablePtr block_cleanup_;
  const void* block_dedupe_token_ = nullptr;
};
```

### Provider-backed read buffer selection

```cpp
inline void BlockFetcher::PrepareBufferForBlockFromFile() {
  if (UNLIKELY(block_buffer_provider_ != nullptr) && !maybe_compressed_) {
    RetainedBlockBufferProvider::Lease lease;
    Status s =
        block_buffer_provider_->Allocate(block_size_with_trailer_, 1, &lease);
    if (!s.ok()) {
      io_status_ = IOStatus::FromStatus(s);
      return;
    }
    used_buf_ = lease.data;
    block_cleanup_ = std::move(lease.cleanup);
    block_dedupe_token_ = lease.dedupe_token;
    return;
  }

  // Existing stack / heap / compressed-buffer path.
}
```

### Attach retained ownership to `BlockContents`

```cpp
void BlockFetcher::GetBlockContents() {
  if (block_buffer_provider_ != nullptr &&
      compression_type() == kNoCompression &&
      used_buf_ != nullptr) {
    *contents_ = BlockContents(Slice(used_buf_, block_size_));
    contents_->cleanup = block_cleanup_;
    contents_->cleanup_dedupe_token = block_dedupe_token_;
    return;
  }

  // Existing logic.
}
```

### Generalized iterator pinning

```cpp
Status BlockBasedTableIterator::PinCurrentKeyValue(PinnedIterKeyValue* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("PinnedIterKeyValue is nullptr");
  }
  out->Reset();
  if (!Valid()) {
    return Status::InvalidArgument("Iterator is not valid");
  }
  if (!PrepareValue()) {
    return status();
  }

  if (TryPinFromBlockCache(out).ok()) {
    return Status::OK();
  }

  const SharedCleanablePtr backing_cleanup = block_iter_.GetBlockBackingCleanup();
  if (backing_cleanup.get() == nullptr) {
    return Status::NotSupported("Current entry has no retainable block backing");
  }

  out->cleanup = backing_cleanup;
  out->cleanup_dedupe_token = block_iter_.GetBlockBackingDedupeToken();
  out->key = block_iter_.key();
  out->user_key = block_iter_.user_key();
  out->value = block_iter_.value();
  out->key_pinned = block_iter_.IsKeyPinned();
  out->value_pinned = true;
  out->pinned_block_cache_usage = 0;
  return Status::OK();
}
```

## Why `BlockContents` Works For Both Paths

`BlockContents` is the narrow waist between:

- block fetch / decompression
- parsed block creation

That makes it the right place to carry generic retained ownership.

Behavior by mode:

- **Block cache enabled:** pinning continues to retain cache handles; `BlockContents.cleanup` is not required for correctness.
- **Provider enabled without block cache:** `BlockContents.cleanup` becomes the retained owner for the current block bytes.

This keeps the current block-cache fast path intact while enabling a new zero-copy path for provider-backed reads.

## Same-Block Multi-Key Behavior

The design supports multiple pinned rows from the same block.

Expected semantics:

- `PinCurrent()` on different keys in the same block is correct.
- Multiple pinned rows can reference the same block backing.
- `AppendPinnedCurrent()` dedupes backing retention by `cleanup_dedupe_token`.

This matches the existing batch behavior in `PinnableKeyValueBatch`.

One important detail:

- Separate `PinnableKeyValue` instances may each retain the same backing independently.
- This is correct, though less efficient than batch dedupe.
- Cross-object dedupe is not required for correctness.

## Why `MemoryAllocator` Alone Is Not Enough

`MemoryAllocator` only defines:

- allocate
- free
- optional usable-size reporting

It does not define:

- retained ownership
- block-level dedupe identity
- safe lifetime sharing across iterator reset

Extending `MemoryAllocator` to return a retention handle would mix two separate concerns:

- where bytes come from
- how block backing remains alive

The new provider abstraction keeps these concerns separate and keeps the RocksDB core generic.

## Applicability To `IOBuf`

The design is generic and does not require RocksDB to depend on `IOBuf`.

An application can implement `RetainedBlockBufferProvider` using:

- `IOBuf`
- refcounted heap slabs
- mmap-backed extents
- shared-memory pools
- custom arenas

The only hard requirement is contiguous writable memory for each loaded block.

`IOBuf` is one implementation strategy, not part of the RocksDB core API contract.

## Main Concerns

### 1. Lifetime and overwrite safety

The provider must not recycle or overwrite a block extent until all outstanding cleanups referring to it are released.

### 2. Capacity exhaustion

If too many blocks stay pinned, the provider can run out of space.

The design should define a policy:

- fail with `MemoryLimit`
- fall back to non-zero-copy copy path
- retry / block

Fallback-to-copy is usually the safest first implementation.

### 3. Fragmentation

An implementation based on one large region may fragment if block sizes vary.
Size classes or slab buckets may be needed for production-quality pools.

### 4. Compression path

The pinned row must reference the final uncompressed block bytes. Temporary compressed buffers should not be used as the retained backing for iterator pinning.

### 5. Sharing semantics

If two iterators read the same SST block with block cache disabled, the provider path must decide whether they:

- share the same retained extent, or
- each receive separate backing

The design works either way. Sharing is an optimization, not a correctness requirement.

### 6. Performance regression risk when unused

The feature must be null-default and explicitly opt-in.

To avoid measurable regression:

- make provider lookup a cheap null check
- preserve current block-cache path
- avoid touching `SharedCleanablePtr` when provider is disabled
- avoid adding extra copies to the normal cache path

The biggest regression risk is object-size growth in hot structures such as `BlockContents` and `Block`. If needed, retained backing can be moved to an optional side object instead of inline fields.

## Benchmark Plan

The benchmark plan should validate both correctness and unused-path cost.

### Functional benchmarks

1. `no_block_cache=true`, provider disabled
   - baseline current behavior
2. `no_block_cache=true`, provider enabled, `PinCurrent()`
   - validate zero-copy and lifetime retention
3. `no_block_cache=true`, provider enabled, `AppendPinnedCurrent()`
   - validate batch dedupe
4. block cache enabled, provider disabled
   - validate no regression in the existing cache-backed path

### Performance measurements

For each configuration, collect:

- point lookup throughput
- forward scan throughput
- CPU per operation
- bytes copied per pinned row
- provider allocation / release counts
- peak retained backing bytes

### Microbenchmarks

Recommended focused cases:

1. single iterator, sequential scan, pin every row
2. single iterator, pin sparse rows
3. multiple iterators scanning the same SST
4. same-block repeated pinning into `PinnableKeyValueBatch`
5. provider exhaustion scenario with many outstanding pins

### Regression gate

When provider is disabled, throughput and CPU should remain within noise relative to the current code on:

- block-cache-enabled scans
- block-cache-disabled scans without pinning

## Implementation Plan

### Phase 1: plumbing

1. Add `RetainedBlockBufferProvider`.
2. Add `block_buffer_provider` to `BlockBasedTableOptions`.
3. Add provider lookup helper in `reader_common.h`.

### Phase 2: block backing propagation

1. Extend `BlockContents` with generic retained cleanup.
2. Teach `BlockFetcher` to allocate from provider when configured.
3. Preserve retained ownership through parsed block creation.

### Phase 3: iterator integration

1. Add accessors from block iterator / block object to current block backing cleanup.
2. Generalize `BlockBasedTableIterator::PinCurrentKeyValue()` to use:
   - block cache handle first
   - provider-backed cleanup second
   - copy fallback otherwise

### Phase 4: testing

1. Add a fake retained-buffer provider for unit tests.
2. Add tests for:
   - zero-copy `PinCurrent()` without block cache
   - `AppendPinnedCurrent()` dedupe on same block
   - lifetime survives iterator destruction
   - fallback behavior when provider is exhausted

### Phase 5: benchmarking

1. Add a focused microbenchmark or test harness.
2. Measure provider-disabled and provider-enabled cases.
3. Confirm that unused-path overhead stays within noise.

## Open Questions

1. Should provider behavior be table-wide (`BlockBasedTableOptions`) or per-read (`ReadOptions`)?
   - Recommendation: table-wide first.
2. Should provider exhaustion fall back to copy or fail?
   - Recommendation: copy fallback first.
3. Should provider-backed reads try to dedupe same-SST-block fetches without block cache?
   - Recommendation: not in v1.
4. Should compressed temporary buffers ever use the provider?
   - Recommendation: no. Only final uncompressed block backing should be retained for `Pin*`.
