//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * HyperClockCache - A lock-free Cache alternative for RocksDB block cache
 * that offers much improved CPU efficiency vs. LRUCache under high parallel
 * load or high contention, with some caveats:
 * <ul>
 * <li>
 * Not a general Cache implementation: can only be used for
 * BlockBasedTableOptions::block_cache, which RocksDB uses in a way that is
 * compatible with HyperClockCache.
 * </li>
 * <li>
 * Requires an extra tuning parameter: see estimated_entry_charge below.
 * Similarly, substantially changing the capacity with SetCapacity could
 * harm efficiency. -&gt; EXPERIMENTAL: the tuning parameter can be set to 0
 * to find the appropriate balance automatically.
 * </li>
 * <li>
 * Cache priorities are less aggressively enforced, which could cause
 * cache dilution from long range scans (unless they use fill_cache=false).
 * </li>
 * <li>
 * Can be worse for small caches, because if almost all of a cache shard is
 * pinned (more likely with non-partitioned filters), then CLOCK eviction
 * becomes very CPU intensive.
 * </li>
 * </ul>
 */
@Experimental("HyperClockCache is still experimental and this API may change in future.")
public class HyperClockCache extends Cache {
  /**
   *
   * @param capacity The fixed size capacity of the cache
   * @param estimatedEntryCharge EXPERIMENTAL: the field can be set to 0 to size the table
   *     dynamically and automatically. See C++ Api for more info.
   * @param numShardBits The cache is sharded to 2^numShardBits shards, by hash of the key
   * @param strictCapacityLimit insert to the cache will fail when cache is full
   */
  public HyperClockCache(final long capacity, final long estimatedEntryCharge, int numShardBits,
      boolean strictCapacityLimit) {
    super(newHyperClockCache(capacity, estimatedEntryCharge, numShardBits, strictCapacityLimit));
  }

  @Override
  protected void disposeInternal(long handle) {
    disposeInternalJni(handle);
  }

  private static native void disposeInternalJni(long handle);

  private static native long newHyperClockCache(final long capacity,
      final long estimatedEntryCharge, int numShardBits, boolean strictCapacityLimit);
}
