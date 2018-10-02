// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * JNI passthrough for MemoryUtil.
 */
public class MemoryUtil {

  /**
   * <p>Returns the approximate memory usage of different types in the input
   * list of DBs and Cache set.  For instance, in the output map the key
   * kMemTableTotal will be associated with the memory
   * usage of all the mem-tables from all the input rocksdb instances.</p>
   *
   * <p>Note that for memory usage inside Cache class, we will
   * only report the usage of the input "cache_set" without
   * including those Cache usage inside the input list "dbs"
   * of DBs.</p>
   *
   * @param dbs List of dbs to collect memory usage for.
   * @param caches Set of caches to collect memory usage for.
   * @return Map from {@link MemoryUsageType} to memory usage as a {@link Long}.
   */
  public static Map<MemoryUsageType, Long> getApproximateMemoryUsageByType(final List<RocksDB> dbs, final Set<Cache> caches) {
    long[] dbHandles = new long[dbs.size()];
    long[] cacheHandles = new long[caches.size()];
    int i = 0;
    for(RocksDB db : dbs) {
      dbHandles[i] = db.nativeHandle_;
      i++;
    }
    i = 0;
    for(Cache cache : caches) {
      cacheHandles[i] = cache.nativeHandle_;
      i++;
    }
    Map<Byte, Long> longOutput = getApproximateMemoryUsageByType(dbHandles, cacheHandles);
    Map<MemoryUsageType, Long> output = new HashMap<>();
    for(Map.Entry<Byte, Long> longEntry : longOutput.entrySet()) {
      output.put(MemoryUsageType.getMemoryUsageType(longEntry.getKey()), longEntry.getValue());
    }
    return output;
  }

  private native static Map<Byte, Long> getApproximateMemoryUsageByType(final long[] dbHandles,
      final long[] cacheHandles);
}
