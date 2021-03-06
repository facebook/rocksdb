// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class LRUCacheTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void newLRUCache() throws RocksDBException {
    final long capacity = 1000;
    final int numShardBits = 16;
    final boolean strictCapacityLimit = true;
    final double highPriPoolRatio = 0.5;
    try(final Cache lruCache = new LRUCache(capacity,
        numShardBits, strictCapacityLimit, highPriPoolRatio)) {
      assertThat(lruCache.getCapacity()).isEqualTo(capacity);
      assertThat(lruCache.getEntries()).isEqualTo(0);
      assertThat(lruCache.getUsage()).isEqualTo(0);
      assertThat(lruCache.getHighPriorityPoolUsage()).isEqualTo(0);
      assertThat(lruCache.getPinnedUsage()).isEqualTo(0);
    }
  }
}
