// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.Test;

public class LRUCacheTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void newLRUCache() {
    final long capacity = 1000;
    final int numShardBits = 16;
    final boolean strictCapacityLimit = true;
    final double highPriPoolRatio = 5;
    try(final Cache lruCache = new LRUCache(capacity,
        numShardBits, strictCapacityLimit, highPriPoolRatio)) {
      //no op
    }
  }
}
