// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

public class WriteBufferManagerTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE = new RocksNativeLibraryResource();

  @Test
  public void newWriteBufferManagerWithCache() {
    final long capacity = 1000;
    try (final Cache lruCache = new LRUCache(capacity);
        final WriteBufferManager wbm = new WriteBufferManager(capacity, lruCache)) {
      assertThat(wbm.getBufferSize()).isEqualTo(capacity);
      assertThat(wbm.getMemoryUsage()).isEqualTo(0);
      assertThat(wbm.getMutableMemtableMemoryUsage()).isEqualTo(0);
    }
  }

  @Test
  public void newWriteBufferManagerWithoutCache() {
    final long capacity = 1000;
    try (final WriteBufferManager wbm = new WriteBufferManager(capacity)) {
      assertThat(wbm.getBufferSize()).isEqualTo(capacity);
      assertThat(wbm.getMemoryUsage()).isEqualTo(0);
      assertThat(wbm.getMutableMemtableMemoryUsage()).isEqualTo(0);
    }
  }
}
