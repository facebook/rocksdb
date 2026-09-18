// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WriteBufferManagerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void construct() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(2000L, cache)) {
      assertThat(wbm).isNotNull();
    }
  }

  @Test
  public void constructWithAllowStall() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(2000L, cache, true)) {
      assertThat(wbm).isNotNull();
      assertThat(wbm.allowStall()).isTrue();
    }
  }

  @Test
  public void enabled() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(2000L, cache)) {
      assertThat(wbm.enabled()).isTrue();
    }
  }

  @Test
  public void enabledWithZeroSize() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(0L, cache)) {
      assertThat(wbm.enabled()).isFalse();
    }
  }

  @Test
  public void costToCache() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(2000L, cache)) {
      assertThat(wbm.costToCache()).isTrue();
    }
  }

  @Test
  public void memoryUsage() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(10 * 1024 * 1024, cache)) {
      assertThat(wbm.memoryUsage()).isEqualTo(0L);
    }
  }

  @Test
  public void mutableMemtableMemoryUsage() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(10 * 1024 * 1024, cache)) {
      assertThat(wbm.mutableMemtableMemoryUsage()).isEqualTo(0L);
    }
  }

  @Test
  public void dummyEntriesInCacheUsage() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(10 * 1024 * 1024, cache)) {
      assertThat(wbm.dummyEntriesInCacheUsage()).isEqualTo(0L);
    }
  }

  @Test
  public void bufferSize() throws RocksDBException {
    final long expectedSize = 2000L;
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(expectedSize, cache)) {
      assertThat(wbm.bufferSize()).isEqualTo(expectedSize);
    }
  }

  @Test
  public void setBufferSize() throws RocksDBException {
    final long initialSize = 2000L;
    final long newSize = 4000L;
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(initialSize, cache)) {
      assertThat(wbm.bufferSize()).isEqualTo(initialSize);
      wbm.setBufferSize(newSize);
      assertThat(wbm.bufferSize()).isEqualTo(newSize);
    }
  }

  @Test
  public void setBufferSizeInvalid() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(2000L, cache)) {
      assertThatThrownBy(() -> wbm.setBufferSize(0L))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Buffer size must be greater than 0");
      assertThatThrownBy(() -> wbm.setBufferSize(-100L))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Buffer size must be greater than 0");
    }
  }

  @Test
  public void setAllowStall() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(2000L, cache, false)) {
      assertThat(wbm.allowStall()).isFalse();
      wbm.setAllowStall(true);
      assertThat(wbm.allowStall()).isTrue();
      wbm.setAllowStall(false);
      assertThat(wbm.allowStall()).isFalse();
    }
  }

  @Test
  public void isStallActive() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(2000L, cache)) {
      assertThat(wbm.isStallActive()).isFalse();
    }
  }

  @Test
  public void isStallThresholdExceeded() throws RocksDBException {
    try (final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(2000L, cache)) {
      assertThat(wbm.isStallThresholdExceeded()).isFalse();
    }
  }

  @Test
  public void integrationWithDB() throws RocksDBException {
    try (final Cache cache = new LRUCache(8 * 1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(8 * 1024 * 1024, cache, false);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setWriteBufferSize(1024 * 1024)
             .setWriteBufferManager(wbm);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      
      // Memory usage may be non-zero due to memtable initialization
      final long initialMemory = wbm.memoryUsage();
      final long initialMutable = wbm.mutableMemtableMemoryUsage();
      
      // Write some data
      final byte[] value = new byte[1024];
      for (int i = 0; i < 100; i++) {
        db.put(("testkey" + i).getBytes(), value);
      }
      
      // Memory usage should increase or stay the same
      assertThat(wbm.memoryUsage()).isGreaterThanOrEqualTo(initialMemory);
      assertThat(wbm.mutableMemtableMemoryUsage()).isGreaterThanOrEqualTo(initialMutable);
      
      // Should not exceed buffer size
      assertThat(wbm.memoryUsage()).isLessThanOrEqualTo(wbm.bufferSize());
    }
  }

  @Test
  public void multipleColumnFamilies() throws RocksDBException {
    try (final Cache cache = new LRUCache(16 * 1024 * 1024);
         final WriteBufferManager wbm = new WriteBufferManager(16 * 1024 * 1024, cache);
         final DBOptions dbOptions = new DBOptions()
             .setCreateIfMissing(true)
             .setCreateMissingColumnFamilies(true)
             .setWriteBufferManager(wbm);
         final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
             .setWriteBufferSize(1024 * 1024)) {
      
      final java.util.List<ColumnFamilyDescriptor> cfDescriptors = new java.util.ArrayList<>();
      cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));
      cfDescriptors.add(new ColumnFamilyDescriptor("cf1".getBytes(), cfOpts));
      
      final java.util.List<ColumnFamilyHandle> cfHandles = new java.util.ArrayList<>();
      
      try (final RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
          cfDescriptors, cfHandles)) {
        
        // Memory is shared across column families
        assertThat(wbm.enabled()).isTrue();
        assertThat(wbm.bufferSize()).isEqualTo(16 * 1024 * 1024);
        
        // Write to both column families
        final byte[] value = new byte[1024];
        for (int i = 0; i < 50; i++) {
          db.put(cfHandles.get(0), ("key_default_" + i).getBytes(), value);
          db.put(cfHandles.get(1), ("key_cf1_" + i).getBytes(), value);
        }
        
        // Total memory should be tracked
        assertThat(wbm.memoryUsage()).isGreaterThan(0L);
        
      } finally {
        for (final ColumnFamilyHandle handle : cfHandles) {
          handle.close();
        }
      }
    }
  }
}
