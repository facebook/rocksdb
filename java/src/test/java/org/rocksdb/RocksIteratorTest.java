// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class RocksIteratorTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void rocksIterator() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());
        iterator.next();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
        iterator.next();
        assertThat(iterator.isValid()).isFalse();
        iterator.seekToLast();
        iterator.prev();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());
        iterator.seekToFirst();
        iterator.seekToLast();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
        iterator.status();
      }

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seek("key0".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());

        iterator.seek("key1".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());

        iterator.seek("key1.5".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());

        iterator.seek("key2".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());

        iterator.seek("key3".getBytes());
        assertThat(iterator.isValid()).isFalse();
      }

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekForPrev("key0".getBytes());
        assertThat(iterator.isValid()).isFalse();

        iterator.seekForPrev("key1".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());

        iterator.seekForPrev("key1.5".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());

        iterator.seekForPrev("key2".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());

        iterator.seekForPrev("key3".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
      }
    }
  }

  @Test
  public void rocksIteratorReleaseAfterCfClose() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             this.dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());

      // Test case: release iterator after default CF close
      try (final RocksIterator iterator = db.newIterator()) {
        // In fact, calling close() on default CF has no effect
        db.getDefaultColumnFamily().close();

        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key".getBytes());
        assertThat(iterator.value()).isEqualTo("value".getBytes());
      }

      // Test case: release iterator after custom CF close
      ColumnFamilyDescriptor cfd1 = new ColumnFamilyDescriptor("cf1".getBytes());
      ColumnFamilyHandle cfHandle1 = db.createColumnFamily(cfd1);
      db.put(cfHandle1, "key1".getBytes(), "value1".getBytes());

      try (final RocksIterator iterator = db.newIterator(cfHandle1)) {
        cfHandle1.close();

        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());
      }

      // Test case: release iterator after custom CF drop & close
      ColumnFamilyDescriptor cfd2 = new ColumnFamilyDescriptor("cf2".getBytes());
      ColumnFamilyHandle cfHandle2 = db.createColumnFamily(cfd2);
      db.put(cfHandle2, "key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator(cfHandle2)) {
        db.dropColumnFamily(cfHandle2);
        cfHandle2.close();

        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
      }
    }
  }
}
