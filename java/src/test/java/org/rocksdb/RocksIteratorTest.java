// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

        ByteBuffer key = ByteBuffer.allocateDirect(2);
        ByteBuffer value = ByteBuffer.allocateDirect(2);
        assertThat(iterator.key(key)).isEqualTo(4);
        assertThat(iterator.value(value)).isEqualTo(6);

        assertThat(key.position()).isEqualTo(0);
        assertThat(key.limit()).isEqualTo(2);
        assertThat(value.position()).isEqualTo(0);
        assertThat(value.limit()).isEqualTo(2);

        byte[] tmp = new byte[2];
        key.get(tmp);
        assertThat(tmp).isEqualTo("ke".getBytes());
        value.get(tmp);
        assertThat(tmp).isEqualTo("va".getBytes());

        key = ByteBuffer.allocateDirect(12);
        value = ByteBuffer.allocateDirect(12);
        assertThat(iterator.key(key)).isEqualTo(4);
        assertThat(iterator.value(value)).isEqualTo(6);
        assertThat(key.position()).isEqualTo(0);
        assertThat(key.limit()).isEqualTo(4);
        assertThat(value.position()).isEqualTo(0);
        assertThat(value.limit()).isEqualTo(6);

        tmp = new byte[4];
        key.get(tmp);
        assertThat(tmp).isEqualTo("key1".getBytes());
        tmp = new byte[6];
        value.get(tmp);
        assertThat(tmp).isEqualTo("value1".getBytes());

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

        key.clear();
        key.put("key1".getBytes());
        key.flip();
        iterator.seek(key);
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.value()).isEqualTo("value1".getBytes());
        assertThat(key.position()).isEqualTo(4);
        assertThat(key.limit()).isEqualTo(4);

        key.clear();
        key.put("key2".getBytes());
        key.flip();
        iterator.seekForPrev(key);
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
        assertThat(key.position()).isEqualTo(4);
        assertThat(key.limit()).isEqualTo(4);
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

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();

        byte[] lastKey;
        do {
          lastKey = iterator.key();
          iterator.next();
        } while (iterator.isValid());

        db.put("key3".getBytes(), "value3".getBytes());
        assertThat(iterator.isValid()).isFalse();
        iterator.refresh();
        iterator.seek(lastKey);
        assertThat(iterator.isValid()).isTrue();

        iterator.next();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key3".getBytes());
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
