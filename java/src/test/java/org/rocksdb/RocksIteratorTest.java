// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

  private void validateByteBufferResult(
      final int fetched, final ByteBuffer byteBuffer, final String expected) {
    assertThat(fetched).isEqualTo(expected.length());
    assertThat(byteBuffer.position()).isEqualTo(0);
    assertThat(byteBuffer.limit()).isEqualTo(Math.min(byteBuffer.remaining(), expected.length()));
    final int bufferSpace = byteBuffer.remaining();
    final byte[] contents = new byte[bufferSpace];
    byteBuffer.get(contents, 0, bufferSpace);
    assertThat(contents).isEqualTo(
        expected.substring(0, bufferSpace).getBytes(StandardCharsets.UTF_8));
  }

  private void validateKey(
      final RocksIterator iterator, final ByteBuffer byteBuffer, final String key) {
    validateByteBufferResult(iterator.key(byteBuffer), byteBuffer, key);
  }

  private void validateValue(
      final RocksIterator iterator, final ByteBuffer byteBuffer, final String value) {
    validateByteBufferResult(iterator.value(byteBuffer), byteBuffer, value);
  }

  @Test
  public void rocksIterator() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        validateKey(iterator, ByteBuffer.allocateDirect(2), "key1");
        validateKey(iterator, ByteBuffer.allocateDirect(2), "key0");
        validateKey(iterator, ByteBuffer.allocateDirect(4), "key1");
        validateKey(iterator, ByteBuffer.allocateDirect(5), "key1");
        validateValue(iterator, ByteBuffer.allocateDirect(2), "value2");
        validateValue(iterator, ByteBuffer.allocateDirect(2), "vasicu");
        validateValue(iterator, ByteBuffer.allocateDirect(8), "value1");

        validateKey(iterator, ByteBuffer.allocate(2), "key1");
        validateKey(iterator, ByteBuffer.allocate(2), "key0");
        validateKey(iterator, ByteBuffer.allocate(4), "key1");
        validateKey(iterator, ByteBuffer.allocate(5), "key1");
        validateValue(iterator, ByteBuffer.allocate(2), "value1");
        validateValue(iterator, ByteBuffer.allocate(8), "value1");

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

        {
          final ByteBuffer key = ByteBuffer.allocate(12);
          key.put("key1".getBytes()).flip();
          iterator.seek(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value1".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);

          validateValue(iterator, ByteBuffer.allocateDirect(12), "value1");
          validateValue(iterator, ByteBuffer.allocateDirect(4), "valu56");
        }

        {
          final ByteBuffer key = ByteBuffer.allocate(12);
          key.put("key2".getBytes()).flip();
          iterator.seekForPrev(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value2".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }

        {
          final ByteBuffer key = ByteBuffer.allocate(12);
          key.put("key1".getBytes()).flip();
          iterator.seek(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value1".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }

        {
          // Check offsets of slice byte buffers
          final ByteBuffer key0 = ByteBuffer.allocate(24);
          key0.put("key2key2".getBytes());
          final ByteBuffer key = key0.slice();
          key.put("key1".getBytes()).flip();
          iterator.seek(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value1".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }

        {
          // Check offsets of slice byte buffers
          final ByteBuffer key0 = ByteBuffer.allocateDirect(24);
          key0.put("key2key2".getBytes());
          final ByteBuffer key = key0.slice();
          key.put("key1".getBytes()).flip();
          iterator.seek(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value1".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }

        {
          final ByteBuffer key = ByteBuffer.allocate(12);
          key.put("key2".getBytes()).flip();
          iterator.seekForPrev(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value2".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }
      }
    }
  }

  @Test
  public void rocksIteratorSeekAndInsert() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

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
      final ColumnFamilyDescriptor cfd1 = new ColumnFamilyDescriptor("cf1".getBytes());
      final ColumnFamilyHandle cfHandle1 = db.createColumnFamily(cfd1);
      db.put(cfHandle1, "key1".getBytes(), "value1".getBytes());

      try (final RocksIterator iterator = db.newIterator(cfHandle1)) {
        cfHandle1.close();

        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());
      }

      // Test case: release iterator after custom CF drop & close
      final ColumnFamilyDescriptor cfd2 = new ColumnFamilyDescriptor("cf2".getBytes());
      final ColumnFamilyHandle cfHandle2 = db.createColumnFamily(cfd2);
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
