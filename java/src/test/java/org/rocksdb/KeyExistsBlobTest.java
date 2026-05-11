//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that {@link RocksDB#keyExists} works correctly with BlobDB enabled.
 * keyExists confirms the key is present in the database (memtable or SST files)
 * without reading blob file values. The JNI implementation calls DB::KeyExists,
 * which internally uses GetImpl with a non-null is_blob_index pointer to skip
 * blob resolution.
 */
public class KeyExistsBlobTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  /** Value large enough to be stored in a blob file (exceeds minBlobSize). */
  private static byte[] largeValue(final int size) {
    final byte[] value = new byte[size];
    for (int i = 0; i < size; i++) {
      value[i] = (byte) (i % 256);
    }
    return value;
  }

  /**
   * Basic correctness: keyExists returns true for existing keys and false for non-existing keys
   * when BlobDB is enabled. This validates that the GetImpl-based implementation correctly
   * handles blob indexes.
   */
  @Test
  public void keyExistsWithBlobDb() throws RocksDBException {
    try (final Options options = new Options()
            .setCreateIfMissing(true)
            .setEnableBlobFiles(true)
            .setMinBlobSize(100)
            .setEnableBlobGarbageCollection(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {

      // Small value (stays in SST, not blob)
      final byte[] smallKey = "small_key".getBytes(UTF_8);
      db.put(smallKey, "small_value".getBytes(UTF_8));

      // Large value (goes to blob file)
      final byte[] largeKey = "large_key".getBytes(UTF_8);
      db.put(largeKey, largeValue(1024));

      // Flush to ensure data is in SST/blob files (not just memtable)
      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }

      // Both should exist
      assertThat(db.keyExists(smallKey)).isTrue();
      assertThat(db.keyExists(largeKey)).isTrue();

      // Non-existing key should not exist
      assertThat(db.keyExists("nonexistent".getBytes(UTF_8))).isFalse();

      // Delete the large key and verify
      db.delete(largeKey);
      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }
      try (final CompactRangeOptions compactOpts = new CompactRangeOptions()) {
        db.compactRange(null, null, compactOpts);
      }
      assertThat(db.keyExists(largeKey)).isFalse();
    }
  }

  /**
   * Verifies keyExists with ReadOptions works correctly with BlobDB.
   */
  @Test
  public void keyExistsWithBlobDbReadOptions() throws RocksDBException {
    try (final Options options = new Options()
            .setCreateIfMissing(true)
            .setEnableBlobFiles(true)
            .setMinBlobSize(100);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
         final ReadOptions readOptions = new ReadOptions()) {

      final byte[] key = "ro_key".getBytes(UTF_8);
      db.put(key, largeValue(512));

      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }

      assertThat(db.keyExists(readOptions, key)).isTrue();
      assertThat(db.keyExists(readOptions, "nonexistent".getBytes(UTF_8))).isFalse();
    }
  }

  /**
   * Verifies keyExists returns true for a key still in the memtable (before flush).
   */
  @Test
  public void keyExistsInMemtableWithBlobDb() throws RocksDBException {
    try (final Options options = new Options()
            .setCreateIfMissing(true)
            .setEnableBlobFiles(true)
            .setMinBlobSize(100);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key = "memtable_key".getBytes(UTF_8);
      db.put(key, largeValue(512));

      // Key should exist in memtable before any flush
      assertThat(db.keyExists(key)).isTrue();
      assertThat(db.keyExists("nonexistent".getBytes(UTF_8))).isFalse();
    }
  }

  /**
   * Verifies that keyExists with BlobDB does NOT read blob files.
   * Uses PerfContext to measure blob read I/O — keyExists should show zero blob reads
   * while get() should show non-zero blob reads for the same key.
   */
  @Test
  public void keyExistsSkipsBlobRead() throws RocksDBException {
    try (final Options options = new Options()
            .setCreateIfMissing(true)
            .setEnableBlobFiles(true)
            .setMinBlobSize(100)
            .setEnableBlobGarbageCollection(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key = "blob_key".getBytes(UTF_8);
      db.put(key, largeValue(2048));

      // Flush to force data into SST + blob files
      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }

      // Enable perf context to track I/O
      db.setPerfLevel(PerfLevel.ENABLE_COUNT);
      final PerfContext perfCtx = db.getPerfContext();

      try {
        // --- keyExists should NOT read blob files ---
        perfCtx.reset();
        assertThat(db.keyExists(key)).isTrue();

        assertThat(perfCtx.getBlobReadCount())
            .as("keyExists should not read blob files")
            .isEqualTo(0);
        assertThat(perfCtx.getBlobReadByte())
            .as("keyExists should not read blob bytes")
            .isEqualTo(0);

        // --- get() SHOULD read blob files (control) ---
        perfCtx.reset();
        final byte[] value = db.get(key);
        assertThat(value).isNotNull();
        assertThat(value.length).isEqualTo(2048);

        assertThat(perfCtx.getBlobReadCount())
            .as("get() should read blob files")
            .isGreaterThan(0);
        assertThat(perfCtx.getBlobReadByte())
            .as("get() should read blob bytes")
            .isGreaterThan(0);
      } finally {
        db.setPerfLevel(PerfLevel.DISABLE);
      }
    }
  }

  /**
   * Verifies keyExists correctness with BlobDB across column families.
   */
  @Test
  public void keyExistsWithBlobDbColumnFamily() throws RocksDBException {
    try (final Options options = new Options()
            .setCreateIfMissing(true)
            .setEnableBlobFiles(true)
            .setMinBlobSize(100);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {

      try (final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
              .setEnableBlobFiles(true)
              .setMinBlobSize(100);
           final ColumnFamilyHandle cf2 = db.createColumnFamily(
              new ColumnFamilyDescriptor("cf2".getBytes(UTF_8), cfOptions))) {

        final byte[] key1 = "key_cf1".getBytes(UTF_8);
        final byte[] key2 = "key_cf2".getBytes(UTF_8);

        db.put(key1, largeValue(512));
        db.put(cf2, key2, largeValue(512));

        try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
          db.flush(flushOptions);
          db.flush(flushOptions, cf2);
        }

        assertThat(db.keyExists(key1)).isTrue();
        assertThat(db.keyExists(cf2, key1)).isFalse();

        assertThat(db.keyExists(key2)).isFalse();
        assertThat(db.keyExists(cf2, key2)).isTrue();
      }
    }
  }

  /**
   * Verifies keyExists with DirectByteBuffer works with BlobDB.
   */
  @Test
  public void keyExistsDirectByteBufferWithBlobDb() throws RocksDBException {
    try (final Options options = new Options()
            .setCreateIfMissing(true)
            .setEnableBlobFiles(true)
            .setMinBlobSize(100);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key = "direct_blob_key".getBytes(UTF_8);
      db.put(key, largeValue(1024));
      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }

      final ByteBuffer keyBuf = ByteBuffer.allocateDirect(key.length);
      keyBuf.put(key);
      keyBuf.flip();
      assertThat(db.keyExists(keyBuf)).isTrue();

      final ByteBuffer missingBuf = ByteBuffer.allocateDirect(10);
      missingBuf.put("missing_xx".getBytes(UTF_8));
      missingBuf.flip();
      assertThat(db.keyExists(missingBuf)).isFalse();
    }
  }
}
