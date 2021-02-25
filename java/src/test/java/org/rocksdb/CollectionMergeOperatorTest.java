// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.CollectionMergeOperator.CollectionOperation.add;
import static org.rocksdb.CollectionMergeOperator.CollectionOperation.remove;
import static org.rocksdb.CollectionMergeOperator.UniqueConstraint.MAKE_UNIQUE;

public class CollectionMergeOperatorTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
          new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void set_add() throws RocksDBException {
    try (final MergeOperator collectionMergeOperator =
             new CollectionMergeOperator((short) 4, MAKE_UNIQUE);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setMergeOperator(collectionMergeOperator);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key1 = "key1".getBytes(UTF_8);

      db.merge(key1, add("1000"));
      db.merge(key1, add("2000"));
      db.merge(key1, add("1000"));
      db.merge(key1, add("3000"));
      db.merge(key1, add("1000"));

      final byte[] value = db.get(key1);
      assertThat(new String(value, UTF_8)).isEqualTo("100020003000");
    }
  }

  @Test
  public void set_add_str_sorted() throws RocksDBException {
    final int fixed_record_len = 4;
    try (final MergeOperator collectionMergeOperator =
             new CollectionMergeOperator((short) fixed_record_len,
                 BuiltinComparator.BYTEWISE_COMPARATOR, MAKE_UNIQUE);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setMergeOperator(collectionMergeOperator);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key1 = "key1".getBytes(UTF_8);

      db.merge(key1, add("3000"));
      db.merge(key1, add("1000"));
      db.merge(key1, add("5000"));
      db.merge(key1, add("2000"));
      db.merge(key1, add("7000"));
      db.merge(key1, add("6000"));

      final byte[] value = db.get(key1);
      assertThat(new String(value, UTF_8))
          .isEqualTo("100020003000500060007000");
    }
  }

  @Test
  public void set_add_uint_sorted() throws RocksDBException {
    final int fixed_record_len = 4;
    try (final MergeOperator collectionMergeOperator =
             new CollectionMergeOperator((short) fixed_record_len,
                 BuiltinComparator.BYTEWISE_COMPARATOR, MAKE_UNIQUE);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setMergeOperator(collectionMergeOperator);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key1 = "key1".getBytes(UTF_8);

      db.merge(key1, add(intToBytesBE(3000)));
      db.merge(key1, add(intToBytesBE(1000)));
      db.merge(key1, add(intToBytesBE(5000)));
      db.merge(key1, add(intToBytesBE(2000)));
      db.merge(key1, add(intToBytesBE(7000)));
      db.merge(key1, add(intToBytesBE(6000)));

      final byte[] value = db.get(key1);
      assertThat(value.length).isEqualTo(fixed_record_len * 6);
      int offset = 0;
      assertThat(intFromBytesBE(value, offset))
          .isEqualTo(1000);
      assertThat(intFromBytesBE(value, offset += fixed_record_len))
          .isEqualTo(2000);
      assertThat(intFromBytesBE(value, offset += fixed_record_len))
          .isEqualTo(3000);
      assertThat(intFromBytesBE(value, offset += fixed_record_len))
          .isEqualTo(5000);
      assertThat(intFromBytesBE(value, offset += fixed_record_len))
          .isEqualTo(6000);
      assertThat(intFromBytesBE(value, offset += fixed_record_len))
          .isEqualTo(7000);
    }
  }

  @Test
  public void vector_add_sorted() throws RocksDBException {
    try (final MergeOperator collectionMergeOperator =
             new CollectionMergeOperator((short) 4,
                 BuiltinComparator.BYTEWISE_COMPARATOR);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setMergeOperator(collectionMergeOperator);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key1 = "key1".getBytes(UTF_8);

      db.merge(key1, add("3000"));
      db.merge(key1, add("1000"));
      db.merge(key1, add("2000"));
      db.merge(key1, add("1000"));
      db.merge(key1, add("5000"));
      db.merge(key1, add("7000"));
      db.merge(key1, add("1000"));
      db.merge(key1, add("6000"));

      final byte[] value = db.get(key1);
      assertThat(new String(value, UTF_8))
          .isEqualTo("10001000100020003000500060007000");
    }
  }

  @Test
  public void vector_addAndRemove_singleRemoveOperand()
      throws RocksDBException {
    try (final MergeOperator collectionMergeOperator =
             new CollectionMergeOperator((short) 4);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setMergeOperator(collectionMergeOperator);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key1 = "key1".getBytes(UTF_8);

      db.merge(key1, add("1000"));
      db.merge(key1, add("2000"));
      db.merge(key1, remove("4000" + "1000"));
      db.merge(key1, add("3000"));
      db.merge(key1, add("6000"));
      db.merge(key1, add("7000"));
      db.merge(key1, remove("2000"));

      final byte[] value = db.get(key1);
      assertThat(new String(value, UTF_8)).isEqualTo("300060007000");
    }
  }

  @Test
  public void vector_addAndRemove_multiRemoveOperand() throws RocksDBException {
    try (final MergeOperator collectionMergeOperator =
             new CollectionMergeOperator((short) 4);
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setMergeOperator(collectionMergeOperator);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key1 = "key1".getBytes(UTF_8);

      db.merge(key1, add("1000"));
      db.merge(key1, add("2000"));
      db.merge(key1, remove("4000"));
      db.merge(key1, remove("1000"));
      db.merge(key1, add("3000"));
      db.merge(key1, add("6000"));
      db.merge(key1, add("7000"));
      db.merge(key1, remove("2000"));

      final byte[] value = db.get(key1);
      assertThat(new String(value, UTF_8)).isEqualTo("300060007000");
    }
  }

  private static byte[] intToBytesBE(final int i) {
    final byte bytes[] = new byte[4];
    bytes[3] = (byte)((i >> 0) & 0xff);
    bytes[2] = (byte)((i >> 8) & 0xff);
    bytes[1] = (byte)((i >> 16) & 0xff);
    bytes[0] = (byte)((i >> 24) & 0xff);
    return bytes;
  }

  private static byte[] intToBytesLE(final int i) {
    final byte bytes[] = new byte[4];
    bytes[0] = (byte)((i >> 0) & 0xff);
    bytes[1] = (byte)((i >> 8) & 0xff);
    bytes[2] = (byte)((i >> 16) & 0xff);
    bytes[3] = (byte)((i >> 24) & 0xff);
    return bytes;
  }

  private static int intFromBytesBE(final byte[] bytes, final int offset) {
    int i = bytes[offset+3] & 0xff;
    i |= (bytes[offset+2] & 0xff) << 8;
    i |= (bytes[offset+1] & 0xff) << 16;
    i |= (bytes[offset] & 0xff) << 24;
    return i;
  }

  private static int intFromBytesLE(final byte[] bytes, final int offset) {
    int i = bytes[offset] & 0xff;
    i |= (bytes[offset+1] & 0xff) << 8;
    i |= (bytes[offset+2] & 0xff) << 16;
    i |= (bytes[offset+3] & 0xff) << 24;
    return i;
  }
}
