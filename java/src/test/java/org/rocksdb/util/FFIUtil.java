// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;

public class FFIUtil {
  /**
   * Get a byte array from a string.
   *
   * Assumes UTF-8 encoding
   *
   * @param string the string
   *
   * @return the bytes.
   */
  public static byte[] ba(final String string) {
    return string.getBytes(UTF_8);
  }

  static final int MAX_LEN = 11; // value100000, which is in turn longer than key100000

  public static class ByteArray {
    private final byte[] array;
    private final String prefix;

    public ByteArray(final int size, final String prefix) {
      array = new byte[size];
      Arrays.fill(array, (byte) 0x30);
      this.prefix = prefix;
    }

    public final byte[] get(final int index) {
      final byte[] prefixI = ba(prefix + index);
      System.arraycopy(prefixI, 0, array, 0, prefixI.length);
      Arrays.fill(array, prefixI.length, MAX_LEN, (byte) 0x30);

      return array;
    }
  }

  public static void usingFFI(final TemporaryFolder dbFolder, final Function<FFIDB, Void> test)
      throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors =
        Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options =
             new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(
             options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList);
         final FFIDB FFIDB = new FFIDB(db, columnFamilyHandleList)) {
      test.apply(FFIDB);
    }
  }
}
