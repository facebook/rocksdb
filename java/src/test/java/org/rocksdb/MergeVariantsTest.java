// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.MergeTest.longFromByteArray;
import static org.rocksdb.MergeTest.longToByteArray;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MergeVariantsTest {
  @FunctionalInterface
  interface FunctionMerge<PDatabase, PLeft, PRight> {
    public void apply(PDatabase db, PLeft two, PRight three) throws RocksDBException;
  }

  @Parameterized.Parameters
  public static List<MergeVariantsTest.FunctionMerge<RocksDB, byte[], byte[]>> data() {
    return Arrays.asList(RocksDB::merge,
        (db, left, right)
            -> db.merge(new WriteOptions(), left, right),
        (db, left, right)
            -> {
          final byte[] left0 =
              ("1234567" + new String(left, StandardCharsets.UTF_8) + "890").getBytes();
          final byte[] right0 =
              ("1234" + new String(right, StandardCharsets.UTF_8) + "567890ab").getBytes();
          db.merge(left0, 7, left.length, right0, 4, right.length);
        },
        (db, left, right)
            -> {
          final byte[] left0 =
              ("1234567" + new String(left, StandardCharsets.UTF_8) + "890").getBytes();
          final byte[] right0 =
              ("1234" + new String(right, StandardCharsets.UTF_8) + "567890ab").getBytes();
          db.merge(new WriteOptions(), left0, 7, left.length, right0, 4, right.length);
        },
        (db, left, right)
            -> {
          final ByteBuffer bbLeft = ByteBuffer.allocateDirect(100);
          final ByteBuffer bbRight = ByteBuffer.allocateDirect(100);
          bbLeft.put(left).flip();
          bbRight.put(right).flip();
          db.merge(new WriteOptions(), bbLeft, bbRight);
        },
        (db, left, right) -> {
          final ByteBuffer bbLeft = ByteBuffer.allocate(100);
          final ByteBuffer bbRight = ByteBuffer.allocate(100);
          bbLeft.put(left).flip();
          bbRight.put(right).flip();
          db.merge(new WriteOptions(), bbLeft, bbRight);
        });
  }

  @Parameterized.Parameter
  public MergeVariantsTest.FunctionMerge<RocksDB, byte[], byte[]> mergeFunction;

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void uint64AddOperatorOption() throws InterruptedException, RocksDBException {
    try (final UInt64AddOperator uint64AddOperator = new UInt64AddOperator();
         final Options opt =
             new Options().setCreateIfMissing(true).setMergeOperator(uint64AddOperator);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      // Writing (long)100 under key
      db.put("key".getBytes(), longToByteArray(100));

      // Writing (long)1 under key
      mergeFunction.apply(db, "key".getBytes(), longToByteArray(1));

      final byte[] value = db.get("key".getBytes());
      final long longValue = longFromByteArray(value);

      assertThat(longValue).isEqualTo(101);
    }
  }
}
