// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.util.MergeEncodings.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MergeTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void stringOption()
      throws InterruptedException, RocksDBException {
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setMergeOperatorName("stringappend");
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // writing aa under key
      db.put("key".getBytes(), "aa".getBytes());
      // merge bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      final byte[] value = db.get("key".getBytes());
      final String strValue = new String(value);
      assertThat(strValue).isEqualTo("aa,bb");
    }
  }

  static byte[] longToByteArray(final long l) {
    final ByteBuffer buf =
        ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN);
    buf.putLong(l);
    return buf.array();
  }

  static long longFromByteArray(final byte[] a) {
    final ByteBuffer buf =
        ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(a);
    buf.position(
        Math.max(buf.position(), Long.SIZE / Byte.SIZE)); // guard against BufferOverflowException
    buf.flip();
    return buf.getLong();
  }

  @Test
  public void uint64AddOption()
      throws InterruptedException, RocksDBException {
    try (final Options opt = new Options()
        .setCreateIfMissing(true)
        .setMergeOperatorName("uint64add");
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // writing (long)100 under key
      db.put("key".getBytes(), longToByteArray(100));
      // merge (long)1 under key
      db.merge("key".getBytes(), longToByteArray(1));

      final byte[] value = db.get("key".getBytes());
      final long longValue = longFromByteArray(value);
      assertThat(longValue).isEqualTo(101);
    }
  }

  @Test
  public void cFStringOption()
      throws InterruptedException, RocksDBException {

    try (final ColumnFamilyOptions cfOpt1 = new ColumnFamilyOptions()
        .setMergeOperatorName("stringappend");
         final ColumnFamilyOptions cfOpt2 = new ColumnFamilyOptions()
             .setMergeOperatorName("stringappend")
    ) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt1),
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt2)
      );

      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions opt = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(opt,
               dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
               columnFamilyHandleList)) {
        try {
          // writing aa under key
          db.put(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), "aa".getBytes());
          // merge bb under key
          db.merge(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), "bb".getBytes());

          final byte[] value = db.get(columnFamilyHandleList.get(1), "cfkey".getBytes());
          final String strValue = new String(value);
          assertThat(strValue).isEqualTo("aa,bb");
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandleList) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void cFUInt64AddOption()
      throws InterruptedException, RocksDBException {

    try (final ColumnFamilyOptions cfOpt1 = new ColumnFamilyOptions()
        .setMergeOperatorName("uint64add");
         final ColumnFamilyOptions cfOpt2 = new ColumnFamilyOptions()
             .setMergeOperatorName("uint64add")
    ) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt1),
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt2)
      );

      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions opt = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(opt,
               dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
               columnFamilyHandleList)) {
        try {
          // writing (long)100 under key
          db.put(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), longToByteArray(100));
          // merge (long)157 under key
          db.merge(columnFamilyHandleList.get(1), "cfkey".getBytes(), longToByteArray(157));

          final byte[] value = db.get(columnFamilyHandleList.get(1), "cfkey".getBytes());
          final long longValue = longFromByteArray(value);
          assertThat(longValue).isEqualTo(257);
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandleList) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void operatorOption()
      throws InterruptedException, RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options opt = new Options()
            .setCreateIfMissing(true)
            .setMergeOperator(stringAppendOperator);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // Writing aa under key
      db.put("key".getBytes(), "aa".getBytes());

      // Writing bb under key
      db.merge("key".getBytes(), "bb".getBytes());

      final byte[] value = db.get("key".getBytes());
      final String strValue = new String(value);

      assertThat(strValue).isEqualTo("aa,bb");
    }
  }

  @Test
  public void uint64AddOperatorOption()
      throws InterruptedException, RocksDBException {
    try (final UInt64AddOperator uint64AddOperator = new UInt64AddOperator();
         final Options opt = new Options()
            .setCreateIfMissing(true)
            .setMergeOperator(uint64AddOperator);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // Writing (long)100 under key
      db.put("key".getBytes(), longToByteArray(100));

      // Writing (long)1 under key
      db.merge("key".getBytes(), longToByteArray(1));

      final byte[] value = db.get("key".getBytes());
      final long longValue = longFromByteArray(value);

      assertThat(longValue).isEqualTo(101);
    }
  }

  @Test
  public void cFOperatorOption()
      throws InterruptedException, RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final ColumnFamilyOptions cfOpt1 = new ColumnFamilyOptions()
             .setMergeOperator(stringAppendOperator);
         final ColumnFamilyOptions cfOpt2 = new ColumnFamilyOptions()
             .setMergeOperator(stringAppendOperator)
    ) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt1),
          new ColumnFamilyDescriptor("new_cf".getBytes(), cfOpt2)
      );
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions opt = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(opt,
               dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
               columnFamilyHandleList)
      ) {
        try {
          // writing aa under key
          db.put(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), "aa".getBytes());
          // merge bb under key
          db.merge(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), "bb".getBytes());
          byte[] value = db.get(columnFamilyHandleList.get(1),
              "cfkey".getBytes());
          final String strValue = new String(value);

          // Test also with createColumnFamily
          try (final ColumnFamilyOptions cfHandleOpts =
                   new ColumnFamilyOptions()
                       .setMergeOperator(stringAppendOperator);
               final ColumnFamilyHandle cfHandle =
                   db.createColumnFamily(
                       new ColumnFamilyDescriptor("new_cf2".getBytes(),
                           cfHandleOpts))
          ) {
            // writing xx under cfkey2
            db.put(cfHandle, "cfkey2".getBytes(), "xx".getBytes());
            // merge yy under cfkey2
            db.merge(cfHandle, new WriteOptions(), "cfkey2".getBytes(),
                "yy".getBytes());
            value = db.get(cfHandle, "cfkey2".getBytes());
            final String strValueTmpCf = new String(value);

            assertThat(strValue).isEqualTo("aa,bb");
            assertThat(strValueTmpCf).isEqualTo("xx,yy");
          }
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              columnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void cFUInt64AddOperatorOption()
      throws InterruptedException, RocksDBException {
    try (final UInt64AddOperator uint64AddOperator = new UInt64AddOperator();
         final ColumnFamilyOptions cfOpt1 = new ColumnFamilyOptions()
             .setMergeOperator(uint64AddOperator);
         final ColumnFamilyOptions cfOpt2 = new ColumnFamilyOptions()
             .setMergeOperator(uint64AddOperator)
    ) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt1),
          new ColumnFamilyDescriptor("new_cf".getBytes(), cfOpt2)
      );
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions opt = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(opt,
               dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
               columnFamilyHandleList)
      ) {
        try {
          // writing (long)100 under key
          db.put(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), longToByteArray(100));
          // merge (long)1 under key
          db.merge(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), longToByteArray(1));
          byte[] value = db.get(columnFamilyHandleList.get(1),
              "cfkey".getBytes());
          final long longValue = longFromByteArray(value);

          // Test also with createColumnFamily
          try (final ColumnFamilyOptions cfHandleOpts =
                   new ColumnFamilyOptions()
                       .setMergeOperator(uint64AddOperator);
               final ColumnFamilyHandle cfHandle =
                   db.createColumnFamily(
                       new ColumnFamilyDescriptor("new_cf2".getBytes(),
                           cfHandleOpts))
          ) {
            // writing (long)200 under cfkey2
            db.put(cfHandle, "cfkey2".getBytes(), longToByteArray(200));
            // merge (long)50 under cfkey2
            db.merge(cfHandle, new WriteOptions(), "cfkey2".getBytes(),
                longToByteArray(50));
            value = db.get(cfHandle, "cfkey2".getBytes());
            final long longValueTmpCf = longFromByteArray(value);

            assertThat(longValue).isEqualTo(101);
            assertThat(longValueTmpCf).isEqualTo(250);
          }
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void cFInt64AddOperatorOption() throws InterruptedException, RocksDBException {
    try (final Int64AddOperator int64AddOperator = new Int64AddOperator();
         final ColumnFamilyOptions cfOpt1 =
             new ColumnFamilyOptions().setMergeOperator(int64AddOperator);
         final ColumnFamilyOptions cfOpt2 =
             new ColumnFamilyOptions().setMergeOperator(int64AddOperator)) {
      final List<ColumnFamilyDescriptor> cfDescriptors =
          Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt1),
              new ColumnFamilyDescriptor("new_cf".getBytes(), cfOpt2));
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions opt =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(
               opt, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList)) {
        try {
          // writing (long)100 under key
          db.put(columnFamilyHandleList.get(1), "cfkey".getBytes(), encodeSigned(50));
          // merge (long)1 under key
          db.merge(columnFamilyHandleList.get(1), "cfkey".getBytes(), encodeSigned(1));
          byte[] value = db.get(columnFamilyHandleList.get(1), "cfkey".getBytes());
          assertThat(decodeSigned(value)).isEqualTo(51);

          // Test also with createColumnFamily
          try (final ColumnFamilyOptions cfHandleOpts =
                   new ColumnFamilyOptions().setMergeOperator(int64AddOperator);
               final ColumnFamilyHandle cfHandle = db.createColumnFamily(
                   new ColumnFamilyDescriptor("new_cf2".getBytes(), cfHandleOpts))) {
            // writing (long)200 under cfkey2
            db.put(cfHandle, "cfkey2".getBytes(), encodeSigned(200));
            // merge (long)50 under cfkey2
            db.merge(cfHandle, new WriteOptions(), "cfkey2".getBytes(), encodeSigned(50));
            value = db.get(cfHandle, "cfkey2".getBytes());

            assertThat(decodeSigned(value)).isEqualTo(250);

            // writing negative value(s) under cfkey3
            db.put(cfHandle, "cfkey3".getBytes(), encodeSigned(-40));
            // merge (long)3 under cfkey3
            db.merge(cfHandle, new WriteOptions(), "cfkey3".getBytes(), encodeSigned(3));
            value = db.get(cfHandle, "cfkey3".getBytes());

            assertThat(decodeSigned(value)).isEqualTo(-37);

            // writing negative value(s) under cfkey4
            db.put(cfHandle, "cfkey4".getBytes(), encodeSigned(-(long) Integer.MIN_VALUE));
            // merge (long)50 under cfkey4
            db.merge(
                cfHandle, new WriteOptions(), "cfkey4".getBytes(), encodeSigned(-Short.MIN_VALUE));
            value = db.get(cfHandle, "cfkey4".getBytes());

            assertThat(decodeSigned(value)).isEqualTo(-(long) Integer.MIN_VALUE - Short.MIN_VALUE);
          }
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              columnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void operatorGcBehaviour()
      throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator()) {
      try (final Options opt = new Options()
              .setCreateIfMissing(true)
              .setMergeOperator(stringAppendOperator);
           final RocksDB db = RocksDB.open(opt,
                   dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }

      // test reuse
      try (final Options opt = new Options()
              .setMergeOperator(stringAppendOperator);
           final RocksDB db = RocksDB.open(opt,
                   dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }

      // test param init
      try (final StringAppendOperator stringAppendOperator2 = new StringAppendOperator();
           final Options opt = new Options()
              .setMergeOperator(stringAppendOperator2);
           final RocksDB db = RocksDB.open(opt,
                   dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }

      // test replace one with another merge operator instance
      try (final Options opt = new Options()
              .setMergeOperator(stringAppendOperator);
           final StringAppendOperator newStringAppendOperator = new StringAppendOperator()) {
        opt.setMergeOperator(newStringAppendOperator);
        try (final RocksDB db = RocksDB.open(opt,
                dbFolder.getRoot().getAbsolutePath())) {
          //no-op
        }
      }
    }
  }

  @Test
  public void uint64AddOperatorGcBehaviour()
      throws RocksDBException {
    try (final UInt64AddOperator uint64AddOperator = new UInt64AddOperator()) {
      try (final Options opt = new Options()
              .setCreateIfMissing(true)
              .setMergeOperator(uint64AddOperator);
           final RocksDB db = RocksDB.open(opt,
                   dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }

      // test reuse
      try (final Options opt = new Options()
              .setMergeOperator(uint64AddOperator);
           final RocksDB db = RocksDB.open(opt,
                   dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }

      // test param init
      try (final UInt64AddOperator uint64AddOperator2 = new UInt64AddOperator();
           final Options opt = new Options()
              .setMergeOperator(uint64AddOperator2);
           final RocksDB db = RocksDB.open(opt,
                   dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }

      // test replace one with another merge operator instance
      try (final Options opt = new Options()
              .setMergeOperator(uint64AddOperator);
           final UInt64AddOperator newUInt64AddOperator = new UInt64AddOperator()) {
        opt.setMergeOperator(newUInt64AddOperator);
        try (final RocksDB db = RocksDB.open(opt,
                dbFolder.getRoot().getAbsolutePath())) {
          //no-op
        }
      }
    }
  }

  @Test
  public void emptyStringAsStringAppendDelimiter() throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator("");
         final Options opt =
             new Options().setCreateIfMissing(true).setMergeOperator(stringAppendOperator);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "aa".getBytes());
      db.merge("key".getBytes(), "bb".getBytes());
      final byte[] value = db.get("key".getBytes());
      assertThat(new String(value)).isEqualTo("aabb");
    }
  }

  @Test
  public void multiCharStringAsStringAppendDelimiter() throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator("<>");
         final Options opt =
             new Options().setCreateIfMissing(true).setMergeOperator(stringAppendOperator);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "aa".getBytes());
      db.merge("key".getBytes(), "bb".getBytes());
      final byte[] value = db.get("key".getBytes());
      assertThat(new String(value)).isEqualTo("aa<>bb");
    }
  }

  @Test
  public void emptyStringInSetMergeOperatorByName() {
    try (final Options opt = new Options()
        .setMergeOperatorName("");
         final ColumnFamilyOptions cOpt = new ColumnFamilyOptions()
             .setMergeOperatorName("")) {
      //no-op
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullStringInSetMergeOperatorByNameOptions() {
    try (final Options opt = new Options()) {
      opt.setMergeOperatorName(null);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void
  nullStringInSetMergeOperatorByNameColumnFamilyOptions() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      opt.setMergeOperatorName(null);
    }
  }
}
