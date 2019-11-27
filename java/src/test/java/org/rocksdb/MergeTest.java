// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class MergeTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

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

  private byte[] longToByteArray(long l) {
    ByteBuffer buf = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
    buf.putLong(l);
    return buf.array();
  }

  private long longFromByteArray(byte[] a) {
    ByteBuffer buf = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
    buf.put(a);
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

          byte[] value = db.get(columnFamilyHandleList.get(1),
              "cfkey".getBytes());
          String strValue = new String(value);
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
          // merge (long)1 under key
          db.merge(columnFamilyHandleList.get(1),
              "cfkey".getBytes(), longToByteArray(1));

          byte[] value = db.get(columnFamilyHandleList.get(1),
              "cfkey".getBytes());
          long longValue = longFromByteArray(value);
          assertThat(longValue).isEqualTo(101);
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
          String strValue = new String(value);

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
            String strValueTmpCf = new String(value);

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
          long longValue = longFromByteArray(value);

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
            long longValueTmpCf = longFromByteArray(value);

            assertThat(longValue).isEqualTo(101);
            assertThat(longValueTmpCf).isEqualTo(250);
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

  private class StringAppendMergeOperatorTest extends AbstractMergeOperator {

    public StringAppendMergeOperatorTest() throws RocksDBException {
      super(true, false, false);
    }

    @Override
    public String name() {
      return "StringAppendMergeOperatorTest";
    }

    private String collect(byte[][] operands) {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < operands.length; i++) {
        if (i > 0)
          sb.append(',');
        sb.append(new String(operands[i]));
      }
      return sb.toString();
    }

    @Override
    public byte[] fullMerge(byte[] key, byte[] oldvalue, byte[][] operands, ReturnType rt) throws RocksDBException {
      String collected = collect(operands);
      if (oldvalue == null) {
        return collected.getBytes();
      }
      return (new String(oldvalue) + ',' + collected).getBytes();
    }

    @Override
    public byte[] partialMultiMerge(byte[] key,  byte[][] operands, ReturnType rt) {
      return collect(operands).getBytes();
    }

    @Override
    public byte[] partialMerge(byte[] key, byte[] left, byte[] right, ReturnType rt) {
      StringBuffer sb = new StringBuffer(new String(left));
      sb.append(',');
      sb.append(new String(right));

      return sb.toString().getBytes();
    }

    @Override
    public boolean shouldMerge(byte[][] operands) {
      return true;
    }
  }

  @Test
  public void mergeWithAbstractOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
    try {
      try (final StringAppendMergeOperatorTest stringAppendOperator = new StringAppendMergeOperatorTest();
           final Options opt = new Options()
             .setCreateIfMissing(true)
             .setMergeOperator(stringAppendOperator);
           final WriteOptions wOpt = new WriteOptions();
           final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())
      ) {
        db.put("key1".getBytes(), "value".getBytes());
        assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());

        // merge key1 with another value portion
        db.merge("key1".getBytes(), "value2".getBytes());
        assertThat(db.get("key1".getBytes())).isEqualTo("value,value2".getBytes());

        // merge key1 with another value portion
        db.merge(wOpt, "key1".getBytes(), "value3".getBytes());
        assertThat(db.get("key1".getBytes())).isEqualTo("value,value2,value3".getBytes());
        db.merge(wOpt, "key1".getBytes(), "value4".getBytes());
        assertThat(db.get("key1".getBytes())).isEqualTo("value,value2,value3,value4".getBytes());

        // merge on non existent key shall insert the value
        db.merge(wOpt, "key2".getBytes(), "xxxx".getBytes());
        assertThat(db.get("key2".getBytes())).isEqualTo("xxxx".getBytes());
      }
    } catch (Exception e){
      throw new RuntimeException(e);
    } finally {
    }
  }
}
