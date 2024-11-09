// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class RocksDBTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void open() throws RocksDBException {
    try (final RocksDB db =
             RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
    }
  }

  @Test
  public void open_opt() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      assertThat(db).isNotNull();
    }
  }

  @Test
  public void openWhenOpen() throws RocksDBException {
    final String dbPath = dbFolder.getRoot().getAbsolutePath();

    try (final RocksDB ignored = RocksDB.open(dbPath)) {
      try (final RocksDB ignored1 = RocksDB.open(dbPath)) {
        fail("Should have thrown an exception when opening the same db twice");
      } catch (final RocksDBException e) {
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.IOError);
        assertThat(e.getStatus().getSubCode()).isEqualTo(Status.SubCode.None);
        assertThat(e.getStatus().getState()).contains("lock ");
      }
    }
  }

  @Test
  public void createColumnFamily() throws RocksDBException {
      final byte[] col1Name = "col1".getBytes(UTF_8);

      try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
           final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
      ) {
        try (final ColumnFamilyHandle col1 =
            db.createColumnFamily(new ColumnFamilyDescriptor(col1Name, cfOpts))) {
          assertThat(col1).isNotNull();
          assertThat(col1.getName()).isEqualTo(col1Name);
        }
      }

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      try (final RocksDB ignored = RocksDB.open(dbFolder.getRoot().getAbsolutePath(),
               Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
                   new ColumnFamilyDescriptor(col1Name)),
               cfHandles)) {
        try {
          assertThat(cfHandles.size()).isEqualTo(2);
          assertThat(cfHandles.get(1)).isNotNull();
          assertThat(cfHandles.get(1).getName()).isEqualTo(col1Name);
        } finally {
          for (final ColumnFamilyHandle cfHandle :
              cfHandles) {
            cfHandle.close();
          }
        }
      }
  }


  @Test
  public void createColumnFamilies() throws RocksDBException {
    final byte[] col1Name = "col1".getBytes(UTF_8);
    final byte[] col2Name = "col2".getBytes(UTF_8);

    List<ColumnFamilyHandle> cfHandles;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
    ) {
      cfHandles =
          db.createColumnFamilies(cfOpts, Arrays.asList(col1Name, col2Name));
      try {
        assertThat(cfHandles).isNotNull();
        assertThat(cfHandles.size()).isEqualTo(2);
        assertThat(cfHandles.get(0).getName()).isEqualTo(col1Name);
        assertThat(cfHandles.get(1).getName()).isEqualTo(col2Name);
      } finally {
        for (final ColumnFamilyHandle cfHandle : cfHandles) {
          cfHandle.close();
        }
      }
    }

    cfHandles = new ArrayList<>();
    try (final RocksDB ignored = RocksDB.open(dbFolder.getRoot().getAbsolutePath(),
             Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
                 new ColumnFamilyDescriptor(col1Name), new ColumnFamilyDescriptor(col2Name)),
             cfHandles)) {
      try {
        assertThat(cfHandles.size()).isEqualTo(3);
        assertThat(cfHandles.get(1)).isNotNull();
        assertThat(cfHandles.get(1).getName()).isEqualTo(col1Name);
        assertThat(cfHandles.get(2)).isNotNull();
        assertThat(cfHandles.get(2).getName()).isEqualTo(col2Name);
      } finally {
        for (final ColumnFamilyHandle cfHandle : cfHandles) {
          cfHandle.close();
        }
      }
    }
  }

  @Test
  public void createColumnFamiliesfromDescriptors() throws RocksDBException {
    final byte[] col1Name = "col1".getBytes(UTF_8);
    final byte[] col2Name = "col2".getBytes(UTF_8);

    List<ColumnFamilyHandle> cfHandles;
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
    ) {
      cfHandles =
          db.createColumnFamilies(Arrays.asList(
              new ColumnFamilyDescriptor(col1Name, cfOpts),
              new ColumnFamilyDescriptor(col2Name, cfOpts)));
      try {
        assertThat(cfHandles).isNotNull();
        assertThat(cfHandles.size()).isEqualTo(2);
        assertThat(cfHandles.get(0).getName()).isEqualTo(col1Name);
        assertThat(cfHandles.get(1).getName()).isEqualTo(col2Name);
      } finally {
        for (final ColumnFamilyHandle cfHandle : cfHandles) {
          cfHandle.close();
        }
      }
    }

    cfHandles = new ArrayList<>();
    try (final RocksDB ignored = RocksDB.open(dbFolder.getRoot().getAbsolutePath(),
             Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
                 new ColumnFamilyDescriptor(col1Name), new ColumnFamilyDescriptor(col2Name)),
             cfHandles)) {
      try {
        assertThat(cfHandles.size()).isEqualTo(3);
        assertThat(cfHandles.get(1)).isNotNull();
        assertThat(cfHandles.get(1).getName()).isEqualTo(col1Name);
        assertThat(cfHandles.get(2)).isNotNull();
        assertThat(cfHandles.get(2).getName()).isEqualTo(col2Name);
      } finally {
        for (final ColumnFamilyHandle cfHandle : cfHandles) {
          cfHandle.close();
        }
      }
    }
  }

  @Test
  public void put() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions opt = new WriteOptions(); final ReadOptions optr = new ReadOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put(opt, "key2".getBytes(), "12345678".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo("12345678".getBytes());

      final ByteBuffer key = ByteBuffer.allocateDirect(12);
      final ByteBuffer value = ByteBuffer.allocateDirect(12);
      key.position(4);
      key.put("key3".getBytes());
      key.position(4).limit(8);
      value.position(4);
      value.put("val3".getBytes());
      value.position(4).limit(8);

      db.put(opt, key, value);

      assertThat(key.position()).isEqualTo(8);
      assertThat(key.limit()).isEqualTo(8);

      assertThat(value.position()).isEqualTo(8);
      assertThat(value.limit()).isEqualTo(8);

      key.position(4);

      final ByteBuffer result = ByteBuffer.allocateDirect(12);
      assertThat(db.get(optr, key, result)).isEqualTo(4);
      assertThat(result.position()).isEqualTo(0);
      assertThat(result.limit()).isEqualTo(4);
      assertThat(key.position()).isEqualTo(8);
      assertThat(key.limit()).isEqualTo(8);

      final byte[] tmp = new byte[4];
      result.get(tmp);
      assertThat(tmp).isEqualTo("val3".getBytes());

      key.position(4);

      final ByteBuffer result2 = ByteBuffer.allocateDirect(12);
      result2.put("abcdefghijkl".getBytes());
      result2.flip().position(3);
      assertThat(db.get(optr, key, result2)).isEqualTo(4);
      assertThat(result2.position()).isEqualTo(3);
      assertThat(result2.limit()).isEqualTo(7);
      assertThat(key.position()).isEqualTo(8);
      assertThat(key.limit()).isEqualTo(8);

      final byte[] tmp2 = new byte[12];
      result2.position(0).limit(12);
      result2.get(tmp2);
      assertThat(tmp2).isEqualTo("abcval3hijkl".getBytes());

      key.position(4);

      result.clear().position(9);
      assertThat(db.get(optr, key, result)).isEqualTo(4);
      assertThat(result.position()).isEqualTo(9);
      assertThat(result.limit()).isEqualTo(12);
      assertThat(key.position()).isEqualTo(8);
      assertThat(key.limit()).isEqualTo(8);
      final byte[] tmp3 = new byte[3];
      result.get(tmp3);
      assertThat(tmp3).isEqualTo("val".getBytes());

      // put
      final Segment key3 = sliceSegment("key3");
      final Segment key4 = sliceSegment("key4");
      final Segment value0 = sliceSegment("value 0");
      final Segment value1 = sliceSegment("value 1");
      db.put(key3.data, key3.offset, key3.len, value0.data, value0.offset, value0.len);
      db.put(opt, key4.data, key4.offset, key4.len, value1.data, value1.offset, value1.len);

      // compare
      Assert.assertTrue(value0.isSamePayload(db.get(key3.data, key3.offset, key3.len)));
      Assert.assertTrue(value1.isSamePayload(db.get(key4.data, key4.offset, key4.len)));
    }
  }

  @Test
  public void putIndirectByteBuffers() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions opt = new WriteOptions(); final ReadOptions optr = new ReadOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put(opt, "key2".getBytes(), "12345678".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo("12345678".getBytes());

      ByteBuffer key = ByteBuffer.allocate(12);
      ByteBuffer value = ByteBuffer.allocate(12);
      key.position(4);
      key.put("key3".getBytes());
      key.position(4).limit(8);
      value.position(4);
      value.put("val3".getBytes());
      value.position(4).limit(8);

      db.put(opt, key, value);

      assertThat(key.position()).isEqualTo(8);
      assertThat(key.limit()).isEqualTo(8);

      assertThat(value.position()).isEqualTo(8);
      assertThat(value.limit()).isEqualTo(8);

      key.position(4);

      ByteBuffer result = ByteBuffer.allocate(12);
      assertThat(db.get(optr, key, result)).isEqualTo(4);
      assertThat(result.position()).isEqualTo(0);
      assertThat(result.limit()).isEqualTo(4);
      assertThat(key.position()).isEqualTo(8);
      assertThat(key.limit()).isEqualTo(8);

      byte[] tmp = new byte[4];
      result.get(tmp);
      assertThat(tmp).isEqualTo("val3".getBytes());

      key.position(4);

      result.clear().position(9);
      assertThat(db.get(optr, key, result)).isEqualTo(4);
      assertThat(result.position()).isEqualTo(9);
      assertThat(result.limit()).isEqualTo(12);
      assertThat(key.position()).isEqualTo(8);
      assertThat(key.limit()).isEqualTo(8);
      byte[] tmp2 = new byte[3];
      result.get(tmp2);
      assertThat(tmp2).isEqualTo("val".getBytes());

      // put
      Segment key3 = sliceSegment("key3");
      Segment key4 = sliceSegment("key4");
      Segment value0 = sliceSegment("value 0");
      Segment value1 = sliceSegment("value 1");
      db.put(key3.data, key3.offset, key3.len, value0.data, value0.offset, value0.len);
      db.put(opt, key4.data, key4.offset, key4.len, value1.data, value1.offset, value1.len);

      // compare
      Assert.assertTrue(value0.isSamePayload(db.get(key3.data, key3.offset, key3.len)));
      Assert.assertTrue(value1.isSamePayload(db.get(key4.data, key4.offset, key4.len)));
    }
  }

  private static Segment sliceSegment(String key) {
    ByteBuffer rawKey = ByteBuffer.allocate(key.length() + 4);
    rawKey.put((byte)0);
    rawKey.put((byte)0);
    rawKey.put(key.getBytes());

    return new Segment(rawKey.array(), 2, key.length());
  }

  private static class Segment {
    final byte[] data;
    final int offset;
    final int len;

    public boolean isSamePayload(final byte[] value) {
      if (value == null) {
        return false;
      }
      if (value.length != len) {
        return false;
      }

      for (int i = 0; i < value.length; i++) {
        if (data[i + offset] != value[i]) {
          return false;
        }
      }

      return true;
    }

    public Segment(final byte[] value, final int offset, final int len) {
      this.data = value;
      this.offset = offset;
      this.len = len;
    }
  }

  @Test
  public void write() throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options options = new Options()
             .setMergeOperator(stringAppendOperator)
             .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath());
         final WriteOptions opts = new WriteOptions()) {

      try (final WriteBatch wb1 = new WriteBatch()) {
        wb1.put("key1".getBytes(), "aa".getBytes());
        wb1.merge("key1".getBytes(), "bb".getBytes());

        try (final WriteBatch wb2 = new WriteBatch()) {
          wb2.put("key2".getBytes(), "xx".getBytes());
          wb2.merge("key2".getBytes(), "yy".getBytes());
          db.write(opts, wb1);
          db.write(opts, wb2);
        }
      }

      assertThat(db.get("key1".getBytes())).isEqualTo(
          "aa,bb".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo(
          "xx,yy".getBytes());
    }
  }

  @Test
  public void getWithOutValue() throws RocksDBException {
    try (final RocksDB db =
             RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      final byte[] outValue = new byte[5];
      // not found value
      int getResult = db.get("keyNotFound".getBytes(), outValue);
      assertThat(getResult).isEqualTo(RocksDB.NOT_FOUND);
      // found value which fits in outValue
      getResult = db.get("key1".getBytes(), outValue);
      assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
      assertThat(outValue).isEqualTo("value".getBytes());
      // found value which fits partially
      getResult = db.get("key2".getBytes(), outValue);
      assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
      assertThat(outValue).isEqualTo("12345".getBytes());
    }
  }

  @Test
  public void getWithOutValueReadOptions() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final ReadOptions rOpt = new ReadOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      final byte[] outValue = new byte[5];
      // not found value
      int getResult = db.get(rOpt, "keyNotFound".getBytes(),
          outValue);
      assertThat(getResult).isEqualTo(RocksDB.NOT_FOUND);
      // found value which fits in outValue
      getResult = db.get(rOpt, "key1".getBytes(), outValue);
      assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
      assertThat(outValue).isEqualTo("value".getBytes());
      // found value which fits partially
      getResult = db.get(rOpt, "key2".getBytes(), outValue);
      assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
      assertThat(outValue).isEqualTo("12345".getBytes());
    }
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void getOutOfArrayMaxSizeValue() throws RocksDBException {
    final int numberOfValueSplits = 10;
    final int splitSize = Integer.MAX_VALUE / numberOfValueSplits;

    final Runtime runtime = Runtime.getRuntime();
    final long neededMemory = ((long) (splitSize)) * (((long) numberOfValueSplits) + 3);
    final boolean isEnoughMemory = runtime.maxMemory() - runtime.totalMemory() > neededMemory;
    Assume.assumeTrue(isEnoughMemory);

    final byte[] valueSplit = new byte[splitSize];
    final byte[] key = "key".getBytes();

    thrown.expect(RocksDBException.class);
    thrown.expectMessage("Requested array size exceeds VM limit");

    // merge (numberOfValueSplits + 1) valueSplit's to get value size exceeding Integer.MAX_VALUE
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options opt = new Options()
                 .setCreateIfMissing(true)
                 .setMergeOperator(stringAppendOperator);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put(key, valueSplit);
      for (int i = 0; i < numberOfValueSplits; i++) {
        db.merge(key, valueSplit);
      }
      db.get(key);
    }
  }

  @Test
  public void multiGetAsList() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final ReadOptions rOpt = new ReadOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      final List<byte[]> lookupKeys = new ArrayList<>();
      lookupKeys.add("key1".getBytes());
      lookupKeys.add("key2".getBytes());
      List<byte[]> results = db.multiGetAsList(lookupKeys);
      assertThat(results).isNotNull();
      assertThat(results).hasSize(lookupKeys.size());
      assertThat(results).
          containsExactly("value".getBytes(), "12345678".getBytes());
      // test same method with ReadOptions
      results = db.multiGetAsList(rOpt, lookupKeys);
      assertThat(results).isNotNull();
      assertThat(results).
          contains("value".getBytes(), "12345678".getBytes());

      // remove existing key
      lookupKeys.remove(1);
      // add non existing key
      lookupKeys.add("key3".getBytes());
      results = db.multiGetAsList(lookupKeys);
      assertThat(results).isNotNull();
      assertThat(results).
          containsExactly("value".getBytes(), null);
      // test same call with readOptions
      results = db.multiGetAsList(rOpt, lookupKeys);
      assertThat(results).isNotNull();
      assertThat(results).contains("value".getBytes());
    }
  }

  @Test
  public void merge() throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options opt = new Options()
            .setCreateIfMissing(true)
            .setMergeOperator(stringAppendOperator);
         final WriteOptions wOpt = new WriteOptions();
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      db.put("key1".getBytes(), "value".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value".getBytes());
      // merge key1 with another value portion
      db.merge("key1".getBytes(), "value2".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value,value2".getBytes());
      // merge key1 with another value portion
      db.merge(wOpt, "key1".getBytes(), "value3".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value,value2,value3".getBytes());
      // merge on non existent key shall insert the value
      db.merge(wOpt, "key2".getBytes(), "xxxx".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo(
          "xxxx".getBytes());

      final Segment key3 = sliceSegment("key3");
      final Segment key4 = sliceSegment("key4");
      final Segment value0 = sliceSegment("value 0");
      final Segment value1 = sliceSegment("value 1");

      db.merge(key3.data, key3.offset, key3.len, value0.data, value0.offset, value0.len);
      db.merge(wOpt, key4.data, key4.offset, key4.len, value1.data, value1.offset, value1.len);

      // compare
      Assert.assertTrue(value0.isSamePayload(db.get(key3.data, key3.offset, key3.len)));
      Assert.assertTrue(value1.isSamePayload(db.get(key4.data, key4.offset, key4.len)));
    }
  }

  @Test
  public void delete() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      db.put("key3".getBytes(), "33".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo(
          "12345678".getBytes());
      assertThat(db.get("key3".getBytes())).isEqualTo("33".getBytes());
      db.delete("key1".getBytes());
      db.delete(wOpt, "key2".getBytes());
      final ByteBuffer key = ByteBuffer.allocateDirect(16);
      key.put("key3".getBytes()).flip();
      db.delete(wOpt, key);
      assertThat(key.position()).isEqualTo(4);
      assertThat(key.limit()).isEqualTo(4);

      assertThat(db.get("key1".getBytes())).isNull();
      assertThat(db.get("key2".getBytes())).isNull();

      final Segment key3 = sliceSegment("key3");
      final Segment key4 = sliceSegment("key4");
      db.put("key3".getBytes(), "key3 value".getBytes());
      db.put("key4".getBytes(), "key4 value".getBytes());

      db.delete(key3.data, key3.offset, key3.len);
      db.delete(wOpt, key4.data, key4.offset, key4.len);

      assertThat(db.get("key3".getBytes())).isNull();
      assertThat(db.get("key4".getBytes())).isNull();
    }
  }

  @Test
  public void singleDelete() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo(
          "value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo(
          "12345678".getBytes());
      db.singleDelete("key1".getBytes());
      db.singleDelete(wOpt, "key2".getBytes());
      assertThat(db.get("key1".getBytes())).isNull();
      assertThat(db.get("key2".getBytes())).isNull();
    }
  }

  @Test
  public void singleDelete_nonExisting() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.singleDelete("key1".getBytes());
      db.singleDelete(wOpt, "key2".getBytes());
      assertThat(db.get("key1".getBytes())).isNull();
      assertThat(db.get("key2".getBytes())).isNull();
    }
  }

  @Test
  public void deleteRange() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      db.put("key3".getBytes(), "abcdefg".getBytes());
      db.put("key4".getBytes(), "xyz".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo("12345678".getBytes());
      assertThat(db.get("key3".getBytes())).isEqualTo("abcdefg".getBytes());
      assertThat(db.get("key4".getBytes())).isEqualTo("xyz".getBytes());
      db.deleteRange("key2".getBytes(), "key4".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());
      assertThat(db.get("key2".getBytes())).isNull();
      assertThat(db.get("key3".getBytes())).isNull();
      assertThat(db.get("key4".getBytes())).isEqualTo("xyz".getBytes());
    }
  }

  @Test
  public void clipColumnFamily() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      db.put("key3".getBytes(), "abcdefg".getBytes());
      db.put("key4".getBytes(), "xyz".getBytes());
      db.put("key5".getBytes(), "qwer".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo("12345678".getBytes());
      assertThat(db.get("key3".getBytes())).isEqualTo("abcdefg".getBytes());
      assertThat(db.get("key4".getBytes())).isEqualTo("xyz".getBytes());
      assertThat(db.get("key5".getBytes())).isEqualTo("qwer".getBytes());
      db.clipColumnFamily(db.getDefaultColumnFamily(), "key2".getBytes(), "key4".getBytes());
      assertThat(db.get("key1".getBytes())).isNull();
      assertThat(db.get("key2".getBytes())).isEqualTo("12345678".getBytes());
      assertThat(db.get("key3".getBytes())).isEqualTo("abcdefg".getBytes());
      assertThat(db.get("key4".getBytes())).isNull();
      assertThat(db.get("key5".getBytes())).isNull();
    }
  }

  @Test
  public void getIntProperty() throws RocksDBException {
    try (
        final Options options = new Options()
            .setCreateIfMissing(true)
            .setMaxWriteBufferNumber(10)
            .setMinWriteBufferNumberToMerge(10);
        final RocksDB db = RocksDB.open(options,
            dbFolder.getRoot().getAbsolutePath());
        final WriteOptions wOpt = new WriteOptions().setDisableWAL(true)
    ) {
      db.put(wOpt, "key1".getBytes(), "value1".getBytes());
      db.put(wOpt, "key2".getBytes(), "value2".getBytes());
      db.put(wOpt, "key3".getBytes(), "value3".getBytes());
      db.put(wOpt, "key4".getBytes(), "value4".getBytes());
      assertThat(db.getLongProperty("rocksdb.num-entries-active-mem-table"))
          .isGreaterThan(0);
      assertThat(db.getLongProperty("rocksdb.cur-size-active-mem-table"))
          .isGreaterThan(0);
    }
  }

  @Test
  public void fullCompactRange() throws RocksDBException {
    try (final Options opt = new Options().
        setCreateIfMissing(true).
        setDisableAutoCompactions(true).
        setCompactionStyle(CompactionStyle.LEVEL).
        setNumLevels(4).
        setWriteBufferSize(100 << 10).
        setLevelZeroFileNumCompactionTrigger(3).
        setTargetFileSizeBase(200 << 10).
        setTargetFileSizeMultiplier(1).
        setMaxBytesForLevelBase(500 << 10).
        setMaxBytesForLevelMultiplier(1).
        setDisableAutoCompactions(false);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // fill database with key/value pairs
      final byte[] b = new byte[10000];
      for (int i = 0; i < 200; i++) {
        rand.nextBytes(b);
        db.put((String.valueOf(i)).getBytes(), b);
      }
      db.compactRange();
    }
  }

  @Test
  public void fullCompactRangeColumnFamily()
      throws RocksDBException {
    try (
        final DBOptions opt = new DBOptions().
            setCreateIfMissing(true).
            setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
            setDisableAutoCompactions(true).
            setCompactionStyle(CompactionStyle.LEVEL).
            setNumLevels(4).
            setWriteBufferSize(100 << 10).
            setLevelZeroFileNumCompactionTrigger(3).
            setTargetFileSizeBase(200 << 10).
            setTargetFileSizeMultiplier(1).
            setMaxBytesForLevelBase(500 << 10).
            setMaxBytesForLevelMultiplier(1).
            setDisableAutoCompactions(false)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts));

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          final byte[] b = new byte[10000];
          for (int i = 0; i < 200; i++) {
            rand.nextBytes(b);
            db.put(columnFamilyHandles.get(1),
                String.valueOf(i).getBytes(), b);
          }
          db.compactRange(columnFamilyHandles.get(1));
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void compactRangeWithKeys()
      throws RocksDBException {
    try (final Options opt = new Options().
        setCreateIfMissing(true).
        setDisableAutoCompactions(true).
        setCompactionStyle(CompactionStyle.LEVEL).
        setNumLevels(4).
        setWriteBufferSize(100 << 10).
        setLevelZeroFileNumCompactionTrigger(3).
        setTargetFileSizeBase(200 << 10).
        setTargetFileSizeMultiplier(1).
        setMaxBytesForLevelBase(500 << 10).
        setMaxBytesForLevelMultiplier(1).
        setDisableAutoCompactions(false);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      // fill database with key/value pairs
      final byte[] b = new byte[10000];
      for (int i = 0; i < 200; i++) {
        rand.nextBytes(b);
        db.put((String.valueOf(i)).getBytes(), b);
      }
      db.compactRange("0".getBytes(), "201".getBytes());
    }
  }

  @Test
  public void compactRangeWithKeysReduce()
      throws RocksDBException {
    try (
        final Options opt = new Options().
            setCreateIfMissing(true).
            setDisableAutoCompactions(true).
            setCompactionStyle(CompactionStyle.LEVEL).
            setNumLevels(4).
            setWriteBufferSize(100 << 10).
            setLevelZeroFileNumCompactionTrigger(3).
            setTargetFileSizeBase(200 << 10).
            setTargetFileSizeMultiplier(1).
            setMaxBytesForLevelBase(500 << 10).
            setMaxBytesForLevelMultiplier(1).
            setDisableAutoCompactions(false);
        final RocksDB db = RocksDB.open(opt,
            dbFolder.getRoot().getAbsolutePath())) {
      // fill database with key/value pairs
      final byte[] b = new byte[10000];
      for (int i = 0; i < 200; i++) {
        rand.nextBytes(b);
        db.put((String.valueOf(i)).getBytes(), b);
      }
      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }
      try (final CompactRangeOptions compactRangeOpts = new CompactRangeOptions()
            .setChangeLevel(true)
            .setTargetLevel(-1)
            .setTargetPathId(0)) {
        db.compactRange(null, "0".getBytes(), "201".getBytes(),
            compactRangeOpts);
      }
    }
  }

  @Test
  public void compactRangeWithKeysColumnFamily()
      throws RocksDBException {
    try (final DBOptions opt = new DBOptions().
        setCreateIfMissing(true).
        setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
             setDisableAutoCompactions(true).
             setCompactionStyle(CompactionStyle.LEVEL).
             setNumLevels(4).
             setWriteBufferSize(100 << 10).
             setLevelZeroFileNumCompactionTrigger(3).
             setTargetFileSizeBase(200 << 10).
             setTargetFileSizeMultiplier(1).
             setMaxBytesForLevelBase(500 << 10).
             setMaxBytesForLevelMultiplier(1).
             setDisableAutoCompactions(false)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles =
          new ArrayList<>();
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          final byte[] b = new byte[10000];
          for (int i = 0; i < 200; i++) {
            rand.nextBytes(b);
            db.put(columnFamilyHandles.get(1),
                String.valueOf(i).getBytes(), b);
          }
          db.compactRange(columnFamilyHandles.get(1),
              "0".getBytes(), "201".getBytes());
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void compactRangeWithKeysReduceColumnFamily()
      throws RocksDBException {
    try (final DBOptions opt = new DBOptions().
        setCreateIfMissing(true).
        setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions().
             setDisableAutoCompactions(true).
             setCompactionStyle(CompactionStyle.LEVEL).
             setNumLevels(4).
             setWriteBufferSize(100 << 10).
             setLevelZeroFileNumCompactionTrigger(3).
             setTargetFileSizeBase(200 << 10).
             setTargetFileSizeMultiplier(1).
             setMaxBytesForLevelBase(500 << 10).
             setMaxBytesForLevelMultiplier(1).
             setDisableAutoCompactions(false)
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      // open database
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try (final CompactRangeOptions compactRangeOpts = new CompactRangeOptions()
            .setChangeLevel(true)
            .setTargetLevel(-1)
            .setTargetPathId(0)) {
          // fill database with key/value pairs
          final byte[] b = new byte[10000];
          for (int i = 0; i < 200; i++) {
            rand.nextBytes(b);
            db.put(columnFamilyHandles.get(1),
                String.valueOf(i).getBytes(), b);
          }
          db.compactRange(columnFamilyHandles.get(1), "0".getBytes(),
              "201".getBytes(), compactRangeOpts);
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void compactRangeToLevel() throws RocksDBException {
    final int NUM_KEYS_PER_L0_FILE = 100;
    final int KEY_SIZE = 20;
    final int VALUE_SIZE = 300;
    final int L0_FILE_SIZE =
        NUM_KEYS_PER_L0_FILE * (KEY_SIZE + VALUE_SIZE);
    final int NUM_L0_FILES = 10;
    final int TEST_SCALE = 5;
    final int KEY_INTERVAL = 100;
    try (final Options opt = new Options()
                                 .setCreateIfMissing(true)
                                 .setCompactionStyle(CompactionStyle.LEVEL)
                                 .setLevelCompactionDynamicLevelBytes(false)
                                 .setNumLevels(5)
                                 .
                             // a slightly bigger write buffer than L0 file
                             // so that we can ensure manual flush always
                             // go before background flush happens.
                             setWriteBufferSize(L0_FILE_SIZE * 2)
                                 .
                             // Disable auto L0 -> L1 compaction
                             setLevelZeroFileNumCompactionTrigger(20)
                                 .setTargetFileSizeBase(L0_FILE_SIZE * 100)
                                 .setTargetFileSizeMultiplier(1)
                                 .
                             // To disable auto compaction
                             setMaxBytesForLevelBase(NUM_L0_FILES * L0_FILE_SIZE * 100)
                                 .setMaxBytesForLevelMultiplier(2)
                                 .setDisableAutoCompactions(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      // fill database with key/value pairs
      final byte[] value = new byte[VALUE_SIZE];
      int int_key = 0;
      for (int round = 0; round < 5; ++round) {
        final int initial_key = int_key;
        for (int f = 1; f <= NUM_L0_FILES; ++f) {
          for (int i = 0; i < NUM_KEYS_PER_L0_FILE; ++i) {
            int_key += KEY_INTERVAL;
            rand.nextBytes(value);

            db.put(String.format("%020d", int_key).getBytes(),
                value);
          }
          try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions);
          }
          // Make sure we do create one more L0 files.
          assertThat(
              db.getProperty("rocksdb.num-files-at-level0")).
              isEqualTo("" + f);
        }

        // Compact all L0 files we just created
        db.compactRange(
            String.format("%020d", initial_key).getBytes(),
            String.format("%020d", int_key - 1).getBytes());
        // Making sure there isn't any L0 files.
        assertThat(
            db.getProperty("rocksdb.num-files-at-level0")).
            isEqualTo("0");
        // Making sure there are some L1 files.
        // Here we only use != 0 instead of a specific number
        // as we don't want the test make any assumption on
        // how compaction works.
        assertThat(
            db.getProperty("rocksdb.num-files-at-level1")).
            isNotEqualTo("0");
        // Because we only compacted those keys we issued
        // in this round, there shouldn't be any L1 -> L2
        // compaction.  So we expect zero L2 files here.
        assertThat(
            db.getProperty("rocksdb.num-files-at-level2")).
            isEqualTo("0");
      }
    }
  }

  @Test
  public void deleteFilesInRange() throws RocksDBException {
    final int KEY_SIZE = 20;
    final int VALUE_SIZE = 1000;
    final int FILE_SIZE = 64000;
    final int NUM_FILES = 10;

    final int KEY_INTERVAL = 10000;
    /*
     * Intention of these options is to end up reliably with 10 files
     * we will be deleting using deleteFilesInRange.
     * It is writing roughly number of keys that will fit in 10 files (target size)
     * It is writing interleaved so that files from memory on L0 will overlap
     * Then compaction cleans everything, and we should end up with 10 files
     */
    try (final Options opt = new Options()
                                 .setCreateIfMissing(true)
                                 .setCompressionType(CompressionType.NO_COMPRESSION)
                                 .setTargetFileSizeBase(FILE_SIZE)
                                 .setWriteBufferSize(FILE_SIZE / 2)
                                 .setDisableAutoCompactions(true)
                                 .setLevelCompactionDynamicLevelBytes(false);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final int records = FILE_SIZE / (KEY_SIZE + VALUE_SIZE);

      // fill database with key/value pairs
      final byte[] value = new byte[VALUE_SIZE];
      int key_init = 0;
      for (int o = 0; o < NUM_FILES; ++o) {
        int int_key = key_init++;
        for (int i = 0; i < records; ++i) {
          int_key += KEY_INTERVAL;
          rand.nextBytes(value);

          db.put(String.format("%020d", int_key).getBytes(), value);
        }
      }
      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }
      db.compactRange();
      // Make sure we do create one more L0 files.
      assertThat(db.getProperty("rocksdb.num-files-at-level0")).isEqualTo("0");

      // Should be 10, but we are OK with asserting +- 2
      int files = Integer.parseInt(db.getProperty("rocksdb.num-files-at-level1"));
      assertThat(files).isBetween(8, 12);

      // Delete lower 60% (roughly). Result should be 5, but we are OK with asserting +- 2
      // Important is that we know something was deleted (JNI call did something)
      // Exact assertions are done in C++ unit tests
      db.deleteFilesInRanges(null,
          Arrays.asList(null, String.format("%020d", records * KEY_INTERVAL * 6 / 10).getBytes()),
          false);
      files = Integer.parseInt(db.getProperty("rocksdb.num-files-at-level1"));
      assertThat(files).isBetween(3, 7);
    }
  }

  @Test
  public void compactRangeToLevelColumnFamily()
      throws RocksDBException {
    final int NUM_KEYS_PER_L0_FILE = 100;
    final int KEY_SIZE = 20;
    final int VALUE_SIZE = 300;
    final int L0_FILE_SIZE =
        NUM_KEYS_PER_L0_FILE * (KEY_SIZE + VALUE_SIZE);
    final int NUM_L0_FILES = 10;
    final int TEST_SCALE = 5;
    final int KEY_INTERVAL = 100;

    try (final DBOptions opt =
             new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts =
             new ColumnFamilyOptions()
                 .setCompactionStyle(CompactionStyle.LEVEL)
                 .setLevelCompactionDynamicLevelBytes(false)
                 .setNumLevels(5)
                 .
             // a slightly bigger write buffer than L0 file
             // so that we can ensure manual flush always
             // go before background flush happens.
             setWriteBufferSize(L0_FILE_SIZE * 2)
                 .
             // Disable auto L0 -> L1 compaction
             setLevelZeroFileNumCompactionTrigger(20)
                 .setTargetFileSizeBase(L0_FILE_SIZE * 100)
                 .setTargetFileSizeMultiplier(1)
                 .
             // To disable auto compaction
             setMaxBytesForLevelBase(NUM_L0_FILES * L0_FILE_SIZE * 100)
                 .setMaxBytesForLevelMultiplier(2)
                 .setDisableAutoCompactions(true)) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      // open database
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          // fill database with key/value pairs
          final byte[] value = new byte[VALUE_SIZE];
          int int_key = 0;
          for (int round = 0; round < 5; ++round) {
            final int initial_key = int_key;
            for (int f = 1; f <= NUM_L0_FILES; ++f) {
              for (int i = 0; i < NUM_KEYS_PER_L0_FILE; ++i) {
                int_key += KEY_INTERVAL;
                rand.nextBytes(value);

                db.put(columnFamilyHandles.get(1),
                    String.format("%020d", int_key).getBytes(),
                    value);
              }
              try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
                db.flush(flushOptions, columnFamilyHandles.get(1));
              }
              // Make sure we do create one more L0 files.
              assertThat(
                  db.getProperty(columnFamilyHandles.get(1),
                      "rocksdb.num-files-at-level0")).
                  isEqualTo("" + f);
            }

            // Compact all L0 files we just created
            db.compactRange(
                columnFamilyHandles.get(1),
                String.format("%020d", initial_key).getBytes(),
                String.format("%020d", int_key - 1).getBytes());
            // Making sure there isn't any L0 files.
            assertThat(
                db.getProperty(columnFamilyHandles.get(1),
                    "rocksdb.num-files-at-level0")).
                isEqualTo("0");
            // Making sure there are some L1 files.
            // Here we only use != 0 instead of a specific number
            // as we don't want the test make any assumption on
            // how compaction works.
            assertThat(
                db.getProperty(columnFamilyHandles.get(1),
                    "rocksdb.num-files-at-level1")).
                isNotEqualTo("0");
            // Because we only compacted those keys we issued
            // in this round, there shouldn't be any L1 -> L2
            // compaction.  So we expect zero L2 files here.
            assertThat(
                db.getProperty(columnFamilyHandles.get(1),
                    "rocksdb.num-files-at-level2")).
                isEqualTo("0");
          }
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void compactRangeWithNullBoundaries() throws RocksDBException {
    try (final Options opt = new Options()
                                 .setCreateIfMissing(true)
                                 .setDisableAutoCompactions(true)
                                 .setCompactionStyle(CompactionStyle.LEVEL)
                                 .setNumLevels(4)
                                 .setWriteBufferSize(100 << 10)
                                 .setLevelZeroFileNumCompactionTrigger(3)
                                 .setTargetFileSizeBase(200 << 10)
                                 .setTargetFileSizeMultiplier(1)
                                 .setMaxBytesForLevelBase(500 << 10)
                                 .setMaxBytesForLevelMultiplier(1)
                                 .setDisableAutoCompactions(true);
         final FlushOptions flushOptions = new FlushOptions();
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final byte[] b = new byte[10000];
      // Create an SST containing key4, key5, and key6
      db.put(("key4").getBytes(), b);
      db.put(("key5").getBytes(), b);
      db.put(("key6").getBytes(), b);
      db.flush(flushOptions);
      // Create a new SST that includes the tombstones of all keys
      db.delete(("key4").getBytes());
      db.delete(("key5").getBytes());
      db.delete(("key6").getBytes());
      db.flush(flushOptions);

      db.compactRange(("key4").getBytes(), null);
      List<LiveFileMetaData> liveFilesMetaData = db.getLiveFilesMetaData();
      assertThat(liveFilesMetaData.size()).isEqualTo(0);
    }
  }

  @Test
  public void continueBackgroundWorkAfterCancelAllBackgroundWork() throws RocksDBException {
    final int KEY_SIZE = 20;
    final int VALUE_SIZE = 300;
    try (final DBOptions opt = new DBOptions().
        setCreateIfMissing(true).
        setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions()
    ) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts)
          );

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      // open the database
      try (final RocksDB db = RocksDB.open(opt,
          dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors,
          columnFamilyHandles)) {
        try {
          db.cancelAllBackgroundWork(true);
          try {
            db.put(new byte[KEY_SIZE], new byte[VALUE_SIZE]);
            try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
              db.flush(flushOptions);
            }
            fail("Expected RocksDBException to be thrown if we attempt to trigger a flush after" +
                " all background work is cancelled.");
          } catch (final RocksDBException ignored) {
          }
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void cancelAllBackgroundWorkTwice() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      // Cancel all background work synchronously
      db.cancelAllBackgroundWork(true);
      // Cancel all background work asynchronously
      db.cancelAllBackgroundWork(false);
    }
  }

  @Test
  public void pauseContinueBackgroundWork() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      db.pauseBackgroundWork();
      db.continueBackgroundWork();
      db.pauseBackgroundWork();
      db.continueBackgroundWork();
    }
  }

  @Test
  public void enableDisableFileDeletions() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      db.disableFileDeletions();
      db.enableFileDeletions();
    }
  }

  @Test
  public void setOptions() throws RocksDBException {
    try (final DBOptions options = new DBOptions()
             .setCreateIfMissing(true)
             .setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions()
             .setWriteBufferSize(4096)) {

      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts));

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      try (final RocksDB db = RocksDB.open(options,
          dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles)) {
        try {
          final MutableColumnFamilyOptions mutableOptions =
              MutableColumnFamilyOptions.builder()
                  .setWriteBufferSize(2048)
                  .build();

          db.setOptions(columnFamilyHandles.get(1), mutableOptions);

        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void destroyDB() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.put("key1".getBytes(), "value".getBytes());
      }
      assertThat(dbFolder.getRoot().exists()
          && Objects.requireNonNull(dbFolder.getRoot().listFiles()).length != 0)
          .isTrue();
      RocksDB.destroyDB(dbPath, options);
      assertThat(dbFolder.getRoot().exists()
          && Objects.requireNonNull(dbFolder.getRoot().listFiles()).length != 0)
          .isFalse();
    }
  }

  @Test(expected = RocksDBException.class)
  public void destroyDBFailIfOpen() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB ignored = RocksDB.open(options, dbPath)) {
        // Fails as the db is open and locked.
        RocksDB.destroyDB(dbPath, options);
      }
    }
  }

  @Test
  public void getApproximateSizes() throws RocksDBException {
    final byte[] key1 = "key1".getBytes(UTF_8);
    final byte[] key2 = "key2".getBytes(UTF_8);
    final byte[] key3 = "key3".getBytes(UTF_8);
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.put(key1, key1);
        db.put(key2, key2);
        db.put(key3, key3);

        final long[] sizes = db.getApproximateSizes(
            Arrays.asList(
                new Range(new Slice(key1), new Slice(key1)),
                new Range(new Slice(key2), new Slice(key3))
            ),
            SizeApproximationFlag.INCLUDE_FILES,
            SizeApproximationFlag.INCLUDE_MEMTABLES);

        assertThat(sizes.length).isEqualTo(2);
        assertThat(sizes[0]).isEqualTo(0);
        assertThat(sizes[1]).isGreaterThanOrEqualTo(1);
      }
    }
  }

  @Test
  public void getApproximateMemTableStats() throws RocksDBException {
    final byte[] key1 = "key1".getBytes(UTF_8);
    final byte[] key2 = "key2".getBytes(UTF_8);
    final byte[] key3 = "key3".getBytes(UTF_8);
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.put(key1, key1);
        db.put(key2, key2);
        db.put(key3, key3);

        final RocksDB.CountAndSize stats =
            db.getApproximateMemTableStats(
                new Range(new Slice(key1), new Slice(key3)));

        assertThat(stats).isNotNull();
        assertThat(stats.count).isGreaterThan(1);
        assertThat(stats.size).isGreaterThan(1);
      }
    }
  }

  @Test
  public void getApproximateMemTableStatsSingleKey() throws RocksDBException {
    final byte[] key1 = "key1".getBytes(UTF_8);
    final byte[] key3 = "key3".getBytes(UTF_8);
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.put(key1, key1);

        final RocksDB.CountAndSize stats =
            db.getApproximateMemTableStats(new Range(new Slice(key1), new Slice(key3)));

        assertThat(stats).isNotNull();
        assertThat(stats.count).isEqualTo(1);
        assertThat(stats.size).isGreaterThan(1);
      }
    }
  }

  @Test
  public void getLiveFilesMetadataWithChecksum() throws RocksDBException {
    final Properties props = new Properties();
    final byte[] key1 = "key1".getBytes(UTF_8);
    props.put("file_checksum_gen_factory", "FileChecksumGenCrc32cFactory");

    try (final DBOptions dbOptions = DBOptions.getDBOptionsFromProps(props);
         final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
         final Options options = new Options(dbOptions, cfOptions).setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();

      // disable WAL so we have a deterministic checksum
      try (final RocksDB db = RocksDB.open(options, dbPath);
           final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true)) {
        db.put(writeOptions, key1, key1);
      }

      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        final List<LiveFileMetaData> expectedFileMetadata = db.getLiveFilesMetaData();
        assertThat(expectedFileMetadata).hasSize(1);
        // ideally we could re-compute here, but CRC32C is a Java 9 feature, so we have no CRC32C
        // implementation available here
        final LiveFileMetaData sstFile = expectedFileMetadata.get(0);
        assertThat(sstFile.fileChecksum()).isNotEmpty();
      }
    }
  }

  @Test
  public void getColumnFamilyMetadataWithChecksum() throws RocksDBException {
    final Properties props = new Properties();
    props.put("file_checksum_gen_factory", "FileChecksumGenCrc32cFactory");
    final String dbPath = dbFolder.getRoot().getAbsolutePath();

    try (final DBOptions dbOptions = DBOptions.getDBOptionsFromProps(props);
         final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
         final Options options = new Options(dbOptions, cfOptions).setCreateIfMissing(true)) {
      try (final RocksDB db = RocksDB.open(options, dbPath);
           final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true)) {
        db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
      }

      try (final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        ColumnFamilyMetaData metadata = db.getColumnFamilyMetaData(); // Exception here
        List<LevelMetaData> levels = metadata.levels();
        assertThat(levels).isNotEmpty();
        List<SstFileMetaData> filesMetadata = levels.get(0).files();
        assertThat(filesMetadata).isNotEmpty();
        assertThat(filesMetadata.get(0).fileChecksum()).isNotNull();
        assertThat(filesMetadata.get(0).fileChecksum()).hasSize(4);
        assertThat(filesMetadata.get(0).fileChecksum()).isNotEqualTo(new byte[] {0, 0, 0, 0});
      }
    }
  }

  @Ignore("TODO(AR) re-enable when ready!")
  @Test
  public void compactFiles() throws RocksDBException {
    final int kTestKeySize = 16;
    final int kTestValueSize = 984;
    final int kEntrySize = kTestKeySize + kTestValueSize;
    final int kEntriesPerBuffer = 100;
    final int writeBufferSize = kEntrySize * kEntriesPerBuffer;
    final byte[] cfName = "pikachu".getBytes(UTF_8);

    try (final Options options = new Options()
                                     .setCreateIfMissing(true)
                                     .setWriteBufferSize(writeBufferSize)
                                     .setCompactionStyle(CompactionStyle.LEVEL)
                                     .setLevelCompactionDynamicLevelBytes(false)
                                     .setTargetFileSizeBase(writeBufferSize)
                                     .setMaxBytesForLevelBase(writeBufferSize * 2)
                                     .setLevel0StopWritesTrigger(2)
                                     .setMaxBytesForLevelMultiplier(2)
                                     .setCompressionType(CompressionType.NO_COMPRESSION)
                                     .setMaxSubcompactions(4)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath);
           final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options)) {
        db.createColumnFamily(new ColumnFamilyDescriptor(cfName,
            cfOptions)).close();
      }

      try (final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options)) {
        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
            new ColumnFamilyDescriptor(cfName, cfOptions)
        );
        final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        try (final DBOptions dbOptions = new DBOptions(options);
             final RocksDB db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles)) {
          try (final FlushOptions flushOptions = new FlushOptions()
                .setWaitForFlush(true)
                .setAllowWriteStall(true);
               final CompactionOptions compactionOptions = new CompactionOptions()) {
            final Random rnd = new Random(301);
            for (int key = 64 * kEntriesPerBuffer; key >= 0; --key) {
              final byte[] value = new byte[kTestValueSize];
              rnd.nextBytes(value);
              db.put(cfHandles.get(1), Integer.toString(key).getBytes(UTF_8),
                  value);
            }
            db.flush(flushOptions, cfHandles);

            final RocksDB.LiveFiles liveFiles = db.getLiveFiles();
            final List<String> compactedFiles =
                db.compactFiles(compactionOptions, cfHandles.get(1),
                    liveFiles.files, 1, -1, null);
            assertThat(compactedFiles).isNotEmpty();
          } finally {
            for (final ColumnFamilyHandle cfHandle : cfHandles) {
              cfHandle.close();
            }
          }
        }
      }
    }
  }

  @Test
  public void enableAutoCompaction() throws RocksDBException {
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)) {
      final List<ColumnFamilyDescriptor> cfDescs =
          Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath, cfDescs, cfHandles)) {
        try {
          db.enableAutoCompaction(cfHandles);
        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void enableAutoCompactionNull() throws RocksDBException {
    try (final DBOptions options = new DBOptions().setCreateIfMissing(true)) {
      final List<ColumnFamilyDescriptor> cfDescs =
          Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath, cfDescs, cfHandles)) {
        try {
          db.enableAutoCompaction(null);
        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void numberLevels() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        assertThat(db.numberLevels()).isEqualTo(7);
      }
    }
  }

  @Test
  public void maxMemCompactionLevel() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        assertThat(db.maxMemCompactionLevel()).isEqualTo(0);
      }
    }
  }

  @Test
  public void level0StopWriteTrigger() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        assertThat(db.level0StopWriteTrigger()).isEqualTo(36);
      }
    }
  }

  @Test
  public void getName() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        assertThat(db.getName()).isEqualTo(dbPath);
      }
    }
  }

  @Test
  public void getEnv() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        assertThat(db.getEnv()).isEqualTo(Env.getDefault());
      }
    }
  }

  @Test
  public void flush() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath);
        final FlushOptions flushOptions = new FlushOptions()) {
        db.flush(flushOptions);
      }
    }
  }

  @Test
  public void flushWal() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.flushWal(true);
      }
    }
  }

  @Test
  public void syncWal() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.syncWal();
      }
    }
  }

  @Test
  public void getLiveFiles() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        final RocksDB.LiveFiles livefiles = db.getLiveFiles(true);
        assertThat(livefiles).isNotNull();
        assertThat(livefiles.manifestFileSize).isEqualTo(116);
        assertThat(livefiles.files.size()).isEqualTo(3);
        assertThat(livefiles.files.get(0)).isEqualTo("/CURRENT");
        assertThat(livefiles.files.get(1)).isEqualTo("/MANIFEST-000005");
        assertThat(livefiles.files.get(2)).isEqualTo("/OPTIONS-000007");
      }
    }
  }

  @Test
  public void getSortedWalFiles() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        final List<LogFile> logFiles = db.getSortedWalFiles();
        assertThat(logFiles).isNotNull();
        assertThat(logFiles.size()).isEqualTo(1);
        assertThat(logFiles.get(0).type())
            .isEqualTo(WalFileType.kAliveLogFile);
      }
    }
  }

  @Test
  public void deleteFile() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.deleteFile("unknown");
      }
    }
  }

  @Test
  public void getLiveFilesMetaData() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        final List<LiveFileMetaData> liveFilesMetaData
            = db.getLiveFilesMetaData();
        assertThat(liveFilesMetaData).isEmpty();
      }
    }
  }

  @Test
  public void getColumnFamilyMetaData() throws RocksDBException {
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)) {
      final List<ColumnFamilyDescriptor> cfDescs =
          Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath, cfDescs, cfHandles)) {
        db.put(cfHandles.get(0), "key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        try {
          final ColumnFamilyMetaData cfMetadata =
              db.getColumnFamilyMetaData(cfHandles.get(0));
          assertThat(cfMetadata).isNotNull();
          assertThat(cfMetadata.name()).isEqualTo(RocksDB.DEFAULT_COLUMN_FAMILY);
          assertThat(cfMetadata.levels().size()).isEqualTo(7);
        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void verifyChecksum() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.verifyChecksum();
      }
    }
  }

  @Test
  public void getPropertiesOfAllTables() throws RocksDBException {
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)) {
      final List<ColumnFamilyDescriptor> cfDescs =
          Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath, cfDescs, cfHandles)) {
        db.put(cfHandles.get(0), "key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        try {
          final Map<String, TableProperties> properties =
              db.getPropertiesOfAllTables(cfHandles.get(0));
          assertThat(properties).isNotNull();
        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void getPropertiesOfTablesInRange() throws RocksDBException {
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)) {
      final List<ColumnFamilyDescriptor> cfDescs =
          Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath, cfDescs, cfHandles)) {
        db.put(cfHandles.get(0), "key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        db.put(cfHandles.get(0), "key2".getBytes(UTF_8), "value2".getBytes(UTF_8));
        db.put(cfHandles.get(0), "key3".getBytes(UTF_8), "value3".getBytes(UTF_8));
        try {
          final Range range = new Range(
              new Slice("key1".getBytes(UTF_8)),
              new Slice("key3".getBytes(UTF_8)));
          final Map<String, TableProperties> properties =
              db.getPropertiesOfTablesInRange(cfHandles.get(0), Collections.singletonList(range));
          assertThat(properties).isNotNull();
        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void suggestCompactRange() throws RocksDBException {
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)) {
      final List<ColumnFamilyDescriptor> cfDescs =
          Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath, cfDescs, cfHandles)) {
        db.put(cfHandles.get(0), "key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        db.put(cfHandles.get(0), "key2".getBytes(UTF_8), "value2".getBytes(UTF_8));
        db.put(cfHandles.get(0), "key3".getBytes(UTF_8), "value3".getBytes(UTF_8));
        try {
          final Range range = db.suggestCompactRange();
          assertThat(range).isNotNull();
        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void suggestCompactRangeCF() throws RocksDBException {
    try (final DBOptions options =
             new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
      final List<ColumnFamilyDescriptor> cfDescs =
          Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new ColumnFamilyOptions()),
              new ColumnFamilyDescriptor("new_cf2".getBytes(), new ColumnFamilyOptions()));

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath, cfDescs, cfHandles)) {
        db.put(cfHandles.get(0), "key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        db.put(cfHandles.get(0), "key2".getBytes(UTF_8), "value2".getBytes(UTF_8));
        db.put(cfHandles.get(0), "key3".getBytes(UTF_8), "value3".getBytes(UTF_8));
        db.put(cfHandles.get(1), "key1_new_cf".getBytes(UTF_8), "value1".getBytes(UTF_8));
        db.put(cfHandles.get(1), "key2_new_cf".getBytes(UTF_8), "value2".getBytes(UTF_8));
        db.put(cfHandles.get(1), "key3_new_cf".getBytes(UTF_8), "value3".getBytes(UTF_8));
        try {
          final Range range =  db.suggestCompactRange(cfHandles.get(0));
          assertThat(range).isNotNull();
          final Range rangeCF = db.suggestCompactRange(cfHandles.get(1));
          assertThat(rangeCF).isNotNull();
          final Range rangeCFEmpty = db.suggestCompactRange(cfHandles.get(2));
          assertThat(rangeCFEmpty).isNotNull();
        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void promoteL0() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        db.promoteL0(2);
      }
    }
  }

  @Test
  public void startTrace() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      final String dbPath = dbFolder.getRoot().getAbsolutePath();
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        final TraceOptions traceOptions = new TraceOptions();

        try (final InMemoryTraceWriter traceWriter = new InMemoryTraceWriter()) {
          db.startTrace(traceOptions, traceWriter);

          db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));

          db.endTrace();

          final List<byte[]> writes = traceWriter.getWrites();
          assertThat(writes.size()).isGreaterThan(0);
        }
      }
    }
  }

  @Test
  public void setDBOptions() throws RocksDBException {
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions new_cf_opts = new ColumnFamilyOptions()
             .setWriteBufferSize(4096)) {

      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts));

      // open database
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      try (final RocksDB db = RocksDB.open(options,
          dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles)) {
        try {
          final MutableDBOptions mutableOptions =
              MutableDBOptions.builder()
                  .setBytesPerSync(1024 * 1027 * 7)
                  .setAvoidFlushDuringShutdown(false)
                  .build();

          db.setDBOptions(mutableOptions);
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void rocksdbVersion() {
    final RocksDB.Version version = RocksDB.rocksdbVersion();
    assertThat(version).isNotNull();
    assertThat(version.getMajor()).isGreaterThan(1);
  }

  @Test
  public void isClosed() throws RocksDBException {
    final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
    assertThat(db.isClosed()).isFalse();
    db.close();
    assertThat(db.isClosed()).isTrue();
  }

  private static class InMemoryTraceWriter extends AbstractTraceWriter {
    private final List<byte[]> writes = new ArrayList<>();
    private volatile boolean closed = false;

    @Override
    public void write(final Slice slice) {
      if (closed) {
        return;
      }
      final byte[] data = slice.data();
      final byte[] dataCopy = new byte[data.length];
      System.arraycopy(data, 0, dataCopy, 0, data.length);
      writes.add(dataCopy);
    }

    @Override
    public void closeWriter() {
      closed = true;
    }

    @Override
    public long getFileSize() {
      long size = 0;
      for (final byte[] write : writes) {
        size += write.length;
      }
      return size;
    }

    public List<byte[]> getWrites() {
      return writes;
    }
  }
}
