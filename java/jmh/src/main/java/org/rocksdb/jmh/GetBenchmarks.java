/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 * This source code is licensed under both the GPLv2 (found in the
 * COPYING file in the root directory) and Apache 2.0 License
 * (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.jmh;

import static org.rocksdb.util.KVUtils.ba;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.FFIDB;
import org.rocksdb.FFIDB.GetOutputSlice;
import org.rocksdb.FFIDB.GetParams;
import org.rocksdb.FFIDB.GetPinnableSlice;
import org.rocksdb.FFILayout;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.FileUtils;

@State(Scope.Benchmark)
public class GetBenchmarks {

  @Param({
      "no_column_family",
      "1_column_family",
      "20_column_families",
      "100_column_families"
  })
  String columnFamilyTestType;

  @Param({"1000", "100000"}) int keyCount;

  @Param({"12", "64", "128"}) int keySize;

  @Param({"64", "1024", "65536"}) int valueSize;

  Path dbDir;
  DBOptions options;
  ReadOptions readOptions;
  int cfs = 0;  // number of column families
  private AtomicInteger cfHandlesIdx;
  ColumnFamilyHandle[] cfHandles;
  RocksDB db;
  FFIDB dbFFI;
  private final AtomicInteger keyIndex = new AtomicInteger();
  private ByteBuffer keyBuf;
  private ByteBuffer valueBuf;
  private byte[] keyArr;
  private byte[] valueArr;

  private MemorySegment keySegment;
  private GetParams getParams;
  private MemorySegment outputSlice;

  private MemorySegment valueSegment;

  @Setup(Level.Trial)
  public void setup() throws IOException, RocksDBException {
    RocksDB.loadLibrary();

    dbDir = Files.createTempDirectory("rocksjava-get-benchmarks");

    options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    readOptions = new ReadOptions();

    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

    if ("1_column_family".equals(columnFamilyTestType)) {
      cfs = 1;
    } else if ("20_column_families".equals(columnFamilyTestType)) {
      cfs = 20;
    } else if ("100_column_families".equals(columnFamilyTestType)) {
      cfs = 100;
    }

    if (cfs > 0) {
      cfHandlesIdx = new AtomicInteger(1);
      for (int i = 1; i <= cfs; i++) {
        cfDescriptors.add(new ColumnFamilyDescriptor(ba("cf" + i)));
      }
    }

    final List<ColumnFamilyHandle> cfHandlesList = new ArrayList<>(cfDescriptors.size());
    db = RocksDB.open(options, dbDir.toAbsolutePath().toString(), cfDescriptors, cfHandlesList);
    cfHandles = cfHandlesList.toArray(new ColumnFamilyHandle[0]);
    dbFFI = new FFIDB(db, cfHandlesList);

    // store initial data for retrieving via get
    keyArr = new byte[keySize];
    valueArr = new byte[valueSize];
    Arrays.fill(keyArr, (byte) 0x30);
    Arrays.fill(valueArr, (byte) 0x30);
    for (int i = 0; i <= cfs; i++) {
      for (int j = 0; j < keyCount; j++) {
        final byte[] keyPrefix = ba("key" + j);
        final byte[] valuePrefix = ba("value" + j);
        System.arraycopy(keyPrefix, 0, keyArr, 0, keyPrefix.length);
        System.arraycopy(valuePrefix, 0, valueArr, 0, valuePrefix.length);
        db.put(cfHandles[i], keyArr, valueArr);
      }
    }

    try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions);
    }

    keyBuf = ByteBuffer.allocateDirect(keySize);
    valueBuf = ByteBuffer.allocateDirect(valueSize);
    Arrays.fill(keyArr, (byte) 0x30);
    Arrays.fill(valueArr, (byte) 0x30);
    keyBuf.put(keyArr);
    keyBuf.flip();
    valueBuf.put(valueArr);
    valueBuf.flip();

    outputSlice = dbFFI.allocateSegment(valueSize);
    getParams = FFIDB.GetParams.create(dbFFI);
    keySegment = dbFFI.allocateSegment(keySize);
    valueSegment = dbFFI.allocateSegment(valueSize);

    keySegment.fill((byte) 0x30);
    valueSegment.fill((byte) 0x30);
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    try {
      for (final ColumnFamilyHandle cfHandle : cfHandles) {
        cfHandle.close();
      }
      db.close();
      options.close();
      readOptions.close();
    } finally {
      FileUtils.delete(dbDir);
    }
  }

  private ColumnFamilyHandle getColumnFamily() {
    if (cfs == 0) {
      return cfHandles[0];
    } else if (cfs == 1) {
      return cfHandles[1];
    } else {
      int idx = cfHandlesIdx.getAndIncrement();
      if (idx > cfs) {
        cfHandlesIdx.set(1); // doesn't ensure a perfect distribution, but it's ok
        idx = 0;
      }
      return cfHandles[idx];
    }
  }

  /**
   * Takes the next position in the index.
   */
  private int next() {
    int idx;
    int nextIdx;
    while (true) {
      idx = keyIndex.get();
      nextIdx = idx + 1;
      if (nextIdx >= keyCount) {
        nextIdx = 0;
      }

      if (keyIndex.compareAndSet(idx, nextIdx)) {
        break;
      }
    }
    return idx;
  }

  /*
   * Random within the key range
   */
  private int random() {
    return (int) (Math.random() * keyCount);
  }

  // String -> byte[]
  private byte[] getKeyArr() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = next();
    final byte[] keyPrefix = ba("key" + keyIdx);
    System.arraycopy(keyPrefix, 0, keyArr, 0, keyPrefix.length);
    Arrays.fill(keyArr, keyPrefix.length, MAX_LEN, (byte) 0x30);
    return keyArr;
  }

  // String -> byte[]
  private byte[] getRandomKeyArr() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = random();
    final byte[] keyPrefix = ba("key" + keyIdx);
    System.arraycopy(keyPrefix, 0, keyArr, 0, keyPrefix.length);
    Arrays.fill(keyArr, keyPrefix.length, MAX_LEN, (byte) 0x30);
    return keyArr;
  }

  // String -> ByteBuffer
  private ByteBuffer getKeyBuf() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = next();
    final String keyStr = "key" + keyIdx;
    for (int i = 0; i < keyStr.length(); ++i) {
      keyBuf.put(i, (byte) keyStr.charAt(i));
    }
    for (int i = keyStr.length(); i < MAX_LEN; ++i) {
      keyBuf.put(i, (byte) 0x30);
    }
    // Reset position for future reading
    keyBuf.position(0);
    return keyBuf;
  }

  // String -> ByteBuffer
  private ByteBuffer getRandomKeyBuf() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = random();
    final String keyStr = "key" + keyIdx;
    for (int i = 0; i < keyStr.length(); ++i) {
      keyBuf.put(i, (byte) keyStr.charAt(i));
    }
    for (int i = keyStr.length(); i < MAX_LEN; ++i) {
      keyBuf.put(i, (byte) 0x30);
    }
    // Reset position for future reading
    keyBuf.position(0);
    return keyBuf;
  }

  private MemorySegment getKeySegment() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = next();
    final String keyStr = "key" + keyIdx;
    for (int i = 0; i < keyStr.length(); ++i) {
      keySegment.set(ValueLayout.JAVA_BYTE, i, (byte) keyStr.charAt(i));
    }
    for (int i = keyStr.length(); i < MAX_LEN; ++i) {
      keySegment.set(ValueLayout.JAVA_BYTE, i, (byte) 0x30);
    }

    return keySegment;
  }

  private MemorySegment getRandomKeySegment() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = random();
    final String keyStr = "key" + keyIdx;
    for (int i = 0; i < keyStr.length(); ++i) {
      keySegment.set(ValueLayout.JAVA_BYTE, i, (byte) keyStr.charAt(i));
    }
    for (int i = keyStr.length(); i < MAX_LEN; ++i) {
      keySegment.set(ValueLayout.JAVA_BYTE, i, (byte) 0x30);
    }

    return keySegment;
  }

  private GetParams getGetParams() {
    return getParams;
  }

  private byte[] getValueArr() {
    return valueArr;
  }

  private ByteBuffer getValueBuf() {
    return valueBuf;
  }

  private MemorySegment getValueSegment() {
    return valueSegment;
  }

  @Benchmark
  public void get(Blackhole blackhole) throws RocksDBException {
    blackhole.consume(db.get(getColumnFamily(), getKeyArr()));
  }

  @Benchmark
  public void preallocatedGet(Blackhole blackhole) throws RocksDBException {
    byte[] value = getValueArr();
    int res = db.get(getColumnFamily(), getKeyArr(), value);
    blackhole.consume(value);
  }

  @Benchmark
  public void preallocatedByteBufferGet(Blackhole blackhole) throws RocksDBException {
    ByteBuffer value = getValueBuf();
    int res = db.get(getColumnFamily(), readOptions, getKeyBuf(), value);
    blackhole.consume(value);
    // For testing correctness:
    //    assert res > 0;
    //    final byte[] ret = new byte[valueSize];
    //    valueBuf.get(ret);
    //    System.out.println(str(ret));
    //    valueBuf.flip();
  }

  @Benchmark
  public void preallocatedByteBufferGetRandom(Blackhole blackhole) throws RocksDBException {
    ByteBuffer value = getValueBuf();
    int res = db.get(getColumnFamily(), readOptions, getRandomKeyBuf(), value);
    blackhole.consume(value);
    // For testing correctness:
    //    assert res > 0;
    //    final byte[] ret = new byte[valueSize];
    //    valueBuf.get(ret);
    //    System.out.println(str(ret));
    //    valueBuf.flip();
  }

  @Benchmark
  public void ffiPreallocatedGet(Blackhole blackhole) throws RocksDBException {
    byte[] value = getValueArr();
    dbFFI.get(getColumnFamily(), getKeySegment(), getGetParams(), value);
    blackhole.consume(value);
  }

  @Benchmark
  public void ffiPreallocatedGetRandom(Blackhole blackhole) throws RocksDBException {
    byte[] value = getValueArr();
    dbFFI.get(getColumnFamily(), getRandomKeySegment(), getGetParams(), value);
    blackhole.consume(value);
  }

  @Benchmark
  public void ffiGet(Blackhole blackhole) throws RocksDBException {
    blackhole.consume(dbFFI.get(getColumnFamily(), getKeySegment(), getGetParams()));
  }

  @Benchmark
  public void ffiGetRandom(Blackhole blackhole) throws RocksDBException {
    blackhole.consume(dbFFI.get(getColumnFamily(), getRandomKeySegment(), getGetParams()));
  }

  @Benchmark
  public void ffiGetPinnableSlice(Blackhole blackhole) throws RocksDBException {
    final GetPinnableSlice result =
        dbFFI.getPinnableSlice(readOptions, getColumnFamily(), getKeySegment(), getGetParams());
    blackhole.consume(result);
    result.pinnableSlice().get().reset();
  }

  @Benchmark
  public void ffiGetOutputSlice(Blackhole blackhole) throws RocksDBException {
    final GetOutputSlice result =
        dbFFI.getOutputSlice(readOptions, getColumnFamily(), outputSlice, getKeySegment());
    blackhole.consume(result);
    blackhole.consume(outputSlice);
  }

  @Benchmark
  public void ffiIdentity(Blackhole blackhole) throws RocksDBException {
    final int result = dbFFI.identity((int) (Math.random() * 256));

    blackhole.consume(result);
  }
}