/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 * This source code is licensed under both the GPLv2 (found in the
 * COPYING file in the root directory) and Apache 2.0 License
 * (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.jmh;

import static org.rocksdb.util.KVUtils.ba;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.rocksdb.*;
import org.rocksdb.util.FileUtils;

@State(Scope.Benchmark)
public class GetNotFoundBenchmarks {
  @Param({"no_column_family", "20_column_families"}) String columnFamilyTestType;

  @Param({"1000", "100000"}) int keyCount;
  private final static int extraKeyCount = 16; // some slack to deal with odd/even tests rounding up key values

  @Param({"12"}) int keySize;

  @Param({"16"}) int valueSize;

  /**
   * Don't create every n-th key
   * Usually, just use "2" for making the half-present array
   * 0 means no missing keys
   */
  @Param({"2", "16", "0"}) int nthMissingKey;

  Path dbDir;
  DBOptions options;
  ReadOptions readOptions;
  int cfs = 0; // number of column families
  private AtomicInteger cfHandlesIdx;
  ColumnFamilyHandle[] cfHandles;
  RocksDB db;
  private final AtomicInteger keyIndex = new AtomicInteger();
  private ByteBuffer keyBuf;
  private ByteBuffer valueBuf;
  private byte[] keyArr;
  private byte[] valueArr;

  @Setup(Level.Trial)
  public void setup() throws IOException, RocksDBException {
    RocksDB.loadLibrary();

    dbDir = Files.createTempDirectory("rocksjava-get-benchmarks");

    options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
    readOptions = new ReadOptions();

    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

    if ("20_column_families".equals(columnFamilyTestType)) {
      cfs = 20;
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

    // store initial data for retrieving via get
    keyArr = new byte[keySize];
    valueArr = new byte[valueSize];
    Arrays.fill(keyArr, (byte) 0x30);
    Arrays.fill(valueArr, (byte) 0x30);
    for (int i = 0; i <= cfs; i++) {
      for (int j = 0; j < keyCount; j++) {
        if (nthMissingKey <= 0 || j % nthMissingKey != 0) {
          final byte[] keyPrefix = ba("key" + j);
          final byte[] valuePrefix = ba("value" + j);
          System.arraycopy(keyPrefix, 0, keyArr, 0, keyPrefix.length);
          System.arraycopy(valuePrefix, 0, valueArr, 0, valuePrefix.length);
          db.put(cfHandles[i], keyArr, valueArr);
        }
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
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    for (final ColumnFamilyHandle cfHandle : cfHandles) {
      cfHandle.close();
    }
    db.close();
    options.close();
    readOptions.close();
    FileUtils.delete(dbDir);
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
  private int next(final int increment) {
    int idx;
    int nextIdx;
    while (true) {
      idx = keyIndex.get();
      nextIdx = idx + increment;
      if (nextIdx >= keyCount) {
        nextIdx = nextIdx % keyCount;
      }
      if (keyIndex.compareAndSet(idx, nextIdx)) {
        break;
      }
    }

    return idx;
  }

  private int next() {
    return next(1);
  }

  // String -> byte[]
  private byte[] getKeyArr(final int keyOffset, final int increment) {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = next(increment);
    final byte[] keyPrefix = ba("key" + (keyIdx + keyOffset));
    System.arraycopy(keyPrefix, 0, keyArr, 0, keyPrefix.length);
    Arrays.fill(keyArr, keyPrefix.length, MAX_LEN, (byte) 0x30);
    return keyArr;
  }

  /**
   * Get even values, these should not be present
   * @throws RocksDBException
   */
  @Benchmark
  public void getNotFoundEven(Blackhole blackhole) throws RocksDBException {
    blackhole.consume(db.get(getColumnFamily(), getKeyArr(0,2)));
  }

  /**
   * Get odd values 1, 3, 5, these should be present
   * @throws RocksDBException
   */
  @Benchmark
  public void getNotFoundOdd(Blackhole blackhole) throws RocksDBException {
    blackhole.consume(db.get(getColumnFamily(), getKeyArr(1,2)));
  }
}