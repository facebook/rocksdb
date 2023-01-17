/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.jmh;

import static org.rocksdb.util.KVUtils.ba;
import static org.rocksdb.util.KVUtils.keys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.*;
import org.rocksdb.util.FileUtils;

@State(Scope.Thread)
public class MultiGetBenchmarks {
  @Param({
      "no_column_family",
      "1_column_family",
      "20_column_families",
      "100_column_families"
  })
  String columnFamilyTestType;

  @Param({"10000", "25000", "100000"}) int keyCount;

  @Param({
          "10",
          "100",
          "1000",
          "10000",
  })
  int multiGetSize;

  @Param({"16", "64", "250", "1000", "4000", "16000"}) int valueSize;
  @Param({"16"}) int keySize; // big enough

  Path dbDir;
  DBOptions options;
  int cfs = 0;  // number of column families
  private AtomicInteger cfHandlesIdx;
  ColumnFamilyHandle[] cfHandles;
  RocksDB db;
  private final AtomicInteger keyIndex = new AtomicInteger();

  @Setup(Level.Trial)
  public void setup() throws IOException, RocksDBException {
    RocksDB.loadLibrary();

    dbDir = Files.createTempDirectory("rocksjava-multiget-benchmarks");

    options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

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

    // store initial data for retrieving via get
    for (int i = 0; i < cfs; i++) {
      for (int j = 0; j < keyCount; j++) {
        final byte[] paddedValue = Arrays.copyOf(ba("value" + j), valueSize);
        db.put(cfHandles[i], ba("key" + j), paddedValue);
      }
    }

    try (final FlushOptions flushOptions = new FlushOptions()
            .setWaitForFlush(true)) {
      db.flush(flushOptions);
    }
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    for (final ColumnFamilyHandle cfHandle : cfHandles) {
      cfHandle.close();
    }
    db.close();
    options.close();
    FileUtils.delete(dbDir);
  }

  private ColumnFamilyHandle getColumnFamily() {
    if (cfs == 0) {
      return cfHandles[0];
    }  else if (cfs == 1) {
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
   * Reserves the next {@inc} positions in the index.
   *
   * @param inc the number by which to increment the index
   * @param limit the limit for the index
   * @return the index before {@code inc} is added
   */
  private int next(final int inc, final int limit) {
    int idx;
    int nextIdx;
    while (true) {
      idx = keyIndex.get();
      nextIdx = idx + inc;
      if (nextIdx >= limit) {
          nextIdx = inc;
      }

      if (keyIndex.compareAndSet(idx, nextIdx)) {
        break;
      }
    }

    if (nextIdx >= limit) {
      return -1;
    } else {
      return idx;
    }
  }

  ByteBuffer keysBuffer;
  ByteBuffer valuesBuffer;

  List<ByteBuffer> valueBuffersList;
  List<ByteBuffer> keyBuffersList;

  @Setup
  public void allocateSliceBuffers() {
    keysBuffer = ByteBuffer.allocateDirect(keyCount * valueSize);
    valuesBuffer = ByteBuffer.allocateDirect(keyCount * valueSize);
    valueBuffersList = new ArrayList<>();
    keyBuffersList = new ArrayList<>();
    for (int i = 0; i < keyCount; i++) {
      valueBuffersList.add(valuesBuffer.slice());
      valuesBuffer.position(i * valueSize);
      keyBuffersList.add(keysBuffer.slice());
      keysBuffer.position(i * keySize);
    }
  }

  @TearDown
  public void freeSliceBuffers() {
    valueBuffersList.clear();
  }

  @Benchmark
  public List<byte[]> multiGet10() throws RocksDBException {
    final int fromKeyIdx = next(multiGetSize, keyCount);
    if (fromKeyIdx >= 0) {
      final List<byte[]> keys = keys(fromKeyIdx, fromKeyIdx + multiGetSize);
      final List<byte[]> valueResults = db.multiGetAsList(keys);
      for (final byte[] result : valueResults) {
        if (result.length != valueSize)
          throw new RuntimeException("Test valueSize assumption wrong");
      }
    }
    return new ArrayList<>();
  }

  public static void main(final String[] args) throws RunnerException {
    final org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder()
            .include(MultiGetBenchmarks.class.getSimpleName())
            .forks(1)
            .jvmArgs("-ea")
            .warmupIterations(1)
            .measurementIterations(2)
            .forks(2)
            .param("columnFamilyTestType=", "1_column_family")
            .param("multiGetSize=", "10", "1000")
            .param("keyCount=", "1000")
            .output("jmh_output")
            .build();

    new Runner(opt).run();
  }
}
