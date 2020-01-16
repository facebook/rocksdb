/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.jmh;

import org.openjdk.jmh.annotations.*;
import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;
import org.rocksdb.util.DirectBytewiseComparator;
import org.rocksdb.util.FileUtils;
import org.rocksdb.util.ReverseBytewiseComparator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.rocksdb.util.KVUtils.ba;

@State(Scope.Benchmark)
public class ComparatorBenchmarks {

  @Param({
      "native_bytewise",
      "native_reverse_bytewise",
      "java_bytewise_adaptive_mutex",
      "java_bytewise_non-adaptive_mutex",
      "java_reverse_bytewise_adaptive_mutex",
      "java_reverse_bytewise_non-adaptive_mutex",
      "java_direct_bytewise_adaptive_mutex",
      "java_direct_bytewise_non-adaptive_mutex"

  })
  public String comparatorName;

  Path dbDir;
  ComparatorOptions comparatorOptions;
  AbstractComparator comparator;
  Options options;
  RocksDB db;

  @Setup(Level.Trial)
  public void setup() throws IOException, RocksDBException {
    RocksDB.loadLibrary();

    dbDir = Files.createTempDirectory("rocksjava-comparator-benchmarks");

    options = new Options()
            .setCreateIfMissing(true);
    if (comparatorName == null || "native_bytewise".equals(comparatorName)) {
      options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
    } else if ("native_reverse_bytewise".equals(comparatorName)) {
      options.setComparator(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);
    } else if ("java_bytewise_adaptive_mutex".equals(comparatorName)) {
      comparatorOptions = new ComparatorOptions()
              .setUseAdaptiveMutex(true);
      comparator = new BytewiseComparator(comparatorOptions);
      options.setComparator(comparator);
    } else if ("java_bytewise_non-adaptive_mutex".equals(comparatorName)) {
      comparatorOptions = new ComparatorOptions()
              .setUseAdaptiveMutex(false);
      comparator = new BytewiseComparator(comparatorOptions);
      options.setComparator(comparator);
    } else if ("java_reverse_bytewise_adaptive_mutex".equals(comparatorName)) {
      comparatorOptions = new ComparatorOptions()
              .setUseAdaptiveMutex(true);
      comparator = new ReverseBytewiseComparator(comparatorOptions);
      options.setComparator(comparator);
    } else if ("java_reverse_bytewise_non-adaptive_mutex".equals(comparatorName)) {
      comparatorOptions = new ComparatorOptions()
              .setUseAdaptiveMutex(false);
      comparator = new ReverseBytewiseComparator(comparatorOptions);
      options.setComparator(comparator);
    } else if ("java_direct_bytewise_adaptive_mutex".equals(comparatorName)) {
      comparatorOptions = new ComparatorOptions()
              .setUseAdaptiveMutex(true);
      comparator = new DirectBytewiseComparator(comparatorOptions);
      options.setComparator(comparator);
    } else if ("java_direct_bytewise_non-adaptive_mutex".equals(comparatorName)) {
      comparatorOptions = new ComparatorOptions()
              .setUseAdaptiveMutex(false);
      comparator = new DirectBytewiseComparator(comparatorOptions);
      options.setComparator(comparator);
    } else {
      throw new IllegalArgumentException("Unknown comparator name: " + comparatorName);
    }

    db = RocksDB.open(options, dbDir.toAbsolutePath().toString());
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    db.close();
    if (comparator != null) {
      comparator.close();
    }
    if (comparatorOptions != null) {
      comparatorOptions.close();
    }
    options.close();
    FileUtils.delete(dbDir);
  }

  @State(Scope.Benchmark)
  public static class Counter {
    private final AtomicInteger count = new AtomicInteger();

    public int next() {
      return count.getAndIncrement();
    }
  }


  @Benchmark
  public void put(final Counter counter) throws RocksDBException {
    final int i = counter.next();
    db.put(ba("key" + i), ba("value" + i));
  }
}
