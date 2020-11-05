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

      "java_bytewise_non-direct_reused-64_adaptive-mutex",
      "java_bytewise_non-direct_reused-64_non-adaptive-mutex",
      "java_bytewise_non-direct_reused-64_thread-local",
      "java_bytewise_direct_reused-64_adaptive-mutex",
      "java_bytewise_direct_reused-64_non-adaptive-mutex",
      "java_bytewise_direct_reused-64_thread-local",
      "java_bytewise_non-direct_no-reuse",
      "java_bytewise_direct_no-reuse",

      "java_reverse_bytewise_non-direct_reused-64_adaptive-mutex",
      "java_reverse_bytewise_non-direct_reused-64_non-adaptive-mutex",
      "java_reverse_bytewise_non-direct_reused-64_thread-local",
      "java_reverse_bytewise_direct_reused-64_adaptive-mutex",
      "java_reverse_bytewise_direct_reused-64_non-adaptive-mutex",
      "java_reverse_bytewise_direct_reused-64_thread-local",
      "java_reverse_bytewise_non-direct_no-reuse",
      "java_reverse_bytewise_direct_no-reuse"
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

    if ("native_bytewise".equals(comparatorName)) {
      options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);

    } else if ("native_reverse_bytewise".equals(comparatorName)) {
      options.setComparator(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);

    } else if (comparatorName.startsWith("java_")) {
      comparatorOptions = new ComparatorOptions();

      if (comparatorName.indexOf("non-direct") > -1) {
        comparatorOptions.setUseDirectBuffer(false);
      } else if (comparatorName.indexOf("direct") > -1) {
        comparatorOptions.setUseDirectBuffer(true);
      }

      if (comparatorName.indexOf("no-reuse") > -1) {
        comparatorOptions.setMaxReusedBufferSize(-1);
      } else if (comparatorName.indexOf("_reused-") > -1) {
        final int idx = comparatorName.indexOf("_reused-");
        String s = comparatorName.substring(idx + 8);
        s = s.substring(0, s.indexOf('_'));
        comparatorOptions.setMaxReusedBufferSize(Integer.parseInt(s));
      }

      if (comparatorName.indexOf("non-adaptive-mutex") > -1) {
        comparatorOptions.setReusedSynchronisationType(ReusedSynchronisationType.MUTEX);
      } else if (comparatorName.indexOf("adaptive-mutex") > -1) {
        comparatorOptions.setReusedSynchronisationType(ReusedSynchronisationType.ADAPTIVE_MUTEX);
      } else if (comparatorName.indexOf("thread-local") > -1) {
        comparatorOptions.setReusedSynchronisationType(ReusedSynchronisationType.THREAD_LOCAL);
      }

      if (comparatorName.startsWith("java_bytewise")) {
        comparator = new BytewiseComparator(comparatorOptions);
      } else if (comparatorName.startsWith("java_reverse_bytewise")) {
        comparator = new ReverseBytewiseComparator(comparatorOptions);
      }

      options.setComparator(comparator);

    } else {
      throw new IllegalArgumentException("Unknown comparatorName: " + comparatorName);
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
