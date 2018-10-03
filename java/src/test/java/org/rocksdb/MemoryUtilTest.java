// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class MemoryUtilTest {

  private static final String MEMTABLE_SIZE = "rocksdb.size-all-mem-tables";
  private static final String UNFLUSHED_MEMTABLE_SIZE = "rocksdb.cur-size-all-mem-tables";
  private static final String TABLE_READERS = "rocksdb.estimate-table-readers-mem";

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void getApproximateMemoryUsageByType() throws RocksDBException {
    try (final Cache cache = new LRUCache(8 * 1024 * 1024);
         final Statistics statistics = new Statistics();
         final Options options =
                 new Options()
                         .setCreateIfMissing(true)
                         .setStatistics(statistics)
                         .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache));
         final FlushOptions flushOptions =
                 new FlushOptions().setWaitForFlush(true);
         final RocksDB db =
                 RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      final byte[] key = "some-key".getBytes(StandardCharsets.UTF_8);
      final byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

      List<RocksDB> dbs = new ArrayList<>(1);
      dbs.add(db);
      Set<Cache> caches = new HashSet<>(1);
      caches.add(cache);
      Map<MemoryUsageType, Long> usage = MemoryUtil.getApproximateMemoryUsageByType(dbs, caches);

      assertThat(usage.get(MemoryUsageType.kMemTableTotal)).isEqualTo(
              db.getAggregatedLongProperty(MEMTABLE_SIZE));
      assertThat(usage.get(MemoryUsageType.kMemTableUnFlushed)).isEqualTo(
              db.getAggregatedLongProperty(UNFLUSHED_MEMTABLE_SIZE));
      assertThat(usage.get(MemoryUsageType.kTableReadersTotal)).isEqualTo(
              db.getAggregatedLongProperty(TABLE_READERS));
      assertThat(usage.get(MemoryUsageType.kCacheTotal)).isEqualTo(0);

      db.put(key, value);
      db.flush(flushOptions);
      db.get(key);

      usage = MemoryUtil.getApproximateMemoryUsageByType(dbs, caches);
      assertThat(usage.get(MemoryUsageType.kMemTableTotal)).isGreaterThan(0);
      assertThat(usage.get(MemoryUsageType.kMemTableTotal)).isEqualTo(
              db.getAggregatedLongProperty(MEMTABLE_SIZE));
      assertThat(usage.get(MemoryUsageType.kMemTableUnFlushed)).isGreaterThan(0);
      assertThat(usage.get(MemoryUsageType.kMemTableUnFlushed)).isEqualTo(
              db.getAggregatedLongProperty(UNFLUSHED_MEMTABLE_SIZE));
      assertThat(usage.get(MemoryUsageType.kTableReadersTotal)).isGreaterThan(0);
      assertThat(usage.get(MemoryUsageType.kTableReadersTotal)).isEqualTo(
              db.getAggregatedLongProperty(TABLE_READERS));
      assertThat(usage.get(MemoryUsageType.kCacheTotal)).isGreaterThan(0);

    }
  }

}
