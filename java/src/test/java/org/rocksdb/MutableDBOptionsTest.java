// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.MutableDBOptions.MutableDBOptionsBuilder;

public class MutableDBOptionsTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void builder() {
    final MutableDBOptionsBuilder builder =
        MutableDBOptions.builder();
        builder
            .setBytesPerSync(1024 * 1024 * 7)
            .setMaxBackgroundJobs(5)
            .setAvoidFlushDuringShutdown(false);

    assertThat(builder.bytesPerSync()).isEqualTo(1024 * 1024 * 7);
    assertThat(builder.maxBackgroundJobs()).isEqualTo(5);
    assertThat(builder.avoidFlushDuringShutdown()).isEqualTo(false);
  }

  @Test(expected = NoSuchElementException.class)
  public void builder_getWhenNotSet() {
    final MutableDBOptionsBuilder builder =
        MutableDBOptions.builder();

    builder.bytesPerSync();
  }

  @Test
  public void builder_build() {
    final MutableDBOptions options = MutableDBOptions
        .builder()
          .setBytesPerSync(1024 * 1024 * 7)
          .setMaxBackgroundJobs(5)
          .build();

    assertThat(options.getKeys().length).isEqualTo(2);
    assertThat(options.getValues().length).isEqualTo(2);
    assertThat(options.getKeys()[0])
        .isEqualTo(
            MutableDBOptions.DBOption.bytes_per_sync.name());
    assertThat(options.getValues()[0]).isEqualTo("7340032");
    assertThat(options.getKeys()[1])
        .isEqualTo(
            MutableDBOptions.DBOption.max_background_jobs.name());
    assertThat(options.getValues()[1]).isEqualTo("5");
  }

  @Test
  public void mutableDBOptions_toString() {
    final String str = MutableDBOptions
        .builder()
        .setMaxOpenFiles(99)
        .setDelayedWriteRate(789)
        .setAvoidFlushDuringShutdown(true)
        .setStrictBytesPerSync(true)
        .build()
        .toString();

    assertThat(str).isEqualTo("max_open_files=99;delayed_write_rate=789;"
        + "avoid_flush_during_shutdown=true;strict_bytes_per_sync=true");
  }

  @Test
  public void mutableDBOptions_parse() {
    final String str = "max_open_files=99;delayed_write_rate=789;"
        + "avoid_flush_during_shutdown=true;"
        + "daily_offpeak_time_utc=02\\:20-19\\:50";

    final MutableDBOptionsBuilder builder =
        MutableDBOptions.parse(str);

    assertThat(builder.maxOpenFiles()).isEqualTo(99);
    assertThat(builder.delayedWriteRate()).isEqualTo(789);
    assertThat(builder.avoidFlushDuringShutdown()).isEqualTo(true);
    assertThat(builder.dailyOffpeakTimeUTC()).isEqualTo("02:20-19:50");
  }

  @Test
  public void listDBOptions() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setDailyOffpeakTimeUTC("23:00-05:30");
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      // Test listColumnFamilies
      MutableDBOptionsBuilder builder = db.getDBOptions();
      assertThat(builder.maxOpenFiles()).isEqualTo(-1);
      assertThat(builder.avoidFlushDuringShutdown()).isEqualTo(false);
      assertThat(builder.dailyOffpeakTimeUTC()).isEqualTo("23:00-05:30");
    }
  }

  @Test
  public void listDBOptions2() throws RocksDBException {
    List<ColumnFamilyDescriptor> cfd = new ArrayList<>();
    cfd.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
    List<ColumnFamilyHandle> cfh = new ArrayList<>();
    try (final DBOptions dbOptions =
             new DBOptions().setCreateIfMissing(true).setDailyOffpeakTimeUTC("23:00-05:30");
         final RocksDB db =
             RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(), cfd, cfh)) {
      // Test listColumnFamilies
      MutableDBOptionsBuilder builder = db.getDBOptions();
      assertThat(builder.maxOpenFiles()).isEqualTo(-1);
      assertThat(builder.avoidFlushDuringShutdown()).isEqualTo(false);
      assertThat(builder.dailyOffpeakTimeUTC()).isEqualTo("23:00-05:30");
    }
  }
}
