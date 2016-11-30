// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class DBOptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void getDBOptionsFromProps() {
    // setup sample properties
    final Properties properties = new Properties();
    properties.put("allow_mmap_reads", "true");
    properties.put("bytes_per_sync", "13");
    try(final DBOptions opt = DBOptions.getDBOptionsFromProps(properties)) {
      assertThat(opt).isNotNull();
      assertThat(String.valueOf(opt.allowMmapReads())).
          isEqualTo(properties.get("allow_mmap_reads"));
      assertThat(String.valueOf(opt.bytesPerSync())).
          isEqualTo(properties.get("bytes_per_sync"));
    }
  }

  @Test
  public void failDBOptionsFromPropsWithIllegalValue() {
    // setup sample properties
    final Properties properties = new Properties();
    properties.put("tomato", "1024");
    properties.put("burger", "2");
    try(final DBOptions opt = DBOptions.getDBOptionsFromProps(properties)) {
      assertThat(opt).isNull();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failDBOptionsFromPropsWithNullValue() {
    try(final DBOptions opt = DBOptions.getDBOptionsFromProps(null)) {
      //no-op
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failDBOptionsFromPropsWithEmptyProps() {
    try(final DBOptions opt = DBOptions.getDBOptionsFromProps(
        new Properties())) {
      //no-op
    }
  }

  @Test
  public void setIncreaseParallelism() {
    try(final DBOptions opt = new DBOptions()) {
      final int threads = Runtime.getRuntime().availableProcessors() * 2;
      opt.setIncreaseParallelism(threads);
    }
  }

  @Test
  public void createIfMissing() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setCreateIfMissing(boolValue);
      assertThat(opt.createIfMissing()).isEqualTo(boolValue);
    }
  }

  @Test
  public void createMissingColumnFamilies() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setCreateMissingColumnFamilies(boolValue);
      assertThat(opt.createMissingColumnFamilies()).isEqualTo(boolValue);
    }
  }

  @Test
  public void errorIfExists() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setErrorIfExists(boolValue);
      assertThat(opt.errorIfExists()).isEqualTo(boolValue);
    }
  }

  @Test
  public void paranoidChecks() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setParanoidChecks(boolValue);
      assertThat(opt.paranoidChecks()).isEqualTo(boolValue);
    }
  }

  @Test
  public void maxTotalWalSize() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxTotalWalSize(longValue);
      assertThat(opt.maxTotalWalSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxOpenFiles() {
    try(final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setMaxOpenFiles(intValue);
      assertThat(opt.maxOpenFiles()).isEqualTo(intValue);
    }
  }

  @Test
  public void disableDataSync() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setDisableDataSync(boolValue);
      assertThat(opt.disableDataSync()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useFsync() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseFsync(boolValue);
      assertThat(opt.useFsync()).isEqualTo(boolValue);
    }
  }

  @Test
  public void dbLogDir() {
    try(final DBOptions opt = new DBOptions()) {
      final String str = "path/to/DbLogDir";
      opt.setDbLogDir(str);
      assertThat(opt.dbLogDir()).isEqualTo(str);
    }
  }

  @Test
  public void walDir() {
    try(final DBOptions opt = new DBOptions()) {
      final String str = "path/to/WalDir";
      opt.setWalDir(str);
      assertThat(opt.walDir()).isEqualTo(str);
    }
  }

  @Test
  public void deleteObsoleteFilesPeriodMicros() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setDeleteObsoleteFilesPeriodMicros(longValue);
      assertThat(opt.deleteObsoleteFilesPeriodMicros()).isEqualTo(longValue);
    }
  }

  @Test
  public void baseBackgroundCompactions() {
    try (final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setBaseBackgroundCompactions(intValue);
      assertThat(opt.baseBackgroundCompactions()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void maxBackgroundCompactions() {
    try(final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setMaxBackgroundCompactions(intValue);
      assertThat(opt.maxBackgroundCompactions()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxSubcompactions() {
    try (final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setMaxSubcompactions(intValue);
      assertThat(opt.maxSubcompactions()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void maxBackgroundFlushes() {
    try(final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setMaxBackgroundFlushes(intValue);
      assertThat(opt.maxBackgroundFlushes()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxLogFileSize() throws RocksDBException {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxLogFileSize(longValue);
      assertThat(opt.maxLogFileSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void logFileTimeToRoll() throws RocksDBException {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setLogFileTimeToRoll(longValue);
      assertThat(opt.logFileTimeToRoll()).isEqualTo(longValue);
    }
  }

  @Test
  public void keepLogFileNum() throws RocksDBException {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setKeepLogFileNum(longValue);
      assertThat(opt.keepLogFileNum()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxManifestFileSize() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxManifestFileSize(longValue);
      assertThat(opt.maxManifestFileSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void tableCacheNumshardbits() {
    try(final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setTableCacheNumshardbits(intValue);
      assertThat(opt.tableCacheNumshardbits()).isEqualTo(intValue);
    }
  }

  @Test
  public void walSizeLimitMB() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setWalSizeLimitMB(longValue);
      assertThat(opt.walSizeLimitMB()).isEqualTo(longValue);
    }
  }

  @Test
  public void walTtlSeconds() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setWalTtlSeconds(longValue);
      assertThat(opt.walTtlSeconds()).isEqualTo(longValue);
    }
  }

  @Test
  public void manifestPreallocationSize() throws RocksDBException {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setManifestPreallocationSize(longValue);
      assertThat(opt.manifestPreallocationSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void allowOsBuffer() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowOsBuffer(boolValue);
      assertThat(opt.allowOsBuffer()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowMmapReads() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapReads(boolValue);
      assertThat(opt.allowMmapReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowMmapWrites() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapWrites(boolValue);
      assertThat(opt.allowMmapWrites()).isEqualTo(boolValue);
    }
  }

  @Test
  public void isFdCloseOnExec() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setIsFdCloseOnExec(boolValue);
      assertThat(opt.isFdCloseOnExec()).isEqualTo(boolValue);
    }
  }

  @Test
  public void statsDumpPeriodSec() {
    try(final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setStatsDumpPeriodSec(intValue);
      assertThat(opt.statsDumpPeriodSec()).isEqualTo(intValue);
    }
  }

  @Test
  public void adviseRandomOnOpen() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAdviseRandomOnOpen(boolValue);
      assertThat(opt.adviseRandomOnOpen()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useAdaptiveMutex() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseAdaptiveMutex(boolValue);
      assertThat(opt.useAdaptiveMutex()).isEqualTo(boolValue);
    }
  }

  @Test
  public void bytesPerSync() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setBytesPerSync(longValue);
      assertThat(opt.bytesPerSync()).isEqualTo(longValue);
    }
  }

  @Test
  public void allowConcurrentMemtableWrite() {
    try (final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowConcurrentMemtableWrite(boolValue);
      assertThat(opt.allowConcurrentMemtableWrite()).isEqualTo(boolValue);
    }
  }

  @Test
  public void enableWriteThreadAdaptiveYield() {
    try (final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setEnableWriteThreadAdaptiveYield(boolValue);
      assertThat(opt.enableWriteThreadAdaptiveYield()).isEqualTo(boolValue);
    }
  }

  @Test
  public void writeThreadMaxYieldUsec() {
    try (final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setWriteThreadMaxYieldUsec(longValue);
      assertThat(opt.writeThreadMaxYieldUsec()).isEqualTo(longValue);
    }
  }

  @Test
  public void writeThreadSlowYieldUsec() {
    try (final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setWriteThreadSlowYieldUsec(longValue);
      assertThat(opt.writeThreadSlowYieldUsec()).isEqualTo(longValue);
    }
  }

  @Test
  public void rateLimiterConfig() {
    try(final DBOptions options = new DBOptions();
        final DBOptions anotherOptions = new DBOptions()) {
      final RateLimiterConfig rateLimiterConfig =
          new GenericRateLimiterConfig(1000, 100 * 1000, 1);
      options.setRateLimiterConfig(rateLimiterConfig);
      // Test with parameter initialization

      anotherOptions.setRateLimiterConfig(
          new GenericRateLimiterConfig(1000));
    }
  }

  @Test
  public void rateLimiter() {
    try(final DBOptions options = new DBOptions();
        final DBOptions anotherOptions = new DBOptions()) {
      final RateLimiter rateLimiter = new RateLimiter(1000, 100 * 1000, 1);
      options.setRateLimiter(rateLimiter);
      // Test with parameter initialization
      anotherOptions.setRateLimiter(
          new RateLimiter(1000));
    }
  }

  @Test
  public void statistics() {
    try(final DBOptions options = new DBOptions()) {
      Statistics statistics = options.createStatistics().
          statisticsPtr();
      assertThat(statistics).isNotNull();

      try(final DBOptions anotherOptions = new DBOptions()) {
        statistics = anotherOptions.statisticsPtr();
        assertThat(statistics).isNotNull();
      }
    }
  }
}
