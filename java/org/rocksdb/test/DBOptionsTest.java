// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.junit.ClassRule;
import org.junit.Test;
import org.rocksdb.*;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class DBOptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void dbOptions() throws RocksDBException {
    testDBOptions(new DBOptions());
  }

  static void testDBOptions(DBOptionsInterface opt) throws RocksDBException {
    Random rand = PlatformRandomHelper.
        getPlatformSpecificRandomFactory();
    { // CreateIfMissing test
      boolean boolValue = rand.nextBoolean();
      opt.setCreateIfMissing(boolValue);
      assertThat(opt.createIfMissing()).
          isEqualTo(boolValue);
    }

    { // CreateMissingColumnFamilies test
      boolean boolValue = rand.nextBoolean();
      opt.setCreateMissingColumnFamilies(boolValue);
      assertThat(opt.createMissingColumnFamilies()).
          isEqualTo(boolValue);
    }

    { // ErrorIfExists test
      boolean boolValue = rand.nextBoolean();
      opt.setErrorIfExists(boolValue);
      assertThat(opt.errorIfExists()).isEqualTo(boolValue);
    }

    { // ParanoidChecks test
      boolean boolValue = rand.nextBoolean();
      opt.setParanoidChecks(boolValue);
      assertThat(opt.paranoidChecks()).
          isEqualTo(boolValue);
    }

    {
      // MaxTotalWalSize test
      long longValue = rand.nextLong();
      opt.setMaxTotalWalSize(longValue);
      assertThat(opt.maxTotalWalSize()).
          isEqualTo(longValue);
    }

    { // MaxOpenFiles test
      int intValue = rand.nextInt();
      opt.setMaxOpenFiles(intValue);
      assertThat(opt.maxOpenFiles()).isEqualTo(intValue);
    }

    { // DisableDataSync test
      boolean boolValue = rand.nextBoolean();
      opt.setDisableDataSync(boolValue);
      assertThat(opt.disableDataSync()).
          isEqualTo(boolValue);
    }

    { // UseFsync test
      boolean boolValue = rand.nextBoolean();
      opt.setUseFsync(boolValue);
      assertThat(opt.useFsync()).isEqualTo(boolValue);
    }

    { // DbLogDir test
      String str = "path/to/DbLogDir";
      opt.setDbLogDir(str);
      assertThat(opt.dbLogDir()).isEqualTo(str);
    }

    { // WalDir test
      String str = "path/to/WalDir";
      opt.setWalDir(str);
      assertThat(opt.walDir()).isEqualTo(str);
    }

    { // DeleteObsoleteFilesPeriodMicros test
      long longValue = rand.nextLong();
      opt.setDeleteObsoleteFilesPeriodMicros(longValue);
      assertThat(opt.deleteObsoleteFilesPeriodMicros()).
          isEqualTo(longValue);
    }

    { // MaxBackgroundCompactions test
      int intValue = rand.nextInt();
      opt.setMaxBackgroundCompactions(intValue);
      assertThat(opt.maxBackgroundCompactions()).
          isEqualTo(intValue);
    }

    { // MaxBackgroundFlushes test
      int intValue = rand.nextInt();
      opt.setMaxBackgroundFlushes(intValue);
      assertThat(opt.maxBackgroundFlushes()).
          isEqualTo(intValue);
    }

    { // MaxLogFileSize test
      long longValue = rand.nextLong();
      opt.setMaxLogFileSize(longValue);
      assertThat(opt.maxLogFileSize()).isEqualTo(longValue);
    }

    { // LogFileTimeToRoll test
      long longValue = rand.nextLong();
      opt.setLogFileTimeToRoll(longValue);
      assertThat(opt.logFileTimeToRoll()).
          isEqualTo(longValue);
    }

    { // KeepLogFileNum test
      long longValue = rand.nextLong();
      opt.setKeepLogFileNum(longValue);
      assertThat(opt.keepLogFileNum()).isEqualTo(longValue);
    }

    { // MaxManifestFileSize test
      long longValue = rand.nextLong();
      opt.setMaxManifestFileSize(longValue);
      assertThat(opt.maxManifestFileSize()).
          isEqualTo(longValue);
    }

    { // TableCacheNumshardbits test
      int intValue = rand.nextInt();
      opt.setTableCacheNumshardbits(intValue);
      assertThat(opt.tableCacheNumshardbits()).
          isEqualTo(intValue);
    }

    { // TableCacheRemoveScanCountLimit test
      int intValue = rand.nextInt();
      opt.setTableCacheRemoveScanCountLimit(intValue);
      assertThat(opt.tableCacheRemoveScanCountLimit()).
          isEqualTo(intValue);
    }

    { // WalSizeLimitMB test
      long longValue = rand.nextLong();
      opt.setWalSizeLimitMB(longValue);
      assertThat(opt.walSizeLimitMB()).isEqualTo(longValue);
    }

    { // WalTtlSeconds test
      long longValue = rand.nextLong();
      opt.setWalTtlSeconds(longValue);
      assertThat(opt.walTtlSeconds()).isEqualTo(longValue);
    }

    { // ManifestPreallocationSize test
      long longValue = rand.nextLong();
      opt.setManifestPreallocationSize(longValue);
      assertThat(opt.manifestPreallocationSize()).
          isEqualTo(longValue);
    }

    { // AllowOsBuffer test
      boolean boolValue = rand.nextBoolean();
      opt.setAllowOsBuffer(boolValue);
      assertThat(opt.allowOsBuffer()).isEqualTo(boolValue);
    }

    { // AllowMmapReads test
      boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapReads(boolValue);
      assertThat(opt.allowMmapReads()).isEqualTo(boolValue);
    }

    { // AllowMmapWrites test
      boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapWrites(boolValue);
      assertThat(opt.allowMmapWrites()).isEqualTo(boolValue);
    }

    { // IsFdCloseOnExec test
      boolean boolValue = rand.nextBoolean();
      opt.setIsFdCloseOnExec(boolValue);
      assertThat(opt.isFdCloseOnExec()).isEqualTo(boolValue);
    }

    { // SkipLogErrorOnRecovery test
      boolean boolValue = rand.nextBoolean();
      opt.setSkipLogErrorOnRecovery(boolValue);
      assertThat(opt.skipLogErrorOnRecovery()).isEqualTo(boolValue);
    }

    { // StatsDumpPeriodSec test
      int intValue = rand.nextInt();
      opt.setStatsDumpPeriodSec(intValue);
      assertThat(opt.statsDumpPeriodSec()).isEqualTo(intValue);
    }

    { // AdviseRandomOnOpen test
      boolean boolValue = rand.nextBoolean();
      opt.setAdviseRandomOnOpen(boolValue);
      assertThat(opt.adviseRandomOnOpen()).isEqualTo(boolValue);
    }

    { // UseAdaptiveMutex test
      boolean boolValue = rand.nextBoolean();
      opt.setUseAdaptiveMutex(boolValue);
      assertThat(opt.useAdaptiveMutex()).isEqualTo(boolValue);
    }

    { // BytesPerSync test
      long longValue = rand.nextLong();
      opt.setBytesPerSync(longValue);
      assertThat(opt.bytesPerSync()).isEqualTo(longValue);
    }
  }

  @Test
  public void rateLimiterConfig() {
    DBOptions options = new DBOptions();
    RateLimiterConfig rateLimiterConfig =
        new GenericRateLimiterConfig(1000, 0, 1);
    options.setRateLimiterConfig(rateLimiterConfig);
    options.dispose();
    // Test with parameter initialization
    DBOptions anotherOptions = new DBOptions();
    anotherOptions.setRateLimiterConfig(
        new GenericRateLimiterConfig(1000));
    anotherOptions.dispose();
  }

  @Test
  public void statistics() {
    DBOptions options = new DBOptions();
    Statistics statistics = options.createStatistics().
        statisticsPtr();
    assertThat(statistics).isNotNull();

    DBOptions anotherOptions = new DBOptions();
    statistics = anotherOptions.statisticsPtr();
    assertThat(statistics).isNotNull();
  }
}
