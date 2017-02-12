// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class OptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void setIncreaseParallelism() {
    try (final Options opt = new Options()) {
      final int threads = Runtime.getRuntime().availableProcessors() * 2;
      opt.setIncreaseParallelism(threads);
    }
  }

  @Test
  public void writeBufferSize() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWriteBufferSize(longValue);
      assertThat(opt.writeBufferSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxWriteBufferNumber() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxWriteBufferNumber(intValue);
      assertThat(opt.maxWriteBufferNumber()).isEqualTo(intValue);
    }
  }

  @Test
  public void minWriteBufferNumberToMerge() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMinWriteBufferNumberToMerge(intValue);
      assertThat(opt.minWriteBufferNumberToMerge()).isEqualTo(intValue);
    }
  }

  @Test
  public void numLevels() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setNumLevels(intValue);
      assertThat(opt.numLevels()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroFileNumCompactionTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroFileNumCompactionTrigger(intValue);
      assertThat(opt.levelZeroFileNumCompactionTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroSlowdownWritesTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroSlowdownWritesTrigger(intValue);
      assertThat(opt.levelZeroSlowdownWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroStopWritesTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroStopWritesTrigger(intValue);
      assertThat(opt.levelZeroStopWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void targetFileSizeBase() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setTargetFileSizeBase(longValue);
      assertThat(opt.targetFileSizeBase()).isEqualTo(longValue);
    }
  }

  @Test
  public void targetFileSizeMultiplier() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setTargetFileSizeMultiplier(intValue);
      assertThat(opt.targetFileSizeMultiplier()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxBytesForLevelBase() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxBytesForLevelBase(longValue);
      assertThat(opt.maxBytesForLevelBase()).isEqualTo(longValue);
    }
  }

  @Test
  public void levelCompactionDynamicLevelBytes() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setLevelCompactionDynamicLevelBytes(boolValue);
      assertThat(opt.levelCompactionDynamicLevelBytes())
          .isEqualTo(boolValue);
    }
  }

  @Test
  public void maxBytesForLevelMultiplier() {
    try (final Options opt = new Options()) {
      final double doubleValue = rand.nextDouble();
      opt.setMaxBytesForLevelMultiplier(doubleValue);
      assertThat(opt.maxBytesForLevelMultiplier()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void maxBytesForLevelMultiplierAdditional() {
    try (final Options opt = new Options()) {
      final int intValue1 = rand.nextInt();
      final int intValue2 = rand.nextInt();
      final int[] ints = new int[]{intValue1, intValue2};
      opt.setMaxBytesForLevelMultiplierAdditional(ints);
      assertThat(opt.maxBytesForLevelMultiplierAdditional()).isEqualTo(ints);
    }
  }

  @Test
  public void maxCompactionBytes() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxCompactionBytes(longValue);
      assertThat(opt.maxCompactionBytes()).isEqualTo(longValue);
    }
  }

  @Test
  public void softRateLimit() {
    try (final Options opt = new Options()) {
      final double doubleValue = rand.nextDouble();
      opt.setSoftRateLimit(doubleValue);
      assertThat(opt.softRateLimit()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void softPendingCompactionBytesLimit() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setSoftPendingCompactionBytesLimit(longValue);
      assertThat(opt.softPendingCompactionBytesLimit()).isEqualTo(longValue);
    }
  }

  @Test
  public void hardRateLimit() {
    try (final Options opt = new Options()) {
      final double doubleValue = rand.nextDouble();
      opt.setHardRateLimit(doubleValue);
      assertThat(opt.hardRateLimit()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void hardPendingCompactionBytesLimit() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setHardPendingCompactionBytesLimit(longValue);
      assertThat(opt.hardPendingCompactionBytesLimit()).isEqualTo(longValue);
    }
  }

  @Test
  public void level0FileNumCompactionTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevel0FileNumCompactionTrigger(intValue);
      assertThat(opt.level0FileNumCompactionTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void level0SlowdownWritesTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevel0SlowdownWritesTrigger(intValue);
      assertThat(opt.level0SlowdownWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void level0StopWritesTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevel0StopWritesTrigger(intValue);
      assertThat(opt.level0StopWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void rateLimitDelayMaxMilliseconds() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setRateLimitDelayMaxMilliseconds(intValue);
      assertThat(opt.rateLimitDelayMaxMilliseconds()).isEqualTo(intValue);
    }
  }

  @Test
  public void arenaBlockSize() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setArenaBlockSize(longValue);
      assertThat(opt.arenaBlockSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void disableAutoCompactions() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setDisableAutoCompactions(boolValue);
      assertThat(opt.disableAutoCompactions()).isEqualTo(boolValue);
    }
  }

  @Test
  public void purgeRedundantKvsWhileFlush() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setPurgeRedundantKvsWhileFlush(boolValue);
      assertThat(opt.purgeRedundantKvsWhileFlush()).isEqualTo(boolValue);
    }
  }

  @Test
  public void verifyChecksumsInCompaction() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setVerifyChecksumsInCompaction(boolValue);
      assertThat(opt.verifyChecksumsInCompaction()).isEqualTo(boolValue);
    }
  }

  @Test
  public void maxSequentialSkipInIterations() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxSequentialSkipInIterations(longValue);
      assertThat(opt.maxSequentialSkipInIterations()).isEqualTo(longValue);
    }
  }

  @Test
  public void inplaceUpdateSupport() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setInplaceUpdateSupport(boolValue);
      assertThat(opt.inplaceUpdateSupport()).isEqualTo(boolValue);
    }
  }

  @Test
  public void inplaceUpdateNumLocks() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setInplaceUpdateNumLocks(longValue);
      assertThat(opt.inplaceUpdateNumLocks()).isEqualTo(longValue);
    }
  }

  @Test
  public void memtablePrefixBloomSizeRatio() {
    try (final Options opt = new Options()) {
      final double doubleValue = rand.nextDouble();
      opt.setMemtablePrefixBloomSizeRatio(doubleValue);
      assertThat(opt.memtablePrefixBloomSizeRatio()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void memtableHugePageSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMemtableHugePageSize(longValue);
      assertThat(opt.memtableHugePageSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void bloomLocality() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setBloomLocality(intValue);
      assertThat(opt.bloomLocality()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxSuccessiveMerges() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxSuccessiveMerges(longValue);
      assertThat(opt.maxSuccessiveMerges()).isEqualTo(longValue);
    }
  }

  @Test
  public void minPartialMergeOperands() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMinPartialMergeOperands(intValue);
      assertThat(opt.minPartialMergeOperands()).isEqualTo(intValue);
    }
  }

  @Test
  public void optimizeFiltersForHits() {
    try (final Options opt = new Options()) {
      final boolean aBoolean = rand.nextBoolean();
      opt.setOptimizeFiltersForHits(aBoolean);
      assertThat(opt.optimizeFiltersForHits()).isEqualTo(aBoolean);
    }
  }

  @Test
  public void createIfMissing() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setCreateIfMissing(boolValue);
      assertThat(opt.createIfMissing()).
          isEqualTo(boolValue);
    }
  }

  @Test
  public void createMissingColumnFamilies() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setCreateMissingColumnFamilies(boolValue);
      assertThat(opt.createMissingColumnFamilies()).
          isEqualTo(boolValue);
    }
  }

  @Test
  public void errorIfExists() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setErrorIfExists(boolValue);
      assertThat(opt.errorIfExists()).isEqualTo(boolValue);
    }
  }

  @Test
  public void paranoidChecks() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setParanoidChecks(boolValue);
      assertThat(opt.paranoidChecks()).
          isEqualTo(boolValue);
    }
  }

  @Test
  public void maxTotalWalSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxTotalWalSize(longValue);
      assertThat(opt.maxTotalWalSize()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void maxOpenFiles() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxOpenFiles(intValue);
      assertThat(opt.maxOpenFiles()).isEqualTo(intValue);
    }
  }

  @Test
  public void disableDataSync() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setDisableDataSync(boolValue);
      assertThat(opt.disableDataSync()).
          isEqualTo(boolValue);
    }
  }

  @Test
  public void useFsync() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseFsync(boolValue);
      assertThat(opt.useFsync()).isEqualTo(boolValue);
    }
  }

  @Test
  public void dbLogDir() {
    try (final Options opt = new Options()) {
      final String str = "path/to/DbLogDir";
      opt.setDbLogDir(str);
      assertThat(opt.dbLogDir()).isEqualTo(str);
    }
  }

  @Test
  public void walDir() {
    try (final Options opt = new Options()) {
      final String str = "path/to/WalDir";
      opt.setWalDir(str);
      assertThat(opt.walDir()).isEqualTo(str);
    }
  }

  @Test
  public void deleteObsoleteFilesPeriodMicros() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setDeleteObsoleteFilesPeriodMicros(longValue);
      assertThat(opt.deleteObsoleteFilesPeriodMicros()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void baseBackgroundCompactions() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setBaseBackgroundCompactions(intValue);
      assertThat(opt.baseBackgroundCompactions()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void maxBackgroundCompactions() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxBackgroundCompactions(intValue);
      assertThat(opt.maxBackgroundCompactions()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void maxSubcompactions() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxSubcompactions(intValue);
      assertThat(opt.maxSubcompactions()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void maxBackgroundFlushes() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxBackgroundFlushes(intValue);
      assertThat(opt.maxBackgroundFlushes()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void maxLogFileSize() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxLogFileSize(longValue);
      assertThat(opt.maxLogFileSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void logFileTimeToRoll() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setLogFileTimeToRoll(longValue);
      assertThat(opt.logFileTimeToRoll()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void keepLogFileNum() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setKeepLogFileNum(longValue);
      assertThat(opt.keepLogFileNum()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxManifestFileSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxManifestFileSize(longValue);
      assertThat(opt.maxManifestFileSize()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void tableCacheNumshardbits() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setTableCacheNumshardbits(intValue);
      assertThat(opt.tableCacheNumshardbits()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void walSizeLimitMB() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWalSizeLimitMB(longValue);
      assertThat(opt.walSizeLimitMB()).isEqualTo(longValue);
    }
  }

  @Test
  public void walTtlSeconds() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWalTtlSeconds(longValue);
      assertThat(opt.walTtlSeconds()).isEqualTo(longValue);
    }
  }

  @Test
  public void manifestPreallocationSize() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setManifestPreallocationSize(longValue);
      assertThat(opt.manifestPreallocationSize()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void useDirectReads() {
    try(final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseDirectReads(boolValue);
      assertThat(opt.useDirectReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useDirectWrites() {
    try(final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseDirectWrites(boolValue);
      assertThat(opt.useDirectWrites()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowMmapReads() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapReads(boolValue);
      assertThat(opt.allowMmapReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowMmapWrites() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapWrites(boolValue);
      assertThat(opt.allowMmapWrites()).isEqualTo(boolValue);
    }
  }

  @Test
  public void isFdCloseOnExec() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setIsFdCloseOnExec(boolValue);
      assertThat(opt.isFdCloseOnExec()).isEqualTo(boolValue);
    }
  }

  @Test
  public void statsDumpPeriodSec() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setStatsDumpPeriodSec(intValue);
      assertThat(opt.statsDumpPeriodSec()).isEqualTo(intValue);
    }
  }

  @Test
  public void adviseRandomOnOpen() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAdviseRandomOnOpen(boolValue);
      assertThat(opt.adviseRandomOnOpen()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useAdaptiveMutex() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseAdaptiveMutex(boolValue);
      assertThat(opt.useAdaptiveMutex()).isEqualTo(boolValue);
    }
  }

  @Test
  public void bytesPerSync() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setBytesPerSync(longValue);
      assertThat(opt.bytesPerSync()).isEqualTo(longValue);
    }
  }

  @Test
  public void allowConcurrentMemtableWrite() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowConcurrentMemtableWrite(boolValue);
      assertThat(opt.allowConcurrentMemtableWrite()).isEqualTo(boolValue);
    }
  }

  @Test
  public void enableWriteThreadAdaptiveYield() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setEnableWriteThreadAdaptiveYield(boolValue);
      assertThat(opt.enableWriteThreadAdaptiveYield()).isEqualTo(boolValue);
    }
  }

  @Test
  public void writeThreadMaxYieldUsec() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWriteThreadMaxYieldUsec(longValue);
      assertThat(opt.writeThreadMaxYieldUsec()).isEqualTo(longValue);
    }
  }

  @Test
  public void writeThreadSlowYieldUsec() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWriteThreadSlowYieldUsec(longValue);
      assertThat(opt.writeThreadSlowYieldUsec()).isEqualTo(longValue);
    }
  }

  @Test
  public void env() {
    try (final Options options = new Options();
         final Env env = Env.getDefault()) {
      options.setEnv(env);
      assertThat(options.getEnv()).isSameAs(env);
    }
  }

  @Test
  public void linkageOfPrepMethods() {
    try (final Options options = new Options()) {
      options.optimizeUniversalStyleCompaction();
      options.optimizeUniversalStyleCompaction(4000);
      options.optimizeLevelStyleCompaction();
      options.optimizeLevelStyleCompaction(3000);
      options.optimizeForPointLookup(10);
      options.prepareForBulkLoad();
    }
  }

  @Test
  public void compressionTypes() {
    try (final Options options = new Options()) {
      for (final CompressionType compressionType :
          CompressionType.values()) {
        options.setCompressionType(compressionType);
        assertThat(options.compressionType()).
            isEqualTo(compressionType);
        assertThat(CompressionType.valueOf("NO_COMPRESSION")).
            isEqualTo(CompressionType.NO_COMPRESSION);
      }
    }
  }

  @Test
  public void compressionPerLevel() {
    try (final ColumnFamilyOptions columnFamilyOptions =
             new ColumnFamilyOptions()) {
      assertThat(columnFamilyOptions.compressionPerLevel()).isEmpty();
      List<CompressionType> compressionTypeList =
          new ArrayList<>();
      for (int i = 0; i < columnFamilyOptions.numLevels(); i++) {
        compressionTypeList.add(CompressionType.NO_COMPRESSION);
      }
      columnFamilyOptions.setCompressionPerLevel(compressionTypeList);
      compressionTypeList = columnFamilyOptions.compressionPerLevel();
      for (final CompressionType compressionType : compressionTypeList) {
        assertThat(compressionType).isEqualTo(
            CompressionType.NO_COMPRESSION);
      }
    }
  }

  @Test
  public void differentCompressionsPerLevel() {
    try (final ColumnFamilyOptions columnFamilyOptions =
             new ColumnFamilyOptions()) {
      columnFamilyOptions.setNumLevels(3);

      assertThat(columnFamilyOptions.compressionPerLevel()).isEmpty();
      List<CompressionType> compressionTypeList = new ArrayList<>();

      compressionTypeList.add(CompressionType.BZLIB2_COMPRESSION);
      compressionTypeList.add(CompressionType.SNAPPY_COMPRESSION);
      compressionTypeList.add(CompressionType.LZ4_COMPRESSION);

      columnFamilyOptions.setCompressionPerLevel(compressionTypeList);
      compressionTypeList = columnFamilyOptions.compressionPerLevel();

      assertThat(compressionTypeList.size()).isEqualTo(3);
      assertThat(compressionTypeList).
          containsExactly(
              CompressionType.BZLIB2_COMPRESSION,
              CompressionType.SNAPPY_COMPRESSION,
              CompressionType.LZ4_COMPRESSION);

    }
  }

  @Test
  public void compactionStyles() {
    try (final Options options = new Options()) {
      for (final CompactionStyle compactionStyle :
          CompactionStyle.values()) {
        options.setCompactionStyle(compactionStyle);
        assertThat(options.compactionStyle()).
            isEqualTo(compactionStyle);
        assertThat(CompactionStyle.valueOf("FIFO")).
            isEqualTo(CompactionStyle.FIFO);
      }
    }
  }

  @Test
  public void maxTableFilesSizeFIFO() {
    try (final Options opt = new Options()) {
      long longValue = rand.nextLong();
      // Size has to be positive
      longValue = (longValue < 0) ? -longValue : longValue;
      longValue = (longValue == 0) ? longValue + 1 : longValue;
      opt.setMaxTableFilesSizeFIFO(longValue);
      assertThat(opt.maxTableFilesSizeFIFO()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void rateLimiterConfig() {
    try (final Options options = new Options();
         final Options anotherOptions = new Options()) {
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
    try (final Options options = new Options();
         final Options anotherOptions = new Options()) {
      final RateLimiter rateLimiter =
          new RateLimiter(1000, 100 * 1000, 1);
      options.setRateLimiter(rateLimiter);
      // Test with parameter initialization
      anotherOptions.setRateLimiter(
          new RateLimiter(1000));
    }
  }

  @Test
  public void shouldSetTestPrefixExtractor() {
    try (final Options options = new Options()) {
      options.useFixedLengthPrefixExtractor(100);
      options.useFixedLengthPrefixExtractor(10);
    }
  }

  @Test
  public void shouldSetTestCappedPrefixExtractor() {
    try (final Options options = new Options()) {
      options.useCappedPrefixExtractor(100);
      options.useCappedPrefixExtractor(10);
    }
  }


  @Test
  public void shouldTestMemTableFactoryName()
      throws RocksDBException {
    try (final Options options = new Options()) {
      options.setMemTableConfig(new VectorMemTableConfig());
      assertThat(options.memTableFactoryName()).
          isEqualTo("VectorRepFactory");
      options.setMemTableConfig(
          new HashLinkedListMemTableConfig());
      assertThat(options.memTableFactoryName()).
          isEqualTo("HashLinkedListRepFactory");
    }
  }

  @Test
  public void statistics() {
    try (final Options options = new Options()) {
      Statistics statistics = options.createStatistics().
          statisticsPtr();
      assertThat(statistics).isNotNull();
      try (final Options anotherOptions = new Options()) {
        statistics = anotherOptions.statisticsPtr();
        assertThat(statistics).isNotNull();
      }
    }
  }
}
