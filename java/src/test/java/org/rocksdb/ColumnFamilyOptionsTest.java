// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnFamilyOptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void getColumnFamilyOptionsFromProps() {
    Properties properties = new Properties();
    properties.put("write_buffer_size", "112");
    properties.put("max_write_buffer_number", "13");

    try (final ColumnFamilyOptions opt = ColumnFamilyOptions.
        getColumnFamilyOptionsFromProps(properties)) {
      // setup sample properties
      assertThat(opt).isNotNull();
      assertThat(String.valueOf(opt.writeBufferSize())).
          isEqualTo(properties.get("write_buffer_size"));
      assertThat(String.valueOf(opt.maxWriteBufferNumber())).
          isEqualTo(properties.get("max_write_buffer_number"));
    }
  }

  @Test
  public void failColumnFamilyOptionsFromPropsWithIllegalValue() {
    // setup sample properties
    final Properties properties = new Properties();
    properties.put("tomato", "1024");
    properties.put("burger", "2");

    try (final ColumnFamilyOptions opt =
             ColumnFamilyOptions.getColumnFamilyOptionsFromProps(properties)) {
      assertThat(opt).isNull();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failColumnFamilyOptionsFromPropsWithNullValue() {
    try (final ColumnFamilyOptions opt =
             ColumnFamilyOptions.getColumnFamilyOptionsFromProps(null)) {
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failColumnFamilyOptionsFromPropsWithEmptyProps() {
    try (final ColumnFamilyOptions opt =
             ColumnFamilyOptions.getColumnFamilyOptionsFromProps(
                 new Properties())) {
    }
  }

  @Test
  public void writeBufferSize() throws RocksDBException {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setWriteBufferSize(longValue);
      assertThat(opt.writeBufferSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxWriteBufferNumber() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setMaxWriteBufferNumber(intValue);
      assertThat(opt.maxWriteBufferNumber()).isEqualTo(intValue);
    }
  }

  @Test
  public void minWriteBufferNumberToMerge() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setMinWriteBufferNumberToMerge(intValue);
      assertThat(opt.minWriteBufferNumberToMerge()).isEqualTo(intValue);
    }
  }

  @Test
  public void numLevels() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setNumLevels(intValue);
      assertThat(opt.numLevels()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroFileNumCompactionTrigger() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroFileNumCompactionTrigger(intValue);
      assertThat(opt.levelZeroFileNumCompactionTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroSlowdownWritesTrigger() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroSlowdownWritesTrigger(intValue);
      assertThat(opt.levelZeroSlowdownWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroStopWritesTrigger() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroStopWritesTrigger(intValue);
      assertThat(opt.levelZeroStopWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void targetFileSizeBase() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setTargetFileSizeBase(longValue);
      assertThat(opt.targetFileSizeBase()).isEqualTo(longValue);
    }
  }

  @Test
  public void targetFileSizeMultiplier() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setTargetFileSizeMultiplier(intValue);
      assertThat(opt.targetFileSizeMultiplier()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxBytesForLevelBase() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxBytesForLevelBase(longValue);
      assertThat(opt.maxBytesForLevelBase()).isEqualTo(longValue);
    }
  }

  @Test
  public void levelCompactionDynamicLevelBytes() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setLevelCompactionDynamicLevelBytes(boolValue);
      assertThat(opt.levelCompactionDynamicLevelBytes())
          .isEqualTo(boolValue);
    }
  }

  @Test
  public void maxBytesForLevelMultiplier() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final double doubleValue = rand.nextDouble();
      opt.setMaxBytesForLevelMultiplier(doubleValue);
      assertThat(opt.maxBytesForLevelMultiplier()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void maxBytesForLevelMultiplierAdditional() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue1 = rand.nextInt();
      final int intValue2 = rand.nextInt();
      final int[] ints = new int[]{intValue1, intValue2};
      opt.setMaxBytesForLevelMultiplierAdditional(ints);
      assertThat(opt.maxBytesForLevelMultiplierAdditional()).isEqualTo(ints);
    }
  }

  @Test
  public void maxCompactionBytes() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxCompactionBytes(longValue);
      assertThat(opt.maxCompactionBytes()).isEqualTo(longValue);
    }
  }

  @Test
  public void softRateLimit() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final double doubleValue = rand.nextDouble();
      opt.setSoftRateLimit(doubleValue);
      assertThat(opt.softRateLimit()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void softPendingCompactionBytesLimit() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setSoftPendingCompactionBytesLimit(longValue);
      assertThat(opt.softPendingCompactionBytesLimit()).isEqualTo(longValue);
    }
  }

  @Test
  public void hardRateLimit() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final double doubleValue = rand.nextDouble();
      opt.setHardRateLimit(doubleValue);
      assertThat(opt.hardRateLimit()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void hardPendingCompactionBytesLimit() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setHardPendingCompactionBytesLimit(longValue);
      assertThat(opt.hardPendingCompactionBytesLimit()).isEqualTo(longValue);
    }
  }

  @Test
  public void level0FileNumCompactionTrigger() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevel0FileNumCompactionTrigger(intValue);
      assertThat(opt.level0FileNumCompactionTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void level0SlowdownWritesTrigger() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevel0SlowdownWritesTrigger(intValue);
      assertThat(opt.level0SlowdownWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void level0StopWritesTrigger() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setLevel0StopWritesTrigger(intValue);
      assertThat(opt.level0StopWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void rateLimitDelayMaxMilliseconds() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setRateLimitDelayMaxMilliseconds(intValue);
      assertThat(opt.rateLimitDelayMaxMilliseconds()).isEqualTo(intValue);
    }
  }

  @Test
  public void arenaBlockSize() throws RocksDBException {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setArenaBlockSize(longValue);
      assertThat(opt.arenaBlockSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void disableAutoCompactions() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setDisableAutoCompactions(boolValue);
      assertThat(opt.disableAutoCompactions()).isEqualTo(boolValue);
    }
  }

  @Test
  public void purgeRedundantKvsWhileFlush() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setPurgeRedundantKvsWhileFlush(boolValue);
      assertThat(opt.purgeRedundantKvsWhileFlush()).isEqualTo(boolValue);
    }
  }

  @Test
  public void verifyChecksumsInCompaction() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setVerifyChecksumsInCompaction(boolValue);
      assertThat(opt.verifyChecksumsInCompaction()).isEqualTo(boolValue);
    }
  }

  @Test
  public void maxSequentialSkipInIterations() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxSequentialSkipInIterations(longValue);
      assertThat(opt.maxSequentialSkipInIterations()).isEqualTo(longValue);
    }
  }

  @Test
  public void inplaceUpdateSupport() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setInplaceUpdateSupport(boolValue);
      assertThat(opt.inplaceUpdateSupport()).isEqualTo(boolValue);
    }
  }

  @Test
  public void inplaceUpdateNumLocks() throws RocksDBException {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setInplaceUpdateNumLocks(longValue);
      assertThat(opt.inplaceUpdateNumLocks()).isEqualTo(longValue);
    }
  }

  @Test
  public void memtablePrefixBloomSizeRatio() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final double doubleValue = rand.nextDouble();
      opt.setMemtablePrefixBloomSizeRatio(doubleValue);
      assertThat(opt.memtablePrefixBloomSizeRatio()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void memtableHugePageSize() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMemtableHugePageSize(longValue);
      assertThat(opt.memtableHugePageSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void bloomLocality() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setBloomLocality(intValue);
      assertThat(opt.bloomLocality()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxSuccessiveMerges() throws RocksDBException {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxSuccessiveMerges(longValue);
      assertThat(opt.maxSuccessiveMerges()).isEqualTo(longValue);
    }
  }

  @Test
  public void minPartialMergeOperands() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final int intValue = rand.nextInt();
      opt.setMinPartialMergeOperands(intValue);
      assertThat(opt.minPartialMergeOperands()).isEqualTo(intValue);
    }
  }

  @Test
  public void optimizeFiltersForHits() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      final boolean aBoolean = rand.nextBoolean();
      opt.setOptimizeFiltersForHits(aBoolean);
      assertThat(opt.optimizeFiltersForHits()).isEqualTo(aBoolean);
    }
  }

  @Test
  public void memTable() throws RocksDBException {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      opt.setMemTableConfig(new HashLinkedListMemTableConfig());
      assertThat(opt.memTableFactoryName()).
          isEqualTo("HashLinkedListRepFactory");
    }
  }

  @Test
  public void comparator() throws RocksDBException {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      opt.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
    }
  }

  @Test
  public void linkageOfPrepMethods() {
    try (final ColumnFamilyOptions options = new ColumnFamilyOptions()) {
      options.optimizeUniversalStyleCompaction();
      options.optimizeUniversalStyleCompaction(4000);
      options.optimizeLevelStyleCompaction();
      options.optimizeLevelStyleCompaction(3000);
      options.optimizeForPointLookup(10);
    }
  }

  @Test
  public void shouldSetTestPrefixExtractor() {
    try (final ColumnFamilyOptions options = new ColumnFamilyOptions()) {
      options.useFixedLengthPrefixExtractor(100);
      options.useFixedLengthPrefixExtractor(10);
    }
  }

  @Test
  public void shouldSetTestCappedPrefixExtractor() {
    try (final ColumnFamilyOptions options = new ColumnFamilyOptions()) {
      options.useCappedPrefixExtractor(100);
      options.useCappedPrefixExtractor(10);
    }
  }

  @Test
  public void compressionTypes() {
    try (final ColumnFamilyOptions columnFamilyOptions
             = new ColumnFamilyOptions()) {
      for (final CompressionType compressionType :
          CompressionType.values()) {
        columnFamilyOptions.setCompressionType(compressionType);
        assertThat(columnFamilyOptions.compressionType()).
            isEqualTo(compressionType);
        assertThat(CompressionType.valueOf("NO_COMPRESSION")).
            isEqualTo(CompressionType.NO_COMPRESSION);
      }
    }
  }

  @Test
  public void compressionPerLevel() {
    try (final ColumnFamilyOptions columnFamilyOptions
             = new ColumnFamilyOptions()) {
      assertThat(columnFamilyOptions.compressionPerLevel()).isEmpty();
      List<CompressionType> compressionTypeList = new ArrayList<>();
      for (int i = 0; i < columnFamilyOptions.numLevels(); i++) {
        compressionTypeList.add(CompressionType.NO_COMPRESSION);
      }
      columnFamilyOptions.setCompressionPerLevel(compressionTypeList);
      compressionTypeList = columnFamilyOptions.compressionPerLevel();
      for (CompressionType compressionType : compressionTypeList) {
        assertThat(compressionType).isEqualTo(
            CompressionType.NO_COMPRESSION);
      }
    }
  }

  @Test
  public void differentCompressionsPerLevel() {
    try (final ColumnFamilyOptions columnFamilyOptions
             = new ColumnFamilyOptions()) {
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
    try (final ColumnFamilyOptions columnFamilyOptions
             = new ColumnFamilyOptions()) {
      for (final CompactionStyle compactionStyle :
          CompactionStyle.values()) {
        columnFamilyOptions.setCompactionStyle(compactionStyle);
        assertThat(columnFamilyOptions.compactionStyle()).
            isEqualTo(compactionStyle);
        assertThat(CompactionStyle.valueOf("FIFO")).
            isEqualTo(CompactionStyle.FIFO);
      }
    }
  }

  @Test
  public void maxTableFilesSizeFIFO() {
    try (final ColumnFamilyOptions opt = new ColumnFamilyOptions()) {
      long longValue = rand.nextLong();
      // Size has to be positive
      longValue = (longValue < 0) ? -longValue : longValue;
      longValue = (longValue == 0) ? longValue + 1 : longValue;
      opt.setMaxTableFilesSizeFIFO(longValue);
      assertThat(opt.maxTableFilesSizeFIFO()).
          isEqualTo(longValue);
    }
  }
}
