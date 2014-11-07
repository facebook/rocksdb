// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.Random;
import org.junit.ClassRule;
import org.junit.Test;
import org.rocksdb.*;

import static org.assertj.core.api.Assertions.assertThat;


public class OptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void options() throws RocksDBException {
    Options opt = null;
    try {
      opt = new Options();
      Random rand = PlatformRandomHelper.
          getPlatformSpecificRandomFactory();
      DBOptionsTest.testDBOptions(opt);

      { // WriteBufferSize test
        long longValue = rand.nextLong();
        opt.setWriteBufferSize(longValue);
        assert (opt.writeBufferSize() == longValue);
      }

      { // MaxWriteBufferNumber test
        int intValue = rand.nextInt();
        opt.setMaxWriteBufferNumber(intValue);
        assert (opt.maxWriteBufferNumber() == intValue);
      }

      { // MinWriteBufferNumberToMerge test
        int intValue = rand.nextInt();
        opt.setMinWriteBufferNumberToMerge(intValue);
        assert (opt.minWriteBufferNumberToMerge() == intValue);
      }

      { // NumLevels test
        int intValue = rand.nextInt();
        opt.setNumLevels(intValue);
        assert (opt.numLevels() == intValue);
      }

      { // LevelFileNumCompactionTrigger test
        int intValue = rand.nextInt();
        opt.setLevelZeroFileNumCompactionTrigger(intValue);
        assert (opt.levelZeroFileNumCompactionTrigger() == intValue);
      }

      { // LevelSlowdownWritesTrigger test
        int intValue = rand.nextInt();
        opt.setLevelZeroSlowdownWritesTrigger(intValue);
        assert (opt.levelZeroSlowdownWritesTrigger() == intValue);
      }

      { // LevelStopWritesTrigger test
        int intValue = rand.nextInt();
        opt.setLevelZeroStopWritesTrigger(intValue);
        assert (opt.levelZeroStopWritesTrigger() == intValue);
      }

      { // MaxMemCompactionLevel test
        int intValue = rand.nextInt();
        opt.setMaxMemCompactionLevel(intValue);
        assert (opt.maxMemCompactionLevel() == intValue);
      }

      { // TargetFileSizeBase test
        long longValue = rand.nextLong();
        opt.setTargetFileSizeBase(longValue);
        assert (opt.targetFileSizeBase() == longValue);
      }

      { // TargetFileSizeMultiplier test
        int intValue = rand.nextInt();
        opt.setTargetFileSizeMultiplier(intValue);
        assert (opt.targetFileSizeMultiplier() == intValue);
      }

      { // MaxBytesForLevelBase test
        long longValue = rand.nextLong();
        opt.setMaxBytesForLevelBase(longValue);
        assert (opt.maxBytesForLevelBase() == longValue);
      }

      { // MaxBytesForLevelMultiplier test
        int intValue = rand.nextInt();
        opt.setMaxBytesForLevelMultiplier(intValue);
        assert (opt.maxBytesForLevelMultiplier() == intValue);
      }

      { // ExpandedCompactionFactor test
        int intValue = rand.nextInt();
        opt.setExpandedCompactionFactor(intValue);
        assert (opt.expandedCompactionFactor() == intValue);
      }

      { // SourceCompactionFactor test
        int intValue = rand.nextInt();
        opt.setSourceCompactionFactor(intValue);
        assert (opt.sourceCompactionFactor() == intValue);
      }

      { // MaxGrandparentOverlapFactor test
        int intValue = rand.nextInt();
        opt.setMaxGrandparentOverlapFactor(intValue);
        assert (opt.maxGrandparentOverlapFactor() == intValue);
      }

      { // SoftRateLimit test
        double doubleValue = rand.nextDouble();
        opt.setSoftRateLimit(doubleValue);
        assert (opt.softRateLimit() == doubleValue);
      }

      { // HardRateLimit test
        double doubleValue = rand.nextDouble();
        opt.setHardRateLimit(doubleValue);
        assert (opt.hardRateLimit() == doubleValue);
      }

      { // RateLimitDelayMaxMilliseconds test
        int intValue = rand.nextInt();
        opt.setRateLimitDelayMaxMilliseconds(intValue);
        assert (opt.rateLimitDelayMaxMilliseconds() == intValue);
      }

      { // ArenaBlockSize test
        long longValue = rand.nextLong();
        opt.setArenaBlockSize(longValue);
        assert (opt.arenaBlockSize() == longValue);
      }

      { // DisableAutoCompactions test
        boolean boolValue = rand.nextBoolean();
        opt.setDisableAutoCompactions(boolValue);
        assert (opt.disableAutoCompactions() == boolValue);
      }

      { // PurgeRedundantKvsWhileFlush test
        boolean boolValue = rand.nextBoolean();
        opt.setPurgeRedundantKvsWhileFlush(boolValue);
        assert (opt.purgeRedundantKvsWhileFlush() == boolValue);
      }

      { // VerifyChecksumsInCompaction test
        boolean boolValue = rand.nextBoolean();
        opt.setVerifyChecksumsInCompaction(boolValue);
        assert (opt.verifyChecksumsInCompaction() == boolValue);
      }

      { // FilterDeletes test
        boolean boolValue = rand.nextBoolean();
        opt.setFilterDeletes(boolValue);
        assert (opt.filterDeletes() == boolValue);
      }

      { // MaxSequentialSkipInIterations test
        long longValue = rand.nextLong();
        opt.setMaxSequentialSkipInIterations(longValue);
        assert (opt.maxSequentialSkipInIterations() == longValue);
      }

      { // InplaceUpdateSupport test
        boolean boolValue = rand.nextBoolean();
        opt.setInplaceUpdateSupport(boolValue);
        assert (opt.inplaceUpdateSupport() == boolValue);
      }

      { // InplaceUpdateNumLocks test
        long longValue = rand.nextLong();
        opt.setInplaceUpdateNumLocks(longValue);
        assert (opt.inplaceUpdateNumLocks() == longValue);
      }

      { // MemtablePrefixBloomBits test
        int intValue = rand.nextInt();
        opt.setMemtablePrefixBloomBits(intValue);
        assert (opt.memtablePrefixBloomBits() == intValue);
      }

      { // MemtablePrefixBloomProbes test
        int intValue = rand.nextInt();
        opt.setMemtablePrefixBloomProbes(intValue);
        assert (opt.memtablePrefixBloomProbes() == intValue);
      }

      { // BloomLocality test
        int intValue = rand.nextInt();
        opt.setBloomLocality(intValue);
        assert (opt.bloomLocality() == intValue);
      }

      { // MaxSuccessiveMerges test
        long longValue = rand.nextLong();
        opt.setMaxSuccessiveMerges(longValue);
        assert (opt.maxSuccessiveMerges() == longValue);
      }

      { // MinPartialMergeOperands test
        int intValue = rand.nextInt();
        opt.setMinPartialMergeOperands(intValue);
        assert (opt.minPartialMergeOperands() == intValue);
      }
    } finally {
      if (opt != null) {
        opt.dispose();
      }
    }
  }

  @Test
  public void rocksEnv() {
    Options options = null;
    try {
      options = new Options();
      RocksEnv rocksEnv = RocksEnv.getDefault();
      options.setEnv(rocksEnv);
      assertThat(options.getEnv()).isSameAs(rocksEnv);
    } finally {
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void linkageOfPrepMethods() {
    Options options = null;
    try {
      options = new Options();
      options.optimizeUniversalStyleCompaction();
      options.optimizeUniversalStyleCompaction(4000);
      options.optimizeLevelStyleCompaction();
      options.optimizeLevelStyleCompaction(3000);
      options.optimizeForPointLookup(10);
      options.prepareForBulkLoad();
    } finally {
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void compressionTypes() {
    Options options = null;
    try {
      options = new Options();
      for (CompressionType compressionType :
          CompressionType.values()) {
        options.setCompressionType(compressionType);
        assertThat(options.compressionType()).
            isEqualTo(compressionType);
      }
    } finally {
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void compactionStyles() {
    Options options = null;
    try {
      options = new Options();
      for (CompactionStyle compactionStyle :
          CompactionStyle.values()) {
        options.setCompactionStyle(compactionStyle);
        assertThat(options.compactionStyle()).
            isEqualTo(compactionStyle);
      }
    } finally {
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void rateLimiterConfig() {
    Options options = null;
    Options anotherOptions = null;
    RateLimiterConfig rateLimiterConfig;
    try {
      options = new Options();
      rateLimiterConfig = new GenericRateLimiterConfig(1000, 0, 1);
      options.setRateLimiterConfig(rateLimiterConfig);
      // Test with parameter initialization
      anotherOptions = new Options();
      anotherOptions.setRateLimiterConfig(
          new GenericRateLimiterConfig(1000));
    } finally {
      if (options != null) {
        options.dispose();
      }
      if (anotherOptions != null) {
        anotherOptions.dispose();
      }
    }
  }

  @Test
  public void shouldSetTestPrefixExtractor() {
    Options options = null;
    try {
      options = new Options();
      options.useFixedLengthPrefixExtractor(100);
      options.useFixedLengthPrefixExtractor(10);
    } finally {
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void shouldTestMemTableFactoryName()
      throws RocksDBException {
    Options options = null;
    try {
      options = new Options();
      options.setMemTableConfig(new VectorMemTableConfig());
      assertThat(options.memTableFactoryName()).
          isEqualTo("VectorRepFactory");
      options.setMemTableConfig(
          new HashLinkedListMemTableConfig());
      assertThat(options.memTableFactoryName()).
          isEqualTo("HashLinkedListRepFactory");
    } finally {
      if (options != null) {
        options.dispose();
      }
    }
  }

  @Test
  public void statistics() {
    Options options = null;
    Options anotherOptions = null;
    try {
      options = new Options();
      Statistics statistics = options.createStatistics().
          statisticsPtr();
      assertThat(statistics).isNotNull();
      anotherOptions = new Options();
      statistics = anotherOptions.statisticsPtr();
      assertThat(statistics).isNotNull();
    } finally {
      if (options != null) {
        options.dispose();
      }
      if (anotherOptions != null) {
        anotherOptions.dispose();
      }
    }
  }
}
