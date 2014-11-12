// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.*;

import java.util.Random;

public class ColumnFamilyOptionsTest {
  static {
    RocksDB.loadLibrary();
  }

  public static void testCFOptions(ColumnFamilyOptionsInterface opt) {
    Random rand = PlatformRandomHelper.
        getPlatformSpecificRandomFactory();
    { // WriteBufferSize test
      try {
        long longValue = rand.nextLong();
        opt.setWriteBufferSize(longValue);
        assert(opt.writeBufferSize() == longValue);
      } catch (RocksDBException e) {
        assert(false);
      }
    }

    { // MaxWriteBufferNumber test
      int intValue = rand.nextInt();
      opt.setMaxWriteBufferNumber(intValue);
      assert(opt.maxWriteBufferNumber() == intValue);
    }

    { // MinWriteBufferNumberToMerge test
      int intValue = rand.nextInt();
      opt.setMinWriteBufferNumberToMerge(intValue);
      assert(opt.minWriteBufferNumberToMerge() == intValue);
    }

    { // NumLevels test
      int intValue = rand.nextInt();
      opt.setNumLevels(intValue);
      assert(opt.numLevels() == intValue);
    }

    { // LevelFileNumCompactionTrigger test
      int intValue = rand.nextInt();
      opt.setLevelZeroFileNumCompactionTrigger(intValue);
      assert(opt.levelZeroFileNumCompactionTrigger() == intValue);
    }

    { // LevelSlowdownWritesTrigger test
      int intValue = rand.nextInt();
      opt.setLevelZeroSlowdownWritesTrigger(intValue);
      assert(opt.levelZeroSlowdownWritesTrigger() == intValue);
    }

    { // LevelStopWritesTrigger test
      int intValue = rand.nextInt();
      opt.setLevelZeroStopWritesTrigger(intValue);
      assert(opt.levelZeroStopWritesTrigger() == intValue);
    }

    { // MaxMemCompactionLevel test
      int intValue = rand.nextInt();
      opt.setMaxMemCompactionLevel(intValue);
      assert(opt.maxMemCompactionLevel() == intValue);
    }

    { // TargetFileSizeBase test
      long longValue = rand.nextLong();
      opt.setTargetFileSizeBase(longValue);
      assert(opt.targetFileSizeBase() == longValue);
    }

    { // TargetFileSizeMultiplier test
      int intValue = rand.nextInt();
      opt.setTargetFileSizeMultiplier(intValue);
      assert(opt.targetFileSizeMultiplier() == intValue);
    }

    { // MaxBytesForLevelBase test
      long longValue = rand.nextLong();
      opt.setMaxBytesForLevelBase(longValue);
      assert(opt.maxBytesForLevelBase() == longValue);
    }

    { // MaxBytesForLevelMultiplier test
      int intValue = rand.nextInt();
      opt.setMaxBytesForLevelMultiplier(intValue);
      assert(opt.maxBytesForLevelMultiplier() == intValue);
    }

    { // ExpandedCompactionFactor test
      int intValue = rand.nextInt();
      opt.setExpandedCompactionFactor(intValue);
      assert(opt.expandedCompactionFactor() == intValue);
    }

    { // SourceCompactionFactor test
      int intValue = rand.nextInt();
      opt.setSourceCompactionFactor(intValue);
      assert(opt.sourceCompactionFactor() == intValue);
    }

    { // MaxGrandparentOverlapFactor test
      int intValue = rand.nextInt();
      opt.setMaxGrandparentOverlapFactor(intValue);
      assert(opt.maxGrandparentOverlapFactor() == intValue);
    }

    { // SoftRateLimit test
      double doubleValue = rand.nextDouble();
      opt.setSoftRateLimit(doubleValue);
      assert(opt.softRateLimit() == doubleValue);
    }

    { // HardRateLimit test
      double doubleValue = rand.nextDouble();
      opt.setHardRateLimit(doubleValue);
      assert(opt.hardRateLimit() == doubleValue);
    }

    { // RateLimitDelayMaxMilliseconds test
      int intValue = rand.nextInt();
      opt.setRateLimitDelayMaxMilliseconds(intValue);
      assert(opt.rateLimitDelayMaxMilliseconds() == intValue);
    }

    { // ArenaBlockSize test
      try {
        long longValue = rand.nextLong();
        opt.setArenaBlockSize(longValue);
        assert(opt.arenaBlockSize() == longValue);
      } catch (RocksDBException e) {
        assert(false);
      }
    }

    { // DisableAutoCompactions test
      boolean boolValue = rand.nextBoolean();
      opt.setDisableAutoCompactions(boolValue);
      assert(opt.disableAutoCompactions() == boolValue);
    }

    { // PurgeRedundantKvsWhileFlush test
      boolean boolValue = rand.nextBoolean();
      opt.setPurgeRedundantKvsWhileFlush(boolValue);
      assert(opt.purgeRedundantKvsWhileFlush() == boolValue);
    }

    { // VerifyChecksumsInCompaction test
      boolean boolValue = rand.nextBoolean();
      opt.setVerifyChecksumsInCompaction(boolValue);
      assert(opt.verifyChecksumsInCompaction() == boolValue);
    }

    { // FilterDeletes test
      boolean boolValue = rand.nextBoolean();
      opt.setFilterDeletes(boolValue);
      assert(opt.filterDeletes() == boolValue);
    }

    { // MaxSequentialSkipInIterations test
      long longValue = rand.nextLong();
      opt.setMaxSequentialSkipInIterations(longValue);
      assert(opt.maxSequentialSkipInIterations() == longValue);
    }

    { // InplaceUpdateSupport test
      boolean boolValue = rand.nextBoolean();
      opt.setInplaceUpdateSupport(boolValue);
      assert(opt.inplaceUpdateSupport() == boolValue);
    }

    { // InplaceUpdateNumLocks test
      try {
        long longValue = rand.nextLong();
        opt.setInplaceUpdateNumLocks(longValue);
        assert(opt.inplaceUpdateNumLocks() == longValue);
      } catch (RocksDBException e) {
        assert(false);
      }
    }

    { // MemtablePrefixBloomBits test
      int intValue = rand.nextInt();
      opt.setMemtablePrefixBloomBits(intValue);
      assert(opt.memtablePrefixBloomBits() == intValue);
    }

    { // MemtablePrefixBloomProbes test
      int intValue = rand.nextInt();
      opt.setMemtablePrefixBloomProbes(intValue);
      assert(opt.memtablePrefixBloomProbes() == intValue);
    }

    { // BloomLocality test
      int intValue = rand.nextInt();
      opt.setBloomLocality(intValue);
      assert(opt.bloomLocality() == intValue);
    }

    { // MaxSuccessiveMerges test
      try {
        long longValue = rand.nextLong();
        opt.setMaxSuccessiveMerges(longValue);
        assert(opt.maxSuccessiveMerges() == longValue);
      } catch (RocksDBException e){
        assert(false);
      }
    }

    { // MinPartialMergeOperands test
      int intValue = rand.nextInt();
      opt.setMinPartialMergeOperands(intValue);
      assert(opt.minPartialMergeOperands() == intValue);
    }
  }

  public static void main(String[] args) {
    ColumnFamilyOptions opt = new ColumnFamilyOptions();
    testCFOptions(opt);
    opt.dispose();
    System.out.println("Passed DBOptionsTest");
  }
}
