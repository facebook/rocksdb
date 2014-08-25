// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.Random;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;

public class OptionsTest {
  static {
    RocksDB.loadLibrary();
  }
  public static void main(String[] args) {
    Options opt = new Options();
    Random rand = new Random();
    { // CreateIfMissing test
      boolean boolValue = rand.nextBoolean();
      opt.setCreateIfMissing(boolValue);
      assert(opt.createIfMissing() == boolValue);
    }

    { // ErrorIfExists test
      boolean boolValue = rand.nextBoolean();
      opt.setErrorIfExists(boolValue);
      assert(opt.errorIfExists() == boolValue);
    }

    { // ParanoidChecks test
      boolean boolValue = rand.nextBoolean();
      opt.setParanoidChecks(boolValue);
      assert(opt.paranoidChecks() == boolValue);
    }

    { // MaxOpenFiles test
      int intValue = rand.nextInt();
      opt.setMaxOpenFiles(intValue);
      assert(opt.maxOpenFiles() == intValue);
    }

    { // DisableDataSync test
      boolean boolValue = rand.nextBoolean();
      opt.setDisableDataSync(boolValue);
      assert(opt.disableDataSync() == boolValue);
    }

    { // UseFsync test
      boolean boolValue = rand.nextBoolean();
      opt.setUseFsync(boolValue);
      assert(opt.useFsync() == boolValue);
    }

    { // DbStatsLogInterval test
      int intValue = rand.nextInt();
      opt.setDbStatsLogInterval(intValue);
      assert(opt.dbStatsLogInterval() == intValue);
    }

    { // DbLogDir test
      String str = "path/to/DbLogDir";
      opt.setDbLogDir(str);
      assert(opt.dbLogDir().equals(str));
    }

    { // WalDir test
      String str = "path/to/WalDir";
      opt.setWalDir(str);
      assert(opt.walDir().equals(str));
    }

    { // DeleteObsoleteFilesPeriodMicros test
      long longValue = rand.nextLong();
      opt.setDeleteObsoleteFilesPeriodMicros(longValue);
      assert(opt.deleteObsoleteFilesPeriodMicros() == longValue);
    }

    { // MaxBackgroundCompactions test
      int intValue = rand.nextInt();
      opt.setMaxBackgroundCompactions(intValue);
      assert(opt.maxBackgroundCompactions() == intValue);
    }

    { // MaxBackgroundFlushes test
      int intValue = rand.nextInt();
      opt.setMaxBackgroundFlushes(intValue);
      assert(opt.maxBackgroundFlushes() == intValue);
    }

    { // MaxLogFileSize test
      long longValue = rand.nextLong();
      opt.setMaxLogFileSize(longValue);
      assert(opt.maxLogFileSize() == longValue);
    }

    { // LogFileTimeToRoll test
      long longValue = rand.nextLong();
      opt.setLogFileTimeToRoll(longValue);
      assert(opt.logFileTimeToRoll() == longValue);
    }

    { // KeepLogFileNum test
      long longValue = rand.nextLong();
      opt.setKeepLogFileNum(longValue);
      assert(opt.keepLogFileNum() == longValue);
    }

    { // MaxManifestFileSize test
      long longValue = rand.nextLong();
      opt.setMaxManifestFileSize(longValue);
      assert(opt.maxManifestFileSize() == longValue);
    }

    { // TableCacheNumshardbits test
      int intValue = rand.nextInt();
      opt.setTableCacheNumshardbits(intValue);
      assert(opt.tableCacheNumshardbits() == intValue);
    }

    { // TableCacheRemoveScanCountLimit test
      int intValue = rand.nextInt();
      opt.setTableCacheRemoveScanCountLimit(intValue);
      assert(opt.tableCacheRemoveScanCountLimit() == intValue);
    }

    { // WalTtlSeconds test
      long longValue = rand.nextLong();
      opt.setWalTtlSeconds(longValue);
      assert(opt.walTtlSeconds() == longValue);
    }

    { // ManifestPreallocationSize test
      long longValue = rand.nextLong();
      opt.setManifestPreallocationSize(longValue);
      assert(opt.manifestPreallocationSize() == longValue);
    }

    { // AllowOsBuffer test
      boolean boolValue = rand.nextBoolean();
      opt.setAllowOsBuffer(boolValue);
      assert(opt.allowOsBuffer() == boolValue);
    }

    { // AllowMmapReads test
      boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapReads(boolValue);
      assert(opt.allowMmapReads() == boolValue);
    }

    { // AllowMmapWrites test
      boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapWrites(boolValue);
      assert(opt.allowMmapWrites() == boolValue);
    }

    { // IsFdCloseOnExec test
      boolean boolValue = rand.nextBoolean();
      opt.setIsFdCloseOnExec(boolValue);
      assert(opt.isFdCloseOnExec() == boolValue);
    }

    { // SkipLogErrorOnRecovery test
      boolean boolValue = rand.nextBoolean();
      opt.setSkipLogErrorOnRecovery(boolValue);
      assert(opt.skipLogErrorOnRecovery() == boolValue);
    }

    { // StatsDumpPeriodSec test
      int intValue = rand.nextInt();
      opt.setStatsDumpPeriodSec(intValue);
      assert(opt.statsDumpPeriodSec() == intValue);
    }

    { // AdviseRandomOnOpen test
      boolean boolValue = rand.nextBoolean();
      opt.setAdviseRandomOnOpen(boolValue);
      assert(opt.adviseRandomOnOpen() == boolValue);
    }

    { // UseAdaptiveMutex test
      boolean boolValue = rand.nextBoolean();
      opt.setUseAdaptiveMutex(boolValue);
      assert(opt.useAdaptiveMutex() == boolValue);
    }

    { // BytesPerSync test
      long longValue = rand.nextLong();
      opt.setBytesPerSync(longValue);
      assert(opt.bytesPerSync() == longValue);
    }

    { // AllowThreadLocal test
      boolean boolValue = rand.nextBoolean();
      opt.setAllowThreadLocal(boolValue);
      assert(opt.allowThreadLocal() == boolValue);
    }

    { // WriteBufferSize test
      long longValue = rand.nextLong();
      opt.setWriteBufferSize(longValue);
      assert(opt.writeBufferSize() == longValue);
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
      int intValue = rand.nextInt();
      opt.setTargetFileSizeBase(intValue);
      assert(opt.targetFileSizeBase() == intValue);
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
      long longValue = rand.nextLong();
      opt.setArenaBlockSize(longValue);
      assert(opt.arenaBlockSize() == longValue);
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
      long longValue = rand.nextLong();
      opt.setInplaceUpdateNumLocks(longValue);
      assert(opt.inplaceUpdateNumLocks() == longValue);
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
      long longValue = rand.nextLong();
      opt.setMaxSuccessiveMerges(longValue);
      assert(opt.maxSuccessiveMerges() == longValue);
    }

    { // MinPartialMergeOperands test
      int intValue = rand.nextInt();
      opt.setMinPartialMergeOperands(intValue);
      assert(opt.minPartialMergeOperands() == intValue);
    }

    opt.dispose();
    System.out.println("Passed OptionsTest");
  }
}
