// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Random;

public class DBOptionsTest {
  static {
    RocksDB.loadLibrary();
  }

  public static void testDBOptions(DBOptionsInterface opt) {
    Random rand = PlatformRandomHelper.
        getPlatformSpecificRandomFactory();
    { // CreateIfMissing test
      boolean boolValue = rand.nextBoolean();
      opt.setCreateIfMissing(boolValue);
      assert(opt.createIfMissing() == boolValue);
    }

    { // CreateMissingColumnFamilies test
      boolean boolValue = rand.nextBoolean();
      opt.setCreateMissingColumnFamilies(boolValue);
      assert(opt.createMissingColumnFamilies() == boolValue);
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

    {
      // MaxTotalWalSize test
      long longValue = rand.nextLong();
      opt.setMaxTotalWalSize(longValue);
      assert(opt.maxTotalWalSize() == longValue);
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
      try {
        long longValue = rand.nextLong();
        opt.setMaxLogFileSize(longValue);
        assert(opt.maxLogFileSize() == longValue);
      } catch (RocksDBException e) {
        System.out.println(e.getMessage());
        assert(false);
      }
    }

    { // LogFileTimeToRoll test
      try {
        long longValue = rand.nextLong();
        opt.setLogFileTimeToRoll(longValue);
        assert(opt.logFileTimeToRoll() == longValue);
      } catch (RocksDBException e) {
        assert(false);
      }
    }

    { // KeepLogFileNum test
      try {
        long longValue = rand.nextLong();
        opt.setKeepLogFileNum(longValue);
        assert(opt.keepLogFileNum() == longValue);
      } catch (RocksDBException e) {
        assert(false);
      }
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
      try {
        long longValue = rand.nextLong();
        opt.setManifestPreallocationSize(longValue);
        assert(opt.manifestPreallocationSize() == longValue);
      } catch (RocksDBException e) {
        assert(false);
      }
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
  }

  public static void main(String[] args) {
    DBOptions opt = new DBOptions();
    testDBOptions(opt);
    opt.dispose();
    System.out.println("Passed DBOptionsTest");
  }
}
