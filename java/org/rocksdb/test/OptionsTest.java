// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import java.util.Random;
import org.rocksdb.Options;

public class OptionsTest {
  static {
    System.loadLibrary("rocksdbjni");
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

    { // WALTtlSeconds test
      long longValue = rand.nextLong();
      opt.setWALTtlSeconds(longValue);
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

    opt.dispose();
    System.out.println("Passed OptionsTest");
  }
}
