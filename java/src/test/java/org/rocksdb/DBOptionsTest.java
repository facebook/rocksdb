// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.ClassRule;
import org.junit.Test;

public class DBOptionsTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void copyConstructor() {
    final DBOptions origOpts = new DBOptions();
    origOpts.setCreateIfMissing(rand.nextBoolean());
    origOpts.setAllow2pc(rand.nextBoolean());
    origOpts.setMaxBackgroundJobs(rand.nextInt(10));
    final DBOptions copyOpts = new DBOptions(origOpts);
    assertThat(origOpts.createIfMissing()).isEqualTo(copyOpts.createIfMissing());
    assertThat(origOpts.allow2pc()).isEqualTo(copyOpts.allow2pc());
  }

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
  public void linkageOfPrepMethods() {
    try (final DBOptions opt = new DBOptions()) {
      opt.optimizeForSmallDb();
    }
  }

  @Test
  public void env() {
    try (final DBOptions opt = new DBOptions();
         final Env env = Env.getDefault()) {
      opt.setEnv(env);
      assertThat(opt.getEnv()).isSameAs(env);
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
  public void maxFileOpeningThreads() {
    try(final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setMaxFileOpeningThreads(intValue);
      assertThat(opt.maxFileOpeningThreads()).isEqualTo(intValue);
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
  public void dbPaths() {
    final List<DbPath> dbPaths = new ArrayList<>();
    dbPaths.add(new DbPath(Paths.get("/a"), 10));
    dbPaths.add(new DbPath(Paths.get("/b"), 100));
    dbPaths.add(new DbPath(Paths.get("/c"), 1000));

    try(final DBOptions opt = new DBOptions()) {
      assertThat(opt.dbPaths()).isEqualTo(Collections.emptyList());

      opt.setDbPaths(dbPaths);

      assertThat(opt.dbPaths()).isEqualTo(dbPaths);
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

  @SuppressWarnings("deprecated")
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

  @SuppressWarnings("deprecated")
  @Test
  public void maxBackgroundFlushes() {
    try(final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setMaxBackgroundFlushes(intValue);
      assertThat(opt.maxBackgroundFlushes()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxBackgroundJobs() {
    try (final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setMaxBackgroundJobs(intValue);
      assertThat(opt.maxBackgroundJobs()).isEqualTo(intValue);
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
  public void recycleLogFileNum() throws RocksDBException {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setRecycleLogFileNum(longValue);
      assertThat(opt.recycleLogFileNum()).isEqualTo(longValue);
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
  public void useDirectReads() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseDirectReads(boolValue);
      assertThat(opt.useDirectReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useDirectIoForFlushAndCompaction() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseDirectIoForFlushAndCompaction(boolValue);
      assertThat(opt.useDirectIoForFlushAndCompaction()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowFAllocate() {
    try(final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowFAllocate(boolValue);
      assertThat(opt.allowFAllocate()).isEqualTo(boolValue);
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
  public void statsPersistPeriodSec() {
    try (final DBOptions opt = new DBOptions()) {
      final int intValue = rand.nextInt();
      opt.setStatsPersistPeriodSec(intValue);
      assertThat(opt.statsPersistPeriodSec()).isEqualTo(intValue);
    }
  }

  @Test
  public void statsHistoryBufferSize() {
    try (final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setStatsHistoryBufferSize(longValue);
      assertThat(opt.statsHistoryBufferSize()).isEqualTo(longValue);
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
  public void dbWriteBufferSize() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setDbWriteBufferSize(longValue);
      assertThat(opt.dbWriteBufferSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void setWriteBufferManager() throws RocksDBException {
    try (final DBOptions opt = new DBOptions(); final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager writeBufferManager = new WriteBufferManager(2000L, cache)) {
      opt.setWriteBufferManager(writeBufferManager);
      assertThat(opt.writeBufferManager()).isEqualTo(writeBufferManager);
    }
  }

  @Test
  public void setWriteBufferManagerWithZeroBufferSize() throws RocksDBException {
    try (final DBOptions opt = new DBOptions(); final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager writeBufferManager = new WriteBufferManager(0L, cache)) {
      opt.setWriteBufferManager(writeBufferManager);
      assertThat(opt.writeBufferManager()).isEqualTo(writeBufferManager);
    }
  }

  @Test
  public void compactionReadaheadSize() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setCompactionReadaheadSize(longValue);
      assertThat(opt.compactionReadaheadSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void randomAccessMaxBufferSize() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setRandomAccessMaxBufferSize(longValue);
      assertThat(opt.randomAccessMaxBufferSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void writableFileMaxBufferSize() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setWritableFileMaxBufferSize(longValue);
      assertThat(opt.writableFileMaxBufferSize()).isEqualTo(longValue);
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
  public void walBytesPerSync() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setWalBytesPerSync(longValue);
      assertThat(opt.walBytesPerSync()).isEqualTo(longValue);
    }
  }

  @Test
  public void strictBytesPerSync() {
    try (final DBOptions opt = new DBOptions()) {
      assertThat(opt.strictBytesPerSync()).isFalse();
      opt.setStrictBytesPerSync(true);
      assertThat(opt.strictBytesPerSync()).isTrue();
    }
  }

  @Test
  public void enableThreadTracking() {
    try (final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setEnableThreadTracking(boolValue);
      assertThat(opt.enableThreadTracking()).isEqualTo(boolValue);
    }
  }

  @Test
  public void delayedWriteRate() {
    try(final DBOptions opt = new DBOptions()) {
      final long longValue = rand.nextLong();
      opt.setDelayedWriteRate(longValue);
      assertThat(opt.delayedWriteRate()).isEqualTo(longValue);
    }
  }

  @Test
  public void enablePipelinedWrite() {
    try(final DBOptions opt = new DBOptions()) {
      assertThat(opt.enablePipelinedWrite()).isFalse();
      opt.setEnablePipelinedWrite(true);
      assertThat(opt.enablePipelinedWrite()).isTrue();
    }
  }

  @Test
  public void unordredWrite() {
    try(final DBOptions opt = new DBOptions()) {
      assertThat(opt.unorderedWrite()).isFalse();
      opt.setUnorderedWrite(true);
      assertThat(opt.unorderedWrite()).isTrue();
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
  public void skipStatsUpdateOnDbOpen() {
    try (final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setSkipStatsUpdateOnDbOpen(boolValue);
      assertThat(opt.skipStatsUpdateOnDbOpen()).isEqualTo(boolValue);
    }
  }

  @Test
  public void walRecoveryMode() {
    try (final DBOptions opt = new DBOptions()) {
      for (final WALRecoveryMode walRecoveryMode : WALRecoveryMode.values()) {
        opt.setWalRecoveryMode(walRecoveryMode);
        assertThat(opt.walRecoveryMode()).isEqualTo(walRecoveryMode);
      }
    }
  }

  @Test
  public void allow2pc() {
    try (final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllow2pc(boolValue);
      assertThat(opt.allow2pc()).isEqualTo(boolValue);
    }
  }

  @Test
  public void rowCache() {
    try (final DBOptions opt = new DBOptions()) {
      assertThat(opt.rowCache()).isNull();

      try(final Cache lruCache = new LRUCache(1000)) {
        opt.setRowCache(lruCache);
        assertThat(opt.rowCache()).isEqualTo(lruCache);
      }

      try(final Cache clockCache = new ClockCache(1000)) {
        opt.setRowCache(clockCache);
        assertThat(opt.rowCache()).isEqualTo(clockCache);
      }
    }
  }

  @Test
  public void walFilter() {
    try (final DBOptions opt = new DBOptions()) {
      assertThat(opt.walFilter()).isNull();

      try (final AbstractWalFilter walFilter = new AbstractWalFilter() {
        @Override
        public void columnFamilyLogNumberMap(
            final Map<Integer, Long> cfLognumber,
            final Map<String, Integer> cfNameId) {
          // no-op
        }

        @Override
        public LogRecordFoundResult logRecordFound(final long logNumber,
            final String logFileName, final WriteBatch batch,
            final WriteBatch newBatch) {
          return new LogRecordFoundResult(
              WalProcessingOption.CONTINUE_PROCESSING, false);
        }

        @Override
        public String name() {
          return "test-wal-filter";
        }
      }) {
        opt.setWalFilter(walFilter);
        assertThat(opt.walFilter()).isEqualTo(walFilter);
      }
    }
  }

  @Test
  public void failIfOptionsFileError() {
    try (final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setFailIfOptionsFileError(boolValue);
      assertThat(opt.failIfOptionsFileError()).isEqualTo(boolValue);
    }
  }

  @Test
  public void dumpMallocStats() {
    try (final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setDumpMallocStats(boolValue);
      assertThat(opt.dumpMallocStats()).isEqualTo(boolValue);
    }
  }

  @Test
  public void avoidFlushDuringRecovery() {
    try (final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAvoidFlushDuringRecovery(boolValue);
      assertThat(opt.avoidFlushDuringRecovery()).isEqualTo(boolValue);
    }
  }

  @Test
  public void avoidFlushDuringShutdown() {
    try (final DBOptions opt = new DBOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAvoidFlushDuringShutdown(boolValue);
      assertThat(opt.avoidFlushDuringShutdown()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowIngestBehind() {
    try (final DBOptions opt = new DBOptions()) {
      assertThat(opt.allowIngestBehind()).isFalse();
      opt.setAllowIngestBehind(true);
      assertThat(opt.allowIngestBehind()).isTrue();
    }
  }

  @Test
  public void twoWriteQueues() {
    try (final DBOptions opt = new DBOptions()) {
      assertThat(opt.twoWriteQueues()).isFalse();
      opt.setTwoWriteQueues(true);
      assertThat(opt.twoWriteQueues()).isTrue();
    }
  }

  @Test
  public void manualWalFlush() {
    try (final DBOptions opt = new DBOptions()) {
      assertThat(opt.manualWalFlush()).isFalse();
      opt.setManualWalFlush(true);
      assertThat(opt.manualWalFlush()).isTrue();
    }
  }

  @Test
  public void atomicFlush() {
    try (final DBOptions opt = new DBOptions()) {
      assertThat(opt.atomicFlush()).isFalse();
      opt.setAtomicFlush(true);
      assertThat(opt.atomicFlush()).isTrue();
    }
  }

  @Test
  public void rateLimiter() {
    try(final DBOptions options = new DBOptions();
        final DBOptions anotherOptions = new DBOptions();
        final RateLimiter rateLimiter = new RateLimiter(1000, 100 * 1000, 1)) {
      options.setRateLimiter(rateLimiter);
      // Test with parameter initialization
      anotherOptions.setRateLimiter(
          new RateLimiter(1000));
    }
  }

  @Test
  public void sstFileManager() throws RocksDBException {
    try (final DBOptions options = new DBOptions();
         final SstFileManager sstFileManager =
             new SstFileManager(Env.getDefault())) {
      options.setSstFileManager(sstFileManager);
    }
  }

  @Test
  public void statistics() {
    try(final DBOptions options = new DBOptions()) {
      final Statistics statistics = options.statistics();
      assertThat(statistics).isNull();
    }

    try(final Statistics statistics = new Statistics();
        final DBOptions options = new DBOptions().setStatistics(statistics);
        final Statistics stats = options.statistics()) {
      assertThat(stats).isNotNull();
    }
  }

  @Test
  public void avoidUnnecessaryBlockingIO() {
    try (final DBOptions options = new DBOptions()) {
      assertThat(options.avoidUnnecessaryBlockingIO()).isEqualTo(false);
      assertThat(options.setAvoidUnnecessaryBlockingIO(true)).isEqualTo(options);
      assertThat(options.avoidUnnecessaryBlockingIO()).isEqualTo(true);
    }
  }

  @Test
  public void persistStatsToDisk() {
    try (final DBOptions options = new DBOptions()) {
      assertThat(options.persistStatsToDisk()).isEqualTo(false);
      assertThat(options.setPersistStatsToDisk(true)).isEqualTo(options);
      assertThat(options.persistStatsToDisk()).isEqualTo(true);
    }
  }

  @Test
  public void writeDbidToManifest() {
    try (final DBOptions options = new DBOptions()) {
      assertThat(options.writeDbidToManifest()).isEqualTo(true);
      assertThat(options.setWriteDbidToManifest(false)).isEqualTo(options);
      assertThat(options.writeDbidToManifest()).isEqualTo(false);
    }
  }

  @Test
  public void logReadaheadSize() {
    try (final DBOptions options = new DBOptions()) {
      assertThat(options.logReadaheadSize()).isEqualTo(0);
      final int size = 1024 * 1024 * 100;
      assertThat(options.setLogReadaheadSize(size)).isEqualTo(options);
      assertThat(options.logReadaheadSize()).isEqualTo(size);
    }
  }

  @Test
  public void bestEffortsRecovery() {
    try (final DBOptions options = new DBOptions()) {
      assertThat(options.bestEffortsRecovery()).isEqualTo(false);
      assertThat(options.setBestEffortsRecovery(true)).isEqualTo(options);
      assertThat(options.bestEffortsRecovery()).isEqualTo(true);
    }
  }

  @Test
  public void maxBgerrorResumeCount() {
    try (final DBOptions options = new DBOptions()) {
      final int INT_MAX = 2147483647;
      assertThat(options.maxBgerrorResumeCount()).isEqualTo(INT_MAX);
      assertThat(options.setMaxBgErrorResumeCount(-1)).isEqualTo(options);
      assertThat(options.maxBgerrorResumeCount()).isEqualTo(-1);
    }
  }

  @Test
  public void bgerrorResumeRetryInterval() {
    try (final DBOptions options = new DBOptions()) {
      assertThat(options.bgerrorResumeRetryInterval()).isEqualTo(1000000);
      final long newRetryInterval = 24 * 3600 * 1000000L;
      assertThat(options.setBgerrorResumeRetryInterval(newRetryInterval)).isEqualTo(options);
      assertThat(options.bgerrorResumeRetryInterval()).isEqualTo(newRetryInterval);
    }
  }

  @Test
  public void maxWriteBatchGroupSizeBytes() {
    try (final DBOptions options = new DBOptions()) {
      assertThat(options.maxWriteBatchGroupSizeBytes()).isEqualTo(1024 * 1024);
      final long size = 1024 * 1024 * 1024 * 10L;
      assertThat(options.setMaxWriteBatchGroupSizeBytes(size)).isEqualTo(options);
      assertThat(options.maxWriteBatchGroupSizeBytes()).isEqualTo(size);
    }
  }

  @Test
  public void skipCheckingSstFileSizesOnDbOpen() {
    try (final DBOptions options = new DBOptions()) {
      assertThat(options.skipCheckingSstFileSizesOnDbOpen()).isEqualTo(false);
      assertThat(options.setSkipCheckingSstFileSizesOnDbOpen(true)).isEqualTo(options);
      assertThat(options.skipCheckingSstFileSizesOnDbOpen()).isEqualTo(true);
    }
  }

  @Test
  public void eventListeners() {
    final AtomicBoolean wasCalled1 = new AtomicBoolean();
    final AtomicBoolean wasCalled2 = new AtomicBoolean();
    try (final DBOptions options = new DBOptions();
         final AbstractEventListener el1 =
             new AbstractEventListener() {
               @Override
               public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
                 wasCalled1.set(true);
               }
             };
         final AbstractEventListener el2 =
             new AbstractEventListener() {
               @Override
               public void onMemTableSealed(final MemTableInfo memTableInfo) {
                 wasCalled2.set(true);
               }
             }) {
      assertThat(options.setListeners(null)).isEqualTo(options);
      assertThat(options.listeners().size()).isEqualTo(0);
      assertThat(options.setListeners(Arrays.asList(el1, el2))).isEqualTo(options);
      final List<AbstractEventListener> listeners = options.listeners();
      assertEquals(el1, listeners.get(0));
      assertEquals(el2, listeners.get(1));
      options.setListeners(Collections.emptyList());
      listeners.get(0).onTableFileDeleted(null);
      assertTrue(wasCalled1.get());
      listeners.get(1).onMemTableSealed(null);
      assertTrue(wasCalled2.get());
      final List<AbstractEventListener> listeners2 = options.listeners();
      assertNotNull(listeners2);
      assertEquals(0, listeners2.size());
    }
  }
}
