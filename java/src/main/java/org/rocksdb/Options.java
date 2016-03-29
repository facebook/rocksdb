// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.ArrayList;
import java.util.List;

/**
 * Options to control the behavior of a database.  It will be used
 * during the creation of a {@link org.rocksdb.RocksDB} (i.e., RocksDB.open()).
 *
 * If {@link #dispose()} function is not called, then it will be GC'd
 * automaticallyand native resources will be released as part of the process.
 */
public class Options extends RocksObject
    implements DBOptionsInterface, ColumnFamilyOptionsInterface,
    MutableColumnFamilyOptionsInterface {
  static {
    RocksDB.loadLibrary();
  }
  /**
   * Construct options for opening a RocksDB.
   *
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::Options} in the c++ side.
   */
  public Options() {
    super(newOptions());
    env_ = Env.getDefault();
  }

  /**
   * Construct options for opening a RocksDB. Reusing database options
   * and column family options.
   *
   * @param dbOptions {@link org.rocksdb.DBOptions} instance
   * @param columnFamilyOptions {@link org.rocksdb.ColumnFamilyOptions}
   *     instance
   */
  public Options(final DBOptions dbOptions,
      final ColumnFamilyOptions columnFamilyOptions) {
    super(newOptions(dbOptions.nativeHandle_,
        columnFamilyOptions.nativeHandle_));
    env_ = Env.getDefault();
  }

  @Override
  public Options setIncreaseParallelism(final int totalThreads) {
    assert(isOwningHandle());
    setIncreaseParallelism(nativeHandle_, totalThreads);
    return this;
  }

  @Override
  public Options setCreateIfMissing(final boolean flag) {
    assert(isOwningHandle());
    setCreateIfMissing(nativeHandle_, flag);
    return this;
  }

  @Override
  public Options setCreateMissingColumnFamilies(final boolean flag) {
    assert(isOwningHandle());
    setCreateMissingColumnFamilies(nativeHandle_, flag);
    return this;
  }

  /**
   * Use the specified object to interact with the environment,
   * e.g. to read/write files, schedule background work, etc.
   * Default: {@link Env#getDefault()}
   *
   * @param env {@link Env} instance.
   * @return the instance of the current Options.
   */
  public Options setEnv(final Env env) {
    assert(isOwningHandle());
    setEnv(nativeHandle_, env.nativeHandle_);
    env_ = env;
    return this;
  }

  /**
   * Returns the set RocksEnv instance.
   *
   * @return {@link RocksEnv} instance set in the Options.
   */
  public Env getEnv() {
    return env_;
  }

  /**
   * <p>Set appropriate parameters for bulk loading.
   * The reason that this is a function that returns "this" instead of a
   * constructor is to enable chaining of multiple similar calls in the future.
   * </p>
   *
   * <p>All data will be in level 0 without any automatic compaction.
   * It's recommended to manually call CompactRange(NULL, NULL) before reading
   * from the database, because otherwise the read can be very slow.</p>
   *
   * @return the instance of the current Options.
   */
  public Options prepareForBulkLoad() {
    prepareForBulkLoad(nativeHandle_);
    return this;
  }

  @Override
  public boolean createIfMissing() {
    assert(isOwningHandle());
    return createIfMissing(nativeHandle_);
  }

  @Override
  public boolean createMissingColumnFamilies() {
    assert(isOwningHandle());
    return createMissingColumnFamilies(nativeHandle_);
  }

  @Override
  public Options optimizeForPointLookup(
      long blockCacheSizeMb) {
    optimizeForPointLookup(nativeHandle_,
        blockCacheSizeMb);
    return this;
  }

  @Override
  public Options optimizeLevelStyleCompaction() {
    optimizeLevelStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public Options optimizeLevelStyleCompaction(
      long memtableMemoryBudget) {
    optimizeLevelStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public Options optimizeUniversalStyleCompaction() {
    optimizeUniversalStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public Options optimizeUniversalStyleCompaction(
      final long memtableMemoryBudget) {
    optimizeUniversalStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public Options setComparator(final BuiltinComparator builtinComparator) {
    assert(isOwningHandle());
    setComparatorHandle(nativeHandle_, builtinComparator.ordinal());
    return this;
  }

  @Override
  public Options setComparator(
      final AbstractComparator<? extends AbstractSlice<?>> comparator) {
    assert(isOwningHandle());
    setComparatorHandle(nativeHandle_, comparator.getNativeHandle());
    comparator_ = comparator;
    return this;
  }

  @Override
  public Options setMergeOperatorName(final String name) {
    assert(isOwningHandle());
    if (name == null) {
      throw new IllegalArgumentException(
          "Merge operator name must not be null.");
    }
    setMergeOperatorName(nativeHandle_, name);
    return this;
  }

  @Override
  public Options setMergeOperator(final MergeOperator mergeOperator) {
    setMergeOperator(nativeHandle_, mergeOperator.newMergeOperatorHandle());
    return this;
  }

  @Override
  public Options setWriteBufferSize(final long writeBufferSize) {
    assert(isOwningHandle());
    setWriteBufferSize(nativeHandle_, writeBufferSize);
    return this;
  }

  @Override
  public long writeBufferSize()  {
    assert(isOwningHandle());
    return writeBufferSize(nativeHandle_);
  }

  @Override
  public Options setMaxWriteBufferNumber(final int maxWriteBufferNumber) {
    assert(isOwningHandle());
    setMaxWriteBufferNumber(nativeHandle_, maxWriteBufferNumber);
    return this;
  }

  @Override
  public int maxWriteBufferNumber() {
    assert(isOwningHandle());
    return maxWriteBufferNumber(nativeHandle_);
  }

  @Override
  public boolean errorIfExists() {
    assert(isOwningHandle());
    return errorIfExists(nativeHandle_);
  }

  @Override
  public Options setErrorIfExists(final boolean errorIfExists) {
    assert(isOwningHandle());
    setErrorIfExists(nativeHandle_, errorIfExists);
    return this;
  }

  @Override
  public boolean paranoidChecks() {
    assert(isOwningHandle());
    return paranoidChecks(nativeHandle_);
  }

  @Override
  public Options setParanoidChecks(final boolean paranoidChecks) {
    assert(isOwningHandle());
    setParanoidChecks(nativeHandle_, paranoidChecks);
    return this;
  }

  @Override
  public int maxOpenFiles() {
    assert(isOwningHandle());
    return maxOpenFiles(nativeHandle_);
  }

  @Override
  public Options setMaxTotalWalSize(final long maxTotalWalSize) {
    assert(isOwningHandle());
    setMaxTotalWalSize(nativeHandle_, maxTotalWalSize);
    return this;
  }

  @Override
  public long maxTotalWalSize() {
    assert(isOwningHandle());
    return maxTotalWalSize(nativeHandle_);
  }

  @Override
  public Options setMaxOpenFiles(final int maxOpenFiles) {
    assert(isOwningHandle());
    setMaxOpenFiles(nativeHandle_, maxOpenFiles);
    return this;
  }

  @Override
  public boolean disableDataSync() {
    assert(isOwningHandle());
    return disableDataSync(nativeHandle_);
  }

  @Override
  public Options setDisableDataSync(final boolean disableDataSync) {
    assert(isOwningHandle());
    setDisableDataSync(nativeHandle_, disableDataSync);
    return this;
  }

  @Override
  public boolean useFsync() {
    assert(isOwningHandle());
    return useFsync(nativeHandle_);
  }

  @Override
  public Options setUseFsync(final boolean useFsync) {
    assert(isOwningHandle());
    setUseFsync(nativeHandle_, useFsync);
    return this;
  }

  @Override
  public String dbLogDir() {
    assert(isOwningHandle());
    return dbLogDir(nativeHandle_);
  }

  @Override
  public Options setDbLogDir(final String dbLogDir) {
    assert(isOwningHandle());
    setDbLogDir(nativeHandle_, dbLogDir);
    return this;
  }

  @Override
  public String walDir() {
    assert(isOwningHandle());
    return walDir(nativeHandle_);
  }

  @Override
  public Options setWalDir(final String walDir) {
    assert(isOwningHandle());
    setWalDir(nativeHandle_, walDir);
    return this;
  }

  @Override
  public long deleteObsoleteFilesPeriodMicros() {
    assert(isOwningHandle());
    return deleteObsoleteFilesPeriodMicros(nativeHandle_);
  }

  @Override
  public Options setDeleteObsoleteFilesPeriodMicros(
      final long micros) {
    assert(isOwningHandle());
    setDeleteObsoleteFilesPeriodMicros(nativeHandle_, micros);
    return this;
  }

  @Override
  public int maxBackgroundCompactions() {
    assert(isOwningHandle());
    return maxBackgroundCompactions(nativeHandle_);
  }

  @Override
  public Options createStatistics() {
    assert(isOwningHandle());
    createStatistics(nativeHandle_);
    return this;
  }

  @Override
  public Statistics statisticsPtr() {
    assert(isOwningHandle());

    long statsPtr = statisticsPtr(nativeHandle_);
    if(statsPtr == 0) {
      createStatistics();
      statsPtr = statisticsPtr(nativeHandle_);
    }

    return new Statistics(statsPtr);
  }

  @Override
  public Options setMaxBackgroundCompactions(
      final int maxBackgroundCompactions) {
    assert(isOwningHandle());
    setMaxBackgroundCompactions(nativeHandle_, maxBackgroundCompactions);
    return this;
  }

  @Override
  public int maxBackgroundFlushes() {
    assert(isOwningHandle());
    return maxBackgroundFlushes(nativeHandle_);
  }

  @Override
  public Options setMaxBackgroundFlushes(
      final int maxBackgroundFlushes) {
    assert(isOwningHandle());
    setMaxBackgroundFlushes(nativeHandle_, maxBackgroundFlushes);
    return this;
  }

  @Override
  public long maxLogFileSize() {
    assert(isOwningHandle());
    return maxLogFileSize(nativeHandle_);
  }

  @Override
  public Options setMaxLogFileSize(final long maxLogFileSize) {
    assert(isOwningHandle());
    setMaxLogFileSize(nativeHandle_, maxLogFileSize);
    return this;
  }

  @Override
  public long logFileTimeToRoll() {
    assert(isOwningHandle());
    return logFileTimeToRoll(nativeHandle_);
  }

  @Override
  public Options setLogFileTimeToRoll(final long logFileTimeToRoll) {
    assert(isOwningHandle());
    setLogFileTimeToRoll(nativeHandle_, logFileTimeToRoll);
    return this;
  }

  @Override
  public long keepLogFileNum() {
    assert(isOwningHandle());
    return keepLogFileNum(nativeHandle_);
  }

  @Override
  public Options setKeepLogFileNum(final long keepLogFileNum) {
    assert(isOwningHandle());
    setKeepLogFileNum(nativeHandle_, keepLogFileNum);
    return this;
  }

  @Override
  public long maxManifestFileSize() {
    assert(isOwningHandle());
    return maxManifestFileSize(nativeHandle_);
  }

  @Override
  public Options setMaxManifestFileSize(
      final long maxManifestFileSize) {
    assert(isOwningHandle());
    setMaxManifestFileSize(nativeHandle_, maxManifestFileSize);
    return this;
  }

  @Override
  public Options setMaxTableFilesSizeFIFO(
    final long maxTableFilesSize) {
    assert(maxTableFilesSize > 0); // unsigned native type
    assert(isOwningHandle());
    setMaxTableFilesSizeFIFO(nativeHandle_, maxTableFilesSize);
    return this;
  }

  @Override
  public long maxTableFilesSizeFIFO() {
    return maxTableFilesSizeFIFO(nativeHandle_);
  }

  @Override
  public int tableCacheNumshardbits() {
    assert(isOwningHandle());
    return tableCacheNumshardbits(nativeHandle_);
  }

  @Override
  public Options setTableCacheNumshardbits(
      final int tableCacheNumshardbits) {
    assert(isOwningHandle());
    setTableCacheNumshardbits(nativeHandle_, tableCacheNumshardbits);
    return this;
  }

  @Override
  public long walTtlSeconds() {
    assert(isOwningHandle());
    return walTtlSeconds(nativeHandle_);
  }

  @Override
  public Options setWalTtlSeconds(final long walTtlSeconds) {
    assert(isOwningHandle());
    setWalTtlSeconds(nativeHandle_, walTtlSeconds);
    return this;
  }

  @Override
  public long walSizeLimitMB() {
    assert(isOwningHandle());
    return walSizeLimitMB(nativeHandle_);
  }

  @Override
  public Options setWalSizeLimitMB(final long sizeLimitMB) {
    assert(isOwningHandle());
    setWalSizeLimitMB(nativeHandle_, sizeLimitMB);
    return this;
  }

  @Override
  public long manifestPreallocationSize() {
    assert(isOwningHandle());
    return manifestPreallocationSize(nativeHandle_);
  }

  @Override
  public Options setManifestPreallocationSize(final long size) {
    assert(isOwningHandle());
    setManifestPreallocationSize(nativeHandle_, size);
    return this;
  }

  @Override
  public boolean allowOsBuffer() {
    assert(isOwningHandle());
    return allowOsBuffer(nativeHandle_);
  }

  @Override
  public Options setAllowOsBuffer(final boolean allowOsBuffer) {
    assert(isOwningHandle());
    setAllowOsBuffer(nativeHandle_, allowOsBuffer);
    return this;
  }

  @Override
  public boolean allowMmapReads() {
    assert(isOwningHandle());
    return allowMmapReads(nativeHandle_);
  }

  @Override
  public Options setAllowMmapReads(final boolean allowMmapReads) {
    assert(isOwningHandle());
    setAllowMmapReads(nativeHandle_, allowMmapReads);
    return this;
  }

  @Override
  public boolean allowMmapWrites() {
    assert(isOwningHandle());
    return allowMmapWrites(nativeHandle_);
  }

  @Override
  public Options setAllowMmapWrites(final boolean allowMmapWrites) {
    assert(isOwningHandle());
    setAllowMmapWrites(nativeHandle_, allowMmapWrites);
    return this;
  }

  @Override
  public boolean isFdCloseOnExec() {
    assert(isOwningHandle());
    return isFdCloseOnExec(nativeHandle_);
  }

  @Override
  public Options setIsFdCloseOnExec(final boolean isFdCloseOnExec) {
    assert(isOwningHandle());
    setIsFdCloseOnExec(nativeHandle_, isFdCloseOnExec);
    return this;
  }

  @Override
  public int statsDumpPeriodSec() {
    assert(isOwningHandle());
    return statsDumpPeriodSec(nativeHandle_);
  }

  @Override
  public Options setStatsDumpPeriodSec(final int statsDumpPeriodSec) {
    assert(isOwningHandle());
    setStatsDumpPeriodSec(nativeHandle_, statsDumpPeriodSec);
    return this;
  }

  @Override
  public boolean adviseRandomOnOpen() {
    return adviseRandomOnOpen(nativeHandle_);
  }

  @Override
  public Options setAdviseRandomOnOpen(final boolean adviseRandomOnOpen) {
    assert(isOwningHandle());
    setAdviseRandomOnOpen(nativeHandle_, adviseRandomOnOpen);
    return this;
  }

  @Override
  public boolean useAdaptiveMutex() {
    assert(isOwningHandle());
    return useAdaptiveMutex(nativeHandle_);
  }

  @Override
  public Options setUseAdaptiveMutex(final boolean useAdaptiveMutex) {
    assert(isOwningHandle());
    setUseAdaptiveMutex(nativeHandle_, useAdaptiveMutex);
    return this;
  }

  @Override
  public long bytesPerSync() {
    return bytesPerSync(nativeHandle_);
  }

  @Override
  public Options setBytesPerSync(final long bytesPerSync) {
    assert(isOwningHandle());
    setBytesPerSync(nativeHandle_, bytesPerSync);
    return this;
  }

  @Override
  public Options setMemTableConfig(final MemTableConfig config) {
    memTableConfig_ = config;
    setMemTableFactory(nativeHandle_, config.newMemTableFactoryHandle());
    return this;
  }

  @Override
  public Options setRateLimiterConfig(final RateLimiterConfig config) {
    rateLimiterConfig_ = config;
    setRateLimiter(nativeHandle_, config.newRateLimiterHandle());
    return this;
  }

  @Override
  public Options setLogger(final Logger logger) {
    assert(isOwningHandle());
    setLogger(nativeHandle_, logger.nativeHandle_);
    return this;
  }

  @Override
  public Options setInfoLogLevel(final InfoLogLevel infoLogLevel) {
    assert(isOwningHandle());
    setInfoLogLevel(nativeHandle_, infoLogLevel.getValue());
    return this;
  }

  @Override
  public InfoLogLevel infoLogLevel() {
    assert(isOwningHandle());
    return InfoLogLevel.getInfoLogLevel(
        infoLogLevel(nativeHandle_));
  }

  @Override
  public String memTableFactoryName() {
    assert(isOwningHandle());
    return memTableFactoryName(nativeHandle_);
  }

  @Override
  public Options setTableFormatConfig(final TableFormatConfig config) {
    tableFormatConfig_ = config;
    setTableFactory(nativeHandle_, config.newTableFactoryHandle());
    return this;
  }

  @Override
  public String tableFactoryName() {
    assert(isOwningHandle());
    return tableFactoryName(nativeHandle_);
  }

  @Override
  public Options useFixedLengthPrefixExtractor(final int n) {
    assert(isOwningHandle());
    useFixedLengthPrefixExtractor(nativeHandle_, n);
    return this;
  }

  @Override
  public Options useCappedPrefixExtractor(final int n) {
    assert(isOwningHandle());
    useCappedPrefixExtractor(nativeHandle_, n);
    return this;
  }

  @Override
  public CompressionType compressionType() {
    return CompressionType.values()[compressionType(nativeHandle_)];
  }

  @Override
  public Options setCompressionPerLevel(
      final List<CompressionType> compressionLevels) {
    final byte[] byteCompressionTypes = new byte[
        compressionLevels.size()];
    for (int i = 0; i < compressionLevels.size(); i++) {
      byteCompressionTypes[i] = compressionLevels.get(i).getValue();
    }
    setCompressionPerLevel(nativeHandle_, byteCompressionTypes);
    return this;
  }

  @Override
  public List<CompressionType> compressionPerLevel() {
    final byte[] byteCompressionTypes =
        compressionPerLevel(nativeHandle_);
    final List<CompressionType> compressionLevels = new ArrayList<>();
    for (final Byte byteCompressionType : byteCompressionTypes) {
      compressionLevels.add(CompressionType.getCompressionType(
          byteCompressionType));
    }
    return compressionLevels;
  }

  @Override
  public Options setCompressionType(CompressionType compressionType) {
    setCompressionType(nativeHandle_, compressionType.getValue());
    return this;
  }

  @Override
  public CompactionStyle compactionStyle() {
    return CompactionStyle.values()[compactionStyle(nativeHandle_)];
  }

  @Override
  public Options setCompactionStyle(
      final CompactionStyle compactionStyle) {
    setCompactionStyle(nativeHandle_, compactionStyle.getValue());
    return this;
  }

  @Override
  public int numLevels() {
    return numLevels(nativeHandle_);
  }

  @Override
  public Options setNumLevels(int numLevels) {
    setNumLevels(nativeHandle_, numLevels);
    return this;
  }

  @Override
  public int levelZeroFileNumCompactionTrigger() {
    return levelZeroFileNumCompactionTrigger(nativeHandle_);
  }

  @Override
  public Options setLevelZeroFileNumCompactionTrigger(
      final int numFiles) {
    setLevelZeroFileNumCompactionTrigger(
        nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroSlowdownWritesTrigger() {
    return levelZeroSlowdownWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setLevelZeroSlowdownWritesTrigger(
      final int numFiles) {
    setLevelZeroSlowdownWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroStopWritesTrigger() {
    return levelZeroStopWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setLevelZeroStopWritesTrigger(
      final int numFiles) {
    setLevelZeroStopWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int maxMemCompactionLevel() {
    return 0;
  }

  @Override
  public Options setMaxMemCompactionLevel(
      final int maxMemCompactionLevel) {
    return this;
  }

  @Override
  public long targetFileSizeBase() {
    return targetFileSizeBase(nativeHandle_);
  }

  @Override
  public Options setTargetFileSizeBase(long targetFileSizeBase) {
    setTargetFileSizeBase(nativeHandle_, targetFileSizeBase);
    return this;
  }

  @Override
  public int targetFileSizeMultiplier() {
    return targetFileSizeMultiplier(nativeHandle_);
  }

  @Override
  public Options setTargetFileSizeMultiplier(int multiplier) {
    setTargetFileSizeMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public Options setMaxBytesForLevelBase(final long maxBytesForLevelBase) {
    setMaxBytesForLevelBase(nativeHandle_, maxBytesForLevelBase);
    return this;
  }

  @Override
  public long maxBytesForLevelBase() {
    return maxBytesForLevelBase(nativeHandle_);
  }

  @Override
  public Options setLevelCompactionDynamicLevelBytes(
      final boolean enableLevelCompactionDynamicLevelBytes) {
    setLevelCompactionDynamicLevelBytes(nativeHandle_,
        enableLevelCompactionDynamicLevelBytes);
    return this;
  }

  @Override
  public boolean levelCompactionDynamicLevelBytes() {
    return levelCompactionDynamicLevelBytes(nativeHandle_);
  }

  @Override
  public int maxBytesForLevelMultiplier() {
    return maxBytesForLevelMultiplier(nativeHandle_);
  }

  @Override
  public Options setMaxBytesForLevelMultiplier(final int multiplier) {
    setMaxBytesForLevelMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public int expandedCompactionFactor() {
    return expandedCompactionFactor(nativeHandle_);
  }

  @Override
  public Options setExpandedCompactionFactor(
      final int expandedCompactionFactor) {
    setExpandedCompactionFactor(nativeHandle_, expandedCompactionFactor);
    return this;
  }

  @Override
  public int sourceCompactionFactor() {
    return sourceCompactionFactor(nativeHandle_);
  }

  @Override
  public Options setSourceCompactionFactor(int sourceCompactionFactor) {
    setSourceCompactionFactor(nativeHandle_, sourceCompactionFactor);
    return this;
  }

  @Override
  public int maxGrandparentOverlapFactor() {
    return maxGrandparentOverlapFactor(nativeHandle_);
  }

  @Override
  public Options setMaxGrandparentOverlapFactor(
      final int maxGrandparentOverlapFactor) {
    setMaxGrandparentOverlapFactor(nativeHandle_, maxGrandparentOverlapFactor);
    return this;
  }

  @Override
  public double softRateLimit() {
    return softRateLimit(nativeHandle_);
  }

  @Override
  public Options setSoftRateLimit(final double softRateLimit) {
    setSoftRateLimit(nativeHandle_, softRateLimit);
    return this;
  }

  @Override
  public double hardRateLimit() {
    return hardRateLimit(nativeHandle_);
  }

  @Override
  public Options setHardRateLimit(double hardRateLimit) {
    setHardRateLimit(nativeHandle_, hardRateLimit);
    return this;
  }

  @Override
  public int rateLimitDelayMaxMilliseconds() {
    return rateLimitDelayMaxMilliseconds(nativeHandle_);
  }

  @Override
  public Options setRateLimitDelayMaxMilliseconds(
      final int rateLimitDelayMaxMilliseconds) {
    setRateLimitDelayMaxMilliseconds(
        nativeHandle_, rateLimitDelayMaxMilliseconds);
    return this;
  }

  @Override
  public long arenaBlockSize() {
    return arenaBlockSize(nativeHandle_);
  }

  @Override
  public Options setArenaBlockSize(final long arenaBlockSize) {
    setArenaBlockSize(nativeHandle_, arenaBlockSize);
    return this;
  }

  @Override
  public boolean disableAutoCompactions() {
    return disableAutoCompactions(nativeHandle_);
  }

  @Override
  public Options setDisableAutoCompactions(
      final boolean disableAutoCompactions) {
    setDisableAutoCompactions(nativeHandle_, disableAutoCompactions);
    return this;
  }

  @Override
  public boolean purgeRedundantKvsWhileFlush() {
    return purgeRedundantKvsWhileFlush(nativeHandle_);
  }

  @Override
  public Options setPurgeRedundantKvsWhileFlush(
      final boolean purgeRedundantKvsWhileFlush) {
    setPurgeRedundantKvsWhileFlush(
        nativeHandle_, purgeRedundantKvsWhileFlush);
    return this;
  }

  @Override
  public boolean verifyChecksumsInCompaction() {
    return verifyChecksumsInCompaction(nativeHandle_);
  }

  @Override
  public Options setVerifyChecksumsInCompaction(
      final boolean verifyChecksumsInCompaction) {
    setVerifyChecksumsInCompaction(
        nativeHandle_, verifyChecksumsInCompaction);
    return this;
  }

  @Override
  public long maxSequentialSkipInIterations() {
    return maxSequentialSkipInIterations(nativeHandle_);
  }

  @Override
  public Options setMaxSequentialSkipInIterations(
      final long maxSequentialSkipInIterations) {
    setMaxSequentialSkipInIterations(nativeHandle_,
        maxSequentialSkipInIterations);
    return this;
  }

  @Override
  public boolean inplaceUpdateSupport() {
    return inplaceUpdateSupport(nativeHandle_);
  }

  @Override
  public Options setInplaceUpdateSupport(
      final boolean inplaceUpdateSupport) {
    setInplaceUpdateSupport(nativeHandle_, inplaceUpdateSupport);
    return this;
  }

  @Override
  public long inplaceUpdateNumLocks() {
    return inplaceUpdateNumLocks(nativeHandle_);
  }

  @Override
  public Options setInplaceUpdateNumLocks(
      final long inplaceUpdateNumLocks) {
    setInplaceUpdateNumLocks(nativeHandle_, inplaceUpdateNumLocks);
    return this;
  }

  @Override
  public double memtablePrefixBloomSizeRatio() {
    return memtablePrefixBloomSizeRatio(nativeHandle_);
  }

  @Override
  public Options setMemtablePrefixBloomSizeRatio(final double memtablePrefixBloomSizeRatio) {
    setMemtablePrefixBloomSizeRatio(nativeHandle_, memtablePrefixBloomSizeRatio);
    return this;
  }

  @Override
  public int bloomLocality() {
    return bloomLocality(nativeHandle_);
  }

  @Override
  public Options setBloomLocality(final int bloomLocality) {
    setBloomLocality(nativeHandle_, bloomLocality);
    return this;
  }

  @Override
  public long maxSuccessiveMerges() {
    return maxSuccessiveMerges(nativeHandle_);
  }

  @Override
  public Options setMaxSuccessiveMerges(long maxSuccessiveMerges) {
    setMaxSuccessiveMerges(nativeHandle_, maxSuccessiveMerges);
    return this;
  }

  @Override
  public int minWriteBufferNumberToMerge() {
    return minWriteBufferNumberToMerge(nativeHandle_);
  }

  @Override
  public Options setMinWriteBufferNumberToMerge(
      final int minWriteBufferNumberToMerge) {
    setMinWriteBufferNumberToMerge(nativeHandle_, minWriteBufferNumberToMerge);
    return this;
  }

  @Override
  public int minPartialMergeOperands() {
    return minPartialMergeOperands(nativeHandle_);
  }

  @Override
  public Options setMinPartialMergeOperands(
      final int minPartialMergeOperands) {
    setMinPartialMergeOperands(nativeHandle_, minPartialMergeOperands);
    return this;
  }

  @Override
  public Options setOptimizeFiltersForHits(
      final boolean optimizeFiltersForHits) {
    setOptimizeFiltersForHits(nativeHandle_, optimizeFiltersForHits);
    return this;
  }

  @Override
  public boolean optimizeFiltersForHits() {
    return optimizeFiltersForHits(nativeHandle_);
  }

  @Override
  public Options
  setMemtableHugePageSize(
      long memtableHugePageSize) {
    setMemtableHugePageSize(nativeHandle_,
        memtableHugePageSize);
    return this;
  }

  @Override
  public long memtableHugePageSize() {
    return memtableHugePageSize(nativeHandle_);
  }

  @Override
  public Options setSoftPendingCompactionBytesLimit(long softPendingCompactionBytesLimit) {
    setSoftPendingCompactionBytesLimit(nativeHandle_,
        softPendingCompactionBytesLimit);
    return this;
  }

  @Override
  public long softPendingCompactionBytesLimit() {
    return softPendingCompactionBytesLimit(nativeHandle_);
  }

  @Override
  public Options setHardPendingCompactionBytesLimit(long hardPendingCompactionBytesLimit) {
    setHardPendingCompactionBytesLimit(nativeHandle_, hardPendingCompactionBytesLimit);
    return this;
  }

  @Override
  public long hardPendingCompactionBytesLimit() {
    return hardPendingCompactionBytesLimit(nativeHandle_);
  }

  @Override
  public Options setLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger) {
    setLevel0FileNumCompactionTrigger(nativeHandle_, level0FileNumCompactionTrigger);
    return this;
  }

  @Override
  public int level0FileNumCompactionTrigger() {
    return level0FileNumCompactionTrigger(nativeHandle_);
  }

  @Override
  public Options setLevel0SlowdownWritesTrigger(int level0SlowdownWritesTrigger) {
    setLevel0SlowdownWritesTrigger(nativeHandle_, level0SlowdownWritesTrigger);
    return this;
  }

  @Override
  public int level0SlowdownWritesTrigger() {
    return level0SlowdownWritesTrigger(nativeHandle_);
  }

  @Override
  public MutableColumnFamilyOptionsInterface setLevel0StopWritesTrigger(int level0StopWritesTrigger) {
    setLevel0StopWritesTrigger(nativeHandle_, level0StopWritesTrigger);
    return this;
  }

  @Override
  public int level0StopWritesTrigger() {
    return level0StopWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setMaxBytesForLevelMultiplierAdditional(int[] maxBytesForLevelMultiplierAdditional) {
    setMaxBytesForLevelMultiplierAdditional(nativeHandle_, maxBytesForLevelMultiplierAdditional);
    return this;
  }

  @Override
  public int[] maxBytesForLevelMultiplierAdditional() {
    return maxBytesForLevelMultiplierAdditional(nativeHandle_);
  }

  @Override
  public Options setParanoidFileChecks(boolean paranoidFileChecks) {
    setParanoidFileChecks(nativeHandle_, paranoidFileChecks);
    return this;
  }

  @Override
  public boolean paranoidFileChecks() {
    return paranoidFileChecks(nativeHandle_);
  }

  private native static long newOptions();
  private native static long newOptions(long dbOptHandle,
      long cfOptHandle);
  @Override protected final native void disposeInternal(final long handle);
  private native void setEnv(long optHandle, long envHandle);
  private native void prepareForBulkLoad(long handle);

  // DB native handles
  private native void setIncreaseParallelism(long handle, int totalThreads);
  private native void setCreateIfMissing(long handle, boolean flag);
  private native boolean createIfMissing(long handle);
  private native void setCreateMissingColumnFamilies(
      long handle, boolean flag);
  private native boolean createMissingColumnFamilies(long handle);
  private native void setErrorIfExists(long handle, boolean errorIfExists);
  private native boolean errorIfExists(long handle);
  private native void setParanoidChecks(
      long handle, boolean paranoidChecks);
  private native boolean paranoidChecks(long handle);
  private native void setRateLimiter(long handle,
      long rateLimiterHandle);
  private native void setLogger(long handle,
      long loggerHandle);
  private native void setInfoLogLevel(long handle, byte logLevel);
  private native byte infoLogLevel(long handle);
  private native void setMaxOpenFiles(long handle, int maxOpenFiles);
  private native int maxOpenFiles(long handle);
  private native void setMaxTotalWalSize(long handle,
      long maxTotalWalSize);
  private native long maxTotalWalSize(long handle);
  private native void createStatistics(long optHandle);
  private native long statisticsPtr(long optHandle);
  private native void setDisableDataSync(long handle, boolean disableDataSync);
  private native boolean disableDataSync(long handle);
  private native boolean useFsync(long handle);
  private native void setUseFsync(long handle, boolean useFsync);
  private native void setDbLogDir(long handle, String dbLogDir);
  private native String dbLogDir(long handle);
  private native void setWalDir(long handle, String walDir);
  private native String walDir(long handle);
  private native void setDeleteObsoleteFilesPeriodMicros(
      long handle, long micros);
  private native long deleteObsoleteFilesPeriodMicros(long handle);
  private native void setMaxBackgroundCompactions(
      long handle, int maxBackgroundCompactions);
  private native int maxBackgroundCompactions(long handle);
  private native void setMaxBackgroundFlushes(
      long handle, int maxBackgroundFlushes);
  private native int maxBackgroundFlushes(long handle);
  private native void setMaxLogFileSize(long handle, long maxLogFileSize)
      throws IllegalArgumentException;
  private native long maxLogFileSize(long handle);
  private native void setLogFileTimeToRoll(
      long handle, long logFileTimeToRoll) throws IllegalArgumentException;
  private native long logFileTimeToRoll(long handle);
  private native void setKeepLogFileNum(long handle, long keepLogFileNum)
      throws IllegalArgumentException;
  private native long keepLogFileNum(long handle);
  private native void setMaxManifestFileSize(
      long handle, long maxManifestFileSize);
  private native long maxManifestFileSize(long handle);
  private native void setMaxTableFilesSizeFIFO(
      long handle, long maxTableFilesSize);
  private native long maxTableFilesSizeFIFO(long handle);
  private native void setTableCacheNumshardbits(
      long handle, int tableCacheNumshardbits);
  private native int tableCacheNumshardbits(long handle);
  private native void setWalTtlSeconds(long handle, long walTtlSeconds);
  private native long walTtlSeconds(long handle);
  private native void setWalSizeLimitMB(long handle, long sizeLimitMB);
  private native long walSizeLimitMB(long handle);
  private native void setManifestPreallocationSize(
      long handle, long size) throws IllegalArgumentException;
  private native long manifestPreallocationSize(long handle);
  private native void setAllowOsBuffer(
      long handle, boolean allowOsBuffer);
  private native boolean allowOsBuffer(long handle);
  private native void setAllowMmapReads(
      long handle, boolean allowMmapReads);
  private native boolean allowMmapReads(long handle);
  private native void setAllowMmapWrites(
      long handle, boolean allowMmapWrites);
  private native boolean allowMmapWrites(long handle);
  private native void setIsFdCloseOnExec(
      long handle, boolean isFdCloseOnExec);
  private native boolean isFdCloseOnExec(long handle);
  private native void setStatsDumpPeriodSec(
      long handle, int statsDumpPeriodSec);
  private native int statsDumpPeriodSec(long handle);
  private native void setAdviseRandomOnOpen(
      long handle, boolean adviseRandomOnOpen);
  private native boolean adviseRandomOnOpen(long handle);
  private native void setUseAdaptiveMutex(
      long handle, boolean useAdaptiveMutex);
  private native boolean useAdaptiveMutex(long handle);
  private native void setBytesPerSync(
      long handle, long bytesPerSync);
  private native long bytesPerSync(long handle);
  // CF native handles
  private native void optimizeForPointLookup(long handle,
      long blockCacheSizeMb);
  private native void optimizeLevelStyleCompaction(long handle,
      long memtableMemoryBudget);
  private native void optimizeUniversalStyleCompaction(long handle,
      long memtableMemoryBudget);
  private native void setComparatorHandle(long handle, int builtinComparator);
  private native void setComparatorHandle(long optHandle,
      long comparatorHandle);
  private native void setMergeOperatorName(
      long handle, String name);
  private native void setMergeOperator(
      long handle, long mergeOperatorHandle);
  private native void setWriteBufferSize(long handle, long writeBufferSize)
      throws IllegalArgumentException;
  private native long writeBufferSize(long handle);
  private native void setMaxWriteBufferNumber(
      long handle, int maxWriteBufferNumber);
  private native int maxWriteBufferNumber(long handle);
  private native void setMinWriteBufferNumberToMerge(
      long handle, int minWriteBufferNumberToMerge);
  private native int minWriteBufferNumberToMerge(long handle);
  private native void setCompressionType(long handle, byte compressionType);
  private native byte compressionType(long handle);
  private native void setCompressionPerLevel(long handle,
      byte[] compressionLevels);
  private native byte[] compressionPerLevel(long handle);
  private native void useFixedLengthPrefixExtractor(
      long handle, int prefixLength);
  private native void useCappedPrefixExtractor(
      long handle, int prefixLength);
  private native void setNumLevels(
      long handle, int numLevels);
  private native int numLevels(long handle);
  private native void setLevelZeroFileNumCompactionTrigger(
      long handle, int numFiles);
  private native int levelZeroFileNumCompactionTrigger(long handle);
  private native void setLevelZeroSlowdownWritesTrigger(
      long handle, int numFiles);
  private native int levelZeroSlowdownWritesTrigger(long handle);
  private native void setLevelZeroStopWritesTrigger(
      long handle, int numFiles);
  private native int levelZeroStopWritesTrigger(long handle);
  private native void setTargetFileSizeBase(
      long handle, long targetFileSizeBase);
  private native long targetFileSizeBase(long handle);
  private native void setTargetFileSizeMultiplier(
      long handle, int multiplier);
  private native int targetFileSizeMultiplier(long handle);
  private native void setMaxBytesForLevelBase(
      long handle, long maxBytesForLevelBase);
  private native long maxBytesForLevelBase(long handle);
  private native void setLevelCompactionDynamicLevelBytes(
      long handle, boolean enableLevelCompactionDynamicLevelBytes);
  private native boolean levelCompactionDynamicLevelBytes(
      long handle);
  private native void setMaxBytesForLevelMultiplier(
      long handle, int multiplier);
  private native int maxBytesForLevelMultiplier(long handle);
  private native void setExpandedCompactionFactor(
      long handle, int expandedCompactionFactor);
  private native int expandedCompactionFactor(long handle);
  private native void setSourceCompactionFactor(
      long handle, int sourceCompactionFactor);
  private native int sourceCompactionFactor(long handle);
  private native void setMaxGrandparentOverlapFactor(
      long handle, int maxGrandparentOverlapFactor);
  private native int maxGrandparentOverlapFactor(long handle);
  private native void setSoftRateLimit(
      long handle, double softRateLimit);
  private native double softRateLimit(long handle);
  private native void setHardRateLimit(
      long handle, double hardRateLimit);
  private native double hardRateLimit(long handle);
  private native void setRateLimitDelayMaxMilliseconds(
      long handle, int rateLimitDelayMaxMilliseconds);
  private native int rateLimitDelayMaxMilliseconds(long handle);
  private native void setArenaBlockSize(
      long handle, long arenaBlockSize) throws IllegalArgumentException;
  private native long arenaBlockSize(long handle);
  private native void setDisableAutoCompactions(
      long handle, boolean disableAutoCompactions);
  private native boolean disableAutoCompactions(long handle);
  private native void setCompactionStyle(long handle, byte compactionStyle);
  private native byte compactionStyle(long handle);
  private native void setPurgeRedundantKvsWhileFlush(
      long handle, boolean purgeRedundantKvsWhileFlush);
  private native boolean purgeRedundantKvsWhileFlush(long handle);
  private native void setVerifyChecksumsInCompaction(
      long handle, boolean verifyChecksumsInCompaction);
  private native boolean verifyChecksumsInCompaction(long handle);
  private native void setMaxSequentialSkipInIterations(
      long handle, long maxSequentialSkipInIterations);
  private native long maxSequentialSkipInIterations(long handle);
  private native void setMemTableFactory(long handle, long factoryHandle);
  private native String memTableFactoryName(long handle);
  private native void setTableFactory(long handle, long factoryHandle);
  private native String tableFactoryName(long handle);
  private native void setInplaceUpdateSupport(
      long handle, boolean inplaceUpdateSupport);
  private native boolean inplaceUpdateSupport(long handle);
  private native void setInplaceUpdateNumLocks(
      long handle, long inplaceUpdateNumLocks)
      throws IllegalArgumentException;
  private native long inplaceUpdateNumLocks(long handle);
  private native void setMemtablePrefixBloomSizeRatio(
      long handle, double memtablePrefixBloomSizeRatio);
  private native double memtablePrefixBloomSizeRatio(long handle);
  private native void setBloomLocality(
      long handle, int bloomLocality);
  private native int bloomLocality(long handle);
  private native void setMaxSuccessiveMerges(
      long handle, long maxSuccessiveMerges)
      throws IllegalArgumentException;
  private native long maxSuccessiveMerges(long handle);
  private native void setMinPartialMergeOperands(
      long handle, int minPartialMergeOperands);
  private native int minPartialMergeOperands(long handle);
  private native void setOptimizeFiltersForHits(long handle,
      boolean optimizeFiltersForHits);
  private native boolean optimizeFiltersForHits(long handle);
  private native void setMemtableHugePageSize(long handle,
      long memtableHugePageSize);
  private native long memtableHugePageSize(long handle);
  private native void setSoftPendingCompactionBytesLimit(long handle,
      long softPendingCompactionBytesLimit);
  private native long softPendingCompactionBytesLimit(long handle);
  private native void setHardPendingCompactionBytesLimit(long handle,
      long hardPendingCompactionBytesLimit);
  private native long hardPendingCompactionBytesLimit(long handle);
  private native void setLevel0FileNumCompactionTrigger(long handle,
      int level0FileNumCompactionTrigger);
  private native int level0FileNumCompactionTrigger(long handle);
  private native void setLevel0SlowdownWritesTrigger(long handle,
      int level0SlowdownWritesTrigger);
  private native int level0SlowdownWritesTrigger(long handle);
  private native void setLevel0StopWritesTrigger(long handle,
      int level0StopWritesTrigger);
  private native int level0StopWritesTrigger(long handle);
  private native void setMaxBytesForLevelMultiplierAdditional(long handle,
      int[] maxBytesForLevelMultiplierAdditional);
  private native int[] maxBytesForLevelMultiplierAdditional(long handle);
  private native void setParanoidFileChecks(long handle,
      boolean paranoidFileChecks);
  private native boolean paranoidFileChecks(long handle);
  // instance variables
  Env env_;
  MemTableConfig memTableConfig_;
  TableFormatConfig tableFormatConfig_;
  RateLimiterConfig rateLimiterConfig_;
  AbstractComparator<? extends AbstractSlice<?>> comparator_;
}
