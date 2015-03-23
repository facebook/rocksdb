// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.Properties;

/**
 * DBOptions to control the behavior of a database.  It will be used
 * during the creation of a {@link org.rocksdb.RocksDB} (i.e., RocksDB.open()).
 *
 * If {@link #dispose()} function is not called, then it will be GC'd
 * automatically and native resources will be released as part of the process.
 */
public class DBOptions extends RocksObject implements DBOptionsInterface {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct DBOptions.
   *
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::DBOptions} in the c++ side.
   */
  public DBOptions() {
    super(newDBOptions());
    numShardBits_ = DEFAULT_NUM_SHARD_BITS;
  }

  /**
   * <p>Method to get a options instance by using pre-configured
   * property values. If one or many values are undefined in
   * the context of RocksDB the method will return a null
   * value.</p>
   *
   * <p><strong>Note</strong>: Property keys can be derived from
   * getter methods within the options class. Example: the method
   * {@code allowMmapReads()} has a property key:
   * {@code allow_mmap_reads}.</p>
   *
   * @param properties {@link java.util.Properties} instance.
   *
   * @return {@link org.rocksdb.DBOptions instance}
   *     or null.
   *
   * @throws java.lang.IllegalArgumentException if null or empty
   *     {@link java.util.Properties} instance is passed to the method call.
   */
  public static DBOptions getDBOptionsFromProps(
      final Properties properties) {
    if (properties == null || properties.size() == 0) {
      throw new IllegalArgumentException(
          "Properties value must contain at least one value.");
    }
    DBOptions dbOptions = null;
    StringBuilder stringBuilder = new StringBuilder();
    for (final String name : properties.stringPropertyNames()){
      stringBuilder.append(name);
      stringBuilder.append("=");
      stringBuilder.append(properties.getProperty(name));
      stringBuilder.append(";");
    }
    long handle = getDBOptionsFromProps(
        stringBuilder.toString());
    if (handle != 0){
      dbOptions = new DBOptions(handle);
    }
    return dbOptions;
  }

  @Override
  public DBOptions setIncreaseParallelism(
      final int totalThreads) {
    assert(isOwningHandle());
    setIncreaseParallelism(nativeHandle_, totalThreads);
    return this;
  }

  @Override
  public DBOptions setCreateIfMissing(final boolean flag) {
    assert(isOwningHandle());
    setCreateIfMissing(nativeHandle_, flag);
    return this;
  }

  @Override
  public boolean createIfMissing() {
    assert(isOwningHandle());
    return createIfMissing(nativeHandle_);
  }

  @Override
  public DBOptions setCreateMissingColumnFamilies(
      final boolean flag) {
    assert(isOwningHandle());
    setCreateMissingColumnFamilies(nativeHandle_, flag);
    return this;
  }

  @Override
  public boolean createMissingColumnFamilies() {
    assert(isOwningHandle());
    return createMissingColumnFamilies(nativeHandle_);
  }

  @Override
  public DBOptions setErrorIfExists(
      final boolean errorIfExists) {
    assert(isOwningHandle());
    setErrorIfExists(nativeHandle_, errorIfExists);
    return this;
  }

  @Override
  public boolean errorIfExists() {
    assert(isOwningHandle());
    return errorIfExists(nativeHandle_);
  }

  @Override
  public DBOptions setParanoidChecks(
      final boolean paranoidChecks) {
    assert(isOwningHandle());
    setParanoidChecks(nativeHandle_, paranoidChecks);
    return this;
  }

  @Override
  public boolean paranoidChecks() {
    assert(isOwningHandle());
    return paranoidChecks(nativeHandle_);
  }

  @Override
  public DBOptions setRateLimiterConfig(
      final RateLimiterConfig config) {
    assert(isOwningHandle());
    rateLimiterConfig_ = config;
    setOldRateLimiter(nativeHandle_, config.newRateLimiterHandle());
    return this;
  }

  @Override
  public DBOptions setRateLimiter(final RateLimiter rateLimiter) {
    assert(isOwningHandle());
    rateLimiter_ = rateLimiter;
    setRateLimiter(nativeHandle_, rateLimiter.nativeHandle_);
    return this;
  }

  @Override
  public DBOptions setLogger(final Logger logger) {
    assert(isOwningHandle());
    setLogger(nativeHandle_, logger.nativeHandle_);
    return this;
  }

  @Override
  public DBOptions setInfoLogLevel(
      final InfoLogLevel infoLogLevel) {
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
  public DBOptions setMaxOpenFiles(
      final int maxOpenFiles) {
    assert(isOwningHandle());
    setMaxOpenFiles(nativeHandle_, maxOpenFiles);
    return this;
  }

  @Override
  public int maxOpenFiles() {
    assert(isOwningHandle());
    return maxOpenFiles(nativeHandle_);
  }

  @Override
  public DBOptions setMaxTotalWalSize(
      final long maxTotalWalSize) {
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
  public DBOptions createStatistics() {
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
  public DBOptions setDisableDataSync(
      final boolean disableDataSync) {
    assert(isOwningHandle());
    setDisableDataSync(nativeHandle_, disableDataSync);
    return this;
  }

  @Override
  public boolean disableDataSync() {
    assert(isOwningHandle());
    return disableDataSync(nativeHandle_);
  }

  @Override
  public DBOptions setUseFsync(
      final boolean useFsync) {
    assert(isOwningHandle());
    setUseFsync(nativeHandle_, useFsync);
    return this;
  }

  @Override
  public boolean useFsync() {
    assert(isOwningHandle());
    return useFsync(nativeHandle_);
  }

  @Override
  public DBOptions setDbLogDir(
      final String dbLogDir) {
    assert(isOwningHandle());
    setDbLogDir(nativeHandle_, dbLogDir);
    return this;
  }

  @Override
  public String dbLogDir() {
    assert(isOwningHandle());
    return dbLogDir(nativeHandle_);
  }

  @Override
  public DBOptions setWalDir(
      final String walDir) {
    assert(isOwningHandle());
    setWalDir(nativeHandle_, walDir);
    return this;
  }

  @Override
  public String walDir() {
    assert(isOwningHandle());
    return walDir(nativeHandle_);
  }

  @Override
  public DBOptions setDeleteObsoleteFilesPeriodMicros(
      final long micros) {
    assert(isOwningHandle());
    setDeleteObsoleteFilesPeriodMicros(nativeHandle_, micros);
    return this;
  }

  @Override
  public long deleteObsoleteFilesPeriodMicros() {
    assert(isOwningHandle());
    return deleteObsoleteFilesPeriodMicros(nativeHandle_);
  }

  @Override
  public void setBaseBackgroundCompactions(
      final int baseBackgroundCompactions) {
    assert(isOwningHandle());
    setBaseBackgroundCompactions(nativeHandle_, baseBackgroundCompactions);
  }

  @Override
  public int baseBackgroundCompactions() {
    assert(isOwningHandle());
    return baseBackgroundCompactions(nativeHandle_);
  }

  @Override
  public DBOptions setMaxBackgroundCompactions(
      final int maxBackgroundCompactions) {
    assert(isOwningHandle());
    setMaxBackgroundCompactions(nativeHandle_, maxBackgroundCompactions);
    return this;
  }

  @Override
  public int maxBackgroundCompactions() {
    assert(isOwningHandle());
    return maxBackgroundCompactions(nativeHandle_);
  }

  @Override
  public void setMaxSubcompactions(final int maxSubcompactions) {
    assert(isOwningHandle());
    setMaxSubcompactions(nativeHandle_, maxSubcompactions);
  }

  @Override
  public int maxSubcompactions() {
    assert(isOwningHandle());
    return maxSubcompactions(nativeHandle_);
  }

  @Override
  public DBOptions setMaxBackgroundFlushes(
      final int maxBackgroundFlushes) {
    assert(isOwningHandle());
    setMaxBackgroundFlushes(nativeHandle_, maxBackgroundFlushes);
    return this;
  }

  @Override
  public int maxBackgroundFlushes() {
    assert(isOwningHandle());
    return maxBackgroundFlushes(nativeHandle_);
  }

  @Override
  public DBOptions setMaxLogFileSize(
      final long maxLogFileSize) {
    assert(isOwningHandle());
    setMaxLogFileSize(nativeHandle_, maxLogFileSize);
    return this;
  }

  @Override
  public long maxLogFileSize() {
    assert(isOwningHandle());
    return maxLogFileSize(nativeHandle_);
  }

  @Override
  public DBOptions setLogFileTimeToRoll(
      final long logFileTimeToRoll) {
    assert(isOwningHandle());
    setLogFileTimeToRoll(nativeHandle_, logFileTimeToRoll);
    return this;
  }

  @Override
  public long logFileTimeToRoll() {
    assert(isOwningHandle());
    return logFileTimeToRoll(nativeHandle_);
  }

  @Override
  public DBOptions setKeepLogFileNum(
      final long keepLogFileNum) {
    assert(isOwningHandle());
    setKeepLogFileNum(nativeHandle_, keepLogFileNum);
    return this;
  }

  @Override
  public long keepLogFileNum() {
    assert(isOwningHandle());
    return keepLogFileNum(nativeHandle_);
  }

  @Override
  public DBOptions setMaxManifestFileSize(
      final long maxManifestFileSize) {
    assert(isOwningHandle());
    setMaxManifestFileSize(nativeHandle_, maxManifestFileSize);
    return this;
  }

  @Override
  public long maxManifestFileSize() {
    assert(isOwningHandle());
    return maxManifestFileSize(nativeHandle_);
  }

  @Override
  public DBOptions setTableCacheNumshardbits(
      final int tableCacheNumshardbits) {
    assert(isOwningHandle());
    setTableCacheNumshardbits(nativeHandle_, tableCacheNumshardbits);
    return this;
  }

  @Override
  public int tableCacheNumshardbits() {
    assert(isOwningHandle());
    return tableCacheNumshardbits(nativeHandle_);
  }

  @Override
  public DBOptions setWalTtlSeconds(
      final long walTtlSeconds) {
    assert(isOwningHandle());
    setWalTtlSeconds(nativeHandle_, walTtlSeconds);
    return this;
  }

  @Override
  public long walTtlSeconds() {
    assert(isOwningHandle());
    return walTtlSeconds(nativeHandle_);
  }

  @Override
  public DBOptions setWalSizeLimitMB(
      final long sizeLimitMB) {
    assert(isOwningHandle());
    setWalSizeLimitMB(nativeHandle_, sizeLimitMB);
    return this;
  }

  @Override
  public long walSizeLimitMB() {
    assert(isOwningHandle());
    return walSizeLimitMB(nativeHandle_);
  }

  @Override
  public DBOptions setManifestPreallocationSize(
      final long size) {
    assert(isOwningHandle());
    setManifestPreallocationSize(nativeHandle_, size);
    return this;
  }

  @Override
  public long manifestPreallocationSize() {
    assert(isOwningHandle());
    return manifestPreallocationSize(nativeHandle_);
  }

  @Override
  public DBOptions setAllowOsBuffer(
      final boolean allowOsBuffer) {
    assert(isOwningHandle());
    setAllowOsBuffer(nativeHandle_, allowOsBuffer);
    return this;
  }

  @Override
  public boolean allowOsBuffer() {
    assert(isOwningHandle());
    return allowOsBuffer(nativeHandle_);
  }

  @Override
  public DBOptions setAllowMmapReads(
      final boolean allowMmapReads) {
    assert(isOwningHandle());
    setAllowMmapReads(nativeHandle_, allowMmapReads);
    return this;
  }

  @Override
  public boolean allowMmapReads() {
    assert(isOwningHandle());
    return allowMmapReads(nativeHandle_);
  }

  @Override
  public DBOptions setAllowMmapWrites(
      final boolean allowMmapWrites) {
    assert(isOwningHandle());
    setAllowMmapWrites(nativeHandle_, allowMmapWrites);
    return this;
  }

  @Override
  public boolean allowMmapWrites() {
    assert(isOwningHandle());
    return allowMmapWrites(nativeHandle_);
  }

  @Override
  public DBOptions setIsFdCloseOnExec(
      final boolean isFdCloseOnExec) {
    assert(isOwningHandle());
    setIsFdCloseOnExec(nativeHandle_, isFdCloseOnExec);
    return this;
  }

  @Override
  public boolean isFdCloseOnExec() {
    assert(isOwningHandle());
    return isFdCloseOnExec(nativeHandle_);
  }

  @Override
  public DBOptions setStatsDumpPeriodSec(
      final int statsDumpPeriodSec) {
    assert(isOwningHandle());
    setStatsDumpPeriodSec(nativeHandle_, statsDumpPeriodSec);
    return this;
  }

  @Override
  public int statsDumpPeriodSec() {
    assert(isOwningHandle());
    return statsDumpPeriodSec(nativeHandle_);
  }

  @Override
  public DBOptions setAdviseRandomOnOpen(
      final boolean adviseRandomOnOpen) {
    assert(isOwningHandle());
    setAdviseRandomOnOpen(nativeHandle_, adviseRandomOnOpen);
    return this;
  }

  @Override
  public boolean adviseRandomOnOpen() {
    return adviseRandomOnOpen(nativeHandle_);
  }

  @Override
  public DBOptions setUseAdaptiveMutex(
      final boolean useAdaptiveMutex) {
    assert(isOwningHandle());
    setUseAdaptiveMutex(nativeHandle_, useAdaptiveMutex);
    return this;
  }

  @Override
  public boolean useAdaptiveMutex() {
    assert(isOwningHandle());
    return useAdaptiveMutex(nativeHandle_);
  }

  @Override
  public DBOptions setBytesPerSync(
      final long bytesPerSync) {
    assert(isOwningHandle());
    setBytesPerSync(nativeHandle_, bytesPerSync);
    return this;
  }

  @Override
  public long bytesPerSync() {
    return bytesPerSync(nativeHandle_);
  }

  @Override
  public void setAllowConcurrentMemtableWrite(
      final boolean allowConcurrentMemtableWrite) {
    setAllowConcurrentMemtableWrite(nativeHandle_,
        allowConcurrentMemtableWrite);
  }

  @Override
  public boolean allowConcurrentMemtableWrite() {
    return allowConcurrentMemtableWrite(nativeHandle_);
  }

  @Override
  public void setEnableWriteThreadAdaptiveYield(
      final boolean enableWriteThreadAdaptiveYield) {
    setEnableWriteThreadAdaptiveYield(nativeHandle_,
        enableWriteThreadAdaptiveYield);
  }

  @Override
  public boolean enableWriteThreadAdaptiveYield() {
    return enableWriteThreadAdaptiveYield(nativeHandle_);
  }

  @Override
  public void setWriteThreadMaxYieldUsec(final long writeThreadMaxYieldUsec) {
    setWriteThreadMaxYieldUsec(nativeHandle_, writeThreadMaxYieldUsec);
  }

  @Override
  public long writeThreadMaxYieldUsec() {
    return writeThreadMaxYieldUsec(nativeHandle_);
  }

  @Override
  public void setWriteThreadSlowYieldUsec(final long writeThreadSlowYieldUsec) {
    setWriteThreadSlowYieldUsec(nativeHandle_, writeThreadSlowYieldUsec);
  }

  @Override
  public long writeThreadSlowYieldUsec() {
    return writeThreadSlowYieldUsec(nativeHandle_);
  }

  static final int DEFAULT_NUM_SHARD_BITS = -1;

 public DBOptions setDelayedWriteRate(final long delayedWriteRate){
   assert(isOwningHandle());
   setDelayedWriteRate(nativeHandle_, delayedWriteRate);
   return this;
}

public long delayedWriteRate(){
  return delayedWriteRate(nativeHandle_);
}


  /**
   * <p>Private constructor to be used by
   * {@link #getDBOptionsFromProps(java.util.Properties)}</p>
   *
   * @param nativeHandle native handle to DBOptions instance.
   */
  private DBOptions(final long nativeHandle) {
    super(nativeHandle);
  }

  private static native long getDBOptionsFromProps(
      String optString);

  private native static long newDBOptions();
  @Override protected final native void disposeInternal(final long handle);

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
  @Deprecated
  private native void setOldRateLimiter(long handle,
      long rateLimiterHandle);
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
  private native void setBaseBackgroundCompactions(long handle,
      int baseBackgroundCompactions);
  private native int baseBackgroundCompactions(long handle);
  private native void setMaxBackgroundCompactions(
      long handle, int maxBackgroundCompactions);
  private native int maxBackgroundCompactions(long handle);
  private native void setMaxSubcompactions(long handle, int maxSubcompactions);
  private native int maxSubcompactions(long handle);
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
  private native void setAllowConcurrentMemtableWrite(long handle,
      boolean allowConcurrentMemtableWrite);
  private native boolean allowConcurrentMemtableWrite(long handle);
  private native void setEnableWriteThreadAdaptiveYield(long handle,
      boolean enableWriteThreadAdaptiveYield);
  private native boolean enableWriteThreadAdaptiveYield(long handle);
  private native void setWriteThreadMaxYieldUsec(long handle,
      long writeThreadMaxYieldUsec);
  private native long writeThreadMaxYieldUsec(long handle);
  private native void setWriteThreadSlowYieldUsec(long handle,
      long writeThreadSlowYieldUsec);
  private native long writeThreadSlowYieldUsec(long handle);

  private native void setDelayedWriteRate(long handle, long delayedWriteRate);
  private native long delayedWriteRate(long handle);

  int numShardBits_;
  RateLimiterConfig rateLimiterConfig_;
  RateLimiter rateLimiter_;
}
