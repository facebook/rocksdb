// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Options to control the behavior of a database.  It will be used
 * during the creation of a RocksDB (i.e., RocksDB.open()).
 *
 * If dispose() function is not called, then it will be GC'd automatically and
 * native resources will be released as part of the process.
 */
public class Options extends RocksObject {
  static final long DEFAULT_CACHE_SIZE = 8 << 20;
  static final int DEFAULT_NUM_SHARD_BITS = -1;
  /**
   * Construct options for opening a RocksDB.
   *
   * This constructor will create (by allocating a block of memory)
   * an rocksdb::Options in the c++ side.
   */
  public Options() {
    super();
    cacheSize_ = DEFAULT_CACHE_SIZE;
    numShardBits_ = DEFAULT_NUM_SHARD_BITS;
    newOptions();
    env_ = RocksEnv.getDefault();
  }

  /**
   * If this value is set to true, then the database will be created
   * if it is missing during RocksDB.open().
   * Default: false
   *
   * @param flag a flag indicating whether to create a database the
   *     specified database in RocksDB.open() operation is missing.
   * @return the instance of the current Options.
   * @see RocksDB.open()
   */
  public Options setCreateIfMissing(boolean flag) {
    assert(isInitialized());
    setCreateIfMissing(nativeHandle_, flag);
    return this;
  }

  /**
   * Use the specified object to interact with the environment,
   * e.g. to read/write files, schedule background work, etc.
   * Default: RocksEnv.getDefault()
   */
  public Options setEnv(RocksEnv env) {
    assert(isInitialized());
    setEnv(nativeHandle_, env.nativeHandle_);
    env_ = env;
    return this;
  }
  private native void setEnv(long optHandle, long envHandle);

  public RocksEnv getEnv() {
    return env_;
  }
  private native long getEnvHandle(long handle);

  /**
   * Return true if the create_if_missing flag is set to true.
   * If true, the database will be created if it is missing.
   *
   * @return true if the createIfMissing option is set to true.
   * @see setCreateIfMissing()
   */
  public boolean createIfMissing() {
    assert(isInitialized());
    return createIfMissing(nativeHandle_);
  }

  /**
   * Amount of data to build up in memory (backed by an unsorted log
   * on disk) before converting to a sorted on-disk file.
   *
   * Larger values increase performance, especially during bulk loads.
   * Up to max_write_buffer_number write buffers may be held in memory
   * at the same time, so you may wish to adjust this parameter
   * to control memory usage.
   *
   * Also, a larger write buffer will result in a longer recovery time
   * the next time the database is opened.
   *
   * Default: 4MB
   * @param writeBufferSize the size of write buffer.
   * @return the instance of the current Options.
   * @see RocksDB.open()
   */
  public Options setWriteBufferSize(long writeBufferSize) {
    assert(isInitialized());
    setWriteBufferSize(nativeHandle_, writeBufferSize);
    return this;
  }

  /**
   * Return size of write buffer size.
   *
   * @return size of write buffer.
   * @see setWriteBufferSize()
   */
  public long writeBufferSize()  {
    assert(isInitialized());
    return writeBufferSize(nativeHandle_);
  }

  /**
   * The maximum number of write buffers that are built up in memory.
   * The default is 2, so that when 1 write buffer is being flushed to
   * storage, new writes can continue to the other write buffer.
   * Default: 2
   *
   * @param maxWriteBufferNumber maximum number of write buffers.
   * @return the instance of the current Options.
   * @see RocksDB.open()
   */
  public Options setMaxWriteBufferNumber(int maxWriteBufferNumber) {
    assert(isInitialized());
    setMaxWriteBufferNumber(nativeHandle_, maxWriteBufferNumber);
    return this;
  }

  /**
   * Returns maximum number of write buffers.
   *
   * @return maximum number of write buffers.
   * @see setMaxWriteBufferNumber()
   */
  public int maxWriteBufferNumber() {
    assert(isInitialized());
    return maxWriteBufferNumber(nativeHandle_);
  }

  /**
   * If true, an error will be thrown during RocksDB.open() if the
   * database already exists.
   *
   * @return if true, an error is raised when the specified database
   *    already exists before open.
   */
  public boolean errorIfExists() {
    assert(isInitialized());
    return errorIfExists(nativeHandle_);
  }
  private native boolean errorIfExists(long handle);

  /**
   * If true, an error will be thrown during RocksDB.open() if the
   * database already exists.
   * Default: false
   *
   * @param errorIfExists if true, an exception will be thrown
   *     during RocksDB.open() if the database already exists.
   * @return the reference to the current option.
   * @see RocksDB.open()
   */
  public Options setErrorIfExists(boolean errorIfExists) {
    assert(isInitialized());
    setErrorIfExists(nativeHandle_, errorIfExists);
    return this;
  }
  private native void setErrorIfExists(long handle, boolean errorIfExists);

  /**
   * If true, the implementation will do aggressive checking of the
   * data it is processing and will stop early if it detects any
   * errors.  This may have unforeseen ramifications: for example, a
   * corruption of one DB entry may cause a large number of entries to
   * become unreadable or for the entire DB to become unopenable.
   * If any of the  writes to the database fails (Put, Delete, Merge, Write),
   * the database will switch to read-only mode and fail all other
   * Write operations.
   *
   * @return a boolean indicating whether paranoid-check is on.
   */
  public boolean paranoidChecks() {
    assert(isInitialized());
    return paranoidChecks(nativeHandle_);
  }
  private native boolean paranoidChecks(long handle);

  /**
   * If true, the implementation will do aggressive checking of the
   * data it is processing and will stop early if it detects any
   * errors.  This may have unforeseen ramifications: for example, a
   * corruption of one DB entry may cause a large number of entries to
   * become unreadable or for the entire DB to become unopenable.
   * If any of the  writes to the database fails (Put, Delete, Merge, Write),
   * the database will switch to read-only mode and fail all other
   * Write operations.
   * Default: true
   *
   * @param paranoidChecks a flag to indicate whether paranoid-check
   *     is on.
   * @return the reference to the current option.
   */
  public Options setParanoidChecks(boolean paranoidChecks) {
    assert(isInitialized());
    setParanoidChecks(nativeHandle_, paranoidChecks);
    return this;
  }
  private native void setParanoidChecks(
      long handle, boolean paranoidChecks);

  /**
   * Number of open files that can be used by the DB.  You may need to
   * increase this if your database has a large working set. Value -1 means
   * files opened are always kept open. You can estimate number of files based
   * on target_file_size_base and target_file_size_multiplier for level-based
   * compaction. For universal-style compaction, you can usually set it to -1.
   *
   * @return the maximum number of open files.
   */
  public int maxOpenFiles() {
    assert(isInitialized());
    return maxOpenFiles(nativeHandle_);
  }
  private native int maxOpenFiles(long handle);

  /**
   * Number of open files that can be used by the DB.  You may need to
   * increase this if your database has a large working set. Value -1 means
   * files opened are always kept open. You can estimate number of files based
   * on target_file_size_base and target_file_size_multiplier for level-based
   * compaction. For universal-style compaction, you can usually set it to -1.
   * Default: 5000
   *
   * @param maxOpenFiles the maximum number of open files.
   * @return the reference to the current option.
   */
  public Options setMaxOpenFiles(int maxOpenFiles) {
    assert(isInitialized());
    setMaxOpenFiles(nativeHandle_, maxOpenFiles);
    return this;
  }
  private native void setMaxOpenFiles(long handle, int maxOpenFiles);

  /**
   * If true, then the contents of data files are not synced
   * to stable storage. Their contents remain in the OS buffers till the
   * OS decides to flush them. This option is good for bulk-loading
   * of data. Once the bulk-loading is complete, please issue a
   * sync to the OS to flush all dirty buffesrs to stable storage.
   *
   * @return if true, then data-sync is disabled.
   */
  public boolean disableDataSync() {
    assert(isInitialized());
    return disableDataSync(nativeHandle_);
  }
  private native boolean disableDataSync(long handle);

  /**
   * If true, then the contents of data files are not synced
   * to stable storage. Their contents remain in the OS buffers till the
   * OS decides to flush them. This option is good for bulk-loading
   * of data. Once the bulk-loading is complete, please issue a
   * sync to the OS to flush all dirty buffesrs to stable storage.
   * Default: false
   *
   * @param disableDataSync a boolean flag to specify whether to
   *     disable data sync.
   * @return the reference to the current option.
   */
  public Options setDisableDataSync(boolean disableDataSync) {
    assert(isInitialized());
    setDisableDataSync(nativeHandle_, disableDataSync);
    return this;
  }
  private native void setDisableDataSync(long handle, boolean disableDataSync);

  /**
   * If true, then every store to stable storage will issue a fsync.
   * If false, then every store to stable storage will issue a fdatasync.
   * This parameter should be set to true while storing data to
   * filesystem like ext3 that can lose files after a reboot.
   *
   * @return true if fsync is used.
   */
  public boolean useFsync() {
    assert(isInitialized());
    return useFsync(nativeHandle_);
  }
  private native boolean useFsync(long handle);

  /**
   * If true, then every store to stable storage will issue a fsync.
   * If false, then every store to stable storage will issue a fdatasync.
   * This parameter should be set to true while storing data to
   * filesystem like ext3 that can lose files after a reboot.
   * Default: false
   *
   * @param useFsync a boolean flag to specify whether to use fsync
   * @return the reference to the current option.
   */
  public Options setUseFsync(boolean useFsync) {
    assert(isInitialized());
    setUseFsync(nativeHandle_, useFsync);
    return this;
  }
  private native void setUseFsync(long handle, boolean useFsync);

  /**
   * The time interval in seconds between each two consecutive stats logs.
   * This number controls how often a new scribe log about
   * db deploy stats is written out.
   * -1 indicates no logging at all.
   *
   * @return the time interval in seconds between each two consecutive
   *     stats logs.
   */
  public int dbStatsLogInterval() {
    assert(isInitialized());
    return dbStatsLogInterval(nativeHandle_);
  }
  private native int dbStatsLogInterval(long handle);

  /**
   * The time interval in seconds between each two consecutive stats logs.
   * This number controls how often a new scribe log about
   * db deploy stats is written out.
   * -1 indicates no logging at all.
   * Default value is 1800 (half an hour).
   *
   * @param dbStatsLogInterval the time interval in seconds between each
   *     two consecutive stats logs.
   * @return the reference to the current option.
   */
  public Options setDbStatsLogInterval(int dbStatsLogInterval) {
    assert(isInitialized());
    setDbStatsLogInterval(nativeHandle_, dbStatsLogInterval);
    return this;
  }
  private native void setDbStatsLogInterval(
      long handle, int dbStatsLogInterval);

  /**
   * Returns the directory of info log.
   *
   * If it is empty, the log files will be in the same dir as data.
   * If it is non empty, the log files will be in the specified dir,
   * and the db data dir's absolute path will be used as the log file
   * name's prefix.
   *
   * @return the path to the info log directory
   */
  public String dbLogDir() {
    assert(isInitialized());
    return dbLogDir(nativeHandle_);
  }
  private native String dbLogDir(long handle);

  /**
   * This specifies the info LOG dir.
   * If it is empty, the log files will be in the same dir as data.
   * If it is non empty, the log files will be in the specified dir,
   * and the db data dir's absolute path will be used as the log file
   * name's prefix.
   *
   * @param dbLogDir the path to the info log directory
   * @return the reference to the current option.
   */
  public Options setDbLogDir(String dbLogDir) {
    assert(isInitialized());
    setDbLogDir(nativeHandle_, dbLogDir);
    return this;
  }
  private native void setDbLogDir(long handle, String dbLogDir);

  /**
   * Returns the path to the write-ahead-logs (WAL) directory.
   *
   * If it is empty, the log files will be in the same dir as data,
   *   dbname is used as the data dir by default
   * If it is non empty, the log files will be in kept the specified dir.
   * When destroying the db,
   *   all log files in wal_dir and the dir itself is deleted
   *
   * @return the path to the write-ahead-logs (WAL) directory.
   */
  public String walDir() {
    assert(isInitialized());
    return walDir(nativeHandle_);
  }
  private native String walDir(long handle);

  /**
   * This specifies the absolute dir path for write-ahead logs (WAL).
   * If it is empty, the log files will be in the same dir as data,
   *   dbname is used as the data dir by default
   * If it is non empty, the log files will be in kept the specified dir.
   * When destroying the db,
   *   all log files in wal_dir and the dir itself is deleted
   *
   * @param walDir the path to the write-ahead-log directory.
   * @return the reference to the current option.
   */
  public Options setWalDir(String walDir) {
    assert(isInitialized());
    setWalDir(nativeHandle_, walDir);
    return this;
  }
  private native void setWalDir(long handle, String walDir);

  /**
   * The periodicity when obsolete files get deleted. The default
   * value is 6 hours. The files that get out of scope by compaction
   * process will still get automatically delete on every compaction,
   * regardless of this setting
   *
   * @return the time interval in micros when obsolete files will be deleted.
   */
  public long deleteObsoleteFilesPeriodMicros() {
    assert(isInitialized());
    return deleteObsoleteFilesPeriodMicros(nativeHandle_);
  }
  private native long deleteObsoleteFilesPeriodMicros(long handle);

  /**
   * The periodicity when obsolete files get deleted. The default
   * value is 6 hours. The files that get out of scope by compaction
   * process will still get automatically delete on every compaction,
   * regardless of this setting
   *
   * @param micros the time interval in micros
   * @return the reference to the current option.
   */
  public Options setDeleteObsoleteFilesPeriodMicros(long micros) {
    assert(isInitialized());
    setDeleteObsoleteFilesPeriodMicros(nativeHandle_, micros);
    return this;
  }
  private native void setDeleteObsoleteFilesPeriodMicros(
      long handle, long micros);

  /**
   * Returns the maximum number of concurrent background compaction jobs,
   * submitted to the default LOW priority thread pool.
   * When increasing this number, we may also want to consider increasing
   * number of threads in LOW priority thread pool.
   * Default: 1
   *
   * @return the maximum number of concurrent background compaction jobs.
   * @see Env.setBackgroundThreads()
   */
  public int maxBackgroundCompactions() {
    assert(isInitialized());
    return maxBackgroundCompactions(nativeHandle_);
  }

  /**
   * Creates statistics object which collects metrics about database operations.
     Statistics objects should not be shared between DB instances as
     it does not use any locks to prevent concurrent updates.
   *
   * @return the instance of the current Options.
   * @see RocksDB.open()
   */
  public Options createStatistics() {
    assert(isInitialized());
    createStatistics(nativeHandle_);
    return this;
  }

  /**
   * Returns statistics object. Calls createStatistics() if
   * C++ returns NULL pointer for statistics.
   *
   * @return the instance of the statistics object.
   * @see createStatistics()
   */
  public Statistics statisticsPtr() {
    assert(isInitialized());

    long statsPtr = statisticsPtr(nativeHandle_);
    if(statsPtr == 0) {
      createStatistics();
      statsPtr = statisticsPtr(nativeHandle_);
    }

    return new Statistics(statsPtr);
  }

  /**
   * Specifies the maximum number of concurrent background compaction jobs,
   * submitted to the default LOW priority thread pool.
   * If you're increasing this, also consider increasing number of threads in
   * LOW priority thread pool. For more information, see
   * Default: 1
   *
   * @param maxBackgroundCompactions the maximum number of background
   *     compaction jobs.
   * @return the reference to the current option.
   *
   * @see Env.setBackgroundThreads()
   * @see maxBackgroundFlushes()
   */
  public Options setMaxBackgroundCompactions(int maxBackgroundCompactions) {
    assert(isInitialized());
    setMaxBackgroundCompactions(nativeHandle_, maxBackgroundCompactions);
    return this;
  }

  /**
   * Returns the maximum number of concurrent background flush jobs.
   * If you're increasing this, also consider increasing number of threads in
   * HIGH priority thread pool. For more information, see
   * Default: 1
   *
   * @return the maximum number of concurrent background flush jobs.
   * @see Env.setBackgroundThreads()
   */
  public int maxBackgroundFlushes() {
    assert(isInitialized());
    return maxBackgroundFlushes(nativeHandle_);
  }
  private native int maxBackgroundFlushes(long handle);

  /**
   * Specifies the maximum number of concurrent background flush jobs.
   * If you're increasing this, also consider increasing number of threads in
   * HIGH priority thread pool. For more information, see
   * Default: 1
   *
   * @param maxBackgroundFlushes
   * @return the reference to the current option.
   *
   * @see Env.setBackgroundThreads()
   * @see maxBackgroundCompactions()
   */
  public Options setMaxBackgroundFlushes(int maxBackgroundFlushes) {
    assert(isInitialized());
    setMaxBackgroundFlushes(nativeHandle_, maxBackgroundFlushes);
    return this;
  }
  private native void setMaxBackgroundFlushes(
      long handle, int maxBackgroundFlushes);

  /**
   * Returns the maximum size of a info log file. If the current log file
   * is larger than this size, a new info log file will be created.
   * If 0, all logs will be written to one log file.
   *
   * @return the maximum size of the info log file.
   */
  public long maxLogFileSize() {
    assert(isInitialized());
    return maxLogFileSize(nativeHandle_);
  }
  private native long maxLogFileSize(long handle);

  /**
   * Specifies the maximum size of a info log file. If the current log file
   * is larger than `max_log_file_size`, a new info log file will
   * be created.
   * If 0, all logs will be written to one log file.
   *
   * @param maxLogFileSize the maximum size of a info log file.
   * @return the reference to the current option.
   */
  public Options setMaxLogFileSize(long maxLogFileSize) {
    assert(isInitialized());
    setMaxLogFileSize(nativeHandle_, maxLogFileSize);
    return this;
  }
  private native void setMaxLogFileSize(long handle, long maxLogFileSize);

  /**
   * Returns the time interval for the info log file to roll (in seconds).
   * If specified with non-zero value, log file will be rolled
   * if it has been active longer than `log_file_time_to_roll`.
   * Default: 0 (disabled)
   *
   * @return the time interval in seconds.
   */
  public long logFileTimeToRoll() {
    assert(isInitialized());
    return logFileTimeToRoll(nativeHandle_);
  }
  private native long logFileTimeToRoll(long handle);

  /**
   * Specifies the time interval for the info log file to roll (in seconds).
   * If specified with non-zero value, log file will be rolled
   * if it has been active longer than `log_file_time_to_roll`.
   * Default: 0 (disabled)
   *
   * @param logFileTimeToRoll the time interval in seconds.
   * @return the reference to the current option.
   */
  public Options setLogFileTimeToRoll(long logFileTimeToRoll) {
    assert(isInitialized());
    setLogFileTimeToRoll(nativeHandle_, logFileTimeToRoll);
    return this;
  }
  private native void setLogFileTimeToRoll(
      long handle, long logFileTimeToRoll);

  /**
   * Returns the maximum number of info log files to be kept.
   * Default: 1000
   *
   * @return the maximum number of info log files to be kept.
   */
  public long keepLogFileNum() {
    assert(isInitialized());
    return keepLogFileNum(nativeHandle_);
  }
  private native long keepLogFileNum(long handle);

  /**
   * Specifies the maximum number of info log files to be kept.
   * Default: 1000
   *
   * @param keepLogFileNum the maximum number of info log files to be kept.
   * @return the reference to the current option.
   */
  public Options setKeepLogFileNum(long keepLogFileNum) {
    assert(isInitialized());
    setKeepLogFileNum(nativeHandle_, keepLogFileNum);
    return this;
  }
  private native void setKeepLogFileNum(long handle, long keepLogFileNum);

  /**
   * Manifest file is rolled over on reaching this limit.
   * The older manifest file be deleted.
   * The default value is MAX_INT so that roll-over does not take place.
   *
   * @return the size limit of a manifest file.
   */
  public long maxManifestFileSize() {
    assert(isInitialized());
    return maxManifestFileSize(nativeHandle_);
  }
  private native long maxManifestFileSize(long handle);

  /**
   * Manifest file is rolled over on reaching this limit.
   * The older manifest file be deleted.
   * The default value is MAX_INT so that roll-over does not take place.
   *
   * @param maxManifestFileSize the size limit of a manifest file.
   * @return the reference to the current option.
   */
  public Options setMaxManifestFileSize(long maxManifestFileSize) {
    assert(isInitialized());
    setMaxManifestFileSize(nativeHandle_, maxManifestFileSize);
    return this;
  }
  private native void setMaxManifestFileSize(
      long handle, long maxManifestFileSize);

  /**
   * Number of shards used for table cache.
   *
   * @return the number of shards used for table cache.
   */
  public int tableCacheNumshardbits() {
    assert(isInitialized());
    return tableCacheNumshardbits(nativeHandle_);
  }
  private native int tableCacheNumshardbits(long handle);

  /**
   * Number of shards used for table cache.
   *
   * @param tableCacheNumshardbits the number of chards
   * @return the reference to the current option.
   */
  public Options setTableCacheNumshardbits(int tableCacheNumshardbits) {
    assert(isInitialized());
    setTableCacheNumshardbits(nativeHandle_, tableCacheNumshardbits);
    return this;
  }
  private native void setTableCacheNumshardbits(
      long handle, int tableCacheNumshardbits);

  /**
   * During data eviction of table's LRU cache, it would be inefficient
   * to strictly follow LRU because this piece of memory will not really
   * be released unless its refcount falls to zero. Instead, make two
   * passes: the first pass will release items with refcount = 1,
   * and if not enough space releases after scanning the number of
   * elements specified by this parameter, we will remove items in LRU
   * order.
   *
   * @return scan count limit
   */
  public int tableCacheRemoveScanCountLimit() {
    assert(isInitialized());
    return tableCacheRemoveScanCountLimit(nativeHandle_);
  }
  private native int tableCacheRemoveScanCountLimit(long handle);

  /**
   * During data eviction of table's LRU cache, it would be inefficient
   * to strictly follow LRU because this piece of memory will not really
   * be released unless its refcount falls to zero. Instead, make two
   * passes: the first pass will release items with refcount = 1,
   * and if not enough space releases after scanning the number of
   * elements specified by this parameter, we will remove items in LRU
   * order.
   *
   * @param limit scan count limit
   * @return the reference to the current option.
   */
  public Options setTableCacheRemoveScanCountLimit(int limit) {
    assert(isInitialized());
    setTableCacheRemoveScanCountLimit(nativeHandle_, limit);
    return this;
  }
  private native void setTableCacheRemoveScanCountLimit(
      long handle, int limit);

  /**
   * WalTtlSeconds() and walSizeLimitMB() affect how archived logs
   * will be deleted.
   * 1. If both set to 0, logs will be deleted asap and will not get into
   *    the archive.
   * 2. If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0,
   *    WAL files will be checked every 10 min and if total size is greater
   *    then WAL_size_limit_MB, they will be deleted starting with the
   *    earliest until size_limit is met. All empty files will be deleted.
   * 3. If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
   *    WAL files will be checked every WAL_ttl_secondsi / 2 and those that
   *    are older than WAL_ttl_seconds will be deleted.
   * 4. If both are not 0, WAL files will be checked every 10 min and both
   *    checks will be performed with ttl being first.
   *
   * @return the wal-ttl seconds
   * @see walSizeLimitMB()
   */
  public long walTtlSeconds() {
    assert(isInitialized());
    return walTtlSeconds(nativeHandle_);
  }
  private native long walTtlSeconds(long handle);

  /**
   * WalTtlSeconds() and walSizeLimitMB() affect how archived logs
   * will be deleted.
   * 1. If both set to 0, logs will be deleted asap and will not get into
   *    the archive.
   * 2. If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0,
   *    WAL files will be checked every 10 min and if total size is greater
   *    then WAL_size_limit_MB, they will be deleted starting with the
   *    earliest until size_limit is met. All empty files will be deleted.
   * 3. If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
   *    WAL files will be checked every WAL_ttl_secondsi / 2 and those that
   *    are older than WAL_ttl_seconds will be deleted.
   * 4. If both are not 0, WAL files will be checked every 10 min and both
   *    checks will be performed with ttl being first.
   *
   * @param walTtlSeconds the ttl seconds
   * @return the reference to the current option.
   * @see setWalSizeLimitMB()
   */
  public Options setWalTtlSeconds(long walTtlSeconds) {
    assert(isInitialized());
    setWalTtlSeconds(nativeHandle_, walTtlSeconds);
    return this;
  }
  private native void setWalTtlSeconds(long handle, long walTtlSeconds);

  /**
   * WalTtlSeconds() and walSizeLimitMB() affect how archived logs
   * will be deleted.
   * 1. If both set to 0, logs will be deleted asap and will not get into
   *    the archive.
   * 2. If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0,
   *    WAL files will be checked every 10 min and if total size is greater
   *    then WAL_size_limit_MB, they will be deleted starting with the
   *    earliest until size_limit is met. All empty files will be deleted.
   * 3. If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
   *    WAL files will be checked every WAL_ttl_secondsi / 2 and those that
   *    are older than WAL_ttl_seconds will be deleted.
   * 4. If both are not 0, WAL files will be checked every 10 min and both
   *    checks will be performed with ttl being first.
   *
   * @return size limit in mega-bytes.
   * @see walSizeLimitMB()
   */
  public long walSizeLimitMB() {
    assert(isInitialized());
    return walSizeLimitMB(nativeHandle_);
  }
  private native long walSizeLimitMB(long handle);

  /**
   * WalTtlSeconds() and walSizeLimitMB() affect how archived logs
   * will be deleted.
   * 1. If both set to 0, logs will be deleted asap and will not get into
   *    the archive.
   * 2. If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0,
   *    WAL files will be checked every 10 min and if total size is greater
   *    then WAL_size_limit_MB, they will be deleted starting with the
   *    earliest until size_limit is met. All empty files will be deleted.
   * 3. If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
   *    WAL files will be checked every WAL_ttl_secondsi / 2 and those that
   *    are older than WAL_ttl_seconds will be deleted.
   * 4. If both are not 0, WAL files will be checked every 10 min and both
   *    checks will be performed with ttl being first.
   *
   * @param sizeLimitMB size limit in mega-bytes.
   * @return the reference to the current option.
   * @see setWalSizeLimitMB()
   */
  public Options setWalSizeLimitMB(long sizeLimitMB) {
    assert(isInitialized());
    setWalSizeLimitMB(nativeHandle_, sizeLimitMB);
    return this;
  }
  private native void setWalSizeLimitMB(long handle, long sizeLimitMB);

  /**
   * Number of bytes to preallocate (via fallocate) the manifest
   * files.  Default is 4mb, which is reasonable to reduce random IO
   * as well as prevent overallocation for mounts that preallocate
   * large amounts of data (such as xfs's allocsize option).
   *
   * @return size in bytes.
   */
  public long manifestPreallocationSize() {
    assert(isInitialized());
    return manifestPreallocationSize(nativeHandle_);
  }
  private native long manifestPreallocationSize(long handle);

  /**
   * Number of bytes to preallocate (via fallocate) the manifest
   * files.  Default is 4mb, which is reasonable to reduce random IO
   * as well as prevent overallocation for mounts that preallocate
   * large amounts of data (such as xfs's allocsize option).
   *
   * @param size the size in byte
   * @return the reference to the current option.
   */
  public Options setManifestPreallocationSize(long size) {
    assert(isInitialized());
    setManifestPreallocationSize(nativeHandle_, size);
    return this;
  }
  private native void setManifestPreallocationSize(
      long handle, long size);

  /**
   * Data being read from file storage may be buffered in the OS
   * Default: true
   *
   * @return if true, then OS buffering is allowed.
   */
  public boolean allowOsBuffer() {
    assert(isInitialized());
    return allowOsBuffer(nativeHandle_);
  }
  private native boolean allowOsBuffer(long handle);

  /**
   * Data being read from file storage may be buffered in the OS
   * Default: true
   *
   * @param allowOsBufferif true, then OS buffering is allowed.
   * @return the reference to the current option.
   */
  public Options setAllowOsBuffer(boolean allowOsBuffer) {
    assert(isInitialized());
    setAllowOsBuffer(nativeHandle_, allowOsBuffer);
    return this;
  }
  private native void setAllowOsBuffer(
      long handle, boolean allowOsBuffer);

  /**
   * Allow the OS to mmap file for reading sst tables.
   * Default: false
   *
   * @return true if mmap reads are allowed.
   */
  public boolean allowMmapReads() {
    assert(isInitialized());
    return allowMmapReads(nativeHandle_);
  }
  private native boolean allowMmapReads(long handle);

  /**
   * Allow the OS to mmap file for reading sst tables.
   * Default: false
   *
   * @param allowMmapReads true if mmap reads are allowed.
   * @return the reference to the current option.
   */
  public Options setAllowMmapReads(boolean allowMmapReads) {
    assert(isInitialized());
    setAllowMmapReads(nativeHandle_, allowMmapReads);
    return this;
  }
  private native void setAllowMmapReads(
      long handle, boolean allowMmapReads);

  /**
   * Allow the OS to mmap file for writing. Default: false
   *
   * @return true if mmap writes are allowed.
   */
  public boolean allowMmapWrites() {
    assert(isInitialized());
    return allowMmapWrites(nativeHandle_);
  }
  private native boolean allowMmapWrites(long handle);

  /**
   * Allow the OS to mmap file for writing. Default: false
   *
   * @param allowMmapWrites true if mmap writes are allowd.
   * @return the reference to the current option.
   */
  public Options setAllowMmapWrites(boolean allowMmapWrites) {
    assert(isInitialized());
    setAllowMmapWrites(nativeHandle_, allowMmapWrites);
    return this;
  }
  private native void setAllowMmapWrites(
      long handle, boolean allowMmapWrites);

  /**
   * Disable child process inherit open files. Default: true
   *
   * @return true if child process inheriting open files is disabled.
   */
  public boolean isFdCloseOnExec() {
    assert(isInitialized());
    return isFdCloseOnExec(nativeHandle_);
  }
  private native boolean isFdCloseOnExec(long handle);

  /**
   * Disable child process inherit open files. Default: true
   *
   * @param isFdCloseOnExec true if child process inheriting open
   *     files is disabled.
   * @return the reference to the current option.
   */
  public Options setIsFdCloseOnExec(boolean isFdCloseOnExec) {
    assert(isInitialized());
    setIsFdCloseOnExec(nativeHandle_, isFdCloseOnExec);
    return this;
  }
  private native void setIsFdCloseOnExec(
      long handle, boolean isFdCloseOnExec);

  /**
   * Skip log corruption error on recovery (If client is ok with
   * losing most recent changes)
   * Default: false
   *
   * @return true if log corruption errors are skipped during recovery.
   */
  public boolean skipLogErrorOnRecovery() {
    assert(isInitialized());
    return skipLogErrorOnRecovery(nativeHandle_);
  }
  private native boolean skipLogErrorOnRecovery(long handle);

  /**
   * Skip log corruption error on recovery (If client is ok with
   * losing most recent changes)
   * Default: false
   *
   * @param skip true if log corruption errors are skipped during recovery.
   * @return the reference to the current option.
   */
  public Options setSkipLogErrorOnRecovery(boolean skip) {
    assert(isInitialized());
    setSkipLogErrorOnRecovery(nativeHandle_, skip);
    return this;
  }
  private native void setSkipLogErrorOnRecovery(
      long handle, boolean skip);

  /**
   * If not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
   * Default: 3600 (1 hour)
   *
   * @return time interval in seconds.
   */
  public int statsDumpPeriodSec() {
    assert(isInitialized());
    return statsDumpPeriodSec(nativeHandle_);
  }
  private native int statsDumpPeriodSec(long handle);

  /**
   * if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
   * Default: 3600 (1 hour)
   *
   * @param statsDumpPeriodSec time interval in seconds.
   * @return the reference to the current option.
   */
  public Options setStatsDumpPeriodSec(int statsDumpPeriodSec) {
    assert(isInitialized());
    setStatsDumpPeriodSec(nativeHandle_, statsDumpPeriodSec);
    return this;
  }
  private native void setStatsDumpPeriodSec(
      long handle, int statsDumpPeriodSec);

  /**
   * If set true, will hint the underlying file system that the file
   * access pattern is random, when a sst file is opened.
   * Default: true
   *
   * @return true if hinting random access is on.
   */
  public boolean adviseRandomOnOpen() {
    return adviseRandomOnOpen(nativeHandle_);
  }
  private native boolean adviseRandomOnOpen(long handle);

  /**
   * If set true, will hint the underlying file system that the file
   * access pattern is random, when a sst file is opened.
   * Default: true
   *
   * @param adviseRandomOnOpen true if hinting random access is on.
   * @return the reference to the current option.
   */
  public Options setAdviseRandomOnOpen(boolean adviseRandomOnOpen) {
    assert(isInitialized());
    setAdviseRandomOnOpen(nativeHandle_, adviseRandomOnOpen);
    return this;
  }
  private native void setAdviseRandomOnOpen(
      long handle, boolean adviseRandomOnOpen);

  /**
   * Use adaptive mutex, which spins in the user space before resorting
   * to kernel. This could reduce context switch when the mutex is not
   * heavily contended. However, if the mutex is hot, we could end up
   * wasting spin time.
   * Default: false
   *
   * @return true if adaptive mutex is used.
   */
  public boolean useAdaptiveMutex() {
    assert(isInitialized());
    return useAdaptiveMutex(nativeHandle_);
  }
  private native boolean useAdaptiveMutex(long handle);

  /**
   * Use adaptive mutex, which spins in the user space before resorting
   * to kernel. This could reduce context switch when the mutex is not
   * heavily contended. However, if the mutex is hot, we could end up
   * wasting spin time.
   * Default: false
   *
   * @param useAdaptiveMutex true if adaptive mutex is used.
   * @return the reference to the current option.
   */
  public Options setUseAdaptiveMutex(boolean useAdaptiveMutex) {
    assert(isInitialized());
    setUseAdaptiveMutex(nativeHandle_, useAdaptiveMutex);
    return this;
  }
  private native void setUseAdaptiveMutex(
      long handle, boolean useAdaptiveMutex);

  /**
   * Allows OS to incrementally sync files to disk while they are being
   * written, asynchronously, in the background.
   * Issue one request for every bytes_per_sync written. 0 turns it off.
   * Default: 0
   *
   * @return size in bytes
   */
  public long bytesPerSync() {
    return bytesPerSync(nativeHandle_);
  }
  private native long bytesPerSync(long handle);

  /**
   * Allows OS to incrementally sync files to disk while they are being
   * written, asynchronously, in the background.
   * Issue one request for every bytes_per_sync written. 0 turns it off.
   * Default: 0
   *
   * @param bytesPerSync size in bytes
   * @return the reference to the current option.
   */
  public Options setBytesPerSync(long bytesPerSync) {
    assert(isInitialized());
    setBytesPerSync(nativeHandle_, bytesPerSync);
    return this;
  }
  private native void setBytesPerSync(
      long handle, long bytesPerSync);

  /**
   * Allow RocksDB to use thread local storage to optimize performance.
   * Default: true
   *
   * @return true if thread-local storage is allowed
   */
  public boolean allowThreadLocal() {
    assert(isInitialized());
    return allowThreadLocal(nativeHandle_);
  }
  private native boolean allowThreadLocal(long handle);

  /**
   * Allow RocksDB to use thread local storage to optimize performance.
   * Default: true
   *
   * @param allowThreadLocal true if thread-local storage is allowed.
   * @return the reference to the current option.
   */
  public Options setAllowThreadLocal(boolean allowThreadLocal) {
    assert(isInitialized());
    setAllowThreadLocal(nativeHandle_, allowThreadLocal);
    return this;
  }
  private native void setAllowThreadLocal(
      long handle, boolean allowThreadLocal);

  /**
   * Set the config for mem-table.
   *
   * @param config the mem-table config.
   * @return the instance of the current Options.
   */
  public Options setMemTableConfig(MemTableConfig config) {
    setMemTableFactory(nativeHandle_, config.newMemTableFactoryHandle());
    return this;
  }

  /**
   * Returns the name of the current mem table representation.
   * Memtable format can be set using setTableFormatConfig.
   *
   * @return the name of the currently-used memtable factory.
   * @see setTableFormatConfig()
   */
  public String memTableFactoryName() {
    assert(isInitialized());
    return memTableFactoryName(nativeHandle_);
  }

  /**
   * Set the config for table format.
   *
   * @param config the table format config.
   * @return the reference of the current Options.
   */
  public Options setTableFormatConfig(TableFormatConfig config) {
    setTableFactory(nativeHandle_, config.newTableFactoryHandle());
    return this;
  }

  /**
   * @return the name of the currently used table factory.
   */
  public String tableFactoryName() {
    assert(isInitialized());
    return tableFactoryName(nativeHandle_);
  }

  /**
   * This prefix-extractor uses the first n bytes of a key as its prefix.
   *
   * In some hash-based memtable representation such as HashLinkedList
   * and HashSkipList, prefixes are used to partition the keys into
   * several buckets.  Prefix extractor is used to specify how to
   * extract the prefix given a key.
   *
   * @param n use the first n bytes of a key as its prefix.
   */
  public Options useFixedLengthPrefixExtractor(int n) {
    assert(isInitialized());
    useFixedLengthPrefixExtractor(nativeHandle_, n);
    return this;
  }

///////////////////////////////////////////////////////////////////////
  /**
   * Number of keys between restart points for delta encoding of keys.
   * This parameter can be changed dynamically.  Most clients should
   * leave this parameter alone.
   * Default: 16
   *
   * @return the number of keys between restart points.
   */
  public int blockRestartInterval() {
    return blockRestartInterval(nativeHandle_);
  }
  private native int blockRestartInterval(long handle);

  /**
   * Number of keys between restart points for delta encoding of keys.
   * This parameter can be changed dynamically.  Most clients should
   * leave this parameter alone.
   * Default: 16
   *
   * @param blockRestartInterval the number of keys between restart points.
   * @return the reference to the current option.
   */
  public Options setBlockRestartInterval(int blockRestartInterval) {
    setBlockRestartInterval(nativeHandle_, blockRestartInterval);
    return this;
  }
  private native void setBlockRestartInterval(
      long handle, int blockRestartInterval);

  /**
   * Compress blocks using the specified compression algorithm.  This
     parameter can be changed dynamically.
   *
   * Default: SNAPPY_COMPRESSION, which gives lightweight but fast compression.
   *
   * @return Compression type.
   */
  public CompressionType compressionType() {
    return CompressionType.values()[compressionType(nativeHandle_)];
  }
  private native byte compressionType(long handle);

  /**
   * Compress blocks using the specified compression algorithm.  This
     parameter can be changed dynamically.
   *
   * Default: SNAPPY_COMPRESSION, which gives lightweight but fast compression.
   *
   * @param compressionType Compression Type.
   * @return the reference to the current option.
   */
  public Options setCompressionType(CompressionType compressionType) {
    setCompressionType(nativeHandle_, compressionType.getValue());
    return this;
  }
  private native void setCompressionType(long handle, byte compressionType);

   /**
   * Compaction style for DB.
   *
   * @return Compaction style.
   */
  public CompactionStyle compactionStyle() {
    return CompactionStyle.values()[compactionStyle(nativeHandle_)];
  }
  private native byte compactionStyle(long handle);

  /**
   * Set compaction style for DB.
   *
   * Default: LEVEL.
   *
   * @param compactionStyle Compaction style.
   * @return the reference to the current option.
   */
  public Options setCompactionStyle(CompactionStyle compactionStyle) {
    setCompactionStyle(nativeHandle_, compactionStyle.getValue());
    return this;
  }
  private native void setCompactionStyle(long handle, byte compactionStyle);

  /**
   * If level-styled compaction is used, then this number determines
   * the total number of levels.
   *
   * @return the number of levels.
   */
  public int numLevels() {
    return numLevels(nativeHandle_);
  }
  private native int numLevels(long handle);

  /**
   * Set the number of levels for this database
   * If level-styled compaction is used, then this number determines
   * the total number of levels.
   *
   * @param numLevels the number of levels.
   * @return the reference to the current option.
   */
  public Options setNumLevels(int numLevels) {
    setNumLevels(nativeHandle_, numLevels);
    return this;
  }
  private native void setNumLevels(
      long handle, int numLevels);

  /**
   * The number of files in leve 0 to trigger compaction from level-0 to
   * level-1.  A value < 0 means that level-0 compaction will not be
   * triggered by number of files at all.
   * Default: 4
   *
   * @return the number of files in level 0 to trigger compaction.
   */
  public int levelZeroFileNumCompactionTrigger() {
    return levelZeroFileNumCompactionTrigger(nativeHandle_);
  }
  private native int levelZeroFileNumCompactionTrigger(long handle);

  /**
   * Number of files to trigger level-0 compaction. A value <0 means that
   * level-0 compaction will not be triggered by number of files at all.
   * Default: 4
   *
   * @param numFiles the number of files in level-0 to trigger compaction.
   * @return the reference to the current option.
   */
  public Options setLevelZeroFileNumCompactionTrigger(
      int numFiles) {
    setLevelZeroFileNumCompactionTrigger(
        nativeHandle_, numFiles);
    return this;
  }
  private native void setLevelZeroFileNumCompactionTrigger(
      long handle, int numFiles);

  /**
   * Soft limit on the number of level-0 files. We start slowing down writes
   * at this point. A value < 0 means that no writing slow down will be
   * triggered by number of files in level-0.
   *
   * @return the soft limit on the number of level-0 files.
   */
  public int levelZeroSlowdownWritesTrigger() {
    return levelZeroSlowdownWritesTrigger(nativeHandle_);
  }
  private native int levelZeroSlowdownWritesTrigger(long handle);

  /**
   * Soft limit on number of level-0 files. We start slowing down writes at this
   * point. A value <0 means that no writing slow down will be triggered by
   * number of files in level-0.
   *
   * @param numFiles soft limit on number of level-0 files.
   * @return the reference to the current option.
   */
  public Options setLevelZeroSlowdownWritesTrigger(
      int numFiles) {
    setLevelZeroSlowdownWritesTrigger(nativeHandle_, numFiles);
    return this;
  }
  private native void setLevelZeroSlowdownWritesTrigger(
      long handle, int numFiles);

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @return the hard limit of the number of level-0 file.
   */
  public int levelZeroStopWritesTrigger() {
    return levelZeroStopWritesTrigger(nativeHandle_);
  }
  private native int levelZeroStopWritesTrigger(long handle);

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @param numFiles the hard limit of the number of level-0 files.
   * @return the reference to the current option.
   */
  public Options setLevelZeroStopWritesTrigger(int numFiles) {
    setLevelZeroStopWritesTrigger(nativeHandle_, numFiles);
    return this;
  }
  private native void setLevelZeroStopWritesTrigger(
      long handle, int numFiles);

  /**
   * The highest level to which a new compacted memtable is pushed if it
   * does not create overlap.  We try to push to level 2 to avoid the
   * relatively expensive level 0=>1 compactions and to avoid some
   * expensive manifest file operations.  We do not push all the way to
   * the largest level since that can generate a lot of wasted disk
   * space if the same key space is being repeatedly overwritten.
   *
   * @return the highest level where a new compacted memtable will be pushed.
   */
  public int maxMemCompactionLevel() {
    return maxMemCompactionLevel(nativeHandle_);
  }
  private native int maxMemCompactionLevel(long handle);

  /**
   * The highest level to which a new compacted memtable is pushed if it
   * does not create overlap.  We try to push to level 2 to avoid the
   * relatively expensive level 0=>1 compactions and to avoid some
   * expensive manifest file operations.  We do not push all the way to
   * the largest level since that can generate a lot of wasted disk
   * space if the same key space is being repeatedly overwritten.
   *
   * @param maxMemCompactionLevel the highest level to which a new compacted
   *     mem-table will be pushed.
   * @return the reference to the current option.
   */
  public Options setMaxMemCompactionLevel(int maxMemCompactionLevel) {
    setMaxMemCompactionLevel(nativeHandle_, maxMemCompactionLevel);
    return this;
  }
  private native void setMaxMemCompactionLevel(
      long handle, int maxMemCompactionLevel);

  /**
   * The target file size for compaction.
   * This targetFileSizeBase determines a level-1 file size.
   * Target file size for level L can be calculated by
   * targetFileSizeBase * (targetFileSizeMultiplier ^ (L-1))
   * For example, if targetFileSizeBase is 2MB and
   * target_file_size_multiplier is 10, then each file on level-1 will
   * be 2MB, and each file on level 2 will be 20MB,
   * and each file on level-3 will be 200MB.
   * by default targetFileSizeBase is 2MB.
   *
   * @return the target size of a level-0 file.
   *
   * @see targetFileSizeMultiplier()
   */
  public int targetFileSizeBase() {
    return targetFileSizeBase(nativeHandle_);
  }
  private native int targetFileSizeBase(long handle);

  /**
   * The target file size for compaction.
   * This targetFileSizeBase determines a level-1 file size.
   * Target file size for level L can be calculated by
   * targetFileSizeBase * (targetFileSizeMultiplier ^ (L-1))
   * For example, if targetFileSizeBase is 2MB and
   * target_file_size_multiplier is 10, then each file on level-1 will
   * be 2MB, and each file on level 2 will be 20MB,
   * and each file on level-3 will be 200MB.
   * by default targetFileSizeBase is 2MB.
   *
   * @param targetFileSizeBase the target size of a level-0 file.
   * @return the reference to the current option.
   *
   * @see setTargetFileSizeMultiplier()
   */
  public Options setTargetFileSizeBase(int targetFileSizeBase) {
    setTargetFileSizeBase(nativeHandle_, targetFileSizeBase);
    return this;
  }
  private native void setTargetFileSizeBase(
      long handle, int targetFileSizeBase);

  /**
   * targetFileSizeMultiplier defines the size ratio between a
   * level-(L+1) file and level-L file.
   * By default targetFileSizeMultiplier is 1, meaning
   * files in different levels have the same target.
   *
   * @return the size ratio between a level-(L+1) file and level-L file.
   */
  public int targetFileSizeMultiplier() {
    return targetFileSizeMultiplier(nativeHandle_);
  }
  private native int targetFileSizeMultiplier(long handle);

  /**
   * targetFileSizeMultiplier defines the size ratio between a
   * level-L file and level-(L+1) file.
   * By default target_file_size_multiplier is 1, meaning
   * files in different levels have the same target.
   *
   * @param multiplier the size ratio between a level-(L+1) file
   *     and level-L file.
   * @return the reference to the current option.
   */
  public Options setTargetFileSizeMultiplier(int multiplier) {
    setTargetFileSizeMultiplier(nativeHandle_, multiplier);
    return this;
  }
  private native void setTargetFileSizeMultiplier(
      long handle, int multiplier);

  /**
   * The upper-bound of the total size of level-1 files in bytes.
   * Maximum number of bytes for level L can be calculated as
   * (maxBytesForLevelBase) * (maxBytesForLevelMultiplier ^ (L-1))
   * For example, if maxBytesForLevelBase is 20MB, and if
   * max_bytes_for_level_multiplier is 10, total data size for level-1
   * will be 20MB, total file size for level-2 will be 200MB,
   * and total file size for level-3 will be 2GB.
   * by default 'maxBytesForLevelBase' is 10MB.
   *
   * @return the upper-bound of the total size of leve-1 files in bytes.
   * @see maxBytesForLevelMultiplier()
   */
  public long maxBytesForLevelBase() {
    return maxBytesForLevelBase(nativeHandle_);
  }
  private native long maxBytesForLevelBase(long handle);

  /**
   * The upper-bound of the total size of level-1 files in bytes.
   * Maximum number of bytes for level L can be calculated as
   * (maxBytesForLevelBase) * (maxBytesForLevelMultiplier ^ (L-1))
   * For example, if maxBytesForLevelBase is 20MB, and if
   * max_bytes_for_level_multiplier is 10, total data size for level-1
   * will be 20MB, total file size for level-2 will be 200MB,
   * and total file size for level-3 will be 2GB.
   * by default 'maxBytesForLevelBase' is 10MB.
   *
   * @return maxBytesForLevelBase the upper-bound of the total size of
   *     leve-1 files in bytes.
   * @return the reference to the current option.
   * @see setMaxBytesForLevelMultiplier()
   */
  public Options setMaxBytesForLevelBase(long maxBytesForLevelBase) {
    setMaxBytesForLevelBase(nativeHandle_, maxBytesForLevelBase);
    return this;
  }
  private native void setMaxBytesForLevelBase(
      long handle, long maxBytesForLevelBase);

  /**
   * The ratio between the total size of level-(L+1) files and the total
   * size of level-L files for all L.
   * DEFAULT: 10
   *
   * @return the ratio between the total size of level-(L+1) files and
   *     the total size of level-L files for all L.
   * @see maxBytesForLevelBase()
   */
  public int maxBytesForLevelMultiplier() {
    return maxBytesForLevelMultiplier(nativeHandle_);
  }
  private native int maxBytesForLevelMultiplier(long handle);

  /**
   * The ratio between the total size of level-(L+1) files and the total
   * size of level-L files for all L.
   * DEFAULT: 10
   *
   * @param multiplier the ratio between the total size of level-(L+1)
   *     files and the total size of level-L files for all L.
   * @return the reference to the current option.
   * @see setMaxBytesForLevelBase()
   */
  public Options setMaxBytesForLevelMultiplier(int multiplier) {
    setMaxBytesForLevelMultiplier(nativeHandle_, multiplier);
    return this;
  }
  private native void setMaxBytesForLevelMultiplier(
      long handle, int multiplier);

  /**
   * Maximum number of bytes in all compacted files.  We avoid expanding
   * the lower level file set of a compaction if it would make the
   * total compaction cover more than
   * (expanded_compaction_factor * targetFileSizeLevel()) many bytes.
   *
   * @return the maximum number of bytes in all compacted files.
   * @see sourceCompactionFactor()
   */
  public int expandedCompactionFactor() {
    return expandedCompactionFactor(nativeHandle_);
  }
  private native int expandedCompactionFactor(long handle);

  /**
   * Maximum number of bytes in all compacted files.  We avoid expanding
   * the lower level file set of a compaction if it would make the
   * total compaction cover more than
   * (expanded_compaction_factor * targetFileSizeLevel()) many bytes.
   *
   * @param expandedCompactionFactor the maximum number of bytes in all
   *     compacted files.
   * @return the reference to the current option.
   * @see setSourceCompactionFactor()
   */
  public Options setExpandedCompactionFactor(int expandedCompactionFactor) {
    setExpandedCompactionFactor(nativeHandle_, expandedCompactionFactor);
    return this;
  }
  private native void setExpandedCompactionFactor(
      long handle, int expandedCompactionFactor);

  /**
   * Maximum number of bytes in all source files to be compacted in a
   * single compaction run. We avoid picking too many files in the
   * source level so that we do not exceed the total source bytes
   * for compaction to exceed
   * (source_compaction_factor * targetFileSizeLevel()) many bytes.
   * Default:1, i.e. pick maxfilesize amount of data as the source of
   * a compaction.
   *
   * @return the maximum number of bytes in all source files to be compactedo.
   * @see expendedCompactionFactor()
   */
  public int sourceCompactionFactor() {
    return sourceCompactionFactor(nativeHandle_);
  }
  private native int sourceCompactionFactor(long handle);

  /**
   * Maximum number of bytes in all source files to be compacted in a
   * single compaction run. We avoid picking too many files in the
   * source level so that we do not exceed the total source bytes
   * for compaction to exceed
   * (source_compaction_factor * targetFileSizeLevel()) many bytes.
   * Default:1, i.e. pick maxfilesize amount of data as the source of
   * a compaction.
   *
   * @param sourceCompactionFactor the maximum number of bytes in all
   *     source files to be compacted in a single compaction run.
   * @return the reference to the current option.
   * @see setExpendedCompactionFactor()
   */
  public Options setSourceCompactionFactor(int sourceCompactionFactor) {
    setSourceCompactionFactor(nativeHandle_, sourceCompactionFactor);
    return this;
  }
  private native void setSourceCompactionFactor(
      long handle, int sourceCompactionFactor);

  /**
   * Control maximum bytes of overlaps in grandparent (i.e., level+2) before we
   * stop building a single file in a level->level+1 compaction.
   *
   * @return maximum bytes of overlaps in "grandparent" level.
   */
  public int maxGrandparentOverlapFactor() {
    return maxGrandparentOverlapFactor(nativeHandle_);
  }
  private native int maxGrandparentOverlapFactor(long handle);

  /**
   * Control maximum bytes of overlaps in grandparent (i.e., level+2) before we
   * stop building a single file in a level->level+1 compaction.
   *
   * @param maxGrandparentOverlapFactor maximum bytes of overlaps in
   *     "grandparent" level.
   * @return the reference to the current option.
   */
  public Options setMaxGrandparentOverlapFactor(
      int maxGrandparentOverlapFactor) {
    setMaxGrandparentOverlapFactor(nativeHandle_, maxGrandparentOverlapFactor);
    return this;
  }
  private native void setMaxGrandparentOverlapFactor(
      long handle, int maxGrandparentOverlapFactor);

  /**
   * Puts are delayed 0-1 ms when any level has a compaction score that exceeds
   * soft_rate_limit. This is ignored when == 0.0.
   * CONSTRAINT: soft_rate_limit <= hard_rate_limit. If this constraint does not
   * hold, RocksDB will set soft_rate_limit = hard_rate_limit
   * Default: 0 (disabled)
   *
   * @return soft-rate-limit for put delay.
   */
  public double softRateLimit() {
    return softRateLimit(nativeHandle_);
  }
  private native double softRateLimit(long handle);

  /**
   * Puts are delayed 0-1 ms when any level has a compaction score that exceeds
   * soft_rate_limit. This is ignored when == 0.0.
   * CONSTRAINT: soft_rate_limit <= hard_rate_limit. If this constraint does not
   * hold, RocksDB will set soft_rate_limit = hard_rate_limit
   * Default: 0 (disabled)
   *
   * @param softRateLimit the soft-rate-limit of a compaction score
   *     for put delay.
   * @return the reference to the current option.
   */
  public Options setSoftRateLimit(double softRateLimit) {
    setSoftRateLimit(nativeHandle_, softRateLimit);
    return this;
  }
  private native void setSoftRateLimit(
      long handle, double softRateLimit);

  /**
   * Puts are delayed 1ms at a time when any level has a compaction score that
   * exceeds hard_rate_limit. This is ignored when <= 1.0.
   * Default: 0 (disabled)
   *
   * @return the hard-rate-limit of a compaction score for put delay.
   */
  public double hardRateLimit() {
    return hardRateLimit(nativeHandle_);
  }
  private native double hardRateLimit(long handle);

  /**
   * Puts are delayed 1ms at a time when any level has a compaction score that
   * exceeds hard_rate_limit. This is ignored when <= 1.0.
   * Default: 0 (disabled)
   *
   * @param hardRateLimit the hard-rate-limit of a compaction score for put
   *     delay.
   * @return the reference to the current option.
   */
  public Options setHardRateLimit(double hardRateLimit) {
    setHardRateLimit(nativeHandle_, hardRateLimit);
    return this;
  }
  private native void setHardRateLimit(
      long handle, double hardRateLimit);

  /**
   * The maximum time interval a put will be stalled when hard_rate_limit
   * is enforced.  If 0, then there is no limit.
   * Default: 1000
   *
   * @return the maximum time interval a put will be stalled when
   *     hard_rate_limit is enforced.
   */
  public int rateLimitDelayMaxMilliseconds() {
    return rateLimitDelayMaxMilliseconds(nativeHandle_);
  }
  private native int rateLimitDelayMaxMilliseconds(long handle);

  /**
   * The maximum time interval a put will be stalled when hard_rate_limit
   * is enforced. If 0, then there is no limit.
   * Default: 1000
   *
   * @param rateLimitDelayMaxMilliseconds the maximum time interval a put
   *     will be stalled.
   * @return the reference to the current option.
   */
  public Options setRateLimitDelayMaxMilliseconds(
      int rateLimitDelayMaxMilliseconds) {
    setRateLimitDelayMaxMilliseconds(
        nativeHandle_, rateLimitDelayMaxMilliseconds);
    return this;
  }
  private native void setRateLimitDelayMaxMilliseconds(
      long handle, int rateLimitDelayMaxMilliseconds);

  /**
   * The size of one block in arena memory allocation.
   * If <= 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   *
   * There are two additonal restriction of the The specified size:
   * (1) size should be in the range of [4096, 2 << 30] and
   * (2) be the multiple of the CPU word (which helps with the memory
   * alignment).
   *
   * We'll automatically check and adjust the size number to make sure it
   * conforms to the restrictions.
   * Default: 0
   *
   * @return the size of an arena block
   */
  public long arenaBlockSize() {
    return arenaBlockSize(nativeHandle_);
  }
  private native long arenaBlockSize(long handle);

  /**
   * The size of one block in arena memory allocation.
   * If <= 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   *
   * There are two additonal restriction of the The specified size:
   * (1) size should be in the range of [4096, 2 << 30] and
   * (2) be the multiple of the CPU word (which helps with the memory
   * alignment).
   *
   * We'll automatically check and adjust the size number to make sure it
   * conforms to the restrictions.
   * Default: 0
   *
   * @param arenaBlockSize the size of an arena block
   * @return the reference to the current option.
   */
  public Options setArenaBlockSize(long arenaBlockSize) {
    setArenaBlockSize(nativeHandle_, arenaBlockSize);
    return this;
  }
  private native void setArenaBlockSize(
      long handle, long arenaBlockSize);

  /**
   * Disable automatic compactions. Manual compactions can still
   * be issued on this column family
   *
   * @return true if auto-compactions are disabled.
   */
  public boolean disableAutoCompactions() {
    return disableAutoCompactions(nativeHandle_);
  }
  private native boolean disableAutoCompactions(long handle);

  /**
   * Disable automatic compactions. Manual compactions can still
   * be issued on this column family
   *
   * @param disableAutoCompactions true if auto-compactions are disabled.
   * @return the reference to the current option.
   */
  public Options setDisableAutoCompactions(boolean disableAutoCompactions) {
    setDisableAutoCompactions(nativeHandle_, disableAutoCompactions);
    return this;
  }
  private native void setDisableAutoCompactions(
      long handle, boolean disableAutoCompactions);

  /**
   * Purge duplicate/deleted keys when a memtable is flushed to storage.
   * Default: true
   *
   * @return true if purging keys is disabled.
   */
  public boolean purgeRedundantKvsWhileFlush() {
    return purgeRedundantKvsWhileFlush(nativeHandle_);
  }
  private native boolean purgeRedundantKvsWhileFlush(long handle);

  /**
   * Purge duplicate/deleted keys when a memtable is flushed to storage.
   * Default: true
   *
   * @param purgeRedundantKvsWhileFlush true if purging keys is disabled.
   * @return the reference to the current option.
   */
  public Options setPurgeRedundantKvsWhileFlush(
      boolean purgeRedundantKvsWhileFlush) {
    setPurgeRedundantKvsWhileFlush(
        nativeHandle_, purgeRedundantKvsWhileFlush);
    return this;
  }
  private native void setPurgeRedundantKvsWhileFlush(
      long handle, boolean purgeRedundantKvsWhileFlush);

  /**
   * If true, compaction will verify checksum on every read that happens
   * as part of compaction
   * Default: true
   *
   * @return true if compaction verifies checksum on every read.
   */
  public boolean verifyChecksumsInCompaction() {
    return verifyChecksumsInCompaction(nativeHandle_);
  }
  private native boolean verifyChecksumsInCompaction(long handle);

  /**
   * If true, compaction will verify checksum on every read that happens
   * as part of compaction
   * Default: true
   *
   * @param verifyChecksumsInCompaction true if compaction verifies
   *     checksum on every read.
   * @return the reference to the current option.
   */
  public Options setVerifyChecksumsInCompaction(
      boolean verifyChecksumsInCompaction) {
    setVerifyChecksumsInCompaction(
        nativeHandle_, verifyChecksumsInCompaction);
    return this;
  }
  private native void setVerifyChecksumsInCompaction(
      long handle, boolean verifyChecksumsInCompaction);

  /**
   * Use KeyMayExist API to filter deletes when this is true.
   * If KeyMayExist returns false, i.e. the key definitely does not exist, then
   * the delete is a noop. KeyMayExist only incurs in-memory look up.
   * This optimization avoids writing the delete to storage when appropriate.
   * Default: false
   *
   * @return true if filter-deletes behavior is on.
   */
  public boolean filterDeletes() {
    return filterDeletes(nativeHandle_);
  }
  private native boolean filterDeletes(long handle);

  /**
   * Use KeyMayExist API to filter deletes when this is true.
   * If KeyMayExist returns false, i.e. the key definitely does not exist, then
   * the delete is a noop. KeyMayExist only incurs in-memory look up.
   * This optimization avoids writing the delete to storage when appropriate.
   * Default: false
   *
   * @param filterDeletes true if filter-deletes behavior is on.
   * @return the reference to the current option.
   */
  public Options setFilterDeletes(boolean filterDeletes) {
    setFilterDeletes(nativeHandle_, filterDeletes);
    return this;
  }
  private native void setFilterDeletes(
      long handle, boolean filterDeletes);

  /**
   * An iteration->Next() sequentially skips over keys with the same
   * user-key unless this option is set. This number specifies the number
   * of keys (with the same userkey) that will be sequentially
   * skipped before a reseek is issued.
   * Default: 8
   *
   * @return the number of keys could be skipped in a iteration.
   */
  public long maxSequentialSkipInIterations() {
    return maxSequentialSkipInIterations(nativeHandle_);
  }
  private native long maxSequentialSkipInIterations(long handle);

  /**
   * An iteration->Next() sequentially skips over keys with the same
   * user-key unless this option is set. This number specifies the number
   * of keys (with the same userkey) that will be sequentially
   * skipped before a reseek is issued.
   * Default: 8
   *
   * @param maxSequentialSkipInIterations the number of keys could
   *     be skipped in a iteration.
   * @return the reference to the current option.
   */
  public Options setMaxSequentialSkipInIterations(long maxSequentialSkipInIterations) {
    setMaxSequentialSkipInIterations(nativeHandle_, maxSequentialSkipInIterations);
    return this;
  }
  private native void setMaxSequentialSkipInIterations(
      long handle, long maxSequentialSkipInIterations);

  /**
   * Allows thread-safe inplace updates.
   * If inplace_callback function is not set,
   *   Put(key, new_value) will update inplace the existing_value iff
   *   * key exists in current memtable
   *   * new sizeof(new_value) <= sizeof(existing_value)
   *   * existing_value for that key is a put i.e. kTypeValue
   * If inplace_callback function is set, check doc for inplace_callback.
   * Default: false.
   *
   * @return true if thread-safe inplace updates are allowed.
   */
  public boolean inplaceUpdateSupport() {
    return inplaceUpdateSupport(nativeHandle_);
  }
  private native boolean inplaceUpdateSupport(long handle);

  /**
   * Allows thread-safe inplace updates.
   * If inplace_callback function is not set,
   *   Put(key, new_value) will update inplace the existing_value iff
   *   * key exists in current memtable
   *   * new sizeof(new_value) <= sizeof(existing_value)
   *   * existing_value for that key is a put i.e. kTypeValue
   * If inplace_callback function is set, check doc for inplace_callback.
   * Default: false.
   *
   * @param inplaceUpdateSupport true if thread-safe inplace updates
   *     are allowed.
   * @return the reference to the current option.
   */
  public Options setInplaceUpdateSupport(boolean inplaceUpdateSupport) {
    setInplaceUpdateSupport(nativeHandle_, inplaceUpdateSupport);
    return this;
  }
  private native void setInplaceUpdateSupport(
      long handle, boolean inplaceUpdateSupport);

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @return the number of locks used for inplace update.
   */
  public long inplaceUpdateNumLocks() {
    return inplaceUpdateNumLocks(nativeHandle_);
  }
  private native long inplaceUpdateNumLocks(long handle);

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @param inplaceUpdateNumLocks the number of locks used for
   *     inplace updates.
   * @return the reference to the current option.
   */
  public Options setInplaceUpdateNumLocks(long inplaceUpdateNumLocks) {
    setInplaceUpdateNumLocks(nativeHandle_, inplaceUpdateNumLocks);
    return this;
  }
  private native void setInplaceUpdateNumLocks(
      long handle, long inplaceUpdateNumLocks);

  /**
   * Returns the number of bits used in the prefix bloom filter.
   *
   * This value will be used only when a prefix-extractor is specified.
   *
   * @return the number of bloom-bits.
   * @see useFixedLengthPrefixExtractor()
   */
  public int memtablePrefixBloomBits() {
    return memtablePrefixBloomBits(nativeHandle_);
  }
  private native int memtablePrefixBloomBits(long handle);

  /**
   * Sets the number of bits used in the prefix bloom filter.
   *
   * This value will be used only when a prefix-extractor is specified.
   *
   * @param memtablePrefixBloomBits the number of bits used in the
   *     prefix bloom filter.
   * @return the reference to the current option.
   */
  public Options setMemtablePrefixBloomBits(int memtablePrefixBloomBits) {
    setMemtablePrefixBloomBits(nativeHandle_, memtablePrefixBloomBits);
    return this;
  }
  private native void setMemtablePrefixBloomBits(
      long handle, int memtablePrefixBloomBits);

  /**
   * The number of hash probes per key used in the mem-table.
   *
   * @return the number of hash probes per key.
   */
  public int memtablePrefixBloomProbes() {
    return memtablePrefixBloomProbes(nativeHandle_);
  }
  private native int memtablePrefixBloomProbes(long handle);

  /**
   * The number of hash probes per key used in the mem-table.
   *
   * @param memtablePrefixBloomProbes the number of hash probes per key.
   * @return the reference to the current option.
   */
  public Options setMemtablePrefixBloomProbes(int memtablePrefixBloomProbes) {
    setMemtablePrefixBloomProbes(nativeHandle_, memtablePrefixBloomProbes);
    return this;
  }
  private native void setMemtablePrefixBloomProbes(
      long handle, int memtablePrefixBloomProbes);

  /**
   * Control locality of bloom filter probes to improve cache miss rate.
   * This option only applies to memtable prefix bloom and plaintable
   * prefix bloom. It essentially limits the max number of cache lines each
   * bloom filter check can touch.
   * This optimization is turned off when set to 0. The number should never
   * be greater than number of probes. This option can boost performance
   * for in-memory workload but should use with care since it can cause
   * higher false positive rate.
   * Default: 0
   *
   * @return the level of locality of bloom-filter probes.
   * @see setMemTablePrefixBloomProbes
   */
  public int bloomLocality() {
    return bloomLocality(nativeHandle_);
  }
  private native int bloomLocality(long handle);

  /**
   * Control locality of bloom filter probes to improve cache miss rate.
   * This option only applies to memtable prefix bloom and plaintable
   * prefix bloom. It essentially limits the max number of cache lines each
   * bloom filter check can touch.
   * This optimization is turned off when set to 0. The number should never
   * be greater than number of probes. This option can boost performance
   * for in-memory workload but should use with care since it can cause
   * higher false positive rate.
   * Default: 0
   *
   * @param bloomLocality the level of locality of bloom-filter probes.
   * @return the reference to the current option.
   */
  public Options setBloomLocality(int bloomLocality) {
    setBloomLocality(nativeHandle_, bloomLocality);
    return this;
  }
  private native void setBloomLocality(
      long handle, int bloomLocality);

  /**
   * Maximum number of successive merge operations on a key in the memtable.
   *
   * When a merge operation is added to the memtable and the maximum number of
   * successive merges is reached, the value of the key will be calculated and
   * inserted into the memtable instead of the merge operation. This will
   * ensure that there are never more than max_successive_merges merge
   * operations in the memtable.
   *
   * Default: 0 (disabled)
   *
   * @return the maximum number of successive merges.
   */
  public long maxSuccessiveMerges() {
    return maxSuccessiveMerges(nativeHandle_);
  }
  private native long maxSuccessiveMerges(long handle);

  /**
   * Maximum number of successive merge operations on a key in the memtable.
   *
   * When a merge operation is added to the memtable and the maximum number of
   * successive merges is reached, the value of the key will be calculated and
   * inserted into the memtable instead of the merge operation. This will
   * ensure that there are never more than max_successive_merges merge
   * operations in the memtable.
   *
   * Default: 0 (disabled)
   *
   * @param maxSuccessiveMerges the maximum number of successive merges.
   * @return the reference to the current option.
   */
  public Options setMaxSuccessiveMerges(long maxSuccessiveMerges) {
    setMaxSuccessiveMerges(nativeHandle_, maxSuccessiveMerges);
    return this;
  }
  private native void setMaxSuccessiveMerges(
      long handle, long maxSuccessiveMerges);

  /**
   * The minimum number of write buffers that will be merged together
   * before writing to storage.  If set to 1, then
   * all write buffers are fushed to L0 as individual files and this increases
   * read amplification because a get request has to check in all of these
   * files. Also, an in-memory merge may result in writing lesser
   * data to storage if there are duplicate records in each of these
   * individual write buffers.  Default: 1
   *
   * @return the minimum number of write buffers that will be merged together.
   */
  public int minWriteBufferNumberToMerge() {
    return minWriteBufferNumberToMerge(nativeHandle_);
  }
  private native int minWriteBufferNumberToMerge(long handle);

  /**
   * The minimum number of write buffers that will be merged together
   * before writing to storage.  If set to 1, then
   * all write buffers are fushed to L0 as individual files and this increases
   * read amplification because a get request has to check in all of these
   * files. Also, an in-memory merge may result in writing lesser
   * data to storage if there are duplicate records in each of these
   * individual write buffers.  Default: 1
   *
   * @param minWriteBufferNumberToMerge the minimum number of write buffers
   *     that will be merged together.
   * @return the reference to the current option.
   */
  public Options setMinWriteBufferNumberToMerge(int minWriteBufferNumberToMerge) {
    setMinWriteBufferNumberToMerge(nativeHandle_, minWriteBufferNumberToMerge);
    return this;
  }
  private native void setMinWriteBufferNumberToMerge(
      long handle, int minWriteBufferNumberToMerge);

  /**
   * The number of partial merge operands to accumulate before partial
   * merge will be performed. Partial merge will not be called
   * if the list of values to merge is less than min_partial_merge_operands.
   *
   * If min_partial_merge_operands < 2, then it will be treated as 2.
   *
   * Default: 2
   *
   * @return
   */
  public int minPartialMergeOperands() {
    return minPartialMergeOperands(nativeHandle_);
  }
  private native int minPartialMergeOperands(long handle);

  /**
   * The number of partial merge operands to accumulate before partial
   * merge will be performed. Partial merge will not be called
   * if the list of values to merge is less than min_partial_merge_operands.
   *
   * If min_partial_merge_operands < 2, then it will be treated as 2.
   *
   * Default: 2
   *
   * @param minPartialMergeOperands
   * @return the reference to the current option.
   */
  public Options setMinPartialMergeOperands(int minPartialMergeOperands) {
    setMinPartialMergeOperands(nativeHandle_, minPartialMergeOperands);
    return this;
  }
  private native void setMinPartialMergeOperands(
      long handle, int minPartialMergeOperands);

  /**
   * Release the memory allocated for the current instance
   * in the c++ side.
   */
  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  static final int DEFAULT_PLAIN_TABLE_BLOOM_BITS_PER_KEY = 10;
  static final double DEFAULT_PLAIN_TABLE_HASH_TABLE_RATIO = 0.75;
  static final int DEFAULT_PLAIN_TABLE_INDEX_SPARSENESS = 16;

  private native void newOptions();
  private native void disposeInternal(long handle);
  private native void setCreateIfMissing(long handle, boolean flag);
  private native boolean createIfMissing(long handle);
  private native void setWriteBufferSize(long handle, long writeBufferSize);
  private native long writeBufferSize(long handle);
  private native void setMaxWriteBufferNumber(
      long handle, int maxWriteBufferNumber);
  private native int maxWriteBufferNumber(long handle);
  private native void setMaxBackgroundCompactions(
      long handle, int maxBackgroundCompactions);
  private native int maxBackgroundCompactions(long handle);
  private native void createStatistics(long optHandle);
  private native long statisticsPtr(long optHandle);

  private native void setMemTableFactory(long handle, long factoryHandle);
  private native String memTableFactoryName(long handle);

  private native void setTableFactory(long handle, long factoryHandle);
  private native String tableFactoryName(long handle);

  private native void useFixedLengthPrefixExtractor(
      long handle, int prefixLength);

  long cacheSize_;
  int numShardBits_;
  RocksEnv env_;
}
