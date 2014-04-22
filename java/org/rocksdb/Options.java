// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Options to control the behavior of a database.  It will be used
 * during the creation of a RocksDB (i.e., RocksDB.open()).
 *
 * Note that dispose() must be called before an Options instance
 * become out-of-scope to release the allocated memory in c++.
 */
public class Options {
  static final long DEFAULT_CACHE_SIZE = 8 << 20;
  /**
   * Construct options for opening a RocksDB.
   *
   * This constructor will create (by allocating a block of memory)
   * an rocksdb::Options in the c++ side.
   */
  public Options() {
    nativeHandle_ = 0;
    cacheSize_ = DEFAULT_CACHE_SIZE;
    newOptions();
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

  /*
   * Approximate size of user data packed per block.  Note that the
   * block size specified here corresponds to uncompressed data.  The
   * actual size of the unit read from disk may be smaller if
   * compression is enabled.  This parameter can be changed dynamically.
   *
   * Default: 4K
   *
   * @param blockSize the size of each block in bytes.
   * @return the instance of the current Options.
   * @see RocksDB.open()
   */
  public Options setBlockSize(long blockSize) {
    assert(isInitialized());
    setBlockSize(nativeHandle_, blockSize);
    return this;
  }

  /*
   * Returns the size of a block in bytes.
   *
   * @return block size.
   * @see setBlockSize()
   */
  public long blockSize() {
    assert(isInitialized());
    return blockSize(nativeHandle_);
  }

  /**
   * Use the specified filter policy to reduce disk reads.
   * @param Filter policy java instance.
   * @return the instance of the current Options.
   * @see RocksDB.open()
   */
  public Options setFilter(Filter filter) {
    assert(isInitialized());
    setFilter0(nativeHandle_, filter.getNativeHandle());
    return this;
  }

  /*
   * Disable compaction triggered by seek.
   * With bloomfilter and fast storage, a miss on one level
   * is very cheap if the file handle is cached in table cache
   * (which is true if max_open_files is large).
   * Default: true
   *
   * @param disableSeekCompaction a boolean value to specify whether
   *     to disable seek compaction.
   * @return the instance of the current Options.
   * @see RocksDB.open()
   */
  public Options setDisableSeekCompaction(boolean disableSeekCompaction) {
    assert(isInitialized());
    setDisableSeekCompaction(nativeHandle_, disableSeekCompaction);
    return this;
  }

  /*
   * Returns true if disable seek compaction is set to true.
   *
   * @return true if disable seek compaction is set to true.
   * @see setDisableSeekCompaction()
   */
  public boolean disableSeekCompaction() {
    assert(isInitialized());
    return disableSeekCompaction(nativeHandle_);
  }

  /**
   * Set the amount of cache in bytes that will be used by RocksDB.
   * If cacheSize is non-positive, then cache will not be used.
   *
   * DEFAULT: 8M
   */
  public Options setCacheSize(long cacheSize) {
    cacheSize_ = cacheSize;
    return this;
  }

  /**
   * @return the amount of cache in bytes that will be used by RocksDB.
   */
  public long cacheSize() {
    return cacheSize_;
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
   * The following two fields affect how archived logs will be deleted.
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
   */
  public long walTtlSeconds() {
    assert(isInitialized());
    return walTtlSeconds(nativeHandle_);
  }
  private native long walTtlSeconds(long handle);

  /**
   * The following two fields affect how archived logs will be deleted.
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
   */
  public Options setWALTtlSeconds(long walTtlSeconds) {
    assert(isInitialized());
    setWALTtlSeconds(nativeHandle_, walTtlSeconds);
    return this;
  }
  private native void setWALTtlSeconds(long handle, long walTtlSeconds);

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

  /**
   * Release the memory allocated for the current instance
   * in the c++ side.
   */
  public synchronized void dispose() {
    if (isInitialized()) {
      dispose0();
    }
  }

  @Override protected void finalize() {
    dispose();
  }

  private boolean isInitialized() {
    return (nativeHandle_ != 0);
  }

  static final int DEFAULT_PLAIN_TABLE_BLOOM_BITS_PER_KEY = 10;
  static final double DEFAULT_PLAIN_TABLE_HASH_TABLE_RATIO = 0.75;
  static final int DEFAULT_PLAIN_TABLE_INDEX_SPARSENESS = 16;

  private native void newOptions();
  private native void dispose0();
  private native void setCreateIfMissing(long handle, boolean flag);
  private native boolean createIfMissing(long handle);
  private native void setWriteBufferSize(long handle, long writeBufferSize);
  private native long writeBufferSize(long handle);
  private native void setMaxWriteBufferNumber(
      long handle, int maxWriteBufferNumber);
  private native int maxWriteBufferNumber(long handle);
  private native void setBlockSize(long handle, long blockSize);
  private native long blockSize(long handle);
  private native void setDisableSeekCompaction(
      long handle, boolean disableSeekCompaction);
  private native boolean disableSeekCompaction(long handle);
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

  private native void setFilter0(long optHandle, long fpHandle);

  long nativeHandle_;
  long cacheSize_;
}
