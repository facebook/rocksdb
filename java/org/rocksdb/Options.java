// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Options to control the behavior of a database.  It will be used
 * during the creation of a RocksDB (i.e., RocksDB::Open()).
 *
 * Note that dispose() must be called before an Options instance
 * become out-of-scope to release the allocated memory in c++.
 */
public class Options {
  /**
   * Construct options for opening a RocksDB.
   *
   * This constructor will create (by allocating a block of memory)
   * an rocksdb::Options in the c++ side.
   */
  public Options() {
    nativeHandle_ = 0;
    newOptions();
  }

  /**
   * If this value is set to true, then the database will be created
   * if it is missing during RocksDB::Open().
   * Default: false
   *
   * @param flag a flag indicating whether to create a database the
   *     specified database in RocksDB::Open() operation is missing.
   * @see RocksDB::Open()
   */
  public void setCreateIfMissing(boolean flag) {
    assert(isInitialized());
    setCreateIfMissing(nativeHandle_, flag);
  }

  /**
   * Return true if the create_if_missing flag is set to true.
   * If true, the database will be created if it is missing.
   *
   * @return return true if the create_if_missing flag is set to true.
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
   * @param size of write buffer
   * @see RocksDB::Open()
   */
  public void setWriteBufferSize(int writeBufferSize) {
    assert(isInitialized());
    setWriteBufferSize(nativeHandle_, writeBufferSize);
  }

  /**
   * Return size of write buffer size.
   *
   * @return size of write buffer.
   * @see setWriteBufferSize()
   */
  public int writeBufferSize()  {
    assert(isInitialized());
    return writeBufferSize(nativeHandle_);
  }

  /**
   * The maximum number of write buffers that are built up in memory.
   * The default is 2, so that when 1 write buffer is being flushed to
   * storage, new writes can continue to the other write buffer.
   * Default: 2
   *
   * @param maximum number of write buffers
   * @see RocksDB::Open()
   */
  public void setMaxWriteBufferNumber(int maxWriteBufferNumber) {
    assert(isInitialized());
    setMaxWriteBufferNumber(nativeHandle_, maxWriteBufferNumber);
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
   * @param block size
   * @see RocksDB::Open()
   */
  public void setBlockSize(int blockSize) {
    assert(isInitialized());
    setBlockSize(nativeHandle_, blockSize);
  }

  /*
   * Returns block size.
   *
   * @return block size.
   * @see setBlockSize()
   */
  public int blockSize() {
    assert(isInitialized());
    return blockSize(nativeHandle_);
  }

  /*
   * Disable compaction triggered by seek.
   * With bloomfilter and fast storage, a miss on one level
   * is very cheap if the file handle is cached in table cache
   * (which is true if max_open_files is large).
   * Default: true
   *
   * @param disable seek compaction
   * @see RocksDB::Open()
   */
  public void setDisableSeekCompaction(boolean disableSeekCompaction) {
    assert(isInitialized());
    setDisableSeekCompaction(nativeHandle_, disableSeekCompaction);
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

  /*
   * Maximum number of concurrent background jobs, submitted to
   * the default LOW priority thread pool
   * Default: 1
   *
   * @param maximum number of concurrent background jobs.
   * @see RocksDB::Open()
   */
  public void setMaxBackgroundCompactions(int maxBackgroundCompactions) {
    assert(isInitialized());
    setMaxBackgroundCompactions(nativeHandle_, maxBackgroundCompactions);
  }

  /*
   * Returns maximum number of background concurrent jobs
   *
   * @return maximum number of background concurrent jobs
   * @see setMaxBackgroundCompactions
   */
  public int maxBackgroundCompactions() {
    assert(isInitialized());
    return maxBackgroundCompactions(nativeHandle_);
  }

  /**
   * Release the memory allocated for the current instance
   * in the c++ side.
   */
  public synchronized void dispose() {
    if (nativeHandle_ != 0) {
      dispose0();
    }
  }

  private boolean isInitialized() {
    return (nativeHandle_ != 0);
  }

  private native void newOptions();
  private native void dispose0();
  private native void setCreateIfMissing(long handle, boolean flag);
  private native boolean createIfMissing(long handle);
  private native void setWriteBufferSize(long handle, int writeBufferSize);
  private native int writeBufferSize(long handle);
  private native void setMaxWriteBufferNumber(long handle, int maxWriteBufferNumber);
  private native int maxWriteBufferNumber(long handle);
  private native void setBlockSize(long handle, int blockSize);
  private native int blockSize(long handle);
  private native void setDisableSeekCompaction(long handle, boolean disableSeekCompaction);
  private native boolean disableSeekCompaction(long handle);
  private native void setMaxBackgroundCompactions(long handle, int maxBackgroundCompactions);
  private native int maxBackgroundCompactions(long handle);

  long nativeHandle_;
}
