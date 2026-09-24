// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Java wrapper over native write_buffer_manager class
 */
public class WriteBufferManager extends RocksObject {
  /**
   * Construct a new instance of WriteBufferManager.
   * <p>
   * Check <a href="https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager">
   *     https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager</a>
   * for more details on when to use it
   *
   * @param bufferSizeBytes buffer size(in bytes) to use for native write_buffer_manager
   * @param cache cache whose memory should be bounded by this write buffer manager
   * @param allowStall if set true, it will enable stalling of writes when memory_usage() exceeds
   *     buffer_size.
   *        It will wait for flush to complete and memory usage to drop down.
   */
  public WriteBufferManager(
      final long bufferSizeBytes, final Cache cache, final boolean allowStall) {
    super(newWriteBufferManagerInstance(bufferSizeBytes, cache.nativeHandle_, allowStall));
    this.allowStall_ = allowStall;
  }

  /**
   * Construct a new instance of WriteBufferManager with default allowStall=false.
   *
   * @param bufferSizeBytes buffer size(in bytes) to use for native write_buffer_manager
   * @param cache cache whose memory should be bounded by this write buffer manager
   */
  public WriteBufferManager(final long bufferSizeBytes, final Cache cache){
    this(bufferSizeBytes, cache, false);
  }

  /**
   * Check if write buffer manager is enabled (buffer_size &gt; 0).
   *
   * @return true if buffer limiting is enabled
   */
  public boolean enabled() {
    assert(isOwningHandle());
    return enabled(nativeHandle_);
  }

  /**
   * Check if cache integration is enabled.
   *
   * @return true if cache cost tracking is enabled
   */
  public boolean costToCache() {
    assert(isOwningHandle());
    return costToCache(nativeHandle_);
  }

  /**
   * Get the total memory used by memtables.
   * Only valid if enabled() returns true.
   *
   * @return total memory usage in bytes
   */
  public long memoryUsage() {
    assert(isOwningHandle());
    return memoryUsage(nativeHandle_);
  }

  /**
   * Get the total memory used by active (mutable) memtables.
   *
   * @return active memtable memory usage in bytes
   */
  public long mutableMemtableMemoryUsage() {
    assert(isOwningHandle());
    return mutableMemtableMemoryUsage(nativeHandle_);
  }

  /**
   * Get the size of dummy entries in cache usage.
   *
   * @return dummy cache entries size in bytes
   */
  public long dummyEntriesInCacheUsage() {
    assert(isOwningHandle());
    return dummyEntriesInCacheUsage(nativeHandle_);
  }

  /**
   * Get the current buffer size limit.
   *
   * @return buffer size in bytes
   */
  public long bufferSize() {
    assert(isOwningHandle());
    return bufferSize(nativeHandle_);
  }

  /**
   * Dynamically change the buffer size.
   * REQUIRED: newSize must be greater than 0.
   *
   * @param newSize new buffer size in bytes (must be &gt; 0)
   */
  public void setBufferSize(final long newSize) {
    assert(isOwningHandle());
    if (newSize <= 0) {
      throw new IllegalArgumentException("Buffer size must be greater than 0");
    }
    setBufferSize(nativeHandle_, newSize);
  }

  /**
   * Dynamically change the allow_stall setting.
   *
   * @param allowStall if true, enables stalling of writes when memory usage exceeds buffer size
   */
  public void setAllowStall(final boolean allowStall) {
    assert(isOwningHandle());
    setAllowStall(nativeHandle_, allowStall);
    this.allowStall_ = allowStall;
  }

  /**
   * Check if write stall is currently active.
   *
   * @return true if stall is active
   */
  public boolean isStallActive() {
    assert(isOwningHandle());
    return isStallActive(nativeHandle_);
  }

  /**
   * Check if stalling threshold has been exceeded.
   *
   * @return true if memory usage &gt;= buffer size
   */
  public boolean isStallThresholdExceeded() {
    assert(isOwningHandle());
    return isStallThresholdExceeded(nativeHandle_);
  }

  /**
   * Get the allow stall setting.
   *
   * @return true if stall is allowed
   */
  public boolean allowStall() {
    return allowStall_;
  }

  private static long newWriteBufferManagerInstance(
      final long bufferSizeBytes, final long cacheHandle, final boolean allowStall) {
    RocksDB.loadLibrary();
    return newWriteBufferManager(bufferSizeBytes, cacheHandle, allowStall);
  }
  private static native long newWriteBufferManager(
      final long bufferSizeBytes, final long cacheHandle, final boolean allowStall);

  @Override
  protected void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native void disposeInternalJni(final long handle);

  private static native boolean enabled(final long handle);
  private static native boolean costToCache(final long handle);
  private static native long memoryUsage(final long handle);
  private static native long mutableMemtableMemoryUsage(final long handle);
  private static native long dummyEntriesInCacheUsage(final long handle);
  private static native long bufferSize(final long handle);
  private static native void setBufferSize(final long handle, final long newSize);
  private static native void setAllowStall(final long handle, final boolean allowStall);
  private static native boolean isStallActive(final long handle);
  private static native boolean isStallThresholdExceeded(final long handle);

  private boolean allowStall_;
}
