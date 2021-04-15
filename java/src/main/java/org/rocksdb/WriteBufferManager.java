// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Java wrapper over native write_buffer_manager class
 */
public class WriteBufferManager extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct a new instance of WriteBufferManager.
   *
   * Check
   * <a href="https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager">
   * https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager</a> for more
   * details on when to use it
   *
   * @param bufferSizeBytes buffer size(in bytes) to use for native
   *                        write_buffer_manager
   * @param cache           cache whose memory should be bounded by this write
   *                        buffer manager
   */
  public WriteBufferManager(final long bufferSizeBytes, final Cache cache) {
    super(newWriteBufferManager(bufferSizeBytes, cache.nativeHandle_));
  }

  /**
   * Returns the buffer size of the write buffer manager
   *
   * @return buffer size.
   *
   */
  public long getBufferSize() {
    assert (isOwningHandle());
    return getBufferSize(this.nativeHandle_);
  }

  /**
   * Returns the memory usage of the write buffer manager
   *
   * @return memory usage.
   *
   */
  public long getMemoryUsage() {
    assert (isOwningHandle());
    return getMemoryUsage(this.nativeHandle_);
  }

  /**
   * Returns the mutable memtable memory usage of the write buffer manager
   *
   * @return mutable memtable memory usage size.
   *
   */
  public long getMutableMemtableMemoryUsage() {
    assert (isOwningHandle());
    return getMutableMemtableMemoryUsage(this.nativeHandle_);
  }

  private native static long newWriteBufferManager(final long bufferSizeBytes, final long cacheHandle);

  private native static long getMemoryUsage(final long handle);

  private native static long getMutableMemtableMemoryUsage(final long handle);

  private native static long getBufferSize(final long handle);

  @Override
  protected native void disposeInternal(final long handle);
}
