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
    super(newWriteBufferManager(bufferSizeBytes, cache.nativeHandle_, allowStall));
    this.allowStall_ = allowStall;
  }

  public WriteBufferManager(final long bufferSizeBytes, final Cache cache){
    this(bufferSizeBytes, cache, false);
  }

  public boolean allowStall() {
    return allowStall_;
  }

  private native static long newWriteBufferManager(
      final long bufferSizeBytes, final long cacheHandle, final boolean allowStall);

  @Override
  protected native void disposeInternal(final long handle);

  private boolean allowStall_;
}
