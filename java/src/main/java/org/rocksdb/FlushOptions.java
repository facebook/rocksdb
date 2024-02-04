// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * FlushOptions to be passed to flush operations of
 * {@link org.rocksdb.RocksDB}.
 */
public class FlushOptions extends RocksObject {
  /**
   * Construct a new instance of FlushOptions.
   */
  public FlushOptions(){
    super(newFlushOptionsInance());
  }

  /**
   * Set if the flush operation shall block until it terminates.
   *
   * @param waitForFlush boolean value indicating if the flush
   *     operations waits for termination of the flush process.
   *
   * @return instance of current FlushOptions.
   */
  public FlushOptions setWaitForFlush(final boolean waitForFlush) {
    assert(isOwningHandle());
    setWaitForFlush(nativeHandle_, waitForFlush);
    return this;
  }

  /**
   * Wait for flush to finished.
   *
   * @return boolean value indicating if the flush operation
   *     waits for termination of the flush process.
   */
  public boolean waitForFlush() {
    assert(isOwningHandle());
    return waitForFlush(nativeHandle_);
  }

  /**
   * Set to true so that flush would proceed immediately even if it means
   * writes will stall for the duration of the flush.
   * <p>
   * Set to false so that the operation will wait until it's possible to do
   * the flush without causing stall or until required flush is performed by
   * someone else (foreground call or background thread).
   * <p>
   * Default: false
   *
   * @param allowWriteStall true to allow writes to stall for flush, false
   *     otherwise.
   *
   * @return instance of current FlushOptions.
   */
  public FlushOptions setAllowWriteStall(final boolean allowWriteStall) {
    assert(isOwningHandle());
    setAllowWriteStall(nativeHandle_, allowWriteStall);
    return this;
  }

  /**
   * Returns true if writes are allowed to stall for flushes to complete, false
   * otherwise.
   *
   * @return true if writes are allowed to stall for flushes
   */
  public boolean allowWriteStall() {
    assert(isOwningHandle());
    return allowWriteStall(nativeHandle_);
  }
  private static long newFlushOptionsInance() {
    RocksDB.loadLibrary();
    return newFlushOptions();
  }
  private static native long newFlushOptions();
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native void disposeInternalJni(final long handle);

  private static native void setWaitForFlush(final long handle, final boolean wait);
  private static native boolean waitForFlush(final long handle);
  private static native void setAllowWriteStall(final long handle, final boolean allowWriteStall);
  private static native boolean allowWriteStall(final long handle);
}
