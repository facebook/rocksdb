// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Options that control write operations.
 *
 * Note that developers should call WriteOptions.dispose() to release the
 * c++ side memory before a WriteOptions instance runs out of scope.
 */
public class WriteOptions {
  public WriteOptions() {
    nativeHandle_ = 0;
    newWriteOptions();
  }

  public synchronized void dispose() {
    if (nativeHandle_ != 0) {
      dispose0(nativeHandle_);
    }
  }

  /**
   * If true, the write will be flushed from the operating system
   * buffer cache (by calling WritableFile::Sync()) before the write
   * is considered complete.  If this flag is true, writes will be
   * slower.
   *
   * If this flag is false, and the machine crashes, some recent
   * writes may be lost.  Note that if it is just the process that
   * crashes (i.e., the machine does not reboot), no writes will be
   * lost even if sync==false.
   *
   * In other words, a DB write with sync==false has similar
   * crash semantics as the "write()" system call.  A DB write
   * with sync==true has similar crash semantics to a "write()"
   * system call followed by "fdatasync()".
   *
   * Default: false
   */
  public void setSync(boolean flag) {
    setSync(nativeHandle_, flag);
  }

  /**
   * If true, the write will be flushed from the operating system
   * buffer cache (by calling WritableFile::Sync()) before the write
   * is considered complete.  If this flag is true, writes will be
   * slower.
   *
   * If this flag is false, and the machine crashes, some recent
   * writes may be lost.  Note that if it is just the process that
   * crashes (i.e., the machine does not reboot), no writes will be
   * lost even if sync==false.
   *
   * In other words, a DB write with sync==false has similar
   * crash semantics as the "write()" system call.  A DB write
   * with sync==true has similar crash semantics to a "write()"
   * system call followed by "fdatasync()".
   */
  public boolean sync() {
    return sync(nativeHandle_);
  }

  /**
   * If true, writes will not first go to the write ahead log,
   * and the write may got lost after a crash.
   */
  public void setDisableWAL(boolean flag) {
    setDisableWAL(nativeHandle_, flag);
  }

  /**
   * If true, writes will not first go to the write ahead log,
   * and the write may got lost after a crash.
   */
  public boolean disableWAL() {
    return disableWAL(nativeHandle_);
  }

  @Override protected void finalize() {
    dispose();
  }

  private native void newWriteOptions();
  private native void setSync(long handle, boolean flag);
  private native boolean sync(long handle);
  private native void setDisableWAL(long handle, boolean flag);
  private native boolean disableWAL(long handle);
  private native void dispose0(long handle);

  protected  long nativeHandle_;
}
