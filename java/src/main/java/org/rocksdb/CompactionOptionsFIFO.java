// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Options for FIFO Compaction
 */
public class CompactionOptionsFIFO extends RocksObject {

  public CompactionOptionsFIFO() {
    super(newCompactionOptionsFIFO());
  }

  /**
   * Once the total sum of table files reaches this, we will delete the oldest
   * table file
   *
   * Default: 1GB
   *
   * @param maxTableFilesSize The maximum size of the table files
   *
   * @return the reference to the current options.
   */
  public CompactionOptionsFIFO setMaxTableFilesSize(
      final long maxTableFilesSize) {
    setMaxTableFilesSize(nativeHandle_, maxTableFilesSize);
    return this;
  }

  /**
   * Once the total sum of table files reaches this, we will delete the oldest
   * table file
   *
   * Default: 1GB
   *
   * @return max table file size in bytes
   */
  public long maxTableFilesSize() {
    return maxTableFilesSize(nativeHandle_);
  }

  private native void setMaxTableFilesSize(long handle, long maxTableFilesSize);
  private native long maxTableFilesSize(long handle);

  private native static long newCompactionOptionsFIFO();
  @Override protected final native void disposeInternal(final long handle);
}
