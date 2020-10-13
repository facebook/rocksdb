// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The config for vector memtable representation.
 */
public class VectorMemTableConfig extends MemTableConfig {
  public static final int DEFAULT_RESERVED_SIZE = 0;

  /**
   * VectorMemTableConfig constructor
   */
  public VectorMemTableConfig() {
    reservedSize_ = DEFAULT_RESERVED_SIZE;
  }

  /**
   * Set the initial size of the vector that will be used
   * by the memtable created based on this config.
   *
   * @param size the initial size of the vector.
   * @return the reference to the current config.
   */
  public VectorMemTableConfig setReservedSize(final int size) {
    reservedSize_ = size;
    return this;
  }

  /**
   * Returns the initial size of the vector used by the memtable
   * created based on this config.
   *
   * @return the initial size of the vector.
   */
  public int reservedSize() {
    return reservedSize_;
  }

  @Override protected long newMemTableFactoryHandle() {
    return newMemTableFactoryHandle(reservedSize_);
  }

  private native long newMemTableFactoryHandle(long reservedSize)
      throws IllegalArgumentException;
  private int reservedSize_;
}
