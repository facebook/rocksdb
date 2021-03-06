// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;


public abstract class Cache extends RocksObject {
  protected Cache(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * @return the maximum configured capacity of the cache
   * @throws RocksDBException
   */
  public long getCapacity() throws RocksDBException {
    return getCapacity(nativeHandle_);
  }

  /**
   * @return the memory size for the entries residing in the cache
   * @throws RocksDBException
   */
  public long getUsage() throws RocksDBException {
    return getUsage(nativeHandle_);
  }

  /**
   * @return the memory size for the entries residing in the cache
   * @throws RocksDBException
   */
  public long getHighPriorityPoolUsage() throws RocksDBException {
    return getHighPriorityPoolUsage(nativeHandle_);
  }

  /**
   * @return the memory size for the entries in use by the system
   * @throws RocksDBException
   */
  public long getPinnedUsage() throws RocksDBException {
    return getPinnedUsage(nativeHandle_);
  }

  /**
   * @return the number of entries in the cache
   * @throws RocksDBException
   */
  public long getEntries() throws RocksDBException {
    return getEntries(nativeHandle_);
  }

  private native long getCapacity(final long handle) throws RocksDBException;
  private native long getUsage(final long handle) throws RocksDBException;
  private native long getHighPriorityPoolUsage(final long handle) throws RocksDBException;
  private native long getPinnedUsage(final long handle) throws RocksDBException;
  private native long getEntries(final long handle) throws RocksDBException;
}
