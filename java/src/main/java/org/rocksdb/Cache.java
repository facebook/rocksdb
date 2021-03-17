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
   * Returns the memory size for the entries
   * residing in cache.
   *
   * @return cache usage size.
   *
   */
  public long getUsage() {
    assert (isOwningHandle());
    return getUsage(this.nativeHandle_);
  }

  /**
   * Returns the memory size for the entries
   * being pinned in cache.
   *
   * @return cache pinned usage size.
   *
   */
  public long getPinnedUsage() {
    assert (isOwningHandle());
    return getPinnedUsage(this.nativeHandle_);
  }

  private native static long getUsage(final long handle);
  private native static long getPinnedUsage(final long handle);
}
