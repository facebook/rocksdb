// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Base class for Cache implementations.
 */
public abstract class Cache extends RocksObject {
  /**
   * Construct a Cache.
   *
   * @param nativeHandle reference to the value of the C++ pointer pointing to the underlying native
   *     RocksDB C++ cache object.
   */
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

  private static native long getUsage(final long handle);
  private static native long getPinnedUsage(final long handle);
}
