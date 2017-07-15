// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * A CompactionFilter allows an application to modify/delete a key-value at
 * the time of compaction.
 *
 * At present we just permit an overriding Java class to wrap a C++
 * implementation
 */
public abstract class AbstractCompactionFilter<T extends AbstractSlice<?>>
    extends RocksObject {

  protected AbstractCompactionFilter(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Deletes underlying C++ compaction pointer.
   *
   * Note that this function should be called only after all
   * RocksDB instances referencing the compaction filter are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected final native void disposeInternal(final long handle);
}
