// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
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
