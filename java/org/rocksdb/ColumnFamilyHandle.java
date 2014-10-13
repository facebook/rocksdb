// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * ColumnFamilyHandle class to hold handles to underlying rocksdb
 * ColumnFamily Pointers.
 */
public class ColumnFamilyHandle extends RocksObject {
  ColumnFamilyHandle(long nativeHandle) {
    super();
    nativeHandle_ = nativeHandle;
  }

  /**
   * Deletes underlying C++ filter pointer.
   *
   * Note that this function should be called only after all
   * RocksDB instances referencing the filter are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  private native void disposeInternal(long handle);

}
