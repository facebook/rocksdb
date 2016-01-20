// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * RocksObject is the base-class of all RocksDB classes that has a pointer to
 * some c++ {@code rocksdb} object.
 *
 * <p>
 * RocksObject has {@code dispose()} function, which releases its associated c++
 * resource.</p>
 * <p>
 * This function can be either called manually, or being called automatically
 * during the regular Java GC process. However, since Java may wrongly assume a
 * RocksObject only contains a long member variable and think it is small in size,
 * Java may give {@code RocksObject} low priority in the GC process. For this, it is
 * suggested to call {@code dispose()} manually. However, it is safe to let
 * {@code RocksObject} go out-of-scope without manually calling {@code dispose()}
 * as {@code dispose()} will be called in the finalizer during the
 * regular GC process.</p>
 */
public abstract class RocksObject extends NativeReference {

  /**
   * A long variable holding c++ pointer pointing to some RocksDB C++ object.
   */
  protected final long nativeHandle_;

  protected RocksObject(final long nativeHandle) {
    super(true);
    this.nativeHandle_ = nativeHandle;
  }

  /**
   * Deletes underlying C++ object pointer.
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  protected abstract void disposeInternal(final long handle);
}
