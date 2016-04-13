// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * RocksMutableObject is an implementation of {@link AbstractNativeReference}
 * whose reference to the underlying native C++ object can change.
 *
 * <p>The use of {@code RocksMutableObject} should be kept to a minimum, as it
 * has synchronization overheads and introduces complexity. Instead it is
 * recommended to use {@link RocksObject} where possible.</p>
 */
public abstract class RocksMutableObject extends AbstractNativeReference {

  /**
   * An mutable reference to the value of the C++ pointer pointing to some
   * underlying native RocksDB C++ object.
   */
  private long nativeHandle_;
  private boolean owningHandle_;

  protected RocksMutableObject() {
  }

  protected RocksMutableObject(final long nativeHandle) {
    this.nativeHandle_ = nativeHandle;
    this.owningHandle_ = true;
  }

  public synchronized void setNativeHandle(final long nativeHandle,
      final boolean owningNativeHandle) {
    this.nativeHandle_ = nativeHandle;
    this.owningHandle_ = owningNativeHandle;
  }

  @Override
  protected synchronized boolean isOwningHandle() {
    return this.owningHandle_;
  }

  /**
   * Gets the value of the C++ pointer pointing to the underlying
   * native C++ object
   *
   * @return the pointer value for the native object
   */
  protected synchronized long getNativeHandle() {
    assert (this.nativeHandle_ != 0);
    return this.nativeHandle_;
  }

  @Override
  public synchronized final void close() {
    if (isOwningHandle()) {
      disposeInternal();
      this.owningHandle_ = false;
      this.nativeHandle_ = 0;
    }
  }

  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  protected abstract void disposeInternal(final long handle);
}
