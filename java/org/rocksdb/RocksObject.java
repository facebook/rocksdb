// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * RocksObject is the base-class of all RocksDB related class that has
 * a pointer to some c++ rocksdb object.  Although RocksObject
 * will release its c++ resource on its finalize() once it has been
 * garbage-collected, it is suggested to call dispose() manually to
 * release its c++ resource once an instance of RocksObject is no
 * longer used.
 */
public abstract class RocksObject {
  protected RocksObject() {
    nativeHandle_ = 0;
    owningHandle_ = true;
  }

  /**
   * Release the c++ object pointed by the native handle.
   *
   * Note that once an instance of RocksObject has been disposed,
   * calling its function will lead undefined behavior.
   */
  public final synchronized void dispose() {
    if (isOwningNativeHandle() && isInitialized()) {
      disposeInternal();
    }
    nativeHandle_ = 0;
    disOwnNativeHandle();
  }

  /**
   * The helper function of dispose() which all subclasses of RocksObject
   * must implement to release their associated C++ resource.
   */
  protected abstract void disposeInternal();

  /**
   * Revoke ownership of the native object.
   *
   * This will prevent the object from attempting to delete the underlying
   * native object in its finalizer. This must be used when another object
   * takes over ownership of the native object or both will attempt to delete
   * the underlying object when garbage collected.
   *
   * When disOwnNativeHandle is called, dispose() will simply set nativeHandle_
   * to 0 without releasing its associated C++ resource.  As a result,
   * incorrectly use this function may cause memory leak.
   */
  protected void disOwnNativeHandle() {
    owningHandle_ = false;
  }

  protected boolean isOwningNativeHandle() {
    return owningHandle_;
  }

  protected boolean isInitialized() {
    return (nativeHandle_ != 0);
  }

  @Override protected void finalize() {
    dispose();
  }

  protected long nativeHandle_;
  private boolean owningHandle_;
}
