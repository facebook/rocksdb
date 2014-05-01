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
  }

  /**
   * Release the c++ object pointed by the native handle.
   */
  public abstract void dispose();

  protected boolean isInitialized() {
    return (nativeHandle_ != 0);
  }

  @Override protected void finalize() {
    dispose();
  }

  protected long nativeHandle_;
}
