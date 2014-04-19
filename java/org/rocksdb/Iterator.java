// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public class Iterator {
  private long nativeHandle_;
  
  public Iterator(long nativeHandle) {
    nativeHandle_ = nativeHandle;
  }
  
  public boolean isValid() {
    assert(isInitialized());
    return isValid0(nativeHandle_);
  }
  
  public void seekToFirst() {
    assert(isInitialized());
    seekToFirst0(nativeHandle_);
  }
  
  public synchronized void close() {
    if(nativeHandle_ != 0) {
      close0(nativeHandle_);
    }
  }
  
  @Override protected void finalize() {
    close();
  }
  
  private boolean isInitialized() {
    return (nativeHandle_ != 0);
  }
  
  private native boolean isValid0(long handle);
  private native void close0(long handle);
  private native void seekToFirst0(long handle);
}