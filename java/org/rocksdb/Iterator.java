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
  
  public void seekToLast() {
    assert(isInitialized());
    seekToLast0(nativeHandle_);
  }
  
  public void next() {
    assert(isInitialized());
    next0(nativeHandle_);
  }
  
  public void prev() {
    assert(isInitialized());
    prev0(nativeHandle_);
  }
  
  public byte[] key() {
    assert(isInitialized());
    return key0(nativeHandle_);
  }
  
  public byte[] value() {
    assert(isInitialized());
    return value0(nativeHandle_);
  }
  
  public void seek(byte[] target) {
    assert(isInitialized());
    seek0(nativeHandle_, target, target.length);
  }
  
  public void status(){
    assert(isInitialized());
    status0(nativeHandle_);
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
  private native void seekToLast0(long handle);
  private native void next0(long handle);
  private native void prev0(long handle);
  private native byte[] key0(long handle);
  private native byte[] value0(long handle);
  private native void seek0(long handle, byte[] target, int targetLen);
  private native void status0(long handle);
}