// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * An iterator yields a sequence of key/value pairs from a source.
 * The following class defines the interface.  Multiple implementations
 * are provided by this library.  In particular, iterators are provided
 * to access the contents of a Table or a DB.
 *
 * Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.
 */
public class RocksIterator extends RocksObject {
  public RocksIterator(long nativeHandle) {
    super();
    nativeHandle_ = nativeHandle;
  }

  /**
   * An iterator is either positioned at a key/value pair, or
   * not valid.  This method returns true iff the iterator is valid.
   * @return true if iterator is valid.
   */
  public boolean isValid() {
    assert(isInitialized());
    return isValid0(nativeHandle_);
  }

  /**
   * Position at the first key in the source.  The iterator is Valid()
   * after this call iff the source is not empty.
   */
  public void seekToFirst() {
    assert(isInitialized());
    seekToFirst0(nativeHandle_);
  }

  /**
   * Position at the last key in the source.  The iterator is
   * Valid() after this call iff the source is not empty.
   */
  public void seekToLast() {
    assert(isInitialized());
    seekToLast0(nativeHandle_);
  }

  /**
   * Moves to the next entry in the source.  After this call, Valid() is
   * true iff the iterator was not positioned at the last entry in the source.
   * REQUIRES: Valid()
   */
  public void next() {
    assert(isInitialized());
    next0(nativeHandle_);
  }

  /**
   * Moves to the previous entry in the source.  After this call, Valid() is
   * true iff the iterator was not positioned at the first entry in source.
   * REQUIRES: Valid()
   */
  public void prev() {
    assert(isInitialized());
    prev0(nativeHandle_);
  }

  /**
   * Return the key for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.
   * REQUIRES: Valid()
   * @return key for the current entry.
   */
  public byte[] key() {
    assert(isInitialized());
    return key0(nativeHandle_);
  }

  /**
   * Return the value for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.
   * REQUIRES: !AtEnd() && !AtStart()
   * @return value for the current entry.
   */
  public byte[] value() {
    assert(isInitialized());
    return value0(nativeHandle_);
  }

  /**
   * Position at the first key in the source that at or past target
   * The iterator is Valid() after this call iff the source contains
   * an entry that comes at or past target.
   */
  public void seek(byte[] target) {
    assert(isInitialized());
    seek0(nativeHandle_, target, target.length);
  }

  /**
   * If an error has occurred, return it.  Else return an ok status.
   * If non-blocking IO is requested and this operation cannot be
   * satisfied without doing some IO, then this returns Status::Incomplete().
   *
   */
  public void status() throws RocksDBException {
    assert(isInitialized());
    status0(nativeHandle_);
  }

  /**
   * Deletes underlying C++ iterator pointer.
   */
  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  private native boolean isValid0(long handle);
  private native void disposeInternal(long handle);
  private native void seekToFirst0(long handle);
  private native void seekToLast0(long handle);
  private native void next0(long handle);
  private native void prev0(long handle);
  private native byte[] key0(long handle);
  private native byte[] value0(long handle);
  private native void seek0(long handle, byte[] target, int targetLen);
  private native void status0(long handle);
}
