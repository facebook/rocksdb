// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * <p>An iterator yields a sequence of key/value pairs from a source.
 * The following class defines the interface. Multiple implementations
 * are provided by this library.  In particular, iterators are provided
 * to access the contents of a Table or a DB.</p>
 *
 * <p>Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.</p>
 *
 * @see org.rocksdb.RocksObject
 */
public class RocksIterator extends RocksObject {
  public RocksIterator(RocksDB rocksDB, long nativeHandle) {
    super();
    nativeHandle_ = nativeHandle;
    // rocksDB must point to a valid RocksDB instance.
    assert(rocksDB != null);
    // RocksIterator must hold a reference to the related RocksDB instance
    // to guarantee that while a GC cycle starts RocksDBIterator instances
    // are freed prior to RocksDB instances.
    rocksDB_ = rocksDB;
  }

  /**
   * An iterator is either positioned at a key/value pair, or
   * not valid.  This method returns true iff the iterator is valid.
   *
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
   * valid after this call iff the source is not empty.
   */
  public void seekToLast() {
    assert(isInitialized());
    seekToLast0(nativeHandle_);
  }

  /**
   * <p>Moves to the next entry in the source.  After this call, Valid() is
   * true iff the iterator was not positioned at the last entry in the source.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   */
  public void next() {
    assert(isInitialized());
    next0(nativeHandle_);
  }

  /**
   * <p>Moves to the previous entry in the source.  After this call, Valid() is
   * true iff the iterator was not positioned at the first entry in source.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   */
  public void prev() {
    assert(isInitialized());
    prev0(nativeHandle_);
  }

  /**
   * <p>Return the key for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   *
   * @return key for the current entry.
   */
  public byte[] key() {
    assert(isInitialized());
    return key0(nativeHandle_);
  }

  /**
   * <p>Return the value for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: !AtEnd() &amp;&amp; !AtStart()</p>
   * @return value for the current entry.
   */
  public byte[] value() {
    assert(isInitialized());
    return value0(nativeHandle_);
  }

  /**
   * <p>Position at the first key in the source that at or past target
   * The iterator is valid after this call iff the source contains
   * an entry that comes at or past target.</p>
   *
   * @param target byte array describing a key or a
   *     key prefix to seek for.
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void status() throws RocksDBException {
    assert(isInitialized());
    status0(nativeHandle_);
  }

  /**
   * <p>Deletes underlying C++ iterator pointer.</p>
   *
   * <p>Note: the underlying handle can only be safely deleted if the RocksDB
   * instance related to a certain RocksIterator is still valid and initialized.
   * Therefore {@code disposeInternal()} checks if the RocksDB is initialized
   * before freeing the native handle.</p>
   */
  @Override protected void disposeInternal() {
    assert(isInitialized());
    if (rocksDB_.isInitialized()) {
      disposeInternal(nativeHandle_);
    }
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

  RocksDB rocksDB_;
}
