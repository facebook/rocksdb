// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * <p>An iterator that yields a sequence of key/value pairs from a source.
 * Multiple implementations are provided by this library.
 * In particular, iterators are provided
 * to access the contents of a Table or a DB.</p>
 *
 * <p>Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.</p>
 *
 * @see org.rocksdb.RocksObject
 */
public class RocksIterator extends RocksObject implements RocksIteratorInterface {
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

  @Override
  public boolean isValid() {
    assert(isInitialized());
    return isValid0(nativeHandle_);
  }

  @Override
  public void seekToFirst() {
    assert(isInitialized());
    seekToFirst0(nativeHandle_);
  }

  @Override
  public void seekToLast() {
    assert(isInitialized());
    seekToLast0(nativeHandle_);
  }

  @Override
  public void seek(byte[] target) {
    assert(isInitialized());
    seek0(nativeHandle_, target, target.length);
  }

  @Override
  public void next() {
    assert(isInitialized());
    next0(nativeHandle_);
  }

  @Override
  public void prev() {
    assert(isInitialized());
    prev0(nativeHandle_);
  }

  @Override
  public void status() throws RocksDBException {
    assert(isInitialized());
    status0(nativeHandle_);
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
   * <p>Deletes underlying C++ iterator pointer.</p>
   *
   * <p>Note: the underlying handle can only be safely deleted if the RocksDB
   * instance related to a certain RocksIterator is still valid and initialized.
   * Therefore {@code disposeInternal()} checks if the RocksDB is initialized
   * before freeing the native handle.</p>
   */
  @Override protected void disposeInternal() {
    synchronized (rocksDB_) {
      assert (isInitialized());
      if (rocksDB_.isInitialized()) {
        disposeInternal(nativeHandle_);
      }
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

  final RocksDB rocksDB_;
}
