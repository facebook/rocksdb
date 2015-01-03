// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public class WBWIRocksIterator extends RocksObject implements RocksIteratorInterface {

  //TODO(AR) abstract common code from WBWIRocksIterator and RocksIterator into AbstractRocksIterator

  final WriteBatchWithIndex wbwi_;

  protected WBWIRocksIterator(WriteBatchWithIndex wbwi, long nativeHandle) {
    super();
    nativeHandle_ = nativeHandle;
    // rocksDB must point to a valid RocksDB instance.
    assert (wbwi != null);
    // WBWIRocksIterator must hold a reference to the related WriteBatchWithIndex instance
    // to guarantee that while a GC cycle starts WBWIRocksIterator instances
    // are freed prior to WriteBatchWithIndex instances.
    wbwi_ = wbwi;
  }

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public void seekToFirst() {

  }

  @Override
  public void seekToLast() {

  }

  @Override
  public void seek(byte[] target) {

  }

  @Override
  public void next() {

  }

  @Override
  public void prev() {

  }

  /**
   * Get the current entry
   */
  public WriteEntry entry() {
    throw new UnsupportedOperationException("NOT YET IMPLEMENTED"); //TODO(AR) implement
  }

  @Override
  public void status() throws RocksDBException {

  }

  /**
   * <p>Deletes underlying C++ iterator pointer.</p>
   * <p/>
   * <p>Note: the underlying handle can only be safely deleted if the WriteBatchWithIndex
   * instance related to a certain WBWIRocksIterator is still valid and initialized.
   * Therefore {@code disposeInternal()} checks if the WriteBatchWithIndex is initialized
   * before freeing the native handle.</p>
   */
  @Override
  protected void disposeInternal() {
    synchronized (wbwi_) {
      assert (isInitialized());
      if (wbwi_.isInitialized()) {
        disposeInternal(nativeHandle_);
      }
    }
  }

  private native void disposeInternal(long handle);

  /**
   * Enumeration of the Write operation
   * that created the record in the Write Batch
   */
  public enum WriteType {
    PutRecord,
    MergeRecord,
    DeleteRecord,
    LogDataRecord
  }

  /**
   * Represents the entry returned by a
   * WBWIRocksIterator
   */
  public static class WriteEntry {
    final WriteType type;
    final Slice key;
    final Slice value;

    public WriteEntry(final WriteType type, final Slice key, final Slice value) {
      this.type = type;
      this.key = key;
      this.value = value;
    }

    public WriteType getType() {
      return type;
    }

    public Slice getKey() {
      return key;
    }

    public Slice getValue() {
      return value;
    }
  }
}
