// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * ColumnFamilyHandle class to hold handles to underlying rocksdb
 * ColumnFamily Pointers.
 */
public class ColumnFamilyHandle extends RocksObject {
  ColumnFamilyHandle(final RocksDB rocksDB,
      final long nativeHandle) {
    super(nativeHandle);
    // rocksDB must point to a valid RocksDB instance;
    assert(rocksDB != null);
    // ColumnFamilyHandle must hold a reference to the related RocksDB instance
    // to guarantee that while a GC cycle starts ColumnFamilyHandle instances
    // are freed prior to RocksDB instances.
    this.rocksDB_ = rocksDB;
  }

  /**
   * <p>Deletes underlying C++ iterator pointer.</p>
   *
   * <p>Note: the underlying handle can only be safely deleted if the RocksDB
   * instance related to a certain ColumnFamilyHandle is still valid and
   * initialized. Therefore {@code disposeInternal()} checks if the RocksDB is
   * initialized before freeing the native handle.</p>
   */
  @Override
  protected void disposeInternal() {
    if(rocksDB_.isOwningHandle()) {
      disposeInternal(nativeHandle_);
    }
  }

  @Override protected final native void disposeInternal(final long handle);

  private final RocksDB rocksDB_;
}
