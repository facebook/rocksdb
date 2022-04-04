// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.api;

import org.rocksdb.RocksDBException;

import java.util.Arrays;
import java.util.Objects;

/**
 * ColumnFamilyHandle class to hold handles to underlying rocksdb
 * ColumnFamily Pointers.
 */
public class ColumnFamilyHandle extends RocksNative {
  /**
   * Constructs column family Java object, which operates on underlying native object.
   *
   * @param rocksDB db instance associated with this column family
   * @param nativeHandle native handle to underlying native ColumnFamily object
   */
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
   * Gets the name of the Column Family.
   *
   * @return The name of the Column Family.
   *
   * @throws RocksDBException if an error occurs whilst retrieving the name.
   */
  public byte[] getName() throws RocksDBException {
    return getName(getNative());
  }

  /**
   * Gets the ID of the Column Family.
   *
   * @return the ID of the Column Family.
   */
  public int getID() throws RocksDBException {
    return getID(getNative());
  }

  /**
   * Gets the up-to-date descriptor of the column family
   * associated with this handle. Since it fills "*desc" with the up-to-date
   * information, this call might internally lock and release DB mutex to
   * access the up-to-date CF options. In addition, all the pointer-typed
   * options cannot be referenced any longer than the original options exist.
   *
   * Note that this function is not supported in RocksDBLite.
   *
   * @return the up-to-date descriptor.
   *
   * @throws RocksDBException if an error occurs whilst retrieving the
   *     descriptor.
   */
  public ColumnFamilyDescriptor getDescriptor() throws RocksDBException {
    return getDescriptor(getNative());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ColumnFamilyHandle that = (ColumnFamilyHandle) o;
    try {
      return rocksDB_.getNative() == that.rocksDB_.getNative() &&
          getID() == that.getID() &&
          Arrays.equals(getName(), that.getName());
    } catch (RocksDBException e) {
      throw new RuntimeException("Cannot compare column family handles", e);
    }
  }

  @Override
  public int hashCode() {
    try {
      int result = Objects.hash(getID(), rocksDB_.getNative());
      result = 31 * result + Arrays.hashCode(getName());
      return result;
    } catch (RocksDBException e) {
      throw new RuntimeException("Cannot calculate hash code of column family handle", e);
    }
  }

  protected boolean isDefaultColumnFamily() throws RocksDBException {
    return getNative() == rocksDB_.getDefaultColumnFamily().getNative();
  }

  private native byte[] getName(final long handle) throws RocksDBException;
  private native int getID(final long handle);
  private native ColumnFamilyDescriptor getDescriptor(final long handle) throws RocksDBException;
  @Override
  protected final native void nativeClose(long nativeReference);

  private final RocksDB rocksDB_;
}
