// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.rocksdb.api.RocksNative;

import java.util.Arrays;
import java.util.Objects;

/**
 * ColumnFamilyHandle class to hold handles to underlying rocksdb
 * ColumnFamily Pointers.
 */
public class ColumnFamilyHandle extends RocksNative {

  /**
   * Constructor called only from JNI.
   *
   * NOTE: we are producing an additional Java Object here to represent the underlying native C++
   * ColumnFamilyHandle object. The underlying object is not owned by ourselves. The Java API user
   * likely already had a ColumnFamilyHandle Java object which owns the underlying C++ object, as
   * they will have been presented it when they opened the database or added a Column Family.
   *
   *
   * TODO(AR) - Potentially a better design would be to cache the active Java Column Family Objects
   * in RocksDB, and return the same Java Object instead of instantiating a new one here. This could
   * also help us to improve the Java API semantics for Java users. See for example
   * https://github.com/facebook/rocksdb/issues/2687.
   *
   * TODO (AP) - not yet implemented in the new reference counted API world.
   * I think the right answer now is to create a separate ColumnFamilyHandle java object, and to let the
   * shared/weak pointers contained in that object (and other ColumnFamilyHandle java objects)
   *
   *
   * @param nativeHandle native handle to the column family.
   */
  ColumnFamilyHandle(final long nativeHandle) {
    super(nativeHandle);
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
  public int getID() {
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
      return getRocksDBAPIHandle(getNative()) == that.getRocksDBAPIHandle(that.getNative()) &&
          getID() == that.getID() &&
          Arrays.equals(getName(), that.getName());
    } catch (RocksDBException e) {
      throw new RuntimeException("Cannot compare column family handles", e);
    }
  }

  @Override
  public int hashCode() {
    try {
      int result = Objects.hash(getID(), getNative());
      result = 31 * result + Arrays.hashCode(getName());
      return result;
    } catch (RocksDBException e) {
      throw new RuntimeException("Cannot calculate hash code of column family handle", e);
    }
  }

  @Override
  protected native void nativeClose(long nativeReference);

  protected boolean isDefaultColumnFamily() {
    return isDefaultColumnFamily(getNative());
  }

  private native boolean isDefaultColumnFamily(final long columnFamilyAPIHandle);
  private native long getRocksDBAPIHandle(final long columnFamilyAPIHandle);
  private native byte[] getName(final long handle) throws RocksDBException;
  private native int getID(final long handle);
  private native ColumnFamilyDescriptor getDescriptor(final long handle) throws RocksDBException;
}
