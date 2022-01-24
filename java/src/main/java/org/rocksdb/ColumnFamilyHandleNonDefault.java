// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * ColumnFamilyHandle class to hold handles to underlying rocksdb
 * ColumnFamily Pointers.
 */
public class ColumnFamilyHandleNonDefault extends ColumnFamilyHandle {

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
   * @param nativeReference native reference to the column family.
   */
  ColumnFamilyHandleNonDefault(final long nativeReference) {
    super(nativeReference);
  }

  @Override
  protected native void nativeClose(long nativeReference);
  @Override
  protected native boolean isLastReference(long nativeAPIReference);

  @Override protected boolean isDefaultColumnFamily() {
    return false;
  }

  @Override protected native boolean equalsByHandle(final long nativeReference, long otherNativeReference);
  @Override protected native byte[] getName(final long handle) throws RocksDBException;
  @Override protected native int getID(final long handle);
  @Override protected native ColumnFamilyDescriptor getDescriptor(final long handle) throws RocksDBException;
}
