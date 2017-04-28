// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Base class for comparators which will receive
 * ByteBuffer based access via org.rocksdb.DirectSlice
 * in their compare method implementation.
 *
 * ByteBuffer based slices perform better when large keys
 * are involved. When using smaller keys consider
 * using @see org.rocksdb.Comparator
 */
public abstract class DirectComparator extends AbstractComparator<DirectSlice> {

  private final long nativeHandle_;

  public DirectComparator(final ComparatorOptions copt) {
    super();
    this.nativeHandle_ = createNewDirectComparator0(copt.nativeHandle_);
  }

  @Override
  protected final long getNativeHandle() {
    return nativeHandle_;
  }

  private native long createNewDirectComparator0(
      final long comparatorOptionsHandle);
}
