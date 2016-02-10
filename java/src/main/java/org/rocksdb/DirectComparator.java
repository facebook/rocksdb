// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

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
