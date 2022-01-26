// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class ColumnFamilyHandleDefault extends ColumnFamilyHandle {

  ColumnFamilyHandleDefault(long nativeReference) {
    super(nativeReference);
  }

  @Override protected native boolean equalsByHandle(final long nativeReference, long otherNativeReference);
  @Override protected native byte[] getName(long handle);
  @Override protected native int getID(long handle);
  @Override protected native ColumnFamilyDescriptor getDescriptor(long handle);
  @Override protected boolean isDefaultColumnFamily() {
    return true;
  }
  @Override protected native void nativeClose(long nativeReference);
  @Override protected boolean isLastReference(long nativeAPIReference) {
    return true;
  }
}
