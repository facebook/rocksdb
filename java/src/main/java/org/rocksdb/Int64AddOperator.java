// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class Int64AddOperator extends MergeOperator {
  protected Int64AddOperator() {
    super(newSharedInt64AddOperator());
  }

  @Override protected native void disposeInternal(final long handle);

  private static native long newSharedInt64AddOperator();
}
