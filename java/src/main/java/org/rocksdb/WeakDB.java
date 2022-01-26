// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class WeakDB extends RocksNative {

  protected WeakDB(long nativeReference) {
    super(nativeReference);
  }

  public boolean isDatabaseOpen() {
    return isDatabaseOpen(getNative());
  }

  private native boolean isDatabaseOpen(long nativeReference);

  @Override
  protected native void nativeClose(long nativeReference);

  @Override
  protected native boolean isLastReference(long nativeAPIReference);
}
