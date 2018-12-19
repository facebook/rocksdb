//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.List;

public abstract class AbstractMergeOperator extends MergeOperator
{
  public AbstractMergeOperator(boolean allowSingleOperand,
                               boolean allowShouldMerge,
                               boolean partialMultiMerge) {
    super(allowSingleOperand ? 1L : 0L, allowShouldMerge ? 1L : 0L, partialMultiMerge ? 1L : 0L);
  }

  abstract public byte[] fullMerge(byte[] key, byte[] oldvalue, byte[][] operands, ReturnType rt) throws RocksDBException;
  abstract public byte[] partialMultiMerge(byte[] key, byte[][] operands, ReturnType rt) throws RocksDBException;
  abstract public byte[] partialMerge(byte[] key, byte[] left, byte[] right, ReturnType rt) throws RocksDBException;
  abstract public boolean shouldMerge(byte[][] operands)throws RocksDBException;
  abstract public String name();

  private boolean getBoolean(long longValue) {
    if (longValue > 0)
      return true;
    return false;
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    boolean allowSingleOperand = getBoolean(nativeParameterHandles[0]);
    boolean allowShouldMerge = getBoolean(nativeParameterHandles[1]);
    boolean partialMultiMerge = getBoolean(nativeParameterHandles[2]);
    return newMergeOperator(allowSingleOperand, allowShouldMerge, partialMultiMerge);
  }

  protected void disposeInternal() { disposeInternal(nativeHandle_); }
  protected final native void disposeInternal(final long handle);

  private native long newMergeOperator(final boolean allowSingleOperand,
                                       final boolean allowShouldMerge,
                                       final boolean partialMultiMerge);
}
