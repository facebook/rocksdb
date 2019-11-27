//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public abstract class AbstractAssociativeMergeOperator extends MergeOperator
{
  public AbstractAssociativeMergeOperator() { super(); }

  /**
   * Merge values of a given key
   *
   * @param key the key to merge
   * @param oldvalue current value of the key
   * @param newvalue new value of the key
   * @param rt indicate if returned value is a reference or not
   * @throws RocksDBException
   * @return merged value of the key
   */
  abstract public byte[] merge(byte[] key, byte[] oldvalue, byte[] newvalue, ReturnType rt) throws RocksDBException;

  /**
    * The name of the MergeOperator. Used to check for MergeOperator
    * mismatches (i.e., a DB created with one MergeOperator is
    * accessed using a different MergeOperator)
    *
    * @return a string with operator name
    */
  abstract public String name();

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return newAssociativeMergeOperator();
  }

  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  protected final native void disposeInternal(final long handle);
  private native long newAssociativeMergeOperator();
}
