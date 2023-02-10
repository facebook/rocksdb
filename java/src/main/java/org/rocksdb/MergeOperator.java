// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * MergeOperator holds an operator to be applied when compacting
 * two merge operands held under the same key in order to obtain a single
 * value.
 */
public class MergeOperator extends RocksObject {
  /**
   * Creates a new MergeOperator based on the input value string and returns the
   * result. The value might be an ID, or ID with properties, or an old-style
   * policy string.
   * Creates a new MergeOperator based on the input opts string
   * @param opts The input string stating the name of the operator and its parameters
   * @return The MergeOperator represented by the input opts
   */
  public static MergeOperator createFromString(final String opts) throws RocksDBException {
    return new MergeOperator(createMergeOperatorFromString(opts));
  }

  /**
   * Creates a new MergeOperator based on the input value string and returns the
   * result.
   * @param cfgOpts Controls how the MergeOperator is created
   * @param opts The input string stating the name of the merge operator and its parameters
   * @return The MergeOperator represented by the input opts
   */
  public static MergeOperator createFromString(final ConfigOptions cfgOpts, final String opts)
      throws RocksDBException {
    return new MergeOperator(createMergeOperatorFromString(cfgOpts.nativeHandle_, opts));
  }

  protected MergeOperator(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Deletes underlying C++ filter pointer.
   *
   * Note that this function should be called only after all
   * RocksDB instances referencing the filter are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  public String getId() {
    assert (isOwningHandle());
    return getId(nativeHandle_);
  }

  public boolean isInstanceOf(String name) {
    assert (isOwningHandle());
    return isInstanceOf(nativeHandle_, name);
  }

  protected native static long createMergeOperatorFromString(final String opts)
      throws RocksDBException;
  protected native static long createMergeOperatorFromString(
      final long cfgHandle, final String opts) throws RocksDBException;
  private native String getId(long handle);
  private native boolean isInstanceOf(long handle, String name);

  @Override protected final native void disposeInternal(final long handle);
}
