//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * CassandraValueMergeOperator is a merge operator that merges two cassandra wide column
 * values.
 */
public class CassandraValueMergeOperator extends MergeOperator {

  private final int gcGracePeriodInSeconds;
  private final int operandsLimit;

  public CassandraValueMergeOperator(int gcGracePeriodInSeconds) {
    this(gcGracePeriodInSeconds, 0);
  }

  public CassandraValueMergeOperator(int gcGracePeriodInSeconds, int operandsLimit) {
    super();
    this.gcGracePeriodInSeconds = gcGracePeriodInSeconds;
    this.operandsLimit = operandsLimit;
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return newSharedCassandraValueMergeOperator(gcGracePeriodInSeconds, operandsLimit);
  }

  private native static long newSharedCassandraValueMergeOperator(int gcGracePeriodInSeconds, int limit);
  @Override protected final native void disposeInternal(final long handle);
}
