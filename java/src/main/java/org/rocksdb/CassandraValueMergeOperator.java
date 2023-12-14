//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Just a Java wrapper around CassandraValueMergeOperator implemented in C++.
 * <p>
 * CassandraValueMergeOperator is a merge operator that merges two cassandra wide column
 * values.
 */
public class CassandraValueMergeOperator extends MergeOperator {

  /**
   * Constructs a new CassandraValueMergeOperator.
   *
   * @param gcGracePeriodInSeconds the grace period in seconds for gc.
   */
  public CassandraValueMergeOperator(final int gcGracePeriodInSeconds) {
    super(newSharedCassandraValueMergeOperator(gcGracePeriodInSeconds, 0));
  }

  /**
   * Constructs a new CassandraValueMergeOperator.
   *
   * @param gcGracePeriodInSeconds the grace period in seconds for gc.
   * @param operandsLimit the maximum size of the operands list before merge is applied.
   */
  public CassandraValueMergeOperator(final int gcGracePeriodInSeconds, final int operandsLimit) {
    super(newSharedCassandraValueMergeOperator(gcGracePeriodInSeconds, operandsLimit));
  }

  private static native long newSharedCassandraValueMergeOperator(
      int gcGracePeriodInSeconds, int limit);

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native void disposeInternalJni(final long handle);
}
