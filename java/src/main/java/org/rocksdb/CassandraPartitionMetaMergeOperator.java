//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 *  merge operator that merges cassandra partition meta data such as
 */
public class CassandraPartitionMetaMergeOperator extends MergeOperator {
  public CassandraPartitionMetaMergeOperator() {
    super(newCassandraPartitionMetaMergeOperator());
  }

  private native static long newCassandraPartitionMetaMergeOperator();
  @Override protected final native void disposeInternal(final long handle);
}
