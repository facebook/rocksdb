// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// This source code is also licensed under the GPLv2 license found in the
// COPYING file in the root directory of this source tree.

package org.rocksdb;

/**
 * CassandraValueMergeOperator is a merge operator that merges two cassandra wide column
 * values.
 */
public class CassandraValueMergeOperator extends MergeOperator {
    public CassandraValueMergeOperator() {
        super(newSharedCassandraValueMergeOperator());
    }

    private native static long newSharedCassandraValueMergeOperator();

    @Override protected final native void disposeInternal(final long handle);
}
