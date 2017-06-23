// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
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
