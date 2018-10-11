// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.

package org.rocksdb;

/**
 * Uint64AddOperator is a merge operator that accumlates a long
 * integer value.
 */
public class UInt64AddOperator extends MergeOperator {
    public UInt64AddOperator() {
        super(newSharedUInt64AddOperator());
    }

    private native static long newSharedUInt64AddOperator();
    @Override protected final native void disposeInternal(final long handle);
}
