// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Uint64AddOperator is a merge operator that accumlates a long
 * integer value.
 */
public class UInt64AddOperator extends MergeOperator {
    public UInt64AddOperator() {
        super();
    }

    @Override
    protected long initializeNative(final long... nativeParameterHandles) {
        return newSharedUInt64AddOperator();
    }

    protected void disposeInternal() { disposeInternal(nativeHandle_); }

    protected final native void disposeInternal(final long handle);
    private native long newSharedUInt64AddOperator();
}
