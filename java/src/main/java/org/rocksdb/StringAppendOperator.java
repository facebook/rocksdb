// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * StringAppendOperator is a merge operator that concatenates
 * two strings.
 */
public class StringAppendOperator extends MergeOperator {
    private char delim;

    public StringAppendOperator() {
        this(',');
    }

    public StringAppendOperator(final char delim) {
        super(delim);
        this.delim = delim;
    }

    @Override
    protected long initializeNative(final long... nativeParameterHandles) {
        this.delim = (char)nativeParameterHandles[0];
        return newSharedStringAppendOperator(delim);
    }

    protected void disposeInternal() { disposeInternal(nativeHandle_); }

    protected final native void disposeInternal(final long handle);
    private native long newSharedStringAppendOperator(final char delim);
}
