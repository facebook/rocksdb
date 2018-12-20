// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.charset.Charset;

/**
 * StringAppendOperator is a merge operator that concatenates
 * two strings.
 */
public class StringAppendOperator extends MergeOperator {
    public StringAppendOperator() {
        this(',');
    }

    public StringAppendOperator(char delim) {
        super(newSharedStringAppendOperator(delim));
    }

    public StringAppendOperator(byte[] delim) {
        super(newSharedStringAppendTESTOperator(delim));
    }

    public StringAppendOperator(String delim) {
        this(delim.getBytes());
    }

    public StringAppendOperator(String delim, Charset charset) {
        this(delim.getBytes(charset));
    }

    private native static long newSharedStringAppendOperator(final char delim);
    private native static long newSharedStringAppendTESTOperator(final byte[] delim);
    @Override protected final native void disposeInternal(final long handle);
}
