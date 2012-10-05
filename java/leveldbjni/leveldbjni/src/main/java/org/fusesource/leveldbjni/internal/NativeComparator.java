/*
 * Copyright (C) 2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *    * Neither the name of FuseSource Corp. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.fusesource.leveldbjni.internal;

import org.fusesource.hawtjni.runtime.*;

import static org.fusesource.hawtjni.runtime.FieldFlag.*;
import static org.fusesource.hawtjni.runtime.MethodFlag.*;
import static org.fusesource.hawtjni.runtime.ArgFlag.*;
import static org.fusesource.hawtjni.runtime.ClassFlag.*;

/**
 * <p>
 * Provides a java interface to the C++ leveldb::Comparator class.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class NativeComparator extends NativeObject {

    @JniClass(name="JNIComparator", flags={STRUCT, CPP})
    static public class ComparatorJNI {

        static {
            NativeDB.LIBRARY.load();
            init();
        }

        @JniMethod(flags={CPP_NEW})
        public static final native long create();
        @JniMethod(flags={CPP_DELETE})
        public static final native void delete(long ptr);

        public static final native void memmove (
                @JniArg(cast="void *") long dest,
                @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) ComparatorJNI src,
                @JniArg(cast="size_t") long size);

        public static final native void memmove (
                @JniArg(cast="void *", flags={NO_IN, CRITICAL}) ComparatorJNI dest,
                @JniArg(cast="const void *") long src,
                @JniArg(cast="size_t") long size);

        @JniField(cast="jobject", flags={POINTER_FIELD})
        long target;

        @JniField(cast="jmethodID", flags={POINTER_FIELD})
        long compare_method;

        @JniField(cast="const char *")
        long name;

        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();

        @JniField(flags={CONSTANT}, accessor="sizeof(struct JNIComparator)")
        static int SIZEOF;

        @JniField(flags={CONSTANT}, cast="const Comparator*", accessor="leveldb::BytewiseComparator()")
        private static long BYTEWISE_COMPARATOR;

    }

    private NativeBuffer name_buffer;
    private long globalRef;

    public NativeComparator() {
        super(ComparatorJNI.create());
        try {
            name_buffer = NativeBuffer.create(name());
            globalRef = NativeDB.DBJNI.NewGlobalRef(this);
            if( globalRef==0 ) {
                throw new RuntimeException("jni call failed: NewGlobalRef");
            }
            ComparatorJNI struct = new ComparatorJNI();
            struct.compare_method = NativeDB.DBJNI.GetMethodID(this.getClass(), "compare", "(JJ)I");
            if( struct.compare_method==0 ) {
                throw new RuntimeException("jni call failed: GetMethodID");
            }
            struct.target = globalRef;
            struct.name = name_buffer.pointer();
            ComparatorJNI.memmove(self, struct, ComparatorJNI.SIZEOF);

        } catch (RuntimeException e) {
            delete();
            throw e;
        }
    }

    public static final NativeComparator BYTEWISE_COMPARATOR = new NativeComparator(ComparatorJNI.BYTEWISE_COMPARATOR) {
        @Override
        public void delete() {
            // we won't really delete this one since it's static.
        }
        @Override
        public int compare(byte[] key1, byte[] key2) {
            throw new UnsupportedOperationException();
        }
        @Override
        public String name() {
            throw new UnsupportedOperationException();
        }
    };

    NativeComparator(long ptr) {
        super(ptr);
    }

    public void delete() {
        if( name_buffer!=null ) {
            name_buffer.delete();
            name_buffer = null;
        }
        if( globalRef!=0 ) {
            NativeDB.DBJNI.DeleteGlobalRef(globalRef);
            globalRef = 0;
        }
    }

    private int compare(long ptr1, long ptr2) {
        NativeSlice s1 = new NativeSlice();
        s1.read(ptr1, 0);
        NativeSlice s2 = new NativeSlice();
        s2.read(ptr2, 0);
        return compare(s1.toByteArray(), s2.toByteArray());
    }

    public abstract int compare(byte[] key1, byte[] key2);
    public abstract String name();

}
