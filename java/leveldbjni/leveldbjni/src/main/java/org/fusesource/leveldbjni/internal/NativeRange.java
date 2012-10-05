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

import static org.fusesource.hawtjni.runtime.ArgFlag.*;
import static org.fusesource.hawtjni.runtime.ClassFlag.CPP;
import static org.fusesource.hawtjni.runtime.ClassFlag.STRUCT;
import static org.fusesource.hawtjni.runtime.FieldFlag.CONSTANT;
import static org.fusesource.hawtjni.runtime.FieldFlag.FIELD_SKIP;
import static org.fusesource.hawtjni.runtime.MethodFlag.CONSTANT_INITIALIZER;

/**
 * Provides a java interface to the C++ leveldb::ReadOptions class.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class NativeRange {

    @JniClass(name="leveldb::Range", flags={STRUCT, CPP})
    static public class RangeJNI {

        static {
            NativeDB.LIBRARY.load();
            init();
        }

        public static final native void memmove (
                @JniArg(cast="void *") long dest,
                @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) RangeJNI src,
                @JniArg(cast="size_t") long size);

        public static final native void memmove (
                @JniArg(cast="void *", flags={NO_IN, CRITICAL}) RangeJNI dest,
                @JniArg(cast="const void *") long src,
                @JniArg(cast="size_t") long size);


        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();

        @JniField(flags={CONSTANT}, accessor="sizeof(struct leveldb::Range)")
        static int SIZEOF;

        @JniField
        NativeSlice start = new NativeSlice();
        @JniField(flags={FIELD_SKIP})
        NativeBuffer start_buffer;

        @JniField
        NativeSlice limit = new NativeSlice();
        @JniField(flags={FIELD_SKIP})
        NativeBuffer limit_buffer;

        public RangeJNI(NativeRange range) {
            start_buffer = NativeBuffer.create(range.start());
            start.set(start_buffer);
            try {
                limit_buffer = NativeBuffer.create(range.limit());
            } catch (OutOfMemoryError e) {
                start_buffer.delete();
                throw e;
            }
            limit.set(limit_buffer);
        }

        public void delete() {
            start_buffer.delete();
            limit_buffer.delete();
        }

        static NativeBuffer arrayCreate(int dimension) {
            return NativeBuffer.create(dimension*SIZEOF);
        }

        void arrayWrite(long buffer, int index) {
            RangeJNI.memmove(PointerMath.add(buffer, SIZEOF * index), this, SIZEOF);
        }

        void arrayRead(long buffer, int index) {
            RangeJNI.memmove(this, PointerMath.add(buffer, SIZEOF * index), SIZEOF);
        }

    }

    final private byte[] start;
    final private byte[] limit;

    public byte[] limit() {
        return limit;
    }

    public byte[] start() {
        return start;
    }

    public NativeRange(byte[] start, byte[] limit) {
        NativeDB.checkArgNotNull(start, "start");
        NativeDB.checkArgNotNull(limit, "limit");
        this.limit = limit;
        this.start = start;
    }
}
