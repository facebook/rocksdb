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
import static org.fusesource.hawtjni.runtime.MethodFlag.CONSTANT_INITIALIZER;
import static org.fusesource.hawtjni.runtime.MethodFlag.CPP_DELETE;

/**
 * Provides a java interface to the C++ leveldb::Slice class.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JniClass(name="leveldb::Slice", flags={STRUCT, CPP})
class NativeSlice {

    @JniClass(name="leveldb::Slice", flags={CPP})
    static class SliceJNI {
        static {
            NativeDB.LIBRARY.load();
            init();
        }

        @JniMethod(flags={CPP_DELETE})
        public static final native void delete(
                long self
                );

        public static final native void memmove (
                @JniArg(cast="void *") long dest,
                @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) NativeSlice src,
                @JniArg(cast="size_t") long size);

        public static final native void memmove (
                @JniArg(cast="void *", flags={NO_IN, CRITICAL}) NativeSlice dest,
                @JniArg(cast="const void *") long src,
                @JniArg(cast="size_t") long size);


        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();

        @JniField(flags={CONSTANT}, accessor="sizeof(struct leveldb::Slice)")
        static int SIZEOF;

    }
    
    
    @JniField(cast="const char*")
    private long data_;
    @JniField(cast="size_t")
    private long size_;

    public NativeSlice() {
    }

    public NativeSlice(long data, long length) {
        this.data_ = data;
        this.size_ = length;
    }

    public NativeSlice(NativeBuffer buffer) {
        this(buffer.pointer(), buffer.capacity());
    }

    public static NativeSlice create(NativeBuffer buffer) {
        if(buffer == null ) {
            return null;
        } else {
            return new NativeSlice(buffer);
        }
    }

    public long data() {
        return data_;
    }

    public NativeSlice data(long data) {
        this.data_ = data;
        return this;
    }

    public long size() {
        return size_;
    }

    public NativeSlice size(long size) {
        this.size_ = size;
        return this;
    }

    public NativeSlice set(NativeSlice buffer) {
        this.size_ = buffer.size_;
        this.data_ = buffer.data_;
        return this;
    }

    public NativeSlice set(NativeBuffer buffer) {
        this.size_ = buffer.capacity();
        this.data_ = buffer.pointer();
        return this;
    }

    public byte[] toByteArray() {
        if( size_ > Integer.MAX_VALUE ) {
            throw new ArrayIndexOutOfBoundsException("Native slice is larger than the maximum Java array");
        }
        byte []rc = new byte[(int) size_];
        NativeBuffer.NativeBufferJNI.buffer_copy(data_, 0, rc, 0, rc.length);
        return rc;
    }
    
    static NativeBuffer arrayCreate(int dimension) {
        return NativeBuffer.create(dimension*SliceJNI.SIZEOF);
    }

    void write(long buffer, int index) {
        SliceJNI.memmove(PointerMath.add(buffer, SliceJNI.SIZEOF*index), this, SliceJNI.SIZEOF);
    }

    void read(long buffer, int index) {
        SliceJNI.memmove(this, PointerMath.add(buffer, SliceJNI.SIZEOF*index), SliceJNI.SIZEOF);
    }
    

}
