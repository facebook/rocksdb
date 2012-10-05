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

import org.fusesource.hawtjni.runtime.JniArg;
import org.fusesource.hawtjni.runtime.JniClass;
import org.fusesource.hawtjni.runtime.JniMethod;
import org.fusesource.hawtjni.runtime.PointerMath;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fusesource.hawtjni.runtime.ArgFlag.*;

/**
 * A NativeBuffer allocates a native buffer on the heap.  It supports
 * creating sub slices/views of that buffer and manages reference tracking
 * so that the the native buffer is freed once all NativeBuffer views
 * are deleted.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class NativeBuffer extends NativeObject {

    @JniClass
    static class NativeBufferJNI {
        static {
            NativeDB.LIBRARY.load();
        }

        @JniMethod(cast="void *")
        public static final native long malloc(
                @JniArg(cast="size_t") long size);

        public static final native void free(
                @JniArg(cast="void *") long self);

//        public static final native void buffer_copy (
//                @JniArg(cast="const void *") long src,
//                @JniArg(cast="size_t") long srcPos,
//                @JniArg(cast="void *") long dest,
//                @JniArg(cast="size_t") long destPos,
//                @JniArg(cast="size_t") long length);

        public static final native void buffer_copy (
                @JniArg(cast="const void *", flags={NO_OUT, CRITICAL}) byte[] src,
                @JniArg(cast="size_t") long srcPos,
                @JniArg(cast="void *") long dest,
                @JniArg(cast="size_t") long destPos,
                @JniArg(cast="size_t") long length);

        public static final native void buffer_copy (
                @JniArg(cast="const void *") long src,
                @JniArg(cast="size_t") long srcPos,
                @JniArg(cast="void *", flags={NO_IN, CRITICAL}) byte[] dest,
                @JniArg(cast="size_t") long destPos,
                @JniArg(cast="size_t") long length);

//        @JniMethod(cast="void *")
//        public static final native long memset (
//                @JniArg(cast="void *") long buffer,
//                int c,
//                @JniArg(cast="size_t") long num);

    }

    private static class Allocation extends NativeObject {
        private final AtomicInteger retained = new AtomicInteger(0);

        private Allocation(long size) {
            super(NativeBufferJNI.malloc(size));
        }

        void retain() {
            assertAllocated();
            retained.incrementAndGet();
        }

        void release() {
            assertAllocated();
            int r = retained.decrementAndGet();
            if( r < 0 ) {
                throw new Error("The object has already been deleted.");
            } else if( r==0 ) {
                NativeBufferJNI.free(self);
            }
            self = 0;
        }
    }

    private static class Pool {
        private final NativeBuffer.Pool prev;
        Allocation allocation;
        long pos;
        long remaining;
        int chunk;

        public Pool(int chunk, Pool prev) {
            this.chunk = chunk;
            this.prev = prev;
        }

        NativeBuffer create(long size) {
            if( size >= chunk ) {
                Allocation allocation = new Allocation(size);
                return new NativeBuffer(allocation, allocation.self, size);
            }

            if( remaining < size ) {
                delete();
            }

            if( allocation == null ) {
                allocate();
            }

            NativeBuffer rc = new NativeBuffer(allocation, pos, size);
            pos = PointerMath.add(pos, size);
            remaining -= size;
            return rc;
        }

        private void allocate() {
            allocation = new NativeBuffer.Allocation(chunk);
            allocation.retain();
            remaining = chunk;
            pos = allocation.self;
        }

        public void delete() {
            if( allocation!=null ) {
                allocation.release();
                allocation = null;
            }
        }
    }

    private final Allocation allocation;
    private final long capacity;

    static final private ThreadLocal<Pool> CURRENT_POOL = new ThreadLocal<Pool>();

    static public NativeBuffer create(long capacity) {
        Pool pool = CURRENT_POOL.get();
        if( pool == null ) {
            Allocation allocation = new Allocation(capacity);
            return new NativeBuffer(allocation, allocation.self, capacity);
        } else {
            return pool.create(capacity);
        }
    }


    public static void pushMemoryPool(int size) {
        Pool original = CURRENT_POOL.get();
        Pool next = new Pool(size, original);
        CURRENT_POOL.set(next);
    }

    public static void popMemoryPool() {
        Pool next = CURRENT_POOL.get();
        next.delete();
        if( next.prev == null ) {
            CURRENT_POOL.remove();
        } else {
            CURRENT_POOL.set(next.prev);
        }
    }

    static public NativeBuffer create(byte[] data) {
        if( data == null ) {
            return null;
        } else {
            return create(data, 0 , data.length);
        }
    }
    
    static public NativeBuffer create(String data) {
        return create(cbytes(data));
    }

    static public NativeBuffer create(byte[] data, int offset, int length) {
        NativeBuffer rc = create(length);
        rc.write(0, data, offset, length);
        return rc;
    }
    
    private NativeBuffer(Allocation allocation, long self, long capacity) {
        super(self);
        this.capacity = capacity;
        this.allocation = allocation;
        this.allocation.retain();
    }

    public NativeBuffer slice(long offset, long length) {
        assertAllocated();
        if( length < 0 ) throw new IllegalArgumentException("length cannot be negative");
        if( offset < 0 ) throw new IllegalArgumentException("offset cannot be negative");
        if( offset+length >= capacity) throw new ArrayIndexOutOfBoundsException("offset + length exceed the length of this buffer");
        return new NativeBuffer(allocation, PointerMath.add(self, offset), length);
    }
    
    static byte[] cbytes(String strvalue) {
        byte[] value = strvalue.getBytes();
        // expand by 1 so we get a null at the end.
        byte[] rc = new byte[value.length+1];
        System.arraycopy(value, 0, rc, 0, value.length);
        return rc;
    }

    public NativeBuffer head(long length) {
        return slice(0, length);
    }

    public NativeBuffer tail(long length) {
        if( capacity-length < 0) throw new ArrayIndexOutOfBoundsException("capacity-length cannot be less than zero");
        return slice(capacity-length, length);
    }

    public void delete() {
        allocation.release();
    }

    public long capacity() {
        return capacity;
    }

    public void write(long at, byte []source, int offset, int length) {
        assertAllocated();
        if( length < 0 ) throw new IllegalArgumentException("length cannot be negative");
        if( offset < 0 ) throw new IllegalArgumentException("offset cannot be negative");
        if( at < 0 ) throw new IllegalArgumentException("at cannot be negative");
        if( at+length > capacity ) throw new ArrayIndexOutOfBoundsException("at + length exceeds the capacity of this object");
        if( offset+length > source.length) throw new ArrayIndexOutOfBoundsException("offset + length exceed the length of the source buffer");
        NativeBufferJNI.buffer_copy(source, offset, self, at, length);
    }

    public void read(long at, byte []target, int offset, int length) {
        assertAllocated();
        if( length < 0 ) throw new IllegalArgumentException("length cannot be negative");
        if( offset < 0 ) throw new IllegalArgumentException("offset cannot be negative");
        if( at < 0 ) throw new IllegalArgumentException("at cannot be negative");
        if( at+length > capacity ) throw new ArrayIndexOutOfBoundsException("at + length exceeds the capacity of this object");
        if( offset+length > target.length) throw new ArrayIndexOutOfBoundsException("offset + length exceed the length of the target buffer");
        NativeBufferJNI.buffer_copy(self, at, target, offset, length);
    }

    public byte[] toByteArray() {
        if( capacity > Integer.MAX_VALUE ) {
            throw new OutOfMemoryError("Native buffer larger than the largest allowed Java byte[]");
        }
        byte [] rc = new byte[(int) capacity];
        read(0, rc, 0, rc.length);
        return rc;
    }
}
