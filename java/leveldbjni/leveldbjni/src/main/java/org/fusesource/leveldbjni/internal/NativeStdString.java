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

import org.fusesource.hawtjni.runtime.JniClass;
import org.fusesource.hawtjni.runtime.JniMethod;

import static org.fusesource.hawtjni.runtime.ClassFlag.CPP;
import static org.fusesource.hawtjni.runtime.MethodFlag.*;

/**
 * Provides a java interface to the C++ std::string class.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class NativeStdString extends NativeObject {

    @JniClass(name="std::string", flags={CPP})
    private static class StdStringJNI {
        static {
            NativeDB.LIBRARY.load();
        }

        @JniMethod(flags={CPP_NEW})
        public static final native long create();

        @JniMethod(flags={CPP_NEW})
        public static final native long create(String value);

        @JniMethod(flags={CPP_DELETE})
        static final native void delete(
                long self);

        @JniMethod(flags={CPP_METHOD}, accessor = "c_str", cast="const char*")
        public static final native long c_str_ptr (
                long self);

        @JniMethod(flags={CPP_METHOD},cast = "size_t")
        public static final native long length (
                long self);

    }

    public NativeStdString(long self) {
        super(self);
    }

    public NativeStdString() {
        super(StdStringJNI.create());
    }

    public void delete() {
        assertAllocated();
        StdStringJNI.delete(self);
        self = 0;
    }

    public String toString() {
        return new String(toByteArray());
    }

    public long length() {
        assertAllocated();
        return StdStringJNI.length(self);
    }

    public byte[] toByteArray() {
        long l = length();
        if( l > Integer.MAX_VALUE ) {
            throw new ArrayIndexOutOfBoundsException("Native string is larger than the maximum Java array");
        }
        byte []rc = new byte[(int) l];
        NativeBuffer.NativeBufferJNI.buffer_copy(StdStringJNI.c_str_ptr(self), 0, rc, 0, rc.length);
        return rc;
    }
}
