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
import org.fusesource.hawtjni.runtime.JniField;
import org.fusesource.hawtjni.runtime.JniMethod;

import java.io.File;
import java.io.IOException;

import static org.fusesource.hawtjni.runtime.ClassFlag.CPP;
import static org.fusesource.hawtjni.runtime.FieldFlag.CONSTANT;
import static org.fusesource.hawtjni.runtime.MethodFlag.*;
import static org.fusesource.hawtjni.runtime.ArgFlag.*;

/**
 * Some miscellaneous utility functions.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Util {

    @JniClass(name="rocksdb::Env", flags={CPP})
    static class EnvJNI {

        static {
            NativeDB.LIBRARY.load();
        }

        @JniMethod(cast = "rocksdb::Env *", accessor = "rocksdb::Env::Default")
        public static final native long Default();

        @JniMethod(flags = {CPP_METHOD})
        public static final native void Schedule(
                long self,
                @JniArg(cast = "void (*)(void*)") long fp,
                @JniArg(cast = "void *") long arg);

    }

    @JniClass(flags={CPP})
    static class UtilJNI {

        static {
            NativeDB.LIBRARY.load();
            init();
        }

        @JniMethod(flags={CONSTANT_INITIALIZER})
        private static final native void init();

        @JniField(flags={CONSTANT}, accessor="1", conditional="defined(_WIN32) || defined(_WIN64)")
        static int ON_WINDOWS;


        @JniMethod(conditional="!defined(_WIN32) && !defined(_WIN64)")
        static final native int link(
                @JniArg(cast="const char*") String source,
                @JniArg(cast="const char*") String target);

        @JniMethod(conditional="defined(_WIN32) || defined(_WIN64)")
        static final native int CreateHardLinkW(
                @JniArg(cast="LPCTSTR", flags={POINTER_ARG, UNICODE}) String target,
                @JniArg(cast="LPCTSTR", flags={POINTER_ARG, UNICODE}) String source,
                @JniArg(cast="LPSECURITY_ATTRIBUTES", flags={POINTER_ARG}) long lpSecurityAttributes);

        @JniMethod(flags={CONSTANT_GETTER})
        public static final native int errno();

        @JniMethod(cast="char *")
        public static final native long strerror(int errnum);

        public static final native int strlen(
                @JniArg(cast="const char *")long s);

    }

    /**
     * Creates a hard link from source to target.
     * @param source
     * @param target
     * @return
     */
    public static void link(File source, File target) throws IOException {
        if( UtilJNI.ON_WINDOWS == 1 ) {
            if( UtilJNI.CreateHardLinkW(target.getCanonicalPath(), source.getCanonicalPath(), 0) == 0) {
                throw new IOException("link failed");
            }
        } else {
            if( UtilJNI.link(source.getCanonicalPath(), target.getCanonicalPath()) != 0 ) {
                throw new IOException("link failed: "+strerror());
            }
        }
    }

    static int errno() {
        return UtilJNI.errno();
    }

    static String strerror() {
        return string(UtilJNI.strerror(errno()));
    }

    static String string(long ptr) {
        if( ptr == 0 )
            return null;
        return new String(new NativeSlice(ptr, UtilJNI.strlen(ptr)).toByteArray());
    }

}
