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
import org.fusesource.hawtjni.runtime.Library;

import java.io.File;
import java.io.IOException;

import static org.fusesource.hawtjni.runtime.ArgFlag.*;
import static org.fusesource.hawtjni.runtime.ClassFlag.CPP;
import static org.fusesource.hawtjni.runtime.MethodFlag.*;

/**
 * The DB object provides the main interface to acessing LevelDB
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class NativeDB extends NativeObject {

    public static final Library LIBRARY = new Library("leveldbjni", NativeDB.class);

    @JniClass(name="leveldb::DB", flags={CPP})
    static class DBJNI {
        static {
            NativeDB.LIBRARY.load();
        }

        @JniMethod(flags={JNI, POINTER_RETURN}, cast="jobject")
        public static final native long NewGlobalRef(
                Object target);

        @JniMethod(flags={JNI}, cast="jobject")
        public static final native void DeleteGlobalRef(
                @JniArg(cast="jobject", flags={POINTER_ARG})
                long target);

        @JniMethod(flags={JNI, POINTER_RETURN}, cast="jmethodID")
        public static final native long GetMethodID(
                @JniArg(cast="jclass", flags={POINTER_ARG})
                Class clazz,
                String name,
                String signature);

        @JniMethod(flags={CPP_DELETE})
        static final native void delete(
                long self
                );

        @JniMethod(copy="leveldb::Status", accessor = "leveldb::DB::Open")
        static final native long Open(
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeOptions options,
                @JniArg(cast="const char*") String path,
                @JniArg(cast="leveldb::DB**") long[] self);

        @JniMethod(copy="leveldb::Status", flags={CPP_METHOD})
        static final native long Put(
                long self,
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeWriteOptions options,
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeSlice key,
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeSlice value
                );

        @JniMethod(copy="leveldb::Status", flags={CPP_METHOD})
        static final native long Delete(
                long self,
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeWriteOptions options,
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeSlice key
                );

        @JniMethod(copy="leveldb::Status", flags={CPP_METHOD})
        static final native long Write(
                long self,
                @JniArg(flags={BY_VALUE}) NativeWriteOptions options,
                @JniArg(cast="leveldb::WriteBatch *") long updates
                );

        @JniMethod(copy="leveldb::Status", flags={CPP_METHOD})
        static final native long Get(
                long self,
                @JniArg(flags={NO_OUT, BY_VALUE}) NativeReadOptions options,
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeSlice key,
                @JniArg(cast="std::string *") long value
                );

        @JniMethod(cast="leveldb::Iterator *", flags={CPP_METHOD})
        static final native long NewIterator(
                long self,
                @JniArg(flags={NO_OUT, BY_VALUE}) NativeReadOptions options
                );

        @JniMethod(cast="leveldb::Snapshot *", flags={CPP_METHOD})
        static final native long GetSnapshot(
                long self);

        @JniMethod(flags={CPP_METHOD})
        static final native void ReleaseSnapshot(
                long self,
                @JniArg(cast="const leveldb::Snapshot *") long snapshot
                );

        @JniMethod(flags={CPP_METHOD})
        static final native void GetApproximateSizes(
                long self,
                @JniArg(cast="const leveldb::Range *") long range,
                int n,
                @JniArg(cast="uint64_t*") long[] sizes
                );

        @JniMethod(flags={CPP_METHOD})
        static final native boolean GetProperty(
                long self,
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeSlice property,
                @JniArg(cast="std::string *") long value
                );

        @JniMethod(copy="leveldb::Status", accessor = "leveldb::DestroyDB")
        static final native long DestroyDB(
                @JniArg(cast="const char*") String path,
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeOptions options);

        @JniMethod(copy="leveldb::Status", accessor = "leveldb::RepairDB")
        static final native long RepairDB(
                @JniArg(cast="const char*") String path,
                @JniArg(flags={BY_VALUE, NO_OUT}) NativeOptions options);

        @JniMethod(flags={CPP_METHOD})
        static final native void CompactRange(
                long self,
                @JniArg(flags={NO_OUT}) NativeSlice begin,
                @JniArg(flags={NO_OUT}) NativeSlice end
                );

        @JniMethod(flags={CPP_METHOD})
        static final native void SuspendCompactions(long self);

        @JniMethod(flags={CPP_METHOD})
        static final native void ResumeCompactions(long self);
    }

    public void delete() {
        assertAllocated();
        DBJNI.delete(self);
        self = 0;
    }

    private NativeDB(long self) {
        super(self);
    }

    public static class DBException extends IOException {
        private final boolean notFound;

        DBException(String s, boolean notFound) {
            super(s);
            this.notFound = notFound;
        }

        public boolean isNotFound() {
            return notFound;
        }
    }

    static void checkStatus(long s) throws DBException {
        NativeStatus status = new NativeStatus(s);
        try {
            if( !status.isOk() ) {
                throw new DBException(status.toString(), status.isNotFound());
            }
        } finally {
            status.delete();
        }
    }

    static void checkArgNotNull(Object value, String name) {
        if(value==null) {
            throw new IllegalArgumentException("The "+name+" argument cannot be null");
        }
    }

    public static NativeDB open(NativeOptions options, File path) throws IOException, DBException {
        checkArgNotNull(options, "options");
        checkArgNotNull(path, "path");
        long rc[] = new long[1];
        try {
            checkStatus(DBJNI.Open(options, path.getCanonicalPath(), rc));
        } catch (IOException e) {
            if( rc[0]!=0 ) {
                DBJNI.delete(rc[0]);
            }
            throw e;
        }
        return new NativeDB(rc[0]);
    }

    public void suspendCompactions() {
        DBJNI.SuspendCompactions(self);
    }

    public void resumeCompactions() {
        DBJNI.ResumeCompactions(self);
    }

    public void put(NativeWriteOptions options, byte[] key, byte[] value) throws DBException {
        checkArgNotNull(options, "options");
        checkArgNotNull(key, "key");
        checkArgNotNull(value, "value");
        NativeBuffer keyBuffer = NativeBuffer.create(key);
        try {
            NativeBuffer valueBuffer = NativeBuffer.create(value);
            try {
                put(options, keyBuffer, valueBuffer);
            } finally {
                valueBuffer.delete();
            }
        } finally {
            keyBuffer.delete();
        }
    }

    private void put(NativeWriteOptions options, NativeBuffer keyBuffer, NativeBuffer valueBuffer) throws DBException {
        put(options, new NativeSlice(keyBuffer), new NativeSlice(valueBuffer));
    }

    private void put(NativeWriteOptions options, NativeSlice keySlice, NativeSlice valueSlice) throws DBException {
        assertAllocated();
        checkStatus(DBJNI.Put(self, options, keySlice, valueSlice));
    }

    public void delete(NativeWriteOptions options, byte[] key) throws DBException {
        checkArgNotNull(options, "options");
        checkArgNotNull(key, "key");
        NativeBuffer keyBuffer = NativeBuffer.create(key);
        try {
            delete(options, keyBuffer);
        } finally {
            keyBuffer.delete();
        }
    }

    private void delete(NativeWriteOptions options, NativeBuffer keyBuffer) throws DBException {
        delete(options, new NativeSlice(keyBuffer));
    }

    private void delete(NativeWriteOptions options, NativeSlice keySlice) throws DBException {
        assertAllocated();
        checkStatus(DBJNI.Delete(self, options, keySlice));
    }

    public void write(NativeWriteOptions options, NativeWriteBatch updates) throws DBException {
        checkArgNotNull(options, "options");
        checkArgNotNull(updates, "updates");
        checkStatus(DBJNI.Write(self, options, updates.pointer()));
    }

    public byte[] get(NativeReadOptions options, byte[] key) throws DBException {
        checkArgNotNull(options, "options");
        checkArgNotNull(key, "key");
        NativeBuffer keyBuffer = NativeBuffer.create(key);
        try {
            return get(options, keyBuffer);
        } finally {
            keyBuffer.delete();
        }
    }

    private byte[] get(NativeReadOptions options, NativeBuffer keyBuffer) throws DBException {
        return get(options, new NativeSlice(keyBuffer));
    }

    private byte[] get(NativeReadOptions options, NativeSlice keySlice) throws DBException {
        assertAllocated();
        NativeStdString result = new NativeStdString();
        try {
            checkStatus(DBJNI.Get(self, options, keySlice, result.pointer()));
            return result.toByteArray();
        } finally {
            result.delete();
        }
    }

    public NativeSnapshot getSnapshot() {
        return new NativeSnapshot(DBJNI.GetSnapshot(self));
    }

    public void releaseSnapshot(NativeSnapshot snapshot) {
        checkArgNotNull(snapshot, "snapshot");
        DBJNI.ReleaseSnapshot(self, snapshot.pointer());
    }

    public NativeIterator iterator(NativeReadOptions options) {
        checkArgNotNull(options, "options");
        return new NativeIterator(DBJNI.NewIterator(self, options));
    }

    public long[] getApproximateSizes(NativeRange... ranges) {
        if( ranges==null ) {
            return null;
        }

        long rc[] = new long[ranges.length];
        NativeRange.RangeJNI structs[] = new NativeRange.RangeJNI[ranges.length];
        if( rc.length> 0 ) {
            NativeBuffer range_array = NativeRange.RangeJNI.arrayCreate(ranges.length);
            try {
                for(int i=0; i < ranges.length; i++) {
                    structs[i] = new NativeRange.RangeJNI(ranges[i]);
                    structs[i].arrayWrite(range_array.pointer(), i);
                }
                DBJNI.GetApproximateSizes(self,range_array.pointer(), ranges.length, rc);
            } finally {
                for(int i=0; i < ranges.length; i++) {
                    if( structs[i] != null ) {
                        structs[i].delete();
                    }
                }
                range_array.delete();
            }
        }
        return rc;
    }

    public String getProperty(String name) {
        checkArgNotNull(name, "name");
        NativeBuffer keyBuffer = NativeBuffer.create(name.getBytes());
        try {
            byte[] property = getProperty(keyBuffer);
            if( property==null ) {
                return null;
            } else {
                return new String(property);
            }
        } finally {
            keyBuffer.delete();
        }
    }

    private byte[] getProperty(NativeBuffer nameBuffer) {
        return getProperty(new NativeSlice(nameBuffer));
    }

    private byte[] getProperty(NativeSlice nameSlice) {
        assertAllocated();
        NativeStdString result = new NativeStdString();
        try {
            if( DBJNI.GetProperty(self, nameSlice, result.pointer()) ) {
                return result.toByteArray();
            } else {
                return null;
            }
        } finally {
            result.delete();
        }
    }

    public void compactRange(byte[] begin, byte[] end) {
        NativeBuffer keyBuffer = NativeBuffer.create(begin);
        try {
            NativeBuffer valueBuffer = NativeBuffer.create(end);
            try {
                compactRange(keyBuffer, valueBuffer);
            } finally {
                if( valueBuffer!=null ) {
                    valueBuffer.delete();
                }
            }
        } finally {
            if( keyBuffer!=null ) {
                keyBuffer.delete();
            }
        }
    }

    private void compactRange( NativeBuffer beginBuffer, NativeBuffer endBuffer) {
        compactRange(NativeSlice.create(beginBuffer), NativeSlice.create(endBuffer));
    }

    private void compactRange( NativeSlice beginSlice, NativeSlice endSlice) {
        assertAllocated();
        DBJNI.CompactRange(self, beginSlice, endSlice);
    }


    static public void destroy(File path, NativeOptions options) throws IOException, DBException {
        checkArgNotNull(options, "options");
        checkArgNotNull(path, "path");
        checkStatus(DBJNI.DestroyDB(path.getCanonicalPath(), options));
    }

    static public void repair(File path, NativeOptions options) throws IOException, DBException {
        checkArgNotNull(options, "options");
        checkArgNotNull(path, "path");
        checkStatus(DBJNI.RepairDB(path.getCanonicalPath(), options));
    }
}
