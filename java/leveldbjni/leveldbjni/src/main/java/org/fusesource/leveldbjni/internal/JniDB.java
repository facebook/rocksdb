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

import org.iq80.leveldb.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JniDB implements DB {

    private final NativeDB db;
    private final NativeCache cache;
    private final NativeComparator comparator;
    private final NativeLogger logger;

    public JniDB(NativeDB db, NativeCache cache, NativeComparator comparator, NativeLogger logger) {
        this.db = db;
        this.cache = cache;
        this.comparator = comparator;
        this.logger = logger;
    }

    public void close() {
        db.delete();
        if(cache!=null) {
            cache.delete();
        }
        if(comparator!=null){
            comparator.delete();
        }
        if(logger!=null) {
            logger.delete();
        }
    }


    public byte[] get(byte[] key) throws DBException {
        return get(key, new ReadOptions());
    }

    public byte[] get(byte[] key, ReadOptions options) throws DBException {
        try {
            return db.get(convert(options), key);
        } catch (NativeDB.DBException e) {
            if(e.isNotFound()) {
                return null;
            }
            throw new DBException(e.getMessage(), e);
        }
    }

    public DBIterator iterator() {
        return iterator(new ReadOptions());
    }

    public DBIterator iterator(ReadOptions options) {
        return new JniDBIterator(db.iterator(convert(options)));
    }

    public void put(byte[] key, byte[] value) throws DBException {
        put(key, value, new WriteOptions());
    }

    public void delete(byte[] key) throws DBException {
        delete(key, new WriteOptions());
    }

    public void write(WriteBatch updates) throws DBException {
        write(updates, new WriteOptions());
    }

    public WriteBatch createWriteBatch() {
        return new JniWriteBatch(new NativeWriteBatch());
    }

    public Snapshot put(byte[] key, byte[] value, WriteOptions options) throws DBException {
        try {
            db.put(convert(options), key, value);
            return null;
        } catch (NativeDB.DBException e) {
            throw new DBException(e.getMessage(), e);
        }
    }

    public Snapshot delete(byte[] key, WriteOptions options) throws DBException {
        try {
            db.delete(convert(options), key);
            return null;
        } catch (NativeDB.DBException e) {
            throw new DBException(e.getMessage(), e);
        }
    }

    public Snapshot write(WriteBatch updates, WriteOptions options) throws DBException {
        try {
            db.write(convert(options), ((JniWriteBatch) updates).writeBatch());
            return null;
        } catch (NativeDB.DBException e) {
            throw new DBException(e.getMessage(), e);
        }
    }

    public Snapshot getSnapshot() {
        return new JniSnapshot(db, db.getSnapshot());
    }

    public long[] getApproximateSizes(Range... ranges) {
        NativeRange args[] = new NativeRange[ranges.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = new NativeRange(ranges[i].start(), ranges[i].limit());
        }
        return db.getApproximateSizes(args);
    }

    public String getProperty(String name) {
        return db.getProperty(name);
    }

    private NativeReadOptions convert(ReadOptions options) {
        if(options==null) {
            return null;
        }
        NativeReadOptions rc = new NativeReadOptions();
        rc.fillCache(options.fillCache());
        rc.verifyChecksums(options.verifyChecksums());
        if(options.snapshot()!=null) {
            rc.snapshot(((JniSnapshot) options.snapshot()).snapshot());
        }
        return rc;
    }

    private NativeWriteOptions convert(WriteOptions options) {
        if(options==null) {
            return null;
        }
        NativeWriteOptions rc = new NativeWriteOptions();
        rc.sync(options.sync());
        if(options.snapshot()) {
            throw new UnsupportedOperationException("WriteOptions snapshot not supported");
        }
        return rc;
    }

    public void compactRange(byte[] begin, byte[] end) throws DBException {
        db.compactRange(begin, end);
    }

//
//  Using a fork of leveldb with db Suspend / Resume methods to avoid
//  having to callback into java.
//
    public void suspendCompactions() throws InterruptedException {
        db.suspendCompactions();
    }
    public void resumeCompactions() {
        db.resumeCompactions();
    }

//    private static class Suspension {
//        static long env = Util.EnvJNI.Default();
//
//        CountDownLatch suspended = new CountDownLatch(1);
//        CountDownLatch resumed = new CountDownLatch(1);
//        Callback callback = new Callback(this, "suspended", 1);
//
//        public Suspension() {
//            Util.EnvJNI.Schedule(env, callback.getAddress(), 0);
//        }
//
//        private long suspended(long arg) {
//            suspended.countDown();
//            try {
//                resumed.await();
//            } catch (InterruptedException e) {
//            } finally {
//                callback.dispose();
//            }
//            return 0;
//        }
//    }
//
//    int suspendCounter = 0;
//    Suspension suspension = null;
//
//    public void suspendCompactions() throws InterruptedException {
//        Suspension s = null;
//        synchronized (this) {
//            suspendCounter++;
//            if( suspendCounter==1 ) {
//                suspension = new Suspension();
//            }
//            s = suspension;
//        }
//        // Don't return until the compactions have suspended.
//        s.suspended.await();
//    }
//
//    synchronized public void resumeCompactions() {
//        suspendCounter--;
//        if( suspendCounter==0 ) {
//            suspension.resumed.countDown();
//            suspension = null;
//        }
//    }
}
