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
package org.fusesource.leveldbjni;

import org.fusesource.leveldbjni.internal.*;
import org.iq80.leveldb.*;

import java.io.*;
import java.net.URL;
import java.util.concurrent.Callable;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JniDBFactory implements DBFactory {

    public static final JniDBFactory factory = new JniDBFactory();
    static {
        NativeDB.LIBRARY.load();
    }

    public static final String VERSION;
    static {
        String v="unknown";
        InputStream is = JniDBFactory.class.getResourceAsStream("version.txt");
        try {
            v = new BufferedReader(new InputStreamReader(is, "UTF-8")).readLine();
        } catch (Throwable e) {
        } finally {
            try {
                is.close();
            } catch (Throwable e) {
            }
        }
        VERSION = v;
    }

    public static byte[] bytes(String value) {
        if( value == null) {
            return null;
        }
        try {
            return value.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String asString(byte value[]) {
        if( value == null) {
            return null;
        }
        try {
            return new String(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    static private class OptionsResourceHolder {

        NativeCache cache = null;
        NativeComparator comparator=null;
        NativeLogger logger=null;
        NativeOptions options;

        public void init(Options value) {

            options = new NativeOptions();
            options.blockRestartInterval(value.blockRestartInterval());
            options.blockSize(value.blockSize());
            options.createIfMissing(value.createIfMissing());
            options.errorIfExists(value.errorIfExists());
            options.maxOpenFiles(value.maxOpenFiles());
            options.paranoidChecks(value.paranoidChecks());
            options.writeBufferSize(value.writeBufferSize());

            switch(value.compressionType()) {
                case NONE:
                    options.compression(NativeCompressionType.kNoCompression);
                    break;
                case SNAPPY:
                    options.compression(NativeCompressionType.kSnappyCompression);
                    break;
            }


            if(value.cacheSize()>0 ) {
                cache = new NativeCache(value.cacheSize());
                options.cache(cache);
            }

            final DBComparator userComparator = value.comparator();
            if(userComparator!=null) {
                comparator = new NativeComparator() {
                    @Override
                    public int compare(byte[] key1, byte[] key2) {
                        return userComparator.compare(key1, key2);
                    }

                    @Override
                    public String name() {
                        return userComparator.name();
                    }
                };
                options.comparator(comparator);
            }

            final Logger userLogger = value.logger();
            if(userLogger!=null) {
                logger = new NativeLogger() {
                    @Override
                    public void log(String message) {
                        userLogger.log(message);
                    }
                };
                options.infoLog(logger);
            }

        }
        public void close() {
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
    }

    public DB open(File path, Options options) throws IOException {
        NativeDB db=null;
        OptionsResourceHolder holder = new OptionsResourceHolder();
        try {
            holder.init(options);
            db = NativeDB.open(holder.options, path);
        } finally {
            // if we could not open up the DB, then clean up the
            // other allocated native resouces..
            if(db==null) {
                holder.close();
            }
        }
        return new JniDB(db, holder.cache, holder.comparator, holder.logger);
    }

    public void destroy(File path, Options options) throws IOException {
        OptionsResourceHolder holder = new OptionsResourceHolder();
        try {
            holder.init(options);
            NativeDB.destroy(path, holder.options);
        } finally {
            holder.close();
        }
    }

    public void repair(File path, Options options) throws IOException {
        OptionsResourceHolder holder = new OptionsResourceHolder();
        try {
            holder.init(options);
            NativeDB.repair(path, holder.options);
        } finally {
            holder.close();
        }
    }

    @Override
    public String toString() {
        return String.format("leveldbjni version %s", VERSION);
    }


    public static void pushMemoryPool(int size) {
        NativeBuffer.pushMemoryPool(size);
    }

    public static void popMemoryPool() {
        NativeBuffer.popMemoryPool();
    }
}
