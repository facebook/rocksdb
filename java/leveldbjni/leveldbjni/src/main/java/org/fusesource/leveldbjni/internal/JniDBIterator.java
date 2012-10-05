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

import org.fusesource.leveldbjni.internal.NativeDB;
import org.fusesource.leveldbjni.internal.NativeIterator;
import org.iq80.leveldb.DBIterator;

import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JniDBIterator implements DBIterator {

    private final NativeIterator iterator;

    JniDBIterator(NativeIterator iterator) {
        this.iterator = iterator;
    }

    public void close() {
        iterator.delete();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void seek(byte[] key) {
        try {
            iterator.seek(key);
        } catch (NativeDB.DBException e) {
            if( e.isNotFound() ) {
                throw new NoSuchElementException();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public void seekToFirst() {
        iterator.seekToFirst();
    }

    public void seekToLast() {
        iterator.seekToLast();
    }


    public Map.Entry<byte[], byte[]> peekNext() {
        if(!iterator.isValid()) {
            throw new NoSuchElementException();
        }
        try {
            return new AbstractMap.SimpleImmutableEntry<byte[],byte[]>(iterator.key(), iterator.value());
        } catch (NativeDB.DBException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNext() {
        return iterator.isValid();
    }

    public Map.Entry<byte[], byte[]> next() {
        Map.Entry<byte[], byte[]> rc = peekNext();
        try {
            iterator.next();
        } catch (NativeDB.DBException e) {
            throw new RuntimeException(e);
        }
        return rc;
    }

    public boolean hasPrev() {
        if( !iterator.isValid() )
            return false;
        try {
            iterator.prev();
            try {
                return iterator.isValid();
            } finally {
                iterator.next();
            }
        } catch (NativeDB.DBException e) {
            throw new RuntimeException(e);
        }
    }

    public Map.Entry<byte[], byte[]> peekPrev() {
        try {
            iterator.prev();
            try {
                return peekNext();
            } finally {
                iterator.next();
            }
        } catch (NativeDB.DBException e) {
            throw new RuntimeException(e);
        }
    }

    public Map.Entry<byte[], byte[]> prev() {
        Map.Entry<byte[], byte[]> rc = peekPrev();
        try {
            iterator.prev();
        } catch (NativeDB.DBException e) {
            throw new RuntimeException(e);
        }
        return rc;
    }


}
