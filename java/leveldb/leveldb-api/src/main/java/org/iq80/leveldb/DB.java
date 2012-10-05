/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb;

import java.io.Closeable;
import java.util.Map;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface DB extends Iterable<Map.Entry<byte[], byte[]>>, Closeable {

    public byte[] get(byte[] key) throws DBException;
    public byte[] get(byte[] key, ReadOptions options) throws DBException;

    public DBIterator iterator();
    public DBIterator iterator(ReadOptions options);

    public void put(byte[] key, byte[] value) throws DBException;
    public void delete(byte[] key) throws DBException;
    public void write(WriteBatch updates) throws DBException;

    public WriteBatch createWriteBatch();

    /**
     * @return null if options.isSnapshot()==false otherwise returns a snapshot
     *         of the DB after this operation.
     */
    public Snapshot put(byte[] key, byte[] value, WriteOptions options) throws DBException;

    /**
     * @return null if options.isSnapshot()==false otherwise returns a snapshot
     *         of the DB after this operation.
     */
    public Snapshot delete(byte[] key, WriteOptions options) throws DBException;

    /**
     * @return null if options.isSnapshot()==false otherwise returns a snapshot
     *         of the DB after this operation.
     */
    public Snapshot write(WriteBatch updates, WriteOptions options) throws DBException;

    public Snapshot getSnapshot();

    public long[] getApproximateSizes(Range ... ranges);
    public String getProperty(String name);
}
