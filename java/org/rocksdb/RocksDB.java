// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.lang.*;
import java.util.*;
import java.io.Closeable;
import java.io.IOException;

/**
 * A RocksDB is a persistent ordered map from keys to values.  It is safe for
 * concurrent access from multiple threads without any external synchronization.
 * All methods of this class could potentially throw RocksDBException, which
 * indicates sth wrong at the rocksdb library side and the call failed.
 */
public class RocksDB {
  public static final int NOT_FOUND = -1;
  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the default options w/ createIfMissing
   * set to true.
   *
   * @param path the path to the rocksdb.
   * @param status an out value indicating the status of the Open().
   * @return a rocksdb instance on success, null if the specified rocksdb can
   *     not be opened.
   *
   * @see Options.setCreateIfMissing()
   * @see Options.createIfMissing()
   */
  public static RocksDB open(String path) throws RocksDBException {
    RocksDB db = new RocksDB();
    db.open0(path);
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path.
   */
  public static RocksDB open(Options options, String path)
      throws RocksDBException {
    RocksDB db = new RocksDB();
    db.open(options.nativeHandle_, path);
    return db;
  }

  public synchronized void close() {
    if (nativeHandle_ != 0) {
      close0();
    }
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   */
  public void put(byte[] key, byte[] value) throws RocksDBException {
    put(key, key.length, value, value.length);
  }

  /**
   * Get the value associated with the specified key.
   *
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   */
  public int get(byte[] key, byte[] value) throws RocksDBException {
    return get(key, key.length, value, value.length);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @see RocksDBException
   */
  public byte[] get(byte[] key) throws RocksDBException {
    return get(key, key.length);
  }

  @Override protected void finalize() {
    close();
  }

  /**
   * Private constructor.
   */
  private RocksDB() {
    nativeHandle_ = 0;
  }

  // native methods
  private native void open0(String path) throws RocksDBException;
  private native void open(long optionsHandle, String path) throws RocksDBException;
  private native void put(
      byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  private native int get(
      byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  private native byte[] get(
      byte[] key, int keyLen) throws RocksDBException;
  private native void close0();

  private long nativeHandle_;
}
