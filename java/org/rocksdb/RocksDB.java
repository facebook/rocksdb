// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.*;
import java.io.IOException;
import org.rocksdb.util.Environment;

/**
 * A RocksDB is a persistent ordered map from keys to values.  It is safe for
 * concurrent access from multiple threads without any external synchronization.
 * All methods of this class could potentially throw RocksDBException, which
 * indicates sth wrong at the RocksDB library side and the call failed.
 */
public class RocksDB extends RocksObject {
  public static final int NOT_FOUND = -1;
  private static final String[] compressionLibs_ = {
      "snappy", "z", "bzip2", "lz4", "lz4hc"};

  static {
    RocksDB.loadLibrary();
  }

  /**
   * Loads the necessary library files.
   * Calling this method twice will have no effect.
   * By default the method extracts the shared library for loading at
   * java.io.tmpdir, however, you can override this temporary location by
   * setting the environment variable ROCKSDB_SHAREDLIB_DIR.
   */
  public static synchronized void loadLibrary() {
    String tmpDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
    // loading possibly necessary libraries.
    for (String lib : compressionLibs_) {
      try {
      System.loadLibrary(lib);
      } catch (UnsatisfiedLinkError e) {
        // since it may be optional, we ignore its loading failure here.
      }
    }
    try
    {
      NativeLibraryLoader.loadLibraryFromJar(tmpDir);
    }
    catch (IOException e)
    {
      throw new RuntimeException("Unable to load the RocksDB shared library" + e);
    }
  }

  /**
   * Tries to load the necessary library files from the given list of
   * directories.
   *
   * @param paths a list of strings where each describes a directory
   *     of a library.
   */
  public static synchronized void loadLibrary(List<String> paths) {
    for (String lib : compressionLibs_) {
      for (String path : paths) {
        try {
          System.load(path + "/" + Environment.getSharedLibraryName(lib));
          break;
        } catch (UnsatisfiedLinkError e) {
          // since they are optional, we ignore loading fails.
        }
      }
    }
    boolean success = false;
    UnsatisfiedLinkError err = null;
    for (String path : paths) {
      try {
        System.load(path + "/" + Environment.getJniLibraryName("rocksdbjni"));
        success = true;
        break;
      } catch (UnsatisfiedLinkError e) {
        err = e;
      }
    }
    if (!success) {
      throw err;
    }
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the default options w/ createIfMissing
   * set to true.
   *
   * @param path the path to the rocksdb.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws org.rocksdb.RocksDBException
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(String path) throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    Options options = new Options();
    return open(options, path);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path and a list
   * of column family names.
   * <p>
   * If opened in read write mode every existing column family name must be passed
   * within the list to this method.</p>
   * <p>
   * If opened in read-only mode only a subset of existing column families must
   * be passed to this method.</p>
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically</p>
   * <p>
   * ColumnFamily handles are disposed when the RocksDB instance is disposed.
   * </p>
   *
   * @param path the path to the rocksdb.
   * @param columnFamilyNames list of column family names
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws org.rocksdb.RocksDBException
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(String path, List<String> columnFamilyNames,
      List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    Options options = new Options();
    return open(options, path, columnFamilyNames, columnFamilyHandles);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path.
   *
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.</p>
   * <p>
   * Options instance can be re-used to open multiple DBs if DB statistics is
   * not used. If DB statistics are required, then its recommended to open DB
   * with new Options instance as underlying native statistics instance does not
   * use any locks to prevent concurrent updates.</p>
   *
   * @param options {@link org.rocksdb.Options} instance.
   * @param path the path to the rocksdb.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws org.rocksdb.RocksDBException
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(Options options, String path)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    RocksDB db = new RocksDB();
    db.open(options.nativeHandle_, path);

    db.storeOptionsInstance(options);
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path and a list
   * of column family names.
   * <p>
   * If opened in read write mode every existing column family name must be passed
   * within the list to this method.</p>
   * <p>
   * If opened in read-only mode only a subset of existing column families must
   * be passed to this method.</p>
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.</p>
   * <p>
   * Options instance can be re-used to open multiple DBs if DB statistics is
   * not used. If DB statistics are required, then its recommended to open DB
   * with new Options instance as underlying native statistics instance does not
   * use any locks to prevent concurrent updates.</p>
   * <p>
   * ColumnFamily handles are disposed when the RocksDB instance is disposed.</p>
   *
   * @param options {@link org.rocksdb.Options} instance.
   * @param path the path to the rocksdb.
   * @param columnFamilyNames list of column family names
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws org.rocksdb.RocksDBException
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(Options options, String path, List<String> columnFamilyNames,
      List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    RocksDB db = new RocksDB();
    List<Long> cfReferences = db.open(options.nativeHandle_, path,
        columnFamilyNames, columnFamilyNames.size());
    for (int i=0; i<columnFamilyNames.size(); i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(cfReferences.get(i)));
    }
    db.storeOptionsInstance(options);
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the default
   * options.
   *
   * @param path the path to the RocksDB.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   * @throws RocksDBException
   */
  public static RocksDB openReadOnly(String path)
      throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    Options options = new Options();
    return openReadOnly(options, path);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the default
   * options.
   *
   * @param path the path to the RocksDB.
   * @param columnFamilyNames list of column family names
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   * @throws RocksDBException
   */
  public static RocksDB openReadOnly(String path, List<String> columnFamilyNames,
      List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    Options options = new Options();
    return openReadOnly(options, path, columnFamilyNames, columnFamilyHandles);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the specified
   * options and db path.
   *
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.
   *
   * @param options {@link Options} instance.
   * @param path the path to the RocksDB.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   * @throws RocksDBException
   */
  public static RocksDB openReadOnly(Options options, String path)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    RocksDB db = new RocksDB();
    db.openROnly(options.nativeHandle_, path);

    db.storeOptionsInstance(options);
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the specified
   * options and db path.
   *
   * <p>This open method allows to open RocksDB using a subset of available
   * column families</p>
   * <p>Options instance *should* not be disposed before all DBs using this
   * options instance have been closed. If user doesn't call options dispose
   * explicitly,then this options instance will be GC'd automatically.</p>
   *
   * @param options {@link Options} instance.
   * @param path the path to the RocksDB.
   * @param columnFamilyNames list of column family names
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   * @throws RocksDBException
   */
  public static RocksDB openReadOnly(Options options, String path,
      List<String> columnFamilyNames, List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    RocksDB db = new RocksDB();
    List<Long> cfReferences = db.openROnly(options.nativeHandle_, path,
        columnFamilyNames, columnFamilyNames.size());
    for (int i=0; i<columnFamilyNames.size(); i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(cfReferences.get(i)));
    }

    db.storeOptionsInstance(options);
    return db;
  }
  /**
   * Static method to determine all available column families for a
   * rocksdb database identified by path
   *
   * @param options Options for opening the database
   * @param path Absolute path to rocksdb database
   * @return List<byte[]> List containing the column family names
   *
   * @throws RocksDBException
   */
  public static List<byte[]> listColumnFamilies(Options options, String path)
      throws RocksDBException {
    return RocksDB.listColumnFamilies(options.nativeHandle_, path);
  }

  private void storeOptionsInstance(Options options) {
    options_ = options;
  }

  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  /**
   * Close the RocksDB instance.
   * This function is equivalent to dispose().
   */
  public void close() {
    dispose();
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @see RocksDBException
   */
  public void put(byte[] key, byte[] value) throws RocksDBException {
    put(nativeHandle_, key, key.length, value, value.length);
  }

  /**
   * Set the database entry for "key" to "value" in the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * throws IllegalArgumentException if column family is not present
   *
   * @see RocksDBException
   */
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key,
      byte[] value) throws RocksDBException {
    put(nativeHandle_, key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @see RocksDBException
   */
  public void put(WriteOptions writeOpts, byte[] key, byte[] value)
      throws RocksDBException {
    put(nativeHandle_, writeOpts.nativeHandle_,
        key, key.length, value, value.length);
  }

  /**
   * Set the database entry for "key" to "value" for the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * throws IllegalArgumentException if column family is not present
   *
   * @see RocksDBException
   * @see IllegalArgumentException
   */
  public void put(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpts,
      byte[] key, byte[] value) throws RocksDBException {
    put(nativeHandle_, writeOpts.nativeHandle_, key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instnace
   * @param key byte array of a key to search for
   * @param value StringBuffer instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(ColumnFamilyHandle columnFamilyHandle,
      byte[] key, StringBuffer value){
    return keyMayExist(key, key.length, columnFamilyHandle.nativeHandle_,
        value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instnace
   * @param key byte array of a key to search for
   * @param value StringBuffer instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(ReadOptions readOptions,
      ColumnFamilyHandle columnFamilyHandle, byte[] key, StringBuffer value){
    return keyMayExist(readOptions.nativeHandle_,
        key, key.length, columnFamilyHandle.nativeHandle_,
        value);
  }

  /**
   * Apply the specified updates to the database.
   *
   * @param writeOpts WriteOptions instance
   * @param updates WriteBatch instance
   *
   * @see RocksDBException
   */
  public void write(WriteOptions writeOpts, WriteBatch updates)
      throws RocksDBException {
    write(writeOpts.nativeHandle_, updates.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key within column family
   *
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @see RocksDBException
   */
  public int get(byte[] key, byte[] value) throws RocksDBException {
    return get(nativeHandle_, key, key.length, value, value.length);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException
   */
  public int get(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
      throws RocksDBException, IllegalArgumentException {
    return get(nativeHandle_, key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
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
   *
   * @throws RocksDBException
   */
  public int get(ReadOptions opt, byte[] key, byte[] value)
      throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_,
               key, key.length, value, value.length);
  }
  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException
   */
  public int get(ColumnFamilyHandle columnFamilyHandle, ReadOptions opt, byte[] key,
      byte[] value) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, key.length, value,
        value.length, columnFamilyHandle.nativeHandle_);
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
   * @throws RocksDBException
   */
  public byte[] get(byte[] key) throws RocksDBException {
    return get(nativeHandle_, key, key.length);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException
   */
  public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key)
      throws RocksDBException {
    return get(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException
   */
  public byte[] get(ReadOptions opt, byte[] key) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, key.length);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException
   */
  public byte[] get(ColumnFamilyHandle columnFamilyHandle, ReadOptions opt,
      byte[] key) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Returns a map of keys for which values were found in DB.
   *
   * @param keys List of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   * entry is the corresponding value in DB.
   *
   * @throws RocksDBException
   */
  public Map<byte[], byte[]> multiGet(List<byte[]> keys)
      throws RocksDBException {
    assert(keys.size() != 0);

    List<byte[]> values = multiGet(
        nativeHandle_, keys, keys.size());

    Map<byte[], byte[]> keyValueMap = new HashMap<byte[], byte[]>();
    for(int i = 0; i < values.size(); i++) {
      if(values.get(i) == null) {
        continue;
      }

      keyValueMap.put(keys.get(i), values.get(i));
    }

    return keyValueMap;
  }

  /**
   * Returns a map of keys for which values were found in DB.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys List of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   * entry is the corresponding value in DB.
   *
   * @throws RocksDBException
   * @throws IllegalArgumentException
   */
  public Map<byte[], byte[]> multiGet(List<ColumnFamilyHandle> columnFamilyHandleList,
      List<byte[]> keys) throws RocksDBException, IllegalArgumentException {
    assert(keys.size() != 0);
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size()!=columnFamilyHandleList.size()) {
        throw new IllegalArgumentException(
            "For each key there must be a ColumnFamilyHandle.");
    }
    List<byte[]> values = multiGet(nativeHandle_, keys, keys.size(),
        columnFamilyHandleList);

    Map<byte[], byte[]> keyValueMap = new HashMap<byte[], byte[]>();
    for(int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        continue;
      }
      keyValueMap.put(keys.get(i), values.get(i));
    }
    return keyValueMap;
  }

  /**
   * Returns a map of keys for which values were found in DB.
   *
   * @param opt Read options.
   * @param keys of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   * entry is the corresponding value in DB.
   *
   * @throws RocksDBException
   */
  public Map<byte[], byte[]> multiGet(ReadOptions opt, List<byte[]> keys)
      throws RocksDBException {
    assert(keys.size() != 0);

    List<byte[]> values = multiGet(
        nativeHandle_, opt.nativeHandle_, keys, keys.size());

    Map<byte[], byte[]> keyValueMap = new HashMap<byte[], byte[]>();
    for(int i = 0; i < values.size(); i++) {
      if(values.get(i) == null) {
        continue;
      }

      keyValueMap.put(keys.get(i), values.get(i));
    }

    return keyValueMap;
  }

  /**
   * Returns a map of keys for which values were found in DB.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param opt Read options.
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   * entry is the corresponding value in DB.
   *
   * @throws RocksDBException
   * @throws java.lang.IllegalArgumentException
   */
  public Map<byte[], byte[]> multiGet(ReadOptions opt,
      List<ColumnFamilyHandle> columnFamilyHandleList, List<byte[]> keys)
      throws RocksDBException {
    assert(keys.size() != 0);
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size()!=columnFamilyHandleList.size()){
      throw new IllegalArgumentException(
          "For each key there must be a ColumnFamilyHandle.");
    }

    List<byte[]> values = multiGet(nativeHandle_, opt.nativeHandle_,
        keys, keys.size(), columnFamilyHandleList);

    Map<byte[], byte[]> keyValueMap = new HashMap<byte[], byte[]>();
    for(int i = 0; i < values.size(); i++) {
      if(values.get(i) == null) {
        continue;
      }
      keyValueMap.put(keys.get(i), values.get(i));
    }

    return keyValueMap;
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param key Key to delete within database
   *
   * @throws RocksDBException
   */
  public void remove(byte[] key) throws RocksDBException {
    remove(nativeHandle_, key, key.length);
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key Key to delete within database
   *
   * @throws RocksDBException
   */
  public void remove(ColumnFamilyHandle columnFamilyHandle, byte[] key)
      throws RocksDBException {
    remove(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException
   */
  public void remove(WriteOptions writeOpt, byte[] key)
      throws RocksDBException {
    remove(nativeHandle_, writeOpt.nativeHandle_, key, key.length);
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException
   */
  public void remove(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpt,
      byte[] key) throws RocksDBException {
    remove(nativeHandle_, writeOpt.nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * DB implements can export properties about their state
   * via this method on a per column family level.
   *
   * <p>If {@code property} is a valid property understood by this DB
   * implementation, fills {@code value} with its current value and
   * returns true. Otherwise returns false.</p>
   *
   * <p>Valid property names include:
   * <ul>
   * <li>"rocksdb.num-files-at-level<N>" - return the number of files at level <N>,
   *     where <N> is an ASCII representation of a level number (e.g. "0").</li>
   * <li>"rocksdb.stats" - returns a multi-line string that describes statistics
   *     about the internal operation of the DB.</li>
   * <li>"rocksdb.sstables" - returns a multi-line string that describes all
   *    of the sstables that make up the db contents.</li>
   *</ul></p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param property to be fetched. See above for examples
   * @return property value
   *
   * @throws RocksDBException
   */
  public String getProperty(ColumnFamilyHandle columnFamilyHandle, String property)
      throws RocksDBException {
    return getProperty0(nativeHandle_, columnFamilyHandle.nativeHandle_, property,
        property.length());
  }

  /**
   * DB implementations can export properties about their state
   * via this method.  If "property" is a valid property understood by this
   * DB implementation, fills "*value" with its current value and returns
   * true.  Otherwise returns false.
   *
   * <p>Valid property names include:
   * <ul>
   * <li>"rocksdb.num-files-at-level<N>" - return the number of files at level <N>,
   *     where <N> is an ASCII representation of a level number (e.g. "0").</li>
   * <li>"rocksdb.stats" - returns a multi-line string that describes statistics
   *     about the internal operation of the DB.</li>
   * <li>"rocksdb.sstables" - returns a multi-line string that describes all
   *    of the sstables that make up the db contents.</li>
   *</ul></p>
   *
   * @param property to be fetched. See above for examples
   * @return property value
   *
   * @throws RocksDBException
   */
  public String getProperty(String property) throws RocksDBException {
    return getProperty0(nativeHandle_, property, property.length());
  }

  /**
   * Return a heap-allocated iterator over the contents of the database.
   * The result of newIterator() is initially invalid (caller must
   * call one of the Seek methods on the iterator before using it).
   *
   * Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   *
   * @return instance of iterator object.
   */
  public RocksIterator newIterator() {
    return new RocksIterator(iterator0(nativeHandle_));
  }

  /**
   * Return a heap-allocated iterator over the contents of the database.
   * The result of newIterator() is initially invalid (caller must
   * call one of the Seek methods on the iterator before using it).
   *
   * Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(ColumnFamilyHandle columnFamilyHandle) {
    return new RocksIterator(iterator0(nativeHandle_, columnFamilyHandle.nativeHandle_));
  }

  /**
   * Returns iterators from a consistent database state across multiple
   * column families. Iterators are heap allocated and need to be deleted
   * before the db is deleted
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @return {@link java.util.List} containing {@link org.rocksdb.RocksIterator}
   *     instances
   *
   * @throws RocksDBException
   */
  public List<RocksIterator> newIterators(
      List<ColumnFamilyHandle> columnFamilyHandleList) throws RocksDBException {
    List<RocksIterator> iterators =
        new ArrayList<RocksIterator>(columnFamilyHandleList.size());

    long[] iteratorRefs = iterators(nativeHandle_, columnFamilyHandleList);
    for (int i=0; i<columnFamilyHandleList.size(); i++){
      iterators.add(new RocksIterator(iteratorRefs[i]));
    }
    return iterators;
  }

  /**
   * Creates a new column family with the name columnFamilyName and
   * allocates a ColumnFamilyHandle within an internal structure.
   * The ColumnFamilyHandle is automatically disposed with DB disposal.
   *
   * @param columnFamilyName Name of column family to be created.
   * @return {@link org.rocksdb.ColumnFamilyHandle} instance
   * @see RocksDBException
   */
  public ColumnFamilyHandle createColumnFamily(String columnFamilyName)
      throws RocksDBException {
    return new ColumnFamilyHandle(createColumnFamily(nativeHandle_,
        columnFamilyName));
  }

  /**
   * Drops the column family identified by columnFamilyName. Internal
   * handles to this column family will be disposed. If the column family
   * is not known removal will fail.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   *
   * @throws RocksDBException
   */
  public void dropColumnFamily(ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException, IllegalArgumentException {
    // throws RocksDBException if something goes wrong
    dropColumnFamily(nativeHandle_, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Private constructor.
   */
  protected RocksDB() {
    super();
  }

  // native methods
  protected native void open(
      long optionsHandle, String path) throws RocksDBException;
  protected native List<Long> open(long optionsHandle, String path,
      List<String> columnFamilyNames, int columnFamilyNamesLength)
      throws RocksDBException;
  protected native static List<byte[]> listColumnFamilies(
      long optionsHandle, String path) throws RocksDBException;
  protected native void openROnly(
      long optionsHandle, String path) throws RocksDBException;
  protected native List<Long> openROnly(
      long optionsHandle, String path, List<String> columnFamilyNames,
      int columnFamilyNamesLength) throws RocksDBException;
  protected native void put(
      long handle, byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  protected native void put(
      long handle, byte[] key, int keyLen,
      byte[] value, int valueLen, long cfHandle) throws RocksDBException;
  protected native void put(
      long handle, long writeOptHandle,
      byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  protected native void put(
      long handle, long writeOptHandle,
      byte[] key, int keyLen,
      byte[] value, int valueLen, long cfHandle) throws RocksDBException;
  protected native void write(
      long writeOptHandle, long batchHandle) throws RocksDBException;
  protected native boolean keyMayExist(byte[] key, int keyLen,
      long cfHandle, StringBuffer stringBuffer);
  protected native boolean keyMayExist(long optionsHandle, byte[] key, int keyLen,
      long cfHandle, StringBuffer stringBuffer);
  protected native int get(
      long handle, byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  protected native int get(
      long handle, byte[] key, int keyLen,
      byte[] value, int valueLen, long cfHandle) throws RocksDBException;
  protected native int get(
      long handle, long readOptHandle, byte[] key, int keyLen,
      byte[] value, int valueLen) throws RocksDBException;
  protected native int get(
      long handle, long readOptHandle, byte[] key, int keyLen,
      byte[] value, int valueLen, long cfHandle) throws RocksDBException;
  protected native List<byte[]> multiGet(
      long dbHandle, List<byte[]> keys, int keysCount);
  protected native List<byte[]> multiGet(
      long dbHandle, List<byte[]> keys, int keysCount, List<ColumnFamilyHandle>
      cfHandles);
  protected native List<byte[]> multiGet(
      long dbHandle, long rOptHandle, List<byte[]> keys, int keysCount);
  protected native List<byte[]> multiGet(
      long dbHandle, long rOptHandle, List<byte[]> keys, int keysCount,
      List<ColumnFamilyHandle> cfHandles);
  protected native byte[] get(
      long handle, byte[] key, int keyLen) throws RocksDBException;
  protected native byte[] get(
      long handle, byte[] key, int keyLen, long cfHandle) throws RocksDBException;
  protected native byte[] get(
      long handle, long readOptHandle,
      byte[] key, int keyLen) throws RocksDBException;
  protected native byte[] get(
      long handle, long readOptHandle,
      byte[] key, int keyLen, long cfHandle) throws RocksDBException;
  protected native void remove(
      long handle, byte[] key, int keyLen) throws RocksDBException;
  protected native void remove(
      long handle, byte[] key, int keyLen, long cfHandle) throws RocksDBException;
  protected native void remove(
      long handle, long writeOptHandle,
      byte[] key, int keyLen) throws RocksDBException;
  protected native void remove(
      long handle, long writeOptHandle,
      byte[] key, int keyLen, long cfHandle) throws RocksDBException;
  protected native String getProperty0(long nativeHandle,
      String property, int propertyLength) throws RocksDBException;
  protected native String getProperty0(long nativeHandle, long cfHandle,
      String property, int propertyLength) throws RocksDBException;
  protected native long iterator0(long handle);
  protected native long iterator0(long handle, long cfHandle);
  protected native long[] iterators(long handle,
      List<ColumnFamilyHandle> columnFamilyNames) throws RocksDBException;
  private native void disposeInternal(long handle);

  private native long createColumnFamily(long handle, String name) throws RocksDBException;
  private native void dropColumnFamily(long handle, long cfHandle) throws RocksDBException;

  protected Options options_;
}
