// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.rocksdb.util.Environment;

/**
 * A RocksDB is a persistent ordered map from keys to values.  It is safe for
 * concurrent access from multiple threads without any external synchronization.
 * All methods of this class could potentially throw RocksDBException, which
 * indicates sth wrong at the RocksDB library side and the call failed.
 */
public class RocksDB extends RocksObject {
  public static final byte[] DEFAULT_COLUMN_FAMILY = "default".getBytes();
  public static final int NOT_FOUND = -1;

  private enum LibraryState {
    NOT_LOADED,
    LOADING,
    LOADED
  }

  private static AtomicReference<LibraryState> libraryLoaded
      = new AtomicReference<>(LibraryState.NOT_LOADED);

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
  public static void loadLibrary() {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      final String tmpDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
      // loading possibly necessary libraries.
      for (final CompressionType compressionType : CompressionType.values()) {
        try {
          if (compressionType.getLibraryName() != null) {
            System.loadLibrary(compressionType.getLibraryName());
          }
        } catch (UnsatisfiedLinkError e) {
          // since it may be optional, we ignore its loading failure here.
        }
      }
      try {
        NativeLibraryLoader.getInstance().loadLibrary(tmpDir);
      } catch (IOException e) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw new RuntimeException("Unable to load the RocksDB shared library"
            + e);
      }

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch(final InterruptedException e) {
        //ignore
      }
    }
  }

  /**
   * Tries to load the necessary library files from the given list of
   * directories.
   *
   * @param paths a list of strings where each describes a directory
   *     of a library.
   */
  public static void loadLibrary(final List<String> paths) {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      for (final CompressionType compressionType : CompressionType.values()) {
        if (compressionType.equals(CompressionType.NO_COMPRESSION)) {
          continue;
        }
        for (final String path : paths) {
          try {
            System.load(path + "/" + Environment.getSharedLibraryFileName(
                compressionType.getLibraryName()));
            break;
          } catch (UnsatisfiedLinkError e) {
            // since they are optional, we ignore loading fails.
          }
        }
      }
      boolean success = false;
      UnsatisfiedLinkError err = null;
      for (final String path : paths) {
        try {
          System.load(path + "/" +
              Environment.getJniLibraryFileName("rocksdbjni"));
          success = true;
          break;
        } catch (UnsatisfiedLinkError e) {
          err = e;
        }
      }
      if (!success) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw err;
      }

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch(final InterruptedException e) {
        //ignore
      }
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final String path) throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    Options options = new Options();
    options.setCreateIfMissing(true);
    return open(options, path);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path and a list
   * of column family names.
   * <p>
   * If opened in read write mode every existing column family name must be
   * passed within the list to this method.</p>
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
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see DBOptions#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    DBOptions options = new DBOptions();
    return open(options, path, columnFamilyDescriptors, columnFamilyHandles);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final Options options, final String path)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    final RocksDB db = new RocksDB(open(options.nativeHandle_, path));
    db.storeOptionsInstance(options);
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path and a list
   * of column family names.
   * <p>
   * If opened in read write mode every existing column family name must be
   * passed within the list to this method.</p>
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
   * ColumnFamily handles are disposed when the RocksDB instance is disposed.
   * </p>
   *
   * @param options {@link org.rocksdb.DBOptions} instance.
   * @param path the path to the rocksdb.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @see DBOptions#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.columnFamilyName();
      cfOptionHandles[i] = cfDescriptor.columnFamilyOptions().nativeHandle_;
    }

    final long[] handles = open(options.nativeHandle_, path, cfNames,
        cfOptionHandles);
    final RocksDB db = new RocksDB(handles[0]);
    db.storeOptionsInstance(options);

    for (int i = 1; i < handles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(db, handles[i]));
    }

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
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final String path)
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
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    final DBOptions options = new DBOptions();
    return openReadOnly(options, path, columnFamilyDescriptors,
        columnFamilyHandles);
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
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final Options options, final String path)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    final RocksDB db = new RocksDB(openROnly(options.nativeHandle_, path));
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
   * @param options {@link DBOptions} instance.
   * @param path the path to the RocksDB.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.columnFamilyName();
      cfOptionHandles[i] = cfDescriptor.columnFamilyOptions().nativeHandle_;
    }

    final long[] handles = openROnly(options.nativeHandle_, path, cfNames,
        cfOptionHandles);
    final RocksDB db = new RocksDB(handles[0]);
    db.storeOptionsInstance(options);

    for (int i = 1; i < handles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(db, handles[i]));
    }

    return db;
  }
  /**
   * Static method to determine all available column families for a
   * rocksdb database identified by path
   *
   * @param options Options for opening the database
   * @param path Absolute path to rocksdb database
   * @return List&lt;byte[]&gt; List containing the column family names
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static List<byte[]> listColumnFamilies(final Options options,
      final String path) throws RocksDBException {
    return Arrays.asList(RocksDB.listColumnFamilies(options.nativeHandle_,
        path));
  }

  private void storeOptionsInstance(DBOptionsInterface options) {
    options_ = options;
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final byte[] key, final byte[] value)
      throws RocksDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    put(nativeHandle_, writeOpts.nativeHandle_,
        key, 0, key.length, value, 0, value.length);
  }

  /**
   * Set the database entry for "key" to "value" for the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * throws IllegalArgumentException if column family is not present
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see IllegalArgumentException
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    put(nativeHandle_, writeOpts.nativeHandle_, key, 0, key.length, value,
        0, value.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param key byte array of a key to search for
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final byte[] key, final StringBuilder value) {
    return keyMayExist(nativeHandle_, key, 0, key.length, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final StringBuilder value) {
    return keyMayExist(nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final ReadOptions readOptions,
      final byte[] key, final StringBuilder value) {
    return keyMayExist(nativeHandle_, readOptions.nativeHandle_,
        key, 0, key.length, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final StringBuilder value) {
    return keyMayExist(nativeHandle_, readOptions.nativeHandle_,
        key, 0, key.length, columnFamilyHandle.nativeHandle_,
        value);
  }

  /**
   * Apply the specified updates to the database.
   *
   * @param writeOpts WriteOptions instance
   * @param updates WriteBatch instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void write(final WriteOptions writeOpts, final WriteBatch updates)
      throws RocksDBException {
    write0(nativeHandle_, writeOpts.nativeHandle_, updates.nativeHandle_);
  }

  /**
   * Apply the specified updates to the database.
   *
   * @param writeOpts WriteOptions instance
   * @param updates WriteBatchWithIndex instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void write(final WriteOptions writeOpts,
      final WriteBatchWithIndex updates) throws RocksDBException {
    write1(nativeHandle_, writeOpts.nativeHandle_, updates.nativeHandle_);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final byte[] key, final byte[] value)
      throws RocksDBException {
    merge(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * Add merge operand for key/value pair in a ColumnFamily.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    merge(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, 0, key.length, value, 0, value.length);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  // TODO(AR) we should improve the #get() API, returning -1 (RocksDB.NOT_FOUND) is not very nice
  // when we could communicate better status into, also the C++ code show that -2 could be returned

  /**
   * Get the value associated with the specified key within column family*
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final byte[] key, final byte[] value) throws RocksDBException {
    return get(nativeHandle_, key, 0, key.length, value, 0, value.length);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final byte[] value) throws RocksDBException, IllegalArgumentException {
    return get(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key.
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ReadOptions opt, final byte[] key,
      final byte[] value) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_,
               key, 0, key.length, value, 0, value.length);
  }
  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key, final byte[] value)
      throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, 0, key.length, value,
        0, value.length, columnFamilyHandle.nativeHandle_);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final byte[] key) throws RocksDBException {
    return get(nativeHandle_, key, 0, key.length);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    return get(nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ReadOptions opt, final byte[] key)
      throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, 0, key.length);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Returns a map of keys for which values were found in DB.
   *
   * @param keys List of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   * entry is the corresponding value in DB.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public Map<byte[], byte[]> multiGet(final List<byte[]> keys)
      throws RocksDBException {
    assert(keys.size() != 0);

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    final byte[][] values = multiGet(nativeHandle_, keysArray, keyOffsets,
        keyLengths);

    final Map<byte[], byte[]> keyValueMap =
        new HashMap<>(computeCapacityHint(values.length));
    for(int i = 0; i < values.length; i++) {
      if(values[i] == null) {
        continue;
      }

      keyValueMap.put(keys.get(i), values[i]);
    }

    return keyValueMap;
  }

  private static int computeCapacityHint(final int estimatedNumberOfItems) {
    // Default load factor for HashMap is 0.75, so N * 1.5 will be at the load
    // limit. We add +1 for a buffer.
    return (int)Math.ceil(estimatedNumberOfItems * 1.5 + 1.0);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   */
  public Map<byte[], byte[]> multiGet(
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final List<byte[]> keys) throws RocksDBException,
      IllegalArgumentException {
    assert(keys.size() != 0);
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size() != columnFamilyHandleList.size()) {
        throw new IllegalArgumentException(
            "For each key there must be a ColumnFamilyHandle.");
    }
    final long[] cfHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      cfHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    final byte[][] values = multiGet(nativeHandle_, keysArray, keyOffsets,
        keyLengths, cfHandles);

    final Map<byte[], byte[]> keyValueMap =
        new HashMap<>(computeCapacityHint(values.length));
    for(int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        continue;
      }
      keyValueMap.put(keys.get(i), values[i]);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public Map<byte[], byte[]> multiGet(final ReadOptions opt,
      final List<byte[]> keys) throws RocksDBException {
    assert(keys.size() != 0);

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    final byte[][] values = multiGet(nativeHandle_, opt.nativeHandle_,
        keysArray, keyOffsets, keyLengths);

    final Map<byte[], byte[]> keyValueMap =
        new HashMap<>(computeCapacityHint(values.length));
    for(int i = 0; i < values.length; i++) {
      if(values[i] == null) {
        continue;
      }

      keyValueMap.put(keys.get(i), values[i]);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   */
  public Map<byte[], byte[]> multiGet(final ReadOptions opt,
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final List<byte[]> keys) throws RocksDBException {
    assert(keys.size() != 0);
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size()!=columnFamilyHandleList.size()){
      throw new IllegalArgumentException(
          "For each key there must be a ColumnFamilyHandle.");
    }
    final long[] cfHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      cfHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    final byte[][] values = multiGet(nativeHandle_, opt.nativeHandle_,
        keysArray, keyOffsets, keyLengths, cfHandles);

    final Map<byte[], byte[]> keyValueMap
        = new HashMap<>(computeCapacityHint(values.length));
    for(int i = 0; i < values.length; i++) {
      if(values[i] == null) {
        continue;
      }
      keyValueMap.put(keys.get(i), values[i]);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #delete(byte[])}
   */
  @Deprecated
  public void remove(final byte[] key) throws RocksDBException {
    delete(key);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final byte[] key) throws RocksDBException {
    delete(nativeHandle_, key, 0, key.length);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #delete(ColumnFamilyHandle, byte[])}
   */
  @Deprecated
  public void remove(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    delete(columnFamilyHandle, key);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
                     final byte[] key) throws RocksDBException {
    delete(nativeHandle_, key, 0, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #delete(WriteOptions, byte[])}
   */
  @Deprecated
  public void remove(final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    delete(writeOpt, key);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, 0, key.length);
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #delete(ColumnFamilyHandle, WriteOptions, byte[])}
   */
  @Deprecated
  public void remove(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    delete(columnFamilyHandle, writeOpt, key);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
                     final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   *
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   *
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental("Performance optimization for a very specific workload")
  public void singleDelete(final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, key, key.length);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   *
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   *
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * @param columnFamilyHandle The column family to delete the key from
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental("Performance optimization for a very specific workload")
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   *
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   *
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * Note: consider setting {@link WriteOptions#setSync(boolean)} true.
   *
   * @param writeOpt Write options for the delete
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental("Performance optimization for a very specific workload")
  public void singleDelete(final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    singleDelete(nativeHandle_, writeOpt.nativeHandle_, key, key.length);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   *
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   *
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * Note: consider setting {@link WriteOptions#setSync(boolean)} true.
   *
   * @param columnFamilyHandle The column family to delete the key from
   * @param writeOpt Write options for the delete
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental("Performance optimization for a very specific workload")
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, writeOpt.nativeHandle_, key, key.length,
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
   * <li>"rocksdb.num-files-at-level&lt;N&gt;" - return the number of files at
   * level &lt;N&gt;, where &lt;N&gt; is an ASCII representation of a level
   * number (e.g. "0").</li>
   * <li>"rocksdb.stats" - returns a multi-line string that describes statistics
   *     about the internal operation of the DB.</li>
   * <li>"rocksdb.sstables" - returns a multi-line string that describes all
   *    of the sstables that make up the db contents.</li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param property to be fetched. See above for examples
   * @return property value
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public String getProperty(final ColumnFamilyHandle columnFamilyHandle,
      final String property) throws RocksDBException {
    return getProperty0(nativeHandle_, columnFamilyHandle.nativeHandle_,
        property, property.length());
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   *
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param beginKey
   *          First key to delete within database (included)
   * @param endKey
   *          Last key to delete within database (excluded)
   *
   * @throws RocksDBException
   *           thrown if error happens in underlying native library.
   */
  public void deleteRange(final byte[] beginKey, final byte[] endKey) throws RocksDBException {
    deleteRange(nativeHandle_, beginKey, 0, beginKey.length, endKey, 0, endKey.length);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   *
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param columnFamilyHandle
   *          {@link org.rocksdb.ColumnFamilyHandle} instance
   * @param beginKey
   *          First key to delete within database (included)
   * @param endKey
   *          Last key to delete within database (excluded)
   *
   * @throws RocksDBException
   *           thrown if error happens in underlying native library.
   */
  public void deleteRange(final ColumnFamilyHandle columnFamilyHandle, final byte[] beginKey,
      final byte[] endKey) throws RocksDBException {
    deleteRange(nativeHandle_, beginKey, 0, beginKey.length, endKey, 0, endKey.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   *
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param writeOpt
   *          WriteOptions to be used with delete operation
   * @param beginKey
   *          First key to delete within database (included)
   * @param endKey
   *          Last key to delete within database (excluded)
   *
   * @throws RocksDBException
   *           thrown if error happens in underlying native library.
   */
  public void deleteRange(final WriteOptions writeOpt, final byte[] beginKey, final byte[] endKey)
      throws RocksDBException {
    deleteRange(nativeHandle_, writeOpt.nativeHandle_, beginKey, 0, beginKey.length, endKey, 0,
        endKey.length);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   *
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpt
   *          WriteOptions to be used with delete operation
   * @param beginKey
   *          First key to delete within database (included)
   * @param endKey
   *          Last key to delete within database (excluded)
   *
   * @throws RocksDBException
   *           thrown if error happens in underlying native library.
   */
  public void deleteRange(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpt,
      final byte[] beginKey, final byte[] endKey) throws RocksDBException {
    deleteRange(nativeHandle_, writeOpt.nativeHandle_, beginKey, 0, beginKey.length, endKey, 0,
        endKey.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * DB implementations can export properties about their state
   * via this method.  If "property" is a valid property understood by this
   * DB implementation, fills "*value" with its current value and returns
   * true.  Otherwise returns false.
   *
   * <p>Valid property names include:
   * <ul>
   * <li>"rocksdb.num-files-at-level&lt;N&gt;" - return the number of files at
   * level &lt;N&gt;, where &lt;N&gt; is an ASCII representation of a level
   * number (e.g. "0").</li>
   * <li>"rocksdb.stats" - returns a multi-line string that describes statistics
   *     about the internal operation of the DB.</li>
   * <li>"rocksdb.sstables" - returns a multi-line string that describes all
   *    of the sstables that make up the db contents.</li>
   *</ul>
   *
   * @param property to be fetched. See above for examples
   * @return property value
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public String getProperty(final String property) throws RocksDBException {
    return getProperty0(nativeHandle_, property, property.length());
  }

  /**
   * <p> Similar to GetProperty(), but only works for a subset of properties
   * whose return value is a numerical value. Return the value as long.</p>
   *
   * <p><strong>Note</strong>: As the returned property is of type
   * {@code uint64_t} on C++ side the returning value can be negative
   * because Java supports in Java 7 only signed long values.</p>
   *
   * <p><strong>Java 7</strong>: To mitigate the problem of the non
   * existent unsigned long tpye, values should be encapsulated using
   * {@link java.math.BigInteger} to reflect the correct value. The correct
   * behavior is guaranteed if {@code 2^64} is added to negative values.</p>
   *
   * <p><strong>Java 8</strong>: In Java 8 the value should be treated as
   * unsigned long using provided methods of type {@link Long}.</p>
   *
   * @param property to be fetched.
   *
   * @return numerical property value.
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public long getLongProperty(final String property) throws RocksDBException {
    return getLongProperty(nativeHandle_, property, property.length());
  }

  /**
   * <p> Similar to GetProperty(), but only works for a subset of properties
   * whose return value is a numerical value. Return the value as long.</p>
   *
   * <p><strong>Note</strong>: As the returned property is of type
   * {@code uint64_t} on C++ side the returning value can be negative
   * because Java supports in Java 7 only signed long values.</p>
   *
   * <p><strong>Java 7</strong>: To mitigate the problem of the non
   * existent unsigned long tpye, values should be encapsulated using
   * {@link java.math.BigInteger} to reflect the correct value. The correct
   * behavior is guaranteed if {@code 2^64} is added to negative values.</p>
   *
   * <p><strong>Java 8</strong>: In Java 8 the value should be treated as
   * unsigned long using provided methods of type {@link Long}.</p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param property to be fetched.
   *
   * @return numerical property value
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public long getLongProperty(final ColumnFamilyHandle columnFamilyHandle,
      final String property) throws RocksDBException {
    return getLongProperty(nativeHandle_, columnFamilyHandle.nativeHandle_,
        property, property.length());
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @return instance of iterator object.
   */
  public RocksIterator newIterator() {
    return new RocksIterator(this, iterator(nativeHandle_));
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @param readOptions {@link ReadOptions} instance.
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(final ReadOptions readOptions) {
    return new RocksIterator(this, iterator(nativeHandle_,
        readOptions.nativeHandle_));
  }

   /**
   * <p>Return a handle to the current DB state. Iterators created with
   * this handle will all observe a stable snapshot of the current DB
   * state. The caller must call ReleaseSnapshot(result) when the
   * snapshot is no longer needed.</p>
   *
   * <p>nullptr will be returned if the DB fails to take a snapshot or does
   * not support snapshot.</p>
   *
   * @return Snapshot {@link Snapshot} instance
   */
  public Snapshot getSnapshot() {
    long snapshotHandle = getSnapshot(nativeHandle_);
    if (snapshotHandle != 0) {
      return new Snapshot(snapshotHandle);
    }
    return null;
  }

  /**
   * Release a previously acquired snapshot.  The caller must not
   * use "snapshot" after this call.
   *
   * @param snapshot {@link Snapshot} instance
   */
  public void releaseSnapshot(final Snapshot snapshot) {
    if (snapshot != null) {
      releaseSnapshot(nativeHandle_, snapshot.nativeHandle_);
    }
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(
      final ColumnFamilyHandle columnFamilyHandle) {
    return new RocksIterator(this, iteratorCF(nativeHandle_,
        columnFamilyHandle.nativeHandle_));
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param readOptions {@link ReadOptions} instance.
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions) {
    return new RocksIterator(this, iteratorCF(nativeHandle_,
        columnFamilyHandle.nativeHandle_, readOptions.nativeHandle_));
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
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<RocksIterator> newIterators(
      final List<ColumnFamilyHandle> columnFamilyHandleList)
      throws RocksDBException {
    return newIterators(columnFamilyHandleList, new ReadOptions());
  }

  /**
   * Returns iterators from a consistent database state across multiple
   * column families. Iterators are heap allocated and need to be deleted
   * before the db is deleted
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param readOptions {@link ReadOptions} instance.
   * @return {@link java.util.List} containing {@link org.rocksdb.RocksIterator}
   *     instances
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<RocksIterator> newIterators(
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final ReadOptions readOptions) throws RocksDBException {

    final long[] columnFamilyHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      columnFamilyHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final long[] iteratorRefs = iterators(nativeHandle_, columnFamilyHandles,
        readOptions.nativeHandle_);

    final List<RocksIterator> iterators = new ArrayList<>(
        columnFamilyHandleList.size());
    for (int i=0; i<columnFamilyHandleList.size(); i++){
      iterators.add(new RocksIterator(this, iteratorRefs[i]));
    }
    return iterators;
  }

  /**
   * Gets the handle for the default column family
   *
   * @return The handle of the default column family
   */
  public ColumnFamilyHandle getDefaultColumnFamily() {
    ColumnFamilyHandle cfHandle = new ColumnFamilyHandle(this,
        getDefaultColumnFamily(nativeHandle_));
    cfHandle.disOwnNativeHandle();
    return cfHandle;
  }

  /**
   * Creates a new column family with the name columnFamilyName and
   * allocates a ColumnFamilyHandle within an internal structure.
   * The ColumnFamilyHandle is automatically disposed with DB disposal.
   *
   * @param columnFamilyDescriptor column family to be created.
   * @return {@link org.rocksdb.ColumnFamilyHandle} instance.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public ColumnFamilyHandle createColumnFamily(
      final ColumnFamilyDescriptor columnFamilyDescriptor)
      throws RocksDBException {
    return new ColumnFamilyHandle(this, createColumnFamily(nativeHandle_,
        columnFamilyDescriptor.columnFamilyName(),
        columnFamilyDescriptor.columnFamilyOptions().nativeHandle_));
  }

  /**
   * Drops the column family identified by columnFamilyName. Internal
   * handles to this column family will be disposed. If the column family
   * is not known removal will fail.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void dropColumnFamily(final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException, IllegalArgumentException {
    // throws RocksDBException if something goes wrong
    dropColumnFamily(nativeHandle_, columnFamilyHandle.nativeHandle_);
    // After the drop the native handle is not valid anymore
    columnFamilyHandle.disOwnNativeHandle();
  }

  /**
   * <p>Flush all memory table data.</p>
   *
   * <p>Note: it must be ensured that the FlushOptions instance
   * is not GC'ed before this method finishes. If the wait parameter is
   * set to false, flush processing is asynchronous.</p>
   *
   * @param flushOptions {@link org.rocksdb.FlushOptions} instance.
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void flush(final FlushOptions flushOptions)
      throws RocksDBException {
    flush(nativeHandle_, flushOptions.nativeHandle_);
  }

  /**
   * <p>Flush all memory table data.</p>
   *
   * <p>Note: it must be ensured that the FlushOptions instance
   * is not GC'ed before this method finishes. If the wait parameter is
   * set to false, flush processing is asynchronous.</p>
   *
   * @param flushOptions {@link org.rocksdb.FlushOptions} instance.
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance.
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void flush(final FlushOptions flushOptions,
      final ColumnFamilyHandle columnFamilyHandle) throws RocksDBException {
    flush(nativeHandle_, flushOptions.nativeHandle_,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(boolean, int, int)}</li>
   * <li>{@link #compactRange(byte[], byte[])}</li>
   * <li>{@link #compactRange(byte[], byte[], boolean, int, int)}</li>
   * </ul>
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange() throws RocksDBException {
    compactRange0(nativeHandle_, false, -1, 0);
  }

  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange()}</li>
   * <li>{@link #compactRange(boolean, int, int)}</li>
   * <li>{@link #compactRange(byte[], byte[], boolean, int, int)}</li>
   * </ul>
   *
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final byte[] begin, final byte[] end)
      throws RocksDBException {
    compactRange0(nativeHandle_, begin, begin.length, end,
        end.length, false, -1, 0);
  }

  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p>Compaction outputs should be placed in options.db_paths
   * [target_path_id]. Behavior is undefined if target_path_id is
   * out of range.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange()}</li>
   * <li>{@link #compactRange(byte[], byte[])}</li>
   * <li>{@link #compactRange(byte[], byte[], boolean, int, int)}</li>
   * </ul>
   *
   * @param reduce_level reduce level after compaction
   * @param target_level target level to compact to
   * @param target_path_id the target path id of output path
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final boolean reduce_level,
      final int target_level, final int target_path_id)
      throws RocksDBException {
    compactRange0(nativeHandle_, reduce_level,
        target_level, target_path_id);
  }


  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p>Compaction outputs should be placed in options.db_paths
   * [target_path_id]. Behavior is undefined if target_path_id is
   * out of range.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange()}</li>
   * <li>{@link #compactRange(boolean, int, int)}</li>
   * <li>{@link #compactRange(byte[], byte[])}</li>
   * </ul>
   *
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   * @param reduce_level reduce level after compaction
   * @param target_level target level to compact to
   * @param target_path_id the target path id of output path
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final byte[] begin, final byte[] end,
      final boolean reduce_level, final int target_level,
      final int target_path_id) throws RocksDBException {
    compactRange0(nativeHandle_, begin, begin.length, end, end.length,
        reduce_level, target_level, target_path_id);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, boolean, int, int)}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[],
   *   boolean, int, int)}
   * </li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance.
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    compactRange(nativeHandle_, false, -1, 0,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(ColumnFamilyHandle)}</li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, boolean, int, int)}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[],
   *   boolean, int, int)}
   * </li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance.
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] begin, final byte[] end) throws RocksDBException {
    compactRange(nativeHandle_, begin, begin.length, end, end.length,
        false, -1, 0, columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p>Compaction outputs should be placed in options.db_paths
   * [target_path_id]. Behavior is undefined if target_path_id is
   * out of range.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(ColumnFamilyHandle)}</li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[],
   *   boolean, int, int)}
   * </li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance.
   * @param reduce_level reduce level after compaction
   * @param target_level target level to compact to
   * @param target_path_id the target path id of output path
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final ColumnFamilyHandle columnFamilyHandle,
      final boolean reduce_level, final int target_level,
      final int target_path_id) throws RocksDBException {
    compactRange(nativeHandle_, reduce_level, target_level,
        target_path_id, columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p>Compaction outputs should be placed in options.db_paths
   * [target_path_id]. Behavior is undefined if target_path_id is
   * out of range.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(ColumnFamilyHandle)}</li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, boolean, int, int)}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * </li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance.
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   * @param reduce_level reduce level after compaction
   * @param target_level target level to compact to
   * @param target_path_id the target path id of output path
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] begin, final byte[] end, final boolean reduce_level,
      final int target_level, final int target_path_id)
      throws RocksDBException {
    compactRange(nativeHandle_, begin, begin.length, end, end.length,
        reduce_level, target_level, target_path_id,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * This function will wait until all currently running background processes
   * finish. After it returns, no background process will be run until
   * {@link #continueBackgroundWork()} is called
   *
   * @throws RocksDBException If an error occurs when pausing background work
   */
  public void pauseBackgroundWork() throws RocksDBException {
    pauseBackgroundWork(nativeHandle_);
  }

  /**
   * Resumes backround work which was suspended by
   * previously calling {@link #pauseBackgroundWork()}
   *
   * @throws RocksDBException If an error occurs when resuming background work
   */
  public void continueBackgroundWork() throws RocksDBException {
    continueBackgroundWork(nativeHandle_);
  }

  /**
   * <p>The sequence number of the most recent transaction.</p>
   *
   * @return sequence number of the most
   *     recent transaction.
   */
  public long getLatestSequenceNumber() {
    return getLatestSequenceNumber(nativeHandle_);
  }

  /**
   * <p>Prevent file deletions. Compactions will continue to occur,
   * but no obsolete files will be deleted. Calling this multiple
   * times have the same effect as calling it once.</p>
   *
   * @throws RocksDBException thrown if operation was not performed
   *     successfully.
   */
  public void disableFileDeletions() throws RocksDBException {
    disableFileDeletions(nativeHandle_);
  }

  /**
   * <p>Allow compactions to delete obsolete files.
   * If force == true, the call to EnableFileDeletions()
   * will guarantee that file deletions are enabled after
   * the call, even if DisableFileDeletions() was called
   * multiple times before.</p>
   *
   * <p>If force == false, EnableFileDeletions will only
   * enable file deletion after it's been called at least
   * as many times as DisableFileDeletions(), enabling
   * the two methods to be called by two threads
   * concurrently without synchronization
   * -- i.e., file deletions will be enabled only after both
   * threads call EnableFileDeletions()</p>
   *
   * @param force boolean value described above.
   *
   * @throws RocksDBException thrown if operation was not performed
   *     successfully.
   */
  public void enableFileDeletions(final boolean force)
      throws RocksDBException {
    enableFileDeletions(nativeHandle_, force);
  }

  /**
   * <p>Returns an iterator that is positioned at a write-batch containing
   * seq_number. If the sequence number is non existent, it returns an iterator
   * at the first available seq_no after the requested seq_no.</p>
   *
   * <p>Must set WAL_ttl_seconds or WAL_size_limit_MB to large values to
   * use this api, else the WAL files will get
   * cleared aggressively and the iterator might keep getting invalid before
   * an update is read.</p>
   *
   * @param sequenceNumber sequence number offset
   *
   * @return {@link org.rocksdb.TransactionLogIterator} instance.
   *
   * @throws org.rocksdb.RocksDBException if iterator cannot be retrieved
   *     from native-side.
   */
  public TransactionLogIterator getUpdatesSince(final long sequenceNumber)
      throws RocksDBException {
    return new TransactionLogIterator(
        getUpdatesSince(nativeHandle_, sequenceNumber));
  }

  public void setOptions(final ColumnFamilyHandle columnFamilyHandle,
                         final MutableColumnFamilyOptions mutableColumnFamilyOptions)
          throws RocksDBException {
    setOptions(nativeHandle_, columnFamilyHandle.nativeHandle_,
            mutableColumnFamilyOptions.getKeys(),
            mutableColumnFamilyOptions.getValues());
  }

  private long[] toNativeHandleList(final List<? extends RocksObject> objectList) {
    final int len = objectList.size();
    final long[] handleList = new long[len];
    for (int i = 0; i < len; i++) {
      handleList[i] = objectList.get(i).nativeHandle_;
    }
    return handleList;
  }

  /**
   * ingestExternalFile will load a list of external SST files (1) into the DB
   * We will try to find the lowest possible level that the file can fit in, and
   * ingest the file into this level (2). A file that have a key range that
   * overlap with the memtable key range will require us to Flush the memtable
   * first before ingesting the file.
   *
   * (1) External SST files can be created using {@link SstFileWriter}
   * (2) We will try to ingest the files to the lowest possible level
   * even if the file compression doesn't match the level compression
   *
   * @param filePathList The list of files to ingest
   * @param ingestExternalFileOptions the options for the ingestion
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void ingestExternalFile(final List<String> filePathList,
      final IngestExternalFileOptions ingestExternalFileOptions)
      throws RocksDBException {
    ingestExternalFile(nativeHandle_, getDefaultColumnFamily().nativeHandle_,
        filePathList.toArray(new String[filePathList.size()]),
        filePathList.size(), ingestExternalFileOptions.nativeHandle_);
  }

  /**
   * ingestExternalFile will load a list of external SST files (1) into the DB
   * We will try to find the lowest possible level that the file can fit in, and
   * ingest the file into this level (2). A file that have a key range that
   * overlap with the memtable key range will require us to Flush the memtable
   * first before ingesting the file.
   *
   * (1) External SST files can be created using {@link SstFileWriter}
   * (2) We will try to ingest the files to the lowest possible level
   * even if the file compression doesn't match the level compression
   *
   * @param columnFamilyHandle The column family for the ingested files
   * @param filePathList The list of files to ingest
   * @param ingestExternalFileOptions the options for the ingestion
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void ingestExternalFile(final ColumnFamilyHandle columnFamilyHandle,
      final List<String> filePathList,
      final IngestExternalFileOptions ingestExternalFileOptions)
      throws RocksDBException {
    ingestExternalFile(nativeHandle_, columnFamilyHandle.nativeHandle_,
        filePathList.toArray(new String[filePathList.size()]),
        filePathList.size(), ingestExternalFileOptions.nativeHandle_);
  }

  /**
   * Private constructor.
   *
   * @param nativeHandle The native handle of the C++ RocksDB object
   */
  protected RocksDB(final long nativeHandle) {
    super(nativeHandle);
  }

  // native methods
  protected native static long open(final long optionsHandle,
      final String path) throws RocksDBException;

  /**
   * @param optionsHandle Native handle pointing to an Options object
   * @param path The directory path for the database files
   * @param columnFamilyNames An array of column family names
   * @param columnFamilyOptions An array of native handles pointing to
   *                            ColumnFamilyOptions objects
   *
   * @return An array of native handles, [0] is the handle of the RocksDB object
   *   [1..1+n] are handles of the ColumnFamilyReferences
   *
   * @throws RocksDBException thrown if the database could not be opened
   */
  protected native static long[] open(final long optionsHandle,
      final String path, final byte[][] columnFamilyNames,
      final long[] columnFamilyOptions) throws RocksDBException;

  protected native static long openROnly(final long optionsHandle,
      final String path) throws RocksDBException;

  /**
   * @param optionsHandle Native handle pointing to an Options object
   * @param path The directory path for the database files
   * @param columnFamilyNames An array of column family names
   * @param columnFamilyOptions An array of native handles pointing to
   *                            ColumnFamilyOptions objects
   *
   * @return An array of native handles, [0] is the handle of the RocksDB object
   *   [1..1+n] are handles of the ColumnFamilyReferences
   *
   * @throws RocksDBException thrown if the database could not be opened
   */
  protected native static long[] openROnly(final long optionsHandle,
      final String path, final byte[][] columnFamilyNames,
      final long[] columnFamilyOptions
  ) throws RocksDBException;

  protected native static byte[][] listColumnFamilies(long optionsHandle,
      String path) throws RocksDBException;
  protected native void put(long handle, byte[] key, int keyOffset,
      int keyLength, byte[] value, int valueOffset, int valueLength)
      throws RocksDBException;
  protected native void put(long handle, byte[] key, int keyOffset,
      int keyLength, byte[] value, int valueOffset, int valueLength,
      long cfHandle) throws RocksDBException;
  protected native void put(long handle, long writeOptHandle, byte[] key,
      int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength) throws RocksDBException;
  protected native void put(long handle, long writeOptHandle, byte[] key,
      int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength, long cfHandle) throws RocksDBException;
  protected native void write0(final long handle, long writeOptHandle,
      long wbHandle) throws RocksDBException;
  protected native void write1(final long handle, long writeOptHandle,
      long wbwiHandle) throws RocksDBException;
  protected native boolean keyMayExist(final long handle, final byte[] key,
      final int keyOffset, final int keyLength,
      final StringBuilder stringBuilder);
  protected native boolean keyMayExist(final long handle, final byte[] key,
      final int keyOffset, final int keyLength, final long cfHandle,
      final StringBuilder stringBuilder);
  protected native boolean keyMayExist(final long handle,
      final long optionsHandle, final byte[] key, final int keyOffset,
      final int keyLength, final StringBuilder stringBuilder);
  protected native boolean keyMayExist(final long handle,
      final long optionsHandle, final byte[] key, final int keyOffset,
      final int keyLength, final long cfHandle,
      final StringBuilder stringBuilder);
  protected native void merge(long handle, byte[] key, int keyOffset,
      int keyLength, byte[] value, int valueOffset, int valueLength)
      throws RocksDBException;
  protected native void merge(long handle, byte[] key, int keyOffset,
      int keyLength, byte[] value, int valueOffset, int valueLength,
      long cfHandle) throws RocksDBException;
  protected native void merge(long handle, long writeOptHandle, byte[] key,
      int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength) throws RocksDBException;
  protected native void merge(long handle, long writeOptHandle, byte[] key,
      int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength, long cfHandle) throws RocksDBException;
  protected native int get(long handle, byte[] key, int keyOffset,
      int keyLength, byte[] value, int valueOffset, int valueLength)
      throws RocksDBException;
  protected native int get(long handle, byte[] key, int keyOffset,
      int keyLength, byte[] value, int valueOffset, int valueLength,
      long cfHandle) throws RocksDBException;
  protected native int get(long handle, long readOptHandle, byte[] key,
      int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength) throws RocksDBException;
  protected native int get(long handle, long readOptHandle, byte[] key,
      int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength, long cfHandle) throws RocksDBException;
  protected native byte[][] multiGet(final long dbHandle, final byte[][] keys,
      final int[] keyOffsets, final int[] keyLengths);
  protected native byte[][] multiGet(final long dbHandle, final byte[][] keys,
      final int[] keyOffsets, final int[] keyLengths,
      final long[] columnFamilyHandles);
  protected native byte[][] multiGet(final long dbHandle, final long rOptHandle,
      final byte[][] keys, final int[] keyOffsets, final int[] keyLengths);
  protected native byte[][] multiGet(final long dbHandle, final long rOptHandle,
      final byte[][] keys, final int[] keyOffsets, final int[] keyLengths,
      final long[] columnFamilyHandles);
  protected native byte[] get(long handle, byte[] key, int keyOffset,
      int keyLength) throws RocksDBException;
  protected native byte[] get(long handle, byte[] key, int keyOffset,
      int keyLength, long cfHandle) throws RocksDBException;
  protected native byte[] get(long handle, long readOptHandle,
      byte[] key, int keyOffset, int keyLength) throws RocksDBException;
  protected native byte[] get(long handle, long readOptHandle, byte[] key,
      int keyOffset, int keyLength, long cfHandle) throws RocksDBException;
  protected native void delete(long handle, byte[] key, int keyOffset,
      int keyLength) throws RocksDBException;
  protected native void delete(long handle, byte[] key, int keyOffset,
      int keyLength, long cfHandle) throws RocksDBException;
  protected native void delete(long handle, long writeOptHandle, byte[] key,
      int keyOffset, int keyLength) throws RocksDBException;
  protected native void delete(long handle, long writeOptHandle, byte[] key,
      int keyOffset, int keyLength, long cfHandle) throws RocksDBException;
  protected native void singleDelete(
      long handle, byte[] key, int keyLen) throws RocksDBException;
  protected native void singleDelete(
      long handle, byte[] key, int keyLen, long cfHandle)
      throws RocksDBException;
  protected native void singleDelete(
      long handle, long writeOptHandle,
      byte[] key, int keyLen) throws RocksDBException;
  protected native void singleDelete(
      long handle, long writeOptHandle,
      byte[] key, int keyLen, long cfHandle) throws RocksDBException;
  protected native void deleteRange(long handle, byte[] beginKey, int beginKeyOffset,
      int beginKeyLength, byte[] endKey, int endKeyOffset, int endKeyLength)
      throws RocksDBException;
  protected native void deleteRange(long handle, byte[] beginKey, int beginKeyOffset,
      int beginKeyLength, byte[] endKey, int endKeyOffset, int endKeyLength, long cfHandle)
      throws RocksDBException;
  protected native void deleteRange(long handle, long writeOptHandle, byte[] beginKey,
      int beginKeyOffset, int beginKeyLength, byte[] endKey, int endKeyOffset, int endKeyLength)
      throws RocksDBException;
  protected native void deleteRange(long handle, long writeOptHandle, byte[] beginKey,
      int beginKeyOffset, int beginKeyLength, byte[] endKey, int endKeyOffset, int endKeyLength,
      long cfHandle) throws RocksDBException;
  protected native String getProperty0(long nativeHandle,
      String property, int propertyLength) throws RocksDBException;
  protected native String getProperty0(long nativeHandle, long cfHandle,
      String property, int propertyLength) throws RocksDBException;
  protected native long getLongProperty(long nativeHandle, String property,
      int propertyLength) throws RocksDBException;
  protected native long getLongProperty(long nativeHandle, long cfHandle,
      String property, int propertyLength) throws RocksDBException;
  protected native long iterator(long handle);
  protected native long iterator(long handle, long readOptHandle);
  protected native long iteratorCF(long handle, long cfHandle);
  protected native long iteratorCF(long handle, long cfHandle,
      long readOptHandle);
  protected native long[] iterators(final long handle,
      final long[] columnFamilyHandles, final long readOptHandle)
      throws RocksDBException;
  protected native long getSnapshot(long nativeHandle);
  protected native void releaseSnapshot(long nativeHandle, long snapshotHandle);
  @Override protected final native void disposeInternal(final long handle);
  private native long getDefaultColumnFamily(long handle);
  private native long createColumnFamily(final long handle,
      final byte[] columnFamilyName, final long columnFamilyOptions)
      throws RocksDBException;
  private native void dropColumnFamily(long handle, long cfHandle)
      throws RocksDBException;
  private native void flush(long handle, long flushOptHandle)
      throws RocksDBException;
  private native void flush(long handle, long flushOptHandle, long cfHandle)
      throws RocksDBException;
  private native void compactRange0(long handle, boolean reduce_level,
      int target_level, int target_path_id) throws RocksDBException;
  private native void compactRange0(long handle, byte[] begin, int beginLen,
      byte[] end, int endLen, boolean reduce_level, int target_level,
      int target_path_id) throws RocksDBException;
  private native void compactRange(long handle, boolean reduce_level,
      int target_level, int target_path_id, long cfHandle)
      throws RocksDBException;
  private native void compactRange(long handle, byte[] begin, int beginLen,
      byte[] end, int endLen, boolean reduce_level, int target_level,
      int target_path_id, long cfHandle) throws RocksDBException;
  private native void pauseBackgroundWork(long handle) throws RocksDBException;
  private native void continueBackgroundWork(long handle) throws RocksDBException;
  private native long getLatestSequenceNumber(long handle);
  private native void disableFileDeletions(long handle) throws RocksDBException;
  private native void enableFileDeletions(long handle, boolean force)
      throws RocksDBException;
  private native long getUpdatesSince(long handle, long sequenceNumber)
      throws RocksDBException;
  private native void setOptions(long handle, long cfHandle, String[] keys,
      String[] values) throws RocksDBException;
  private native void ingestExternalFile(long handle, long cfHandle,
      String[] filePathList, int filePathListLen,
      long ingest_external_file_options_handle) throws RocksDBException;
  protected DBOptionsInterface options_;
}
