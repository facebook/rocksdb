// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.List;

/**
 * Database with Transaction support.
 */
public class OptimisticTransactionDB extends RocksDB
    implements TransactionalDB<OptimisticTransactionOptions> {

  /**
   * Private constructor.
   *
   * @param nativeHandle The native handle of the C++ OptimisticTransactionDB
   *     object
   */
  private OptimisticTransactionDB(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Open an OptimisticTransactionDB similar to
   * {@link RocksDB#open(Options, String)}.
   *
   * @param options {@link org.rocksdb.Options} instance.
   * @param path the path to the rocksdb.
   *
   * @return a {@link OptimisticTransactionDB} instance on success, null if the
   * specified {@link OptimisticTransactionDB} can not be opened.
   *
   * @throws RocksDBException if an error occurs whilst opening the database.
   */
  public static OptimisticTransactionDB open(final Options options,
      final String path) throws RocksDBException {
    final OptimisticTransactionDB otdb = new OptimisticTransactionDB(open(
        options.nativeHandle_, path));

    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    otdb.storeOptionsInstance(options);

    otdb.storeDefaultColumnFamilyHandle(otdb.makeDefaultColumnFamilyHandle());

    return otdb;
  }

  /**
   * Open an OptimisticTransactionDB similar to
   * {@link RocksDB#open(DBOptions, String, List, List)}.
   *
   * @param dbOptions {@link org.rocksdb.DBOptions} instance.
   * @param path the path to the rocksdb.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *
   * @return a {@link OptimisticTransactionDB} instance on success, null if the
   *     specified {@link OptimisticTransactionDB} can not be opened.
   *
   * @throws RocksDBException if an error occurs whilst opening the database.
   */
  public static OptimisticTransactionDB open(final DBOptions dbOptions, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
    return open(dbOptions, new OptimisticTransactionDBOptions(), path, columnFamilyDescriptors,
        columnFamilyHandles);
  }

  /**
   * Open an OptimisticTransactionDB similar to
   * {@link RocksDB#open(DBOptions, String, List, List)}.
   *
   * @param dbOptions {@link org.rocksdb.DBOptions} instance.
   * @param optimisticDbOptions {@link org.rocksdb.OptimisticTransactionDBOptions} instance.
   * @param path the path to the rocksdb.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *
   * @return a {@link OptimisticTransactionDB} instance on success, null if the
   *     specified {@link OptimisticTransactionDB} can not be opened.
   *
   * @throws RocksDBException if an error occurs whilst opening the database.
   */
  public static OptimisticTransactionDB open(final DBOptions dbOptions,
      final OptimisticTransactionDBOptions optimisticDbOptions, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
    int defaultColumnFamilyIndex = -1;
    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.getName();
      cfOptionHandles[i] = cfDescriptor.getOptions().nativeHandle_;
      if (Arrays.equals(cfDescriptor.getName(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
        defaultColumnFamilyIndex = i;
      }
    }
    if (defaultColumnFamilyIndex < 0) {
      throw new IllegalArgumentException(
          "You must provide the default column family in your columnFamilyDescriptors");
    }

    final long[] handles = open(
        dbOptions.nativeHandle_, optimisticDbOptions.nativeHandle_, path, cfNames, cfOptionHandles);
    final OptimisticTransactionDB otdb =
        new OptimisticTransactionDB(handles[0]);

    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    otdb.storeOptionsInstance(dbOptions);

    for (int i = 1; i < handles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(otdb, handles[i]));
    }

    otdb.ownedColumnFamilyHandles.addAll(columnFamilyHandles);
    otdb.storeDefaultColumnFamilyHandle(columnFamilyHandles.get(defaultColumnFamilyIndex));

    return otdb;
  }

  /**
   * This is similar to {@link #close()} except that it
   * throws an exception if any error occurs.
   * <p>
   * This will not fsync the WAL files.
   * If syncing is required, the caller must first call {@link #syncWal()}
   * or {@link #write(WriteOptions, WriteBatch)} using an empty write batch
   * with {@link WriteOptions#setSync(boolean)} set to true.
   * <p>
   * See also {@link #close()}.
   *
   * @throws RocksDBException if an error occurs whilst closing.
   */
  @Override
  public void closeE() throws RocksDBException {
    if (owningHandle_.compareAndSet(true, false)) {
      try {
        closeDatabase(nativeHandle_);
      } finally {
        disposeInternal();
      }
    }
  }

  /**
   * This is similar to {@link #closeE()} except that it
   * silently ignores any errors.
   * <p>
   * This will not fsync the WAL files.
   * If syncing is required, the caller must first call {@link #syncWal()}
   * or {@link #write(WriteOptions, WriteBatch)} using an empty write batch
   * with {@link WriteOptions#setSync(boolean)} set to true.
   * <p>
   * See also {@link #close()}.
   */
  @SuppressWarnings("PMD.EmptyCatchBlock")
  @Override
  public void close() {
    for (final ColumnFamilyHandle columnFamilyHandle : // NOPMD - CloseResource
        ownedColumnFamilyHandles) {
      columnFamilyHandle.close();
    }
    ownedColumnFamilyHandles.clear();

    if (owningHandle_.compareAndSet(true, false)) {
      try {
        closeDatabase(nativeHandle_);
      } catch (final RocksDBException e) {
        // silently ignore the error report
      } finally {
        disposeInternal();
      }
    }
  }

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions) {
    return new Transaction(this, beginTransaction(nativeHandle_,
        writeOptions.nativeHandle_));
  }

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions,
      final OptimisticTransactionOptions optimisticTransactionOptions) {
    return new Transaction(this, beginTransaction(nativeHandle_,
        writeOptions.nativeHandle_,
        optimisticTransactionOptions.nativeHandle_));
  }

  // TODO(AR) consider having beingTransaction(... oldTransaction) set a
  // reference count inside Transaction, so that we can always call
  // Transaction#close but the object is only disposed when there are as many
  // closes as beginTransaction. Makes the try-with-resources paradigm easier for
  // java developers

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions,
      final Transaction oldTransaction) {
    final long jtxn_handle = beginTransaction_withOld(nativeHandle_,
        writeOptions.nativeHandle_, oldTransaction.nativeHandle_);

    // RocksJava relies on the assumption that
    // we do not allocate a new Transaction object
    // when providing an old_txn
    assert(jtxn_handle == oldTransaction.nativeHandle_);

    return oldTransaction;
  }

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions,
      final OptimisticTransactionOptions optimisticTransactionOptions,
      final Transaction oldTransaction) {
    final long jtxn_handle = beginTransaction_withOld(nativeHandle_,
        writeOptions.nativeHandle_, optimisticTransactionOptions.nativeHandle_,
        oldTransaction.nativeHandle_);

    // RocksJava relies on the assumption that
    // we do not allocate a new Transaction object
    // when providing an old_txn
    assert(jtxn_handle == oldTransaction.nativeHandle_);

    return oldTransaction;
  }

  /**
   * Get the underlying {@link OccValidationPolicy}.
   *
   * @return The underlying {@link OccValidationPolicy}.
   */
  public OccValidationPolicy occValidationPolicy() {
    return OccValidationPolicy.getOccValidationPolicy(occValidationPolicy(nativeHandle_));
  }

  /**
   * Get the underlying database that was opened.
   *
   * @return The underlying database that was opened.
   */
  public RocksDB getBaseDB() {
    final RocksDB db = new RocksDB(getBaseDB(nativeHandle_));
    db.disOwnNativeHandle();
    return db;
  }

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  protected static native long open(final long optionsHandle,
      final String path) throws RocksDBException;
  protected static native long[] open(final long optionsHandle,
      final long optimisticDbOptionsHandle, final String path, final byte[][] columnFamilyNames,
      final long[] columnFamilyOptions);
  private static native void closeDatabase(final long handle) throws RocksDBException;
  private static native long beginTransaction(final long handle, final long writeOptionsHandle);
  private static native long beginTransaction(final long handle, final long writeOptionsHandle,
      final long optimisticTransactionOptionsHandle);
  private static native long beginTransaction_withOld(
      final long handle, final long writeOptionsHandle, final long oldTransactionHandle);
  private static native long beginTransaction_withOld(final long handle,
      final long writeOptionsHandle, final long optimisticTransactionOptionsHandle,
      final long oldTransactionHandle);
  private static native byte occValidationPolicy(final long handle);
  private static native long getBaseDB(final long handle);
}
