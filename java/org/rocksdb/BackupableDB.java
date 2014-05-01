// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * A subclass of RocksDB which supports backup-related operations.
 *
 * @see BackupableDBOptions
 */
public class BackupableDB extends RocksDB {
  /**
   * Open a BackupableDB under the specified path.
   * Note that the backup path should be set properly in the
   * input BackupableDBOptions.
   *
   * @param opt options for db.
   * @param bopt backup related options.
   * @param the db path for storing data.  The path for storing
   *     backup should be specified in the BackupableDBOptions.
   * @return reference to the opened BackupableDB.
   */
  public static BackupableDB open(
      Options opt, BackupableDBOptions bopt, String db_path)
      throws RocksDBException {
    // since BackupableDB c++ will handle the life cycle of
    // the returned RocksDB of RocksDB.open(), here we store
    // it as a BackupableDB member variable to avoid GC.
    BackupableDB bdb = new BackupableDB(RocksDB.open(opt, db_path));
    bdb.open(bdb.db_.nativeHandle_, bopt.nativeHandle_);

    return bdb;
  }

  /**
   * Captures the state of the database in the latest backup.
   * Note that this function is not thread-safe.
   *
   * @param flushBeforeBackup if true, then all data will be flushed
   *     before creating backup.
   */
  public void createNewBackup(boolean flushBeforeBackup) {
    createNewBackup(nativeHandle_, flushBeforeBackup);
  }


  /**
   * Close the BackupableDB instance and release resource.
   *
   * Internally, BackupableDB owns the rocksdb::DB pointer to its
   * associated RocksDB.  The release of that RocksDB pointer is
   * handled in the destructor of the c++ rocksdb::BackupableDB and
   * should be transparent to Java developers.
   */
  @Override public synchronized void close() {
    if (isInitialized()) {
      super.close();
    }
  }

  /**
   * A protected construction that will be used in the static factory
   * method BackupableDB.open().
   */
  protected BackupableDB(RocksDB db) {
    super();
    db_ = db;
  }

  @Override protected void finalize() {
    close();
  }

  protected native void open(long rocksDBHandle, long backupDBOptionsHandle);
  protected native void createNewBackup(long handle, boolean flag);

  private final RocksDB db_;
}
