// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.List;

/**
 * A subclass of RocksDB which supports backup-related operations.
 *
 * @see org.rocksdb.BackupableDBOptions
 */
public class BackupableDB extends RocksDB {
  /**
   * Open a {@code BackupableDB} under the specified path.
   * Note that the backup path should be set properly in the
   * input BackupableDBOptions.
   *
   * @param opt {@link org.rocksdb.Options} to set for the database.
   * @param bopt {@link org.rocksdb.BackupableDBOptions} to use.
   * @param db_path Path to store data to. The path for storing the backup should be
   *     specified in the {@link org.rocksdb.BackupableDBOptions}.
   * @return BackupableDB reference to the opened database.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static BackupableDB open(
      Options opt, BackupableDBOptions bopt, String db_path)
      throws RocksDBException {

    RocksDB db = RocksDB.open(opt, db_path);
    BackupableDB bdb = new BackupableDB();
    bdb.open(db.nativeHandle_, bopt.nativeHandle_);

    // Prevent the RocksDB object from attempting to delete
    // the underly C++ DB object.
    db.disOwnNativeHandle();

    return bdb;
  }

  /**
   * Captures the state of the database in the latest backup.
   * Note that this function is not thread-safe.
   *
   * @param flushBeforeBackup if true, then all data will be flushed
   *     before creating backup.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void createNewBackup(boolean flushBeforeBackup)
      throws RocksDBException {
    createNewBackup(nativeHandle_, flushBeforeBackup);
  }

  /**
   * Deletes old backups, keeping latest numBackupsToKeep alive.
   *
   * @param numBackupsToKeep Number of latest backups to keep.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void purgeOldBackups(int numBackupsToKeep)
      throws RocksDBException {
    purgeOldBackups(nativeHandle_, numBackupsToKeep);
  }

  /**
   * Deletes a specific backup.
   *
   * @param backupId of backup to delete.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void deleteBackup(int backupId) throws RocksDBException {
    deleteBackup0(nativeHandle_, backupId);
  }

  /**
   * Returns a list of {@link BackupInfo} instances, which describe
   * already made backups.
   *
   * @return List of {@link BackupInfo} instances.
   */
  public List<BackupInfo> getBackupInfos() {
    return getBackupInfo(nativeHandle_);
  }

  /**
   * Close the BackupableDB instance and release resource.
   *
   * Internally, BackupableDB owns the {@code rocksdb::DB} pointer to its associated
   * {@link org.rocksdb.RocksDB}. The release of that RocksDB pointer is handled in the destructor
   * of the c++ {@code rocksdb::BackupableDB} and should be transparent to Java developers.
   */
  @Override public synchronized void close() {
    if (isInitialized()) {
      super.close();
    }
  }

  /**
   * A protected construction that will be used in the static factory
   * method {@link #open(Options, BackupableDBOptions, String)}.
   */
  protected BackupableDB() {
    super();
  }

  @Override protected void finalize() throws Throwable {
    close();
    super.finalize();
  }

  protected native void open(long rocksDBHandle, long backupDBOptionsHandle);
  protected native void createNewBackup(long handle, boolean flag)
      throws RocksDBException;
  protected native void purgeOldBackups(long handle, int numBackupsToKeep)
      throws RocksDBException;
  private native void deleteBackup0(long nativeHandle, int backupId)
      throws RocksDBException;
  protected native List<BackupInfo> getBackupInfo(long handle);
}
