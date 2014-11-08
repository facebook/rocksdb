// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.List;

/**
 * This class is used to access information about backups and restore from them.
 *
 * Note that dispose() must be called before this instance become out-of-scope
 * to release the allocated memory in c++.
 *
 */
public class RestoreBackupableDB extends RocksObject {
  /**
   * Constructor
   *
   * @param options {@link org.rocksdb.BackupableDBOptions} instance
   */
  public RestoreBackupableDB(BackupableDBOptions options) {
    super();
    nativeHandle_ = newRestoreBackupableDB(options.nativeHandle_);
  }

  /**
   * Restore from backup with backup_id
   * IMPORTANT -- if options_.share_table_files == true and you restore DB
   * from some backup that is not the latest, and you start creating new
   * backups from the new DB, they will probably fail.
   *
   * Example: Let's say you have backups 1, 2, 3, 4, 5 and you restore 3.
   * If you add new data to the DB and try creating a new backup now, the
   * database will diverge from backups 4 and 5 and the new backup will fail.
   * If you want to create new backup, you will first have to delete backups 4
   * and 5.
   *
   * @param backupId id pointing to backup
   * @param dbDir database directory to restore to
   * @param walDir directory where wal files are located
   * @param restoreOptions {@link org.rocksdb.RestoreOptions} instance.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void restoreDBFromBackup(long backupId, String dbDir, String walDir,
      RestoreOptions restoreOptions) throws RocksDBException {
    restoreDBFromBackup0(nativeHandle_, backupId, dbDir, walDir,
        restoreOptions.nativeHandle_);
  }

  /**
   * Restore from the latest backup.
   *
   * @param dbDir database directory to restore to
   * @param walDir directory where wal files are located
   * @param restoreOptions {@link org.rocksdb.RestoreOptions} instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void restoreDBFromLatestBackup(String dbDir, String walDir,
      RestoreOptions restoreOptions) throws RocksDBException {
    restoreDBFromLatestBackup0(nativeHandle_, dbDir, walDir,
        restoreOptions.nativeHandle_);
  }

  /**
   * Deletes old backups, keeping latest numBackupsToKeep alive.
   *
   * @param numBackupsToKeep of latest backups to keep
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void purgeOldBackups(int numBackupsToKeep) throws RocksDBException {
    purgeOldBackups0(nativeHandle_, numBackupsToKeep);
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
   * Release the memory allocated for the current instance
   * in the c++ side.
   */
  @Override public synchronized void disposeInternal() {
    assert(isInitialized());
    dispose(nativeHandle_);
  }

  private native long newRestoreBackupableDB(long options);
  private native void restoreDBFromBackup0(long nativeHandle, long backupId,
      String dbDir, String walDir, long restoreOptions)
      throws RocksDBException;
  private native void restoreDBFromLatestBackup0(long nativeHandle,
      String dbDir, String walDir, long restoreOptions)
      throws RocksDBException;
  private native void purgeOldBackups0(long nativeHandle, int numBackupsToKeep)
      throws RocksDBException;
  private native void deleteBackup0(long nativeHandle, int backupId)
      throws RocksDBException;
  protected native List<BackupInfo> getBackupInfo(long handle);
  private native void dispose(long nativeHandle);
}
