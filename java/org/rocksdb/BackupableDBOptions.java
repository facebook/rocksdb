// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * BackupableDBOptions to control the behavior of a backupable database.
 * It will be used during the creation of a BackupableDB.
 *
 * Note that dispose() must be called before an Options instance
 * become out-of-scope to release the allocated memory in c++.
 *
 * @param path Where to keep the backup files. Has to be different than dbname.
       Best to set this to dbname_ + "/backups"
 * @param shareTableFiles If share_table_files == true, backup will assume that
 *     table files with same name have the same contents. This enables
 *     incremental backups and avoids unnecessary data copies. If
 *     share_table_files == false, each backup will be on its own and will not
 *     share any data with other backups. default: true
 * @param sync If sync == true, we can guarantee you'll get consistent backup
 *     even on a machine crash/reboot. Backup process is slower with sync
 *     enabled. If sync == false, we don't guarantee anything on machine reboot.
 *     However, chances are some of the backups are consistent. Default: true
 * @param destroyOldData If true, it will delete whatever backups there are
 *     already. Default: false
 * @param backupLogFiles If false, we won't backup log files. This option can be
 *     useful for backing up in-memory databases where log file are persisted,
 *     but table files are in memory. Default: true
 * @param backupRateLimit Max bytes that can be transferred in a second during
 *     backup. If 0 or negative, then go as fast as you can. Default: 0
 * @param restoreRateLimit Max bytes that can be transferred in a second during
 *     restore. If 0 or negative, then go as fast as you can. Default: 0
 */
public class BackupableDBOptions extends RocksObject {
  public BackupableDBOptions(String path, boolean shareTableFiles, boolean sync,
      boolean destroyOldData, boolean backupLogFiles, long backupRateLimit,
      long restoreRateLimit) {
    super();

    backupRateLimit = (backupRateLimit <= 0) ? 0 : backupRateLimit;
    restoreRateLimit = (restoreRateLimit <= 0) ? 0 : restoreRateLimit;

    newBackupableDBOptions(path, shareTableFiles, sync, destroyOldData,
        backupLogFiles, backupRateLimit, restoreRateLimit);
  }

  /**
   * Returns the path to the BackupableDB directory.
   *
   * @return the path to the BackupableDB directory.
   */
  public String backupDir() {
    assert(isInitialized());
    return backupDir(nativeHandle_);
  }

  /**
   * Release the memory allocated for the current instance
   * in the c++ side.
   */
  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  private native void newBackupableDBOptions(String path,
      boolean shareTableFiles, boolean sync, boolean destroyOldData,
      boolean backupLogFiles, long backupRateLimit, long restoreRateLimit);
  private native String backupDir(long handle);
  private native void disposeInternal(long handle);
}
