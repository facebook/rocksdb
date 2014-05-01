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
 */
public class BackupableDBOptions extends RocksObject {
  public BackupableDBOptions(String path) {
    super();
    newBackupableDBOptions(path);
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
  @Override public synchronized void dispose() {
    if (isInitialized()) {
      dispose(nativeHandle_);
    }
  }

  private native void newBackupableDBOptions(String path);
  private native String backupDir(long handle);
  private native void dispose(long handle);
}
