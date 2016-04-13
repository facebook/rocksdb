// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * RestoreOptions to control the behavior of restore.
 *
 * Note that dispose() must be called before this instance become out-of-scope
 * to release the allocated memory in c++.
 *
 */
public class RestoreOptions extends RocksObject {
  /**
   * Constructor
   *
   * @param keepLogFiles If true, restore won't overwrite the existing log files
   *   in wal_dir. It will also move all log files from archive directory to
   *   wal_dir. Use this option in combination with
   *   BackupableDBOptions::backup_log_files = false for persisting in-memory
   *   databases.
   *   Default: false
   */
  public RestoreOptions(final boolean keepLogFiles) {
    super(newRestoreOptions(keepLogFiles));
  }

  private native static long newRestoreOptions(boolean keepLogFiles);
  @Override protected final native void disposeInternal(final long handle);
}
