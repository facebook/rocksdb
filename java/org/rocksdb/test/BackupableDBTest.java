// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.*;

public class BackupableDBTest {
  static final String db_path = "/tmp/backupablejni_db";
  static final String backup_path = "/tmp/backupablejni_db_backup";
  static {
    RocksDB.loadLibrary();
  }
  public static void main(String[] args) {

    Options opt = new Options();
    opt.setCreateIfMissing(true);

    BackupableDBOptions bopt = new BackupableDBOptions(backup_path, false,
        true, false, true, 0, 0);
    BackupableDB bdb = null;

    try {
      bdb = BackupableDB.open(opt, bopt, db_path);

      bdb.put("abc".getBytes(), "def".getBytes());
      bdb.put("ghi".getBytes(), "jkl".getBytes());
      bdb.createNewBackup(true);

      // delete record after backup
      bdb.remove("abc".getBytes());
      byte[] value = bdb.get("abc".getBytes());
      assert(value == null);
      bdb.close();

      // restore from backup
      RestoreOptions ropt = new RestoreOptions(false);
      RestoreBackupableDB rdb = new RestoreBackupableDB(bopt);
      rdb.restoreDBFromLatestBackup(db_path, db_path,
          ropt);
      rdb.dispose();
      ropt.dispose();

      // verify that backed up data contains deleted record
      bdb = BackupableDB.open(opt, bopt, db_path);
      value = bdb.get("abc".getBytes());
      assert(new String(value).equals("def"));

      System.out.println("Backup and restore test passed");
    } catch (RocksDBException e) {
      System.err.format("[ERROR]: %s%n", e);
      e.printStackTrace();
    } finally {
      opt.dispose();
      bopt.dispose();
      if (bdb != null) {
        bdb.close();
      }
    }
  }
}
