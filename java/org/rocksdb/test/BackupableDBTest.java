// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.rocksdb.*;

import java.util.List;

public class BackupableDBTest {
  static final String db_path = "/tmp/rocksdbjni_backupable_db_test";
  static final String backup_path = "/tmp/rocksdbjni_backupable_db_backup_test";
  static {
    RocksDB.loadLibrary();
  }
  public static void main(String[] args) {

    Options opt = new Options();
    opt.setCreateIfMissing(true);

    BackupableDBOptions bopt = new BackupableDBOptions(backup_path, false,
        true, false, true, 0, 0);
    BackupableDB bdb = null;
    List<BackupInfo> backupInfos;
    List<BackupInfo> restoreInfos;
    try {
      bdb = BackupableDB.open(opt, bopt, db_path);

      bdb.put("abc".getBytes(), "def".getBytes());
      bdb.put("ghi".getBytes(), "jkl".getBytes());

      backupInfos = bdb.getBackupInfos();
      assert(backupInfos.size() == 0);

      bdb.createNewBackup(true);

      backupInfos = bdb.getBackupInfos();
      assert(backupInfos.size() == 1);

      // Retrieving backup infos twice shall not
      // lead to different results
      List<BackupInfo> tmpBackupInfo = bdb.getBackupInfos();
      assert(tmpBackupInfo.get(0).backupId() ==
          backupInfos.get(0).backupId());
      assert(tmpBackupInfo.get(0).timestamp() ==
          backupInfos.get(0).timestamp());
      assert(tmpBackupInfo.get(0).size() ==
          backupInfos.get(0).size());
      assert(tmpBackupInfo.get(0).numberFiles() ==
          backupInfos.get(0).numberFiles());

      // delete record after backup
      bdb.remove("abc".getBytes());
      byte[] value = bdb.get("abc".getBytes());
      assert(value == null);
      bdb.close();

      // restore from backup
      RestoreOptions ropt = new RestoreOptions(false);
      RestoreBackupableDB rdb = new RestoreBackupableDB(bopt);

      // getting backup infos from restorable db should
      // lead to the same infos as from backupable db
      restoreInfos = rdb.getBackupInfos();
      assert(restoreInfos.size() == backupInfos.size());
      assert(restoreInfos.get(0).backupId() ==
          backupInfos.get(0).backupId());
      assert(restoreInfos.get(0).timestamp() ==
          backupInfos.get(0).timestamp());
      assert(restoreInfos.get(0).size() ==
          backupInfos.get(0).size());
      assert(restoreInfos.get(0).numberFiles() ==
          backupInfos.get(0).numberFiles());

      rdb.restoreDBFromLatestBackup(db_path, db_path,
          ropt);
      // do nothing because there is only one backup
      rdb.purgeOldBackups(1);
      restoreInfos = rdb.getBackupInfos();
      assert(restoreInfos.size() == 1);
      rdb.dispose();
      ropt.dispose();

      // verify that backed up data contains deleted record
      bdb = BackupableDB.open(opt, bopt, db_path);
      value = bdb.get("abc".getBytes());
      assert(new String(value).equals("def"));

      bdb.createNewBackup(false);
      // after new backup there must be two backup infos
      backupInfos = bdb.getBackupInfos();
      assert(backupInfos.size() == 2);
      // deleting the backup must be possible using the
      // id provided by backup infos
      bdb.deleteBackup(backupInfos.get(1).backupId());
      // after deletion there should only be one info
      backupInfos = bdb.getBackupInfos();
      assert(backupInfos.size() == 1);
      bdb.createNewBackup(false);
      bdb.createNewBackup(false);
      bdb.createNewBackup(false);
      backupInfos = bdb.getBackupInfos();
      assert(backupInfos.size() == 4);
      // purge everything and keep two
      bdb.purgeOldBackups(2);
      // backup infos need to be two
      backupInfos = bdb.getBackupInfos();
      assert(backupInfos.size() == 2);
      assert(backupInfos.get(0).backupId() == 4);
      assert(backupInfos.get(1).backupId() == 5);
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
