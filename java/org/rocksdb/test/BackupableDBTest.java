// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb.test;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BackupableDBTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule
  public TemporaryFolder backupFolder = new TemporaryFolder();

  @Test
  public void backupableDb() throws RocksDBException {
    Options opt = null;
    BackupableDBOptions bopt = null;
    BackupableDB bdb = null;
    RestoreOptions ropt = null;
    RestoreBackupableDB rdb = null;
    try {
      opt = new Options();
      opt.setCreateIfMissing(true);

      bopt = new BackupableDBOptions(
          backupFolder.getRoot().getAbsolutePath(), false,
          true, false, true, 0, 0);
      assertThat(bopt.backupDir()).isEqualTo(
          backupFolder.getRoot().getAbsolutePath());

      List<BackupInfo> backupInfos;
      List<BackupInfo> restoreInfos;

      bdb = BackupableDB.open(opt, bopt,
          dbFolder.getRoot().getAbsolutePath());
      bdb.put("abc".getBytes(), "def".getBytes());
      bdb.put("ghi".getBytes(), "jkl".getBytes());

      backupInfos = bdb.getBackupInfos();
      assertThat(backupInfos.size()).
          isEqualTo(0);

      bdb.createNewBackup(true);
      backupInfos = bdb.getBackupInfos();
      assertThat(backupInfos.size()).
          isEqualTo(1);

      // Retrieving backup infos twice shall not
      // lead to different results
      List<BackupInfo> tmpBackupInfo = bdb.getBackupInfos();
      assertThat(tmpBackupInfo.get(0).backupId()).
          isEqualTo(backupInfos.get(0).backupId());
      assertThat(tmpBackupInfo.get(0).timestamp()).
          isEqualTo(backupInfos.get(0).timestamp());
      assertThat(tmpBackupInfo.get(0).size()).
          isEqualTo(backupInfos.get(0).size());
      assertThat(tmpBackupInfo.get(0).numberFiles()).
          isEqualTo(backupInfos.get(0).numberFiles());

      // delete record after backup
      bdb.remove("abc".getBytes());
      byte[] value = bdb.get("abc".getBytes());
      assertThat(value).isNull();
      bdb.close();

      // restore from backup
      ropt = new RestoreOptions(false);
      rdb = new RestoreBackupableDB(bopt);

      // getting backup infos from restorable db should
      // lead to the same infos as from backupable db
      restoreInfos = rdb.getBackupInfos();
      assertThat(restoreInfos.size()).
          isEqualTo(backupInfos.size());
      assertThat(restoreInfos.get(0).backupId()).
          isEqualTo(backupInfos.get(0).backupId());
      assertThat(restoreInfos.get(0).timestamp()).
          isEqualTo(backupInfos.get(0).timestamp());
      assertThat(restoreInfos.get(0).size()).
          isEqualTo(backupInfos.get(0).size());
      assertThat(restoreInfos.get(0).numberFiles()).
          isEqualTo(backupInfos.get(0).numberFiles());

      rdb.restoreDBFromLatestBackup(
          dbFolder.getRoot().getAbsolutePath(),
          dbFolder.getRoot().getAbsolutePath(),
          ropt);
      // do nothing because there is only one backup
      rdb.purgeOldBackups(1);
      restoreInfos = rdb.getBackupInfos();
      assertThat(restoreInfos.size()).
          isEqualTo(1);
      rdb.dispose();
      ropt.dispose();

      // verify that backed up data contains deleted record
      bdb = BackupableDB.open(opt, bopt,
          dbFolder.getRoot().getAbsolutePath());
      value = bdb.get("abc".getBytes());
      assertThat(new String(value)).
          isEqualTo("def");

      bdb.createNewBackup(false);
      // after new backup there must be two backup infos
      backupInfos = bdb.getBackupInfos();
      assertThat(backupInfos.size()).
          isEqualTo(2);
      // deleting the backup must be possible using the
      // id provided by backup infos
      bdb.deleteBackup(backupInfos.get(1).backupId());
      // after deletion there should only be one info
      backupInfos = bdb.getBackupInfos();
      assertThat(backupInfos.size()).
          isEqualTo(1);
      bdb.createNewBackup(false);
      bdb.createNewBackup(false);
      bdb.createNewBackup(false);
      backupInfos = bdb.getBackupInfos();
      assertThat(backupInfos.size()).
          isEqualTo(4);
      // purge everything and keep two
      bdb.purgeOldBackups(2);
      // backup infos need to be two
      backupInfos = bdb.getBackupInfos();
      assertThat(backupInfos.size()).
          isEqualTo(2);
      assertThat(backupInfos.get(0).backupId()).
          isEqualTo(4);
      assertThat(backupInfos.get(1).backupId()).
          isEqualTo(5);
    } finally {
      if (opt != null) {
        opt.dispose();
      }
      if (bopt != null) {
        bopt.dispose();
      }
      if (bdb != null) {
        bdb.close();
      }
      if (ropt != null) {
        ropt.dispose();
      }
      if (rdb != null) {
        rdb.dispose();
      }
    }
  }
}
