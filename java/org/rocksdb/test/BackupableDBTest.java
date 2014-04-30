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

    BackupableDBOptions bopt = new BackupableDBOptions(backup_path);
    BackupableDB bdb = null;

    try {
      bdb = BackupableDB.open(opt, bopt, db_path);
      bdb.put("hello".getBytes(), "BackupableDB".getBytes());
      bdb.createNewBackup(true);
      byte[] value = bdb.get("hello".getBytes());
      assert(new String(value).equals("BackupableDB"));
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
