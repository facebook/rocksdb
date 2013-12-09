//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "utilities/stackable_db.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

#include <string>
#include <map>
#include <vector>

namespace rocksdb {

struct BackupableDBOptions {
  // Where to keep the backup files. Has to be different than dbname_
  // Best to set this to dbname_ + "/backups"
  // Required
  std::string backup_dir;

  // Backup Env object. It will be used for backup file I/O. If it's
  // nullptr, backups will be written out using DBs Env. If it's
  // non-nullptr, backup's I/O will be performed using this object.
  // If you want to have backups on HDFS, use HDFS Env here!
  // Default: nullptr
  Env* backup_env;

  // Backup info and error messages will be written to info_log
  // if non-nullptr.
  // Default: nullptr
  Logger* info_log;

  // If sync == true, we can guarantee you'll get consistent backup even
  // on a machine crash/reboot. Backup process is slower with sync enabled.
  // If sync == false, we don't guarantee anything on machine reboot. However,
  // chances are some of the backups are consistent.
  // Default: true
  bool sync;

  // If true, it will delete whatever backups there are already
  // Default: false
  bool destroy_old_data;

  explicit BackupableDBOptions(const std::string& _backup_dir,
                               Env* _backup_env = nullptr,
                               Logger* _info_log = nullptr,
                               bool _sync = true,
                               bool _destroy_old_data = false) :
      backup_dir(_backup_dir),
      backup_env(_backup_env),
      info_log(_info_log),
      sync(_sync),
      destroy_old_data(_destroy_old_data) { }
};

class BackupEngine;

typedef uint32_t BackupID;

struct BackupInfo {
  BackupID backup_id;
  int64_t timestamp;
  uint64_t size;

  BackupInfo() {}
  BackupInfo(BackupID _backup_id, int64_t _timestamp, uint64_t _size)
      : backup_id(_backup_id), timestamp(_timestamp), size(_size) {}
};

// Stack your DB with BackupableDB to be able to backup the DB
class BackupableDB : public StackableDB {
 public:
  // BackupableDBOptions have to be the same as the ones used in a previous
  // incarnation of the DB
  BackupableDB(DB* db, const BackupableDBOptions& options);
  virtual ~BackupableDB();

  // Captures the state of the database in the latest backup
  // NOT a thread safe call
  Status CreateNewBackup(bool flush_before_backup = false);
  // Returns info about backups in backup_info
  void GetBackupInfo(std::vector<BackupInfo>* backup_info);
  // deletes old backups, keeping latest num_backups_to_keep alive
  Status PurgeOldBackups(uint32_t num_backups_to_keep);
  // deletes a specific backup
  Status DeleteBackup(BackupID backup_id);

 private:
  BackupEngine* backup_engine_;
};

// Use this class to access information about backups and restore from them
class RestoreBackupableDB {
  public:
   RestoreBackupableDB(Env* db_env, const BackupableDBOptions& options);
   ~RestoreBackupableDB();

   // Returns info about backups in backup_info
   void GetBackupInfo(std::vector<BackupInfo>* backup_info);

   // restore from backup with backup_id
   // IMPORTANT -- if you restore from some backup that is not the latest,
   // you HAVE to delete all the newer backups immediately, before creating
   // new backup on the restored database. Otherwise, your new backups
   // will be corrupted.
   // TODO should we enforce this somehow?
   Status RestoreDBFromBackup(BackupID backup_id, const std::string& db_dir,
                              const std::string& wal_dir);

   // restore from the latest backup
   Status RestoreDBFromLatestBackup(const std::string& db_dir,
                                    const std::string& wal_dir);
   // deletes old backups, keeping latest num_backups_to_keep alive
   Status PurgeOldBackups(uint32_t num_backups_to_keep);
   // deletes a specific backup
   Status DeleteBackup(BackupID backup_id);

 private:
  BackupEngine* backup_engine_;
};

} // rocksdb namespace
