//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef ROCKSDB_LITE

#include <cinttypes>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "rocksdb/utilities/stackable_db.h"

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// The default DB file checksum function name.
constexpr char kDbFileChecksumFuncName[] = "FileChecksumCrc32c";
// The default BackupEngine file checksum function name.
constexpr char kBackupFileChecksumFuncName[] = "crc32c";

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

  // If share_table_files == true, backup will assume that table files with
  // same name have the same contents. This enables incremental backups and
  // avoids unnecessary data copies.
  // If share_table_files == false, each backup will be on its own and will
  // not share any data with other backups.
  // default: true
  bool share_table_files;

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

  // If false, we won't backup log files. This option can be useful for backing
  // up in-memory databases where log file are persisted, but table files are in
  // memory.
  // Default: true
  bool backup_log_files;

  // Max bytes that can be transferred in a second during backup.
  // If 0, go as fast as you can
  // Default: 0
  uint64_t backup_rate_limit;

  // Backup rate limiter. Used to control transfer speed for backup. If this is
  // not null, backup_rate_limit is ignored.
  // Default: nullptr
  std::shared_ptr<RateLimiter> backup_rate_limiter{nullptr};

  // Max bytes that can be transferred in a second during restore.
  // If 0, go as fast as you can
  // Default: 0
  uint64_t restore_rate_limit;

  // Restore rate limiter. Used to control transfer speed during restore. If
  // this is not null, restore_rate_limit is ignored.
  // Default: nullptr
  std::shared_ptr<RateLimiter> restore_rate_limiter{nullptr};

  // Only used if share_table_files is set to true. Setting to false is
  // DEPRECATED and potentially dangerous because in that case BackupEngine
  // can lose data if backing up databases with distinct or divergent
  // history, for example if restoring from a backup other than the latest,
  // writing to the DB, and creating another backup. Setting to true (default)
  // prevents these issues by ensuring that different table files (SSTs) with
  // the same number are treated as distinct. See
  // share_files_with_checksum_naming and ShareFilesNaming.
  //
  // Default: true
  bool share_files_with_checksum;

  // Up to this many background threads will copy files for CreateNewBackup()
  // and RestoreDBFromBackup()
  // Default: 1
  int max_background_operations;

  // During backup user can get callback every time next
  // callback_trigger_interval_size bytes being copied.
  // Default: 4194304
  uint64_t callback_trigger_interval_size;

  // For BackupEngineReadOnly, Open() will open at most this many of the
  // latest non-corrupted backups.
  //
  // Note: this setting is ignored (behaves like INT_MAX) for any kind of
  // writable BackupEngine because it would inhibit accounting for shared
  // files for proper backup deletion, including purging any incompletely
  // created backups on creation of a new backup.
  //
  // Default: INT_MAX
  int max_valid_backups_to_open;

  // ShareFilesNaming describes possible naming schemes for backup
  // table file names when the table files are stored in the shared_checksum
  // directory (i.e., both share_table_files and share_files_with_checksum
  // are true).
  enum ShareFilesNaming : uint32_t {
    // Backup SST filenames are <file_number>_<crc32c>_<file_size>.sst
    // where <crc32c> is an unsigned decimal integer. This is the
    // original/legacy naming scheme for share_files_with_checksum,
    // with two problems:
    // * At massive scale, collisions on this triple with different file
    //   contents is plausible.
    // * Determining the name to use requires computing the checksum,
    //   so generally requires reading the whole file even if the file
    //   is already backed up.
    // ** ONLY RECOMMENDED FOR PRESERVING OLD BEHAVIOR **
    kLegacyCrc32cAndFileSize = 1U,

    // Backup SST filenames are <file_number>_s<db_session_id>.sst. This
    // pair of values should be very strongly unique for a given SST file
    // and easily determined before computing a checksum. The 's' indicates
    // the value is a DB session id, not a checksum.
    //
    // Exceptions:
    // * For old SST files without a DB session id, kLegacyCrc32cAndFileSize
    //   will be used instead, matching the names assigned by RocksDB versions
    //   not supporting the newer naming scheme.
    // * See also flags below.
    kUseDbSessionId = 2U,

    kMaskNoNamingFlags = 0xffffU,

    // If not already part of the naming scheme, insert
    //   _<file_size>
    // before .sst in the name. In case of user code actually parsing the
    // last _<whatever> before the .sst as the file size, this preserves that
    // feature of kLegacyCrc32cAndFileSize. In other words, this option makes
    // official that unofficial feature of the backup metadata.
    //
    // We do not consider SST file sizes to have sufficient entropy to
    // contribute significantly to naming uniqueness.
    kFlagIncludeFileSize = 1U << 31,

    kMaskNamingFlags = ~kMaskNoNamingFlags,
  };

  // Naming option for share_files_with_checksum table files. See
  // ShareFilesNaming for details.
  //
  // Modifying this option cannot introduce a downgrade compatibility issue
  // because RocksDB can read, restore, and delete backups using different file
  // names, and it's OK for a backup directory to use a mixture of table file
  // naming schemes.
  //
  // However, modifying this option and saving more backups to the same
  // directory can lead to the same file getting saved again to that
  // directory, under the new shared name in addition to the old shared
  // name.
  //
  // Default: kUseDbSessionId | kFlagIncludeFileSize
  //
  // Note: This option comes into effect only if both share_files_with_checksum
  // and share_table_files are true.
  ShareFilesNaming share_files_with_checksum_naming;

  void Dump(Logger* logger) const;

  explicit BackupableDBOptions(
      const std::string& _backup_dir, Env* _backup_env = nullptr,
      bool _share_table_files = true, Logger* _info_log = nullptr,
      bool _sync = true, bool _destroy_old_data = false,
      bool _backup_log_files = true, uint64_t _backup_rate_limit = 0,
      uint64_t _restore_rate_limit = 0, int _max_background_operations = 1,
      uint64_t _callback_trigger_interval_size = 4 * 1024 * 1024,
      int _max_valid_backups_to_open = INT_MAX,
      ShareFilesNaming _share_files_with_checksum_naming =
          static_cast<ShareFilesNaming>(kUseDbSessionId | kFlagIncludeFileSize))
      : backup_dir(_backup_dir),
        backup_env(_backup_env),
        share_table_files(_share_table_files),
        info_log(_info_log),
        sync(_sync),
        destroy_old_data(_destroy_old_data),
        backup_log_files(_backup_log_files),
        backup_rate_limit(_backup_rate_limit),
        restore_rate_limit(_restore_rate_limit),
        share_files_with_checksum(true),
        max_background_operations(_max_background_operations),
        callback_trigger_interval_size(_callback_trigger_interval_size),
        max_valid_backups_to_open(_max_valid_backups_to_open),
        share_files_with_checksum_naming(_share_files_with_checksum_naming) {
    assert(share_table_files || !share_files_with_checksum);
    assert((share_files_with_checksum_naming & kMaskNoNamingFlags) != 0);
  }
};

inline BackupableDBOptions::ShareFilesNaming operator&(
    BackupableDBOptions::ShareFilesNaming lhs,
    BackupableDBOptions::ShareFilesNaming rhs) {
  uint32_t l = static_cast<uint32_t>(lhs);
  uint32_t r = static_cast<uint32_t>(rhs);
  assert(r == BackupableDBOptions::kMaskNoNamingFlags ||
         (r & BackupableDBOptions::kMaskNoNamingFlags) == 0);
  return static_cast<BackupableDBOptions::ShareFilesNaming>(l & r);
}

inline BackupableDBOptions::ShareFilesNaming operator|(
    BackupableDBOptions::ShareFilesNaming lhs,
    BackupableDBOptions::ShareFilesNaming rhs) {
  uint32_t l = static_cast<uint32_t>(lhs);
  uint32_t r = static_cast<uint32_t>(rhs);
  assert((r & BackupableDBOptions::kMaskNoNamingFlags) == 0);
  return static_cast<BackupableDBOptions::ShareFilesNaming>(l | r);
}

struct CreateBackupOptions {
  // Flush will always trigger if 2PC is enabled.
  // If write-ahead logs are disabled, set flush_before_backup=true to
  // avoid losing unflushed key/value pairs from the memtable.
  bool flush_before_backup = false;

  // Callback for reporting progress, based on callback_trigger_interval_size.
  std::function<void()> progress_callback = []() {};

  // If false, background_thread_cpu_priority is ignored.
  // Otherwise, the cpu priority can be decreased,
  // if you try to increase the priority, the priority will not change.
  // The initial priority of the threads is CpuPriority::kNormal,
  // so you can decrease to priorities lower than kNormal.
  bool decrease_background_thread_cpu_priority = false;
  CpuPriority background_thread_cpu_priority = CpuPriority::kNormal;
};

struct RestoreOptions {
  // If true, restore won't overwrite the existing log files in wal_dir. It will
  // also move all log files from archive directory to wal_dir. Use this option
  // in combination with BackupableDBOptions::backup_log_files = false for
  // persisting in-memory databases.
  // Default: false
  bool keep_log_files;

  explicit RestoreOptions(bool _keep_log_files = false)
      : keep_log_files(_keep_log_files) {}
};

struct BackupFileInfo {
  // File name and path relative to the backup_dir directory.
  std::string relative_filename;

  // Size of the file in bytes, not including filesystem overheads.
  uint64_t size;
};

typedef uint32_t BackupID;

struct BackupInfo {
  BackupID backup_id;
  // Creation time, according to GetCurrentTime
  int64_t timestamp;

  // Total size in bytes (based on file payloads, not including filesystem
  // overheads or backup meta file)
  uint64_t size;

  // Number of backed up files, some of which might be shared with other
  // backups. Does not include backup meta file.
  uint32_t number_files;

  // Backup API user metadata
  std::string app_metadata;

  // Backup file details, if requested
  std::vector<BackupFileInfo> file_details;

  BackupInfo() {}

  BackupInfo(BackupID _backup_id, int64_t _timestamp, uint64_t _size,
             uint32_t _number_files, const std::string& _app_metadata)
      : backup_id(_backup_id),
        timestamp(_timestamp),
        size(_size),
        number_files(_number_files),
        app_metadata(_app_metadata) {}
};

class BackupStatistics {
 public:
  BackupStatistics() {
    number_success_backup = 0;
    number_fail_backup = 0;
  }

  BackupStatistics(uint32_t _number_success_backup,
                   uint32_t _number_fail_backup)
      : number_success_backup(_number_success_backup),
        number_fail_backup(_number_fail_backup) {}

  ~BackupStatistics() {}

  void IncrementNumberSuccessBackup();
  void IncrementNumberFailBackup();

  uint32_t GetNumberSuccessBackup() const;
  uint32_t GetNumberFailBackup() const;

  std::string ToString() const;

 private:
  uint32_t number_success_backup;
  uint32_t number_fail_backup;
};

// A backup engine for accessing information about backups and restoring from
// them.
// BackupEngineReadOnly is not extensible.
class BackupEngineReadOnly {
 public:
  virtual ~BackupEngineReadOnly() {}

  static Status Open(const BackupableDBOptions& options, Env* db_env,
                     BackupEngineReadOnly** backup_engine_ptr);
  // keep for backward compatibility.
  static Status Open(Env* db_env, const BackupableDBOptions& options,
                     BackupEngineReadOnly** backup_engine_ptr) {
    return BackupEngineReadOnly::Open(options, db_env, backup_engine_ptr);
  }

  // Returns info about backups in backup_info
  // You can GetBackupInfo safely, even with other BackupEngine performing
  // backups on the same directory.
  // Setting include_file_details=true provides information about each
  // backed-up file in BackupInfo::file_details.
  virtual void GetBackupInfo(std::vector<BackupInfo>* backup_info,
                             bool include_file_details = false) const = 0;

  // Returns info about corrupt backups in corrupt_backups
  virtual void GetCorruptedBackups(
      std::vector<BackupID>* corrupt_backup_ids) const = 0;

  // Restoring DB from backup is NOT safe when there is another BackupEngine
  // running that might call DeleteBackup() or PurgeOldBackups(). It is caller's
  // responsibility to synchronize the operation, i.e. don't delete the backup
  // when you're restoring from it
  // See also the corresponding doc in BackupEngine
  virtual Status RestoreDBFromBackup(const RestoreOptions& options,
                                     BackupID backup_id,
                                     const std::string& db_dir,
                                     const std::string& wal_dir) = 0;

  // keep for backward compatibility.
  virtual Status RestoreDBFromBackup(
      BackupID backup_id, const std::string& db_dir, const std::string& wal_dir,
      const RestoreOptions& options = RestoreOptions()) {
    return RestoreDBFromBackup(options, backup_id, db_dir, wal_dir);
  }

  // See the corresponding doc in BackupEngine
  virtual Status RestoreDBFromLatestBackup(const RestoreOptions& options,
                                           const std::string& db_dir,
                                           const std::string& wal_dir) = 0;

  // keep for backward compatibility.
  virtual Status RestoreDBFromLatestBackup(
      const std::string& db_dir, const std::string& wal_dir,
      const RestoreOptions& options = RestoreOptions()) {
    return RestoreDBFromLatestBackup(options, db_dir, wal_dir);
  }

  // If verify_with_checksum is true, this function
  // inspects the current checksums and file sizes of backup files to see if
  // they match our expectation.
  //
  // If verify_with_checksum is false, this function
  // checks that each file exists and that the size of the file matches our
  // expectation. It does not check file checksum.
  //
  // If this BackupEngine created the backup, it compares the files' current
  // sizes (and current checksum) against the number of bytes written to
  // them (and the checksum calculated) during creation.
  // Otherwise, it compares the files' current sizes (and checksums) against
  // their sizes (and checksums) when the BackupEngine was opened.
  //
  // Returns Status::OK() if all checks are good
  virtual Status VerifyBackup(BackupID backup_id,
                              bool verify_with_checksum = false) = 0;
};

// A backup engine for creating new backups.
// BackupEngine is not extensible.
class BackupEngine {
 public:
  virtual ~BackupEngine() {}

  // BackupableDBOptions have to be the same as the ones used in previous
  // BackupEngines for the same backup directory.
  static Status Open(const BackupableDBOptions& options, Env* db_env,
                     BackupEngine** backup_engine_ptr);

  // keep for backward compatibility.
  static Status Open(Env* db_env, const BackupableDBOptions& options,
                     BackupEngine** backup_engine_ptr) {
    return BackupEngine::Open(options, db_env, backup_engine_ptr);
  }

  // same as CreateNewBackup, but stores extra application metadata.
  virtual Status CreateNewBackupWithMetadata(
      const CreateBackupOptions& options, DB* db,
      const std::string& app_metadata) = 0;

  // keep here for backward compatibility.
  virtual Status CreateNewBackupWithMetadata(
      DB* db, const std::string& app_metadata, bool flush_before_backup = false,
      std::function<void()> progress_callback = []() {}) {
    CreateBackupOptions options;
    options.flush_before_backup = flush_before_backup;
    options.progress_callback = progress_callback;
    return CreateNewBackupWithMetadata(options, db, app_metadata);
  }

  // Captures the state of the database in the latest backup
  // NOT a thread safe call
  virtual Status CreateNewBackup(const CreateBackupOptions& options, DB* db) {
    return CreateNewBackupWithMetadata(options, db, "");
  }

  // keep here for backward compatibility.
  virtual Status CreateNewBackup(DB* db, bool flush_before_backup = false,
                                 std::function<void()> progress_callback =
                                     []() {}) {
    CreateBackupOptions options;
    options.flush_before_backup = flush_before_backup;
    options.progress_callback = progress_callback;
    return CreateNewBackup(options, db);
  }

  // Deletes old backups, keeping latest num_backups_to_keep alive.
  // See also DeleteBackup.
  virtual Status PurgeOldBackups(uint32_t num_backups_to_keep) = 0;

  // Deletes a specific backup. If this operation (or PurgeOldBackups)
  // is not completed due to crash, power failure, etc. the state
  // will be cleaned up the next time you call DeleteBackup,
  // PurgeOldBackups, or GarbageCollect.
  virtual Status DeleteBackup(BackupID backup_id) = 0;

  // Call this from another thread if you want to stop the backup
  // that is currently happening. It will return immediatelly, will
  // not wait for the backup to stop.
  // The backup will stop ASAP and the call to CreateNewBackup will
  // return Status::Incomplete(). It will not clean up after itself, but
  // the state will remain consistent. The state will be cleaned up the
  // next time you call CreateNewBackup or GarbageCollect.
  virtual void StopBackup() = 0;

  // Returns info about backups in backup_info
  virtual void GetBackupInfo(std::vector<BackupInfo>* backup_info,
                             bool include_file_details = false) const = 0;

  // Returns info about corrupt backups in corrupt_backups
  virtual void GetCorruptedBackups(
      std::vector<BackupID>* corrupt_backup_ids) const = 0;

  // restore from backup with backup_id
  // IMPORTANT -- if options_.share_table_files == true,
  // options_.share_files_with_checksum == false, you restore DB from some
  // backup that is not the latest, and you start creating new backups from the
  // new DB, they will probably fail.
  //
  // Example: Let's say you have backups 1, 2, 3, 4, 5 and you restore 3.
  // If you add new data to the DB and try creating a new backup now, the
  // database will diverge from backups 4 and 5 and the new backup will fail.
  // If you want to create new backup, you will first have to delete backups 4
  // and 5.
  virtual Status RestoreDBFromBackup(const RestoreOptions& options,
                                     BackupID backup_id,
                                     const std::string& db_dir,
                                     const std::string& wal_dir) = 0;

  // keep for backward compatibility.
  virtual Status RestoreDBFromBackup(
      BackupID backup_id, const std::string& db_dir, const std::string& wal_dir,
      const RestoreOptions& options = RestoreOptions()) {
    return RestoreDBFromBackup(options, backup_id, db_dir, wal_dir);
  }

  // restore from the latest backup
  virtual Status RestoreDBFromLatestBackup(const RestoreOptions& options,
                                           const std::string& db_dir,
                                           const std::string& wal_dir) = 0;

  // keep for backward compatibility.
  virtual Status RestoreDBFromLatestBackup(
      const std::string& db_dir, const std::string& wal_dir,
      const RestoreOptions& options = RestoreOptions()) {
    return RestoreDBFromLatestBackup(options, db_dir, wal_dir);
  }

  // If verify_with_checksum is true, this function
  // inspects the current checksums and file sizes of backup files to see if
  // they match our expectation.
  //
  // If verify_with_checksum is false, this function
  // checks that each file exists and that the size of the file matches our
  // expectation. It does not check file checksum.
  //
  // Returns Status::OK() if all checks are good
  virtual Status VerifyBackup(BackupID backup_id,
                              bool verify_with_checksum = false) = 0;

  // Will delete any files left over from incomplete creation or deletion of
  // a backup. This is not normally needed as those operations also clean up
  // after prior incomplete calls to the same kind of operation (create or
  // delete).
  // NOTE: This is not designed to delete arbitrary files added to the backup
  // directory outside of BackupEngine, and clean-up is always subject to
  // permissions on and availability of the underlying filesystem.
  virtual Status GarbageCollect() = 0;
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
