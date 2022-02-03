//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <string>

#include "port/port.h"

#include "db/compaction/compaction.h"
#include "file/delete_scheduler.h"
#include "rocksdb/sst_file_manager.h"

namespace ROCKSDB_NAMESPACE {
class ErrorHandler;
class FileSystem;
class SystemClock;
class Logger;

// SstFileManager is used to track SST and blob files in the DB and control
// their deletion rate. All SstFileManager public functions are thread-safe.
class SstFileManagerImpl : public SstFileManager {
 public:
  explicit SstFileManagerImpl(const std::shared_ptr<SystemClock>& clock,
                              const std::shared_ptr<FileSystem>& fs,
                              const std::shared_ptr<Logger>& logger,
                              int64_t rate_bytes_per_sec,
                              double max_trash_db_ratio,
                              uint64_t bytes_max_delete_chunk);

  ~SstFileManagerImpl();

  // DB will call OnAddFile whenever a new sst/blob file is added.
  Status OnAddFile(const std::string& file_path);

  // Overload where size of the file is provided by the caller rather than
  // queried from the filesystem. This is an optimization.
  Status OnAddFile(const std::string& file_path, uint64_t file_size);

  // DB will call OnDeleteFile whenever a sst/blob file is deleted.
  Status OnDeleteFile(const std::string& file_path);

  // DB will call OnMoveFile whenever a sst/blob file is move to a new path.
  Status OnMoveFile(const std::string& old_path, const std::string& new_path,
                    uint64_t* file_size = nullptr);

  // Update the maximum allowed space that should be used by RocksDB, if
  // the total size of the SST and blob files exceeds max_allowed_space, writes
  // to RocksDB will fail.
  //
  // Setting max_allowed_space to 0 will disable this feature, maximum allowed
  // space will be infinite (Default value).
  //
  // thread-safe.
  void SetMaxAllowedSpaceUsage(uint64_t max_allowed_space) override;

  void SetCompactionBufferSize(uint64_t compaction_buffer_size) override;

  // Return true if the total size of SST and blob files exceeded the maximum
  // allowed space usage.
  //
  // thread-safe.
  bool IsMaxAllowedSpaceReached() override;

  bool IsMaxAllowedSpaceReachedIncludingCompactions() override;

  // Returns true is there is enough (approximate) space for the specified
  // compaction. Space is approximate because this function conservatively
  // estimates how much space is currently being used by compactions (i.e.
  // if a compaction has started, this function bumps the used space by
  // the full compaction size).
  bool EnoughRoomForCompaction(ColumnFamilyData* cfd,
                               const std::vector<CompactionInputFiles>& inputs,
                               const Status& bg_error);

  // Bookkeeping so total_file_sizes_ goes back to normal after compaction
  // finishes
  void OnCompactionCompletion(Compaction* c);

  uint64_t GetCompactionsReservedSize();

  // Return the total size of all tracked files.
  uint64_t GetTotalSize() override;

  // Return a map containing all tracked files and there corresponding sizes.
  std::unordered_map<std::string, uint64_t> GetTrackedFiles() override;

  // Return delete rate limit in bytes per second.
  virtual int64_t GetDeleteRateBytesPerSecond() override;

  // Update the delete rate limit in bytes per second.
  virtual void SetDeleteRateBytesPerSecond(int64_t delete_rate) override;

  // Return trash/DB size ratio where new files will be deleted immediately
  virtual double GetMaxTrashDBRatio() override;

  // Update trash/DB size ratio where new files will be deleted immediately
  virtual void SetMaxTrashDBRatio(double ratio) override;

  // Return the total size of trash files
  uint64_t GetTotalTrashSize() override;

  // Called by each DB instance using this sst file manager to reserve
  // disk buffer space for recovery from out of space errors
  void ReserveDiskBuffer(uint64_t buffer, const std::string& path);

  // Set a flag upon encountering disk full. May enqueue the ErrorHandler
  // instance for background polling and recovery
  void StartErrorRecovery(ErrorHandler* db, Status bg_error);

  // Remove the given Errorhandler instance from the recovery queue. Its
  // not guaranteed
  bool CancelErrorRecovery(ErrorHandler* db);

  // Mark file as trash and schedule it's deletion. If force_bg is set, it
  // forces the file to be deleting in the background regardless of DB size,
  // except when rate limited delete is disabled
  virtual Status ScheduleFileDeletion(const std::string& file_path,
                                      const std::string& dir_to_sync,
                                      const bool force_bg = false);

  // Wait for all files being deleteing in the background to finish or for
  // destructor to be called.
  virtual void WaitForEmptyTrash();

  DeleteScheduler* delete_scheduler() { return &delete_scheduler_; }

  // Stop the error recovery background thread. This should be called only
  // once in the object's lifetime, and before the destructor
  void Close();

  void SetStatisticsPtr(const std::shared_ptr<Statistics>& stats) override {
    stats_ = stats;
    delete_scheduler_.SetStatisticsPtr(stats);
  }

 private:
  // REQUIRES: mutex locked
  void OnAddFileImpl(const std::string& file_path, uint64_t file_size);
  // REQUIRES: mutex locked
  void OnDeleteFileImpl(const std::string& file_path);

  void ClearError();
  bool CheckFreeSpace() {
    return bg_err_.severity() == Status::Severity::kSoftError;
  }

  std::shared_ptr<SystemClock> clock_;
  std::shared_ptr<FileSystem> fs_;
  std::shared_ptr<Logger> logger_;
  // Mutex to protect tracked_files_, total_files_size_
  port::Mutex mu_;
  // The summation of the sizes of all files in tracked_files_ map
  uint64_t total_files_size_;
  // Compactions should only execute if they can leave at least
  // this amount of buffer space for logs and flushes
  uint64_t compaction_buffer_size_;
  // Estimated size of the current ongoing compactions
  uint64_t cur_compactions_reserved_size_;
  // A map containing all tracked files and there sizes
  //  file_path => file_size
  std::unordered_map<std::string, uint64_t> tracked_files_;
  // The maximum allowed space (in bytes) for sst and blob files.
  uint64_t max_allowed_space_;
  // DeleteScheduler used to throttle file deletition.
  DeleteScheduler delete_scheduler_;
  port::CondVar cv_;
  // Flag to force error recovery thread to exit
  bool closing_;
  // Background error recovery thread
  std::unique_ptr<port::Thread> bg_thread_;
  // A path in the filesystem corresponding to this SFM. This is used for
  // calling Env::GetFreeSpace. Posix requires a path in the filesystem
  std::string path_;
  // Save the current background error
  Status bg_err_;
  // Amount of free disk headroom before allowing recovery from hard errors
  uint64_t reserved_disk_buffer_;
  // For soft errors, amount of free disk space before we can allow
  // compactions to run full throttle. If disk space is below this trigger,
  // compactions will be gated by free disk space > input size
  uint64_t free_space_trigger_;
  // List of database error handler instances tracked by this SstFileManager.
  std::list<ErrorHandler*> error_handler_list_;
  // Pointer to ErrorHandler instance that is currently processing recovery
  ErrorHandler* cur_instance_;
  std::shared_ptr<Statistics> stats_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
