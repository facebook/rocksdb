// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A checkpoint is an openable snapshot of a database at a point in time.

#pragma once

#include <functional>
#include <optional>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class DB;
class ColumnFamilyHandle;
struct LiveFileMetaData;
class Logger;
struct ExportImportFilesMetaData;

struct CheckpointEngineOptions {
  // Backup Env object. It will be used for backup file I/O. If it's
  // nullptr, backups will be written out using DBs Env. If it's
  // non-nullptr, backup's I/O will be performed using this object.
  // For CheckpointEngine, this is the Env/FileSystem for saving checkpoints.
  Env* backup_env = nullptr;

  // Backup info and error messages will be written to info_log
  // if non-nullptr.
  Logger* info_log = nullptr;

  // If sync == true, we can guarantee you'll get consistent backup and
  // restore even on a machine crash/reboot. Backup and restore processes are
  // slower with sync enabled. If sync == false, we can only guarantee that
  // other previously synced backups and restores are not modified while
  // creating a new one.
  bool sync = true;

  // If false, we won't backup/checkpoint log files. This option can be useful
  // for backing up in-memory databases where log file are persisted, but table
  // files are in memory.
  bool backup_log_files = true;

  // (Experimental - subject to change or removal) When taking a backup and
  // saving file temperature info (minimum schema_version is 2), there are
  // two potential sources of truth for the placement of files into temperature
  // tiers: (a) the current file temperature reported by the FileSystem or
  // (b) the expected file temperature recorded in DB manifest. When this
  // option is false (default), (b) overrides (a) if both are not UNKNOWN.
  // When true, (a) overrides (b) if both are not UNKNOWN. Regardless of this
  // setting, a known temperature overrides UNKNOWN.
  bool current_temperatures_override_manifest = false;

  // ***NOCOMMIT***: This one is completely new and would need to be
  // implemented. It can be put off until later though, just assume checkpoint
  // uses LinkFile and backup does not.

  // If true and the destination directory is on the same filesystem as the
  // source, then LinkFile will be used for immutable files instead of
  // copying them. On many filesystems, this is much faster and more space
  // efficient than copying, but relies on all users of the filesystem not to
  // modify files that RocksDB treats as immutable. If false, files will always
  // be copied.
  // Default is interepreted as true for checkpoint and false for backup.
  std::optional<bool> use_link_file_when_available;

  // Size of the buffer in bytes used for reading files.
  // Enables optimally configuring the IO size based on the storage backend.
  // If specified, takes precendence over the rate limiter burst size (if
  // specified) as well as kDefaultCopyFileBufferSize.
  // If 0, the rate limiter burst size (if specified) or
  // kDefaultCopyFileBufferSize will be used.
  uint64_t io_buffer_size = 0;

  // Max bytes that can be transferred in a second during backup.
  // If 0, go as fast as you can
  // This limit only applies to writes. To also limit reads,
  // a rate limiter able to also limit reads (e.g, its mode = kAllIo)
  // have to be passed in through the option "backup_rate_limiter"
  uint64_t backup_rate_limit = 0;

  // Backup/checkpoint rate limiter. Used to control transfer speed for backup.
  // If this is not null, backup_rate_limit is ignored. Default: nullptr
  std::shared_ptr<RateLimiter> backup_rate_limiter{nullptr};

  // Up to this many background threads will be used to copy files & compute
  // checksums for CreateNewBackup() and RestoreDBFromBackup().
  int max_background_operations = 1;

  // During backup user can get callback every time next
  // callback_trigger_interval_size bytes being copied.
  uint64_t callback_trigger_interval_size = 4 * 1024 * 1024;
};

struct CreateCheckpointOrBackupOptions {
  // Callback for reporting progress, based on
  // callback_trigger_interval_size.
  //
  // An exception thrown from the callback will result in
  // Status::Aborted from the operation.
  std::function<void()> progress_callback = {};

  // If false, background_thread_cpu_priority is ignored.
  // Otherwise, the cpu priority can be decreased,
  // if you try to increase the priority, the priority will not change.
  // The initial priority of the threads is CpuPriority::kNormal,
  // so you can decrease to priorities lower than kNormal.
  bool decrease_background_thread_cpu_priority = false;
  CpuPriority background_thread_cpu_priority = CpuPriority::kNormal;
};

struct CreateCheckpointOptions : public CreateCheckpointOrBackupOptions {
  // ***NOCOMMIT***: This one is not migrated from CreateBackupOptions but from
  // the legacy checkpoint API

  // If the total log file size is equal or larger than this value, then a flush
  // is triggered for all the column families. The archived log size will not be
  // included when calculating the total log size. Flush will always trigger if
  // it is 2PC regardless of this setting.
  //
  // The default value is 0, which means flush is always triggered. If you move
  // away from the default, the checkpoint may not contain up-to-date data if
  // WAL writing is not always enabled.
  // NOTE/TODO: Reconcile with CreateBackupOptions::flush_before_backup.
  uint64_t log_size_for_flush = 0;

  // ***NOCOMMIT***: Consider (for the future) adding incremental checkpoint
  // support like incremental backup restore.
};

// TODO: doc
class CheckpointEngineBase {
 public:
  virtual ~CheckpointEngineBase() {}

  // TODO: doc
  virtual IOStatus CreateCheckpoint(
      DB* source_db, const std::string& destination_dir,
      uint64_t* sequence_number_ptr = nullptr,
      const CreateCheckpointOptions& options = {}) = 0;

  // TODO: doc
  virtual IOStatus ExportColumnFamily(
      DB* source_db, ColumnFamilyHandle* handle,
      const std::string& destination_dir,
      ExportImportFilesMetaData* out_metadata,
      uint64_t* sequence_number_ptr = nullptr,
      const CreateCheckpointOptions& options = {}) = 0;
};

// TODO: doc
class CheckpointEngine : public CheckpointEngineBase {
 public:
  // TODO: doc
  // Because this doesn't open any files or directories yet, failures are
  // mostly for configuration issues.
  static Status Open(const CheckpointEngineOptions& options,
                     std::unique_ptr<CheckpointEngine>* out_checkpoint_engine);
};

// Legacy Checkpoint interface
class Checkpoint {
 public:
  // Creates a Checkpoint object to be used for creating openable snapshots
  static Status Create(DB* db, Checkpoint** checkpoint_ptr);

  // Builds an openable snapshot of RocksDB. checkpoint_dir should contain an
  // absolute path. The specified directory should not exist, since it will be
  // created by the API.
  // When a checkpoint is created,
  // (1) SST and blob files are hard linked if the output directory is on the
  // same filesystem as the database, and copied otherwise.
  // (2) other required files (like MANIFEST) are always copied.
  // log_size_for_flush: if the total log file size is equal or larger than
  // this value, then a flush is triggered for all the column families.
  // The archived log size will not be included when calculating the total log
  // size.
  // The default value is 0, which means flush is always triggered. If you move
  // away from the default, the checkpoint may not contain up-to-date data
  // if WAL writing is not always enabled.
  // Flush will always trigger if it is 2PC.
  // sequence_number_ptr: if it is not nullptr, the value it points to will be
  // set to a sequence number guaranteed to be part of the DB, not necessarily
  // the latest. The default value of this parameter is nullptr.
  // NOTE: db_paths and cf_paths are not supported for creating checkpoints
  // and NotSupported will be returned when the DB (without WALs) uses more
  // than one directory.
  virtual Status CreateCheckpoint(const std::string& checkpoint_dir,
                                  uint64_t log_size_for_flush = 0,
                                  uint64_t* sequence_number_ptr = nullptr);

  // Exports all live SST files of a specified Column Family onto export_dir,
  // returning SST files information in metadata.
  // - SST files will be created as hard links when the directory specified
  //   is in the same partition as the db directory, copied otherwise.
  // - export_dir should not already exist and will be created by this API.
  // - Always triggers a flush.
  virtual Status ExportColumnFamily(ColumnFamilyHandle* handle,
                                    const std::string& export_dir,
                                    ExportImportFilesMetaData** metadata);

  virtual ~Checkpoint() {}
};

}  // namespace ROCKSDB_NAMESPACE
