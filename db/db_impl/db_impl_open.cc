//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cinttypes>

#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/periodic_task_scheduler.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/persistent_stats_history.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/wal_filter.h"
#include "test_util/sync_point.h"
#include "util/rate_limiter_impl.h"
#include "util/string_util.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {
Options SanitizeOptions(const std::string& dbname, const Options& src,
                        bool read_only, Status* logger_creation_s) {
  auto db_options =
      SanitizeOptions(dbname, DBOptions(src), read_only, logger_creation_s);
  ImmutableDBOptions immutable_db_options(db_options);
  auto cf_options =
      SanitizeOptions(immutable_db_options, ColumnFamilyOptions(src));
  return Options(db_options, cf_options);
}

DBOptions SanitizeOptions(const std::string& dbname, const DBOptions& src,
                          bool read_only, Status* logger_creation_s) {
  DBOptions result(src);

  if (result.env == nullptr) {
    result.env = Env::Default();
  }

  // result.max_open_files means an "infinite" open files.
  if (result.max_open_files != -1) {
    int max_max_open_files = port::GetMaxOpenFiles();
    if (max_max_open_files == -1) {
      max_max_open_files = 0x400000;
    }
    ClipToRange(&result.max_open_files, 20, max_max_open_files);
    TEST_SYNC_POINT_CALLBACK("SanitizeOptions::AfterChangeMaxOpenFiles",
                             &result.max_open_files);
  }

  if (result.info_log == nullptr && !read_only) {
    Status s = CreateLoggerFromOptions(dbname, result, &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
      if (logger_creation_s) {
        *logger_creation_s = s;
      }
    }
  }

  if (!result.write_buffer_manager) {
    result.write_buffer_manager.reset(
        new WriteBufferManager(result.db_write_buffer_size));
  }
  auto bg_job_limits = DBImpl::GetBGJobLimits(
      result.max_background_flushes, result.max_background_compactions,
      result.max_background_jobs, true /* parallelize_compactions */);
  result.env->IncBackgroundThreadsIfNeeded(bg_job_limits.max_compactions,
                                           Env::Priority::LOW);
  result.env->IncBackgroundThreadsIfNeeded(bg_job_limits.max_flushes,
                                           Env::Priority::HIGH);

  if (result.rate_limiter.get() != nullptr) {
    if (result.bytes_per_sync == 0) {
      result.bytes_per_sync = 1024 * 1024;
    }
  }

  if (result.delayed_write_rate == 0) {
    if (result.rate_limiter.get() != nullptr) {
      result.delayed_write_rate = result.rate_limiter->GetBytesPerSecond();
    }
    if (result.delayed_write_rate == 0) {
      result.delayed_write_rate = 16 * 1024 * 1024;
    }
  }

  if (result.WAL_ttl_seconds > 0 || result.WAL_size_limit_MB > 0) {
    result.recycle_log_file_num = false;
  }

  if (result.recycle_log_file_num &&
      (result.wal_recovery_mode ==
           WALRecoveryMode::kTolerateCorruptedTailRecords ||
       result.wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency)) {
    // - kTolerateCorruptedTailRecords is inconsistent with recycle log file
    //   feature. WAL recycling expects recovery success upon encountering a
    //   corrupt record at the point where new data ends and recycled data
    //   remains at the tail. However, `kTolerateCorruptedTailRecords` must fail
    //   upon encountering any such corrupt record, as it cannot differentiate
    //   between this and a real corruption, which would cause committed updates
    //   to be truncated -- a violation of the recovery guarantee.
    // - kPointInTimeRecovery and kAbsoluteConsistency are incompatible with
    //   recycle log file feature temporarily due to a bug found introducing a
    //   hole in the recovered data
    //   (https://github.com/facebook/rocksdb/pull/7252#issuecomment-673766236).
    //   Besides this bug, we believe the features are fundamentally compatible.
    result.recycle_log_file_num = 0;
  }

  if (result.db_paths.size() == 0) {
    result.db_paths.emplace_back(dbname, std::numeric_limits<uint64_t>::max());
  } else if (result.wal_dir.empty()) {
    // Use dbname as default
    result.wal_dir = dbname;
  }
  if (!result.wal_dir.empty()) {
    // If there is a wal_dir already set, check to see if the wal_dir is the
    // same as the dbname AND the same as the db_path[0] (which must exist from
    // a few lines ago). If the wal_dir matches both of these values, then clear
    // the wal_dir value, which will make wal_dir == dbname.  Most likely this
    // condition was the result of reading an old options file where we forced
    // wal_dir to be set (to dbname).
    auto npath = NormalizePath(dbname + "/");
    if (npath == NormalizePath(result.wal_dir + "/") &&
        npath == NormalizePath(result.db_paths[0].path + "/")) {
      result.wal_dir.clear();
    }
  }

  if (!result.wal_dir.empty() && result.wal_dir.back() == '/') {
    result.wal_dir = result.wal_dir.substr(0, result.wal_dir.size() - 1);
  }

  // Force flush on DB open if 2PC is enabled, since with 2PC we have no
  // guarantee that consecutive log files have consecutive sequence id, which
  // make recovery complicated.
  if (result.allow_2pc) {
    result.avoid_flush_during_recovery = false;
  }

  ImmutableDBOptions immutable_db_options(result);
  if (!immutable_db_options.IsWalDirSameAsDBPath()) {
    // Either the WAL dir and db_paths[0]/db_name are not the same, or we
    // cannot tell for sure. In either case, assume they're different and
    // explicitly cleanup the trash log files (bypass DeleteScheduler)
    // Do this first so even if we end up calling
    // DeleteScheduler::CleanupDirectory on the same dir later, it will be
    // safe
    std::vector<std::string> filenames;
    IOOptions io_opts;
    io_opts.do_not_recurse = true;
    auto wal_dir = immutable_db_options.GetWalDir();
    Status s = immutable_db_options.fs->GetChildren(
        wal_dir, io_opts, &filenames, /*IODebugContext*=*/nullptr);
    s.PermitUncheckedError();  //**TODO: What to do on error?
    for (std::string& filename : filenames) {
      if (filename.find(".log.trash", filename.length() -
                                          std::string(".log.trash").length()) !=
          std::string::npos) {
        std::string trash_file = wal_dir + "/" + filename;
        result.env->DeleteFile(trash_file).PermitUncheckedError();
      }
    }
  }

  // Create a default SstFileManager for purposes of tracking compaction size
  // and facilitating recovery from out of space errors.
  if (result.sst_file_manager.get() == nullptr) {
    std::shared_ptr<SstFileManager> sst_file_manager(
        NewSstFileManager(result.env, result.info_log));
    result.sst_file_manager = sst_file_manager;
  }

  // Supported wal compression types
  if (!StreamingCompressionTypeSupported(result.wal_compression)) {
    result.wal_compression = kNoCompression;
    ROCKS_LOG_WARN(result.info_log,
                   "wal_compression is disabled since only zstd is supported");
  }

  if (!result.paranoid_checks) {
    result.skip_checking_sst_file_sizes_on_db_open = true;
    ROCKS_LOG_INFO(result.info_log,
                   "file size check will be skipped during open.");
  }

  return result;
}

namespace {
Status ValidateOptionsByTable(
    const DBOptions& db_opts,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (auto& cf : column_families) {
    s = ValidateOptions(db_opts, cf.options);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}
}  // namespace

Status DBImpl::ValidateOptions(
    const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (auto& cfd : column_families) {
    s = ColumnFamilyData::ValidateOptions(db_options, cfd.options);
    if (!s.ok()) {
      return s;
    }
  }
  s = ValidateOptions(db_options);
  return s;
}

Status DBImpl::ValidateOptions(const DBOptions& db_options) {
  if (db_options.db_paths.size() > 4) {
    return Status::NotSupported(
        "More than four DB paths are not supported yet. ");
  }

  if (db_options.allow_mmap_reads && db_options.use_direct_reads) {
    // Protect against assert in PosixMMapReadableFile constructor
    return Status::NotSupported(
        "If memory mapped reads (allow_mmap_reads) are enabled "
        "then direct I/O reads (use_direct_reads) must be disabled. ");
  }

  if (db_options.allow_mmap_writes &&
      db_options.use_direct_io_for_flush_and_compaction) {
    return Status::NotSupported(
        "If memory mapped writes (allow_mmap_writes) are enabled "
        "then direct I/O writes (use_direct_io_for_flush_and_compaction) must "
        "be disabled. ");
  }

  if (db_options.keep_log_file_num == 0) {
    return Status::InvalidArgument("keep_log_file_num must be greater than 0");
  }

  if (db_options.unordered_write &&
      !db_options.allow_concurrent_memtable_write) {
    return Status::InvalidArgument(
        "unordered_write is incompatible with "
        "!allow_concurrent_memtable_write");
  }

  if (db_options.unordered_write && db_options.enable_pipelined_write) {
    return Status::InvalidArgument(
        "unordered_write is incompatible with enable_pipelined_write");
  }

  if (db_options.atomic_flush && db_options.enable_pipelined_write) {
    return Status::InvalidArgument(
        "atomic_flush is incompatible with enable_pipelined_write");
  }

  if (db_options.use_direct_io_for_flush_and_compaction &&
      0 == db_options.writable_file_max_buffer_size) {
    return Status::InvalidArgument(
        "writes in direct IO require writable_file_max_buffer_size > 0");
  }

  if (db_options.daily_offpeak_time_utc != "") {
    int start_time, end_time;
    if (!TryParseTimeRangeString(db_options.daily_offpeak_time_utc, start_time,
                                 end_time)) {
      return Status::InvalidArgument(
          "daily_offpeak_time_utc should be set in the format HH:mm-HH:mm "
          "(e.g. 04:30-07:30)");
    } else if (start_time == end_time) {
      return Status::InvalidArgument(
          "start_time and end_time cannot be the same");
    }
  }

  if (!db_options.write_dbid_to_manifest && !db_options.write_identity_file) {
    return Status::InvalidArgument(
        "write_dbid_to_manifest and write_identity_file cannot both be false");
  }
  return Status::OK();
}

Status DBImpl::NewDB(std::vector<std::string>* new_filenames) {
  VersionEdit new_db_edit;
  const WriteOptions write_options(Env::IOActivity::kDBOpen);
  Status s = SetupDBId(write_options, /*read_only=*/false, /*is_new_db=*/true,
                       /*is_retry=*/false, &new_db_edit);
  if (!s.ok()) {
    return s;
  }
  new_db_edit.SetLogNumber(0);
  new_db_edit.SetNextFile(2);
  new_db_edit.SetLastSequence(0);

  ROCKS_LOG_INFO(immutable_db_options_.info_log, "Creating manifest 1 \n");
  const std::string manifest = DescriptorFileName(dbname_, 1);
  {
    if (fs_->FileExists(manifest, IOOptions(), nullptr).ok()) {
      fs_->DeleteFile(manifest, IOOptions(), nullptr).PermitUncheckedError();
    }
    std::unique_ptr<FSWritableFile> file;
    FileOptions file_options = fs_->OptimizeForManifestWrite(file_options_);
    // DB option takes precedence when not kUnknown
    if (immutable_db_options_.metadata_write_temperature !=
        Temperature::kUnknown) {
      file_options.temperature =
          immutable_db_options_.metadata_write_temperature;
    }
    s = NewWritableFile(fs_.get(), manifest, &file, file_options);
    if (!s.ok()) {
      return s;
    }
    FileTypeSet tmp_set = immutable_db_options_.checksum_handoff_file_types;
    file->SetPreallocationBlockSize(
        immutable_db_options_.manifest_preallocation_size);
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(file), manifest, file_options, immutable_db_options_.clock,
        io_tracer_, nullptr /* stats */,
        Histograms::HISTOGRAM_ENUM_MAX /* hist_type */,
        immutable_db_options_.listeners, nullptr,
        tmp_set.Contains(FileType::kDescriptorFile),
        tmp_set.Contains(FileType::kDescriptorFile)));
    log::Writer log(std::move(file_writer), 0, false);
    std::string record;
    new_db_edit.EncodeTo(&record);
    s = log.AddRecord(write_options, record);
    if (s.ok()) {
      s = SyncManifest(&immutable_db_options_, write_options, log.file());
    }
  }
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(write_options, fs_.get(), dbname_, 1,
                       immutable_db_options_.metadata_write_temperature,
                       directories_.GetDbDir());
    if (new_filenames) {
      new_filenames->emplace_back(
          manifest.substr(manifest.find_last_of("/\\") + 1));
    }
  } else {
    fs_->DeleteFile(manifest, IOOptions(), nullptr).PermitUncheckedError();
  }
  return s;
}

IOStatus DBImpl::CreateAndNewDirectory(
    FileSystem* fs, const std::string& dirname,
    std::unique_ptr<FSDirectory>* directory) {
  // We call CreateDirIfMissing() as the directory may already exist (if we
  // are reopening a DB), when this happens we don't want creating the
  // directory to cause an error. However, we need to check if creating the
  // directory fails or else we may get an obscure message about the lock
  // file not existing. One real-world example of this occurring is if
  // env->CreateDirIfMissing() doesn't create intermediate directories, e.g.
  // when dbname_ is "dir/db" but when "dir" doesn't exist.
  IOStatus io_s = fs->CreateDirIfMissing(dirname, IOOptions(), nullptr);
  if (!io_s.ok()) {
    return io_s;
  }
  return fs->NewDirectory(dirname, IOOptions(), directory, nullptr);
}

IOStatus Directories::SetDirectories(FileSystem* fs, const std::string& dbname,
                                     const std::string& wal_dir,
                                     const std::vector<DbPath>& data_paths) {
  IOStatus io_s = DBImpl::CreateAndNewDirectory(fs, dbname, &db_dir_);
  if (!io_s.ok()) {
    return io_s;
  }
  if (!wal_dir.empty() && dbname != wal_dir) {
    io_s = DBImpl::CreateAndNewDirectory(fs, wal_dir, &wal_dir_);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  data_dirs_.clear();
  for (auto& p : data_paths) {
    const std::string db_path = p.path;
    if (db_path == dbname) {
      data_dirs_.emplace_back(nullptr);
    } else {
      std::unique_ptr<FSDirectory> path_directory;
      io_s = DBImpl::CreateAndNewDirectory(fs, db_path, &path_directory);
      if (!io_s.ok()) {
        return io_s;
      }
      data_dirs_.emplace_back(path_directory.release());
    }
  }
  assert(data_dirs_.size() == data_paths.size());
  return IOStatus::OK();
}

Status DBImpl::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families, bool read_only,
    bool error_if_wal_file_exists, bool error_if_data_exists_in_wals,
    bool is_retry, uint64_t* recovered_seq, RecoveryContext* recovery_ctx,
    bool* can_retry) {
  mutex_.AssertHeld();

  const WriteOptions write_options(Env::IOActivity::kDBOpen);
  bool tmp_is_new_db = false;
  bool& is_new_db = recovery_ctx ? recovery_ctx->is_new_db_ : tmp_is_new_db;
  assert(db_lock_ == nullptr);
  std::vector<std::string> files_in_dbname;
  if (!read_only) {
    Status s = directories_.SetDirectories(fs_.get(), dbname_,
                                           immutable_db_options_.wal_dir,
                                           immutable_db_options_.db_paths);
    if (!s.ok()) {
      return s;
    }

    s = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!s.ok()) {
      return s;
    }

    std::string current_fname = CurrentFileName(dbname_);
    // Path to any MANIFEST file in the db dir. It does not matter which one.
    // Since best-efforts recovery ignores CURRENT file, existence of a
    // MANIFEST indicates the recovery to recover existing db. If no MANIFEST
    // can be found, a new db will be created.
    std::string manifest_path;
    if (!immutable_db_options_.best_efforts_recovery) {
      s = env_->FileExists(current_fname);
    } else {
      s = Status::NotFound();
      IOOptions io_opts;
      io_opts.do_not_recurse = true;
      Status io_s = immutable_db_options_.fs->GetChildren(
          dbname_, io_opts, &files_in_dbname, /*IODebugContext*=*/nullptr);
      if (!io_s.ok()) {
        s = io_s;
        files_in_dbname.clear();
      }
      for (const std::string& file : files_in_dbname) {
        uint64_t number = 0;
        FileType type = kWalFile;  // initialize
        if (ParseFileName(file, &number, &type) && type == kDescriptorFile) {
          uint64_t bytes;
          s = env_->GetFileSize(DescriptorFileName(dbname_, number), &bytes);
          if (s.ok() && bytes != 0) {
            // Found non-empty MANIFEST (descriptor log), thus best-efforts
            // recovery does not have to treat the db as empty.
            manifest_path = dbname_ + "/" + file;
            break;
          }
        }
      }
    }
    if (s.IsNotFound()) {
      if (immutable_db_options_.create_if_missing) {
        s = NewDB(&files_in_dbname);
        is_new_db = true;
        if (!s.ok()) {
          return s;
        }
      } else {
        return Status::InvalidArgument(
            current_fname, "does not exist (create_if_missing is false)");
      }
    } else if (s.ok()) {
      if (immutable_db_options_.error_if_exists) {
        return Status::InvalidArgument(dbname_,
                                       "exists (error_if_exists is true)");
      }
    } else {
      // Unexpected error reading file
      assert(s.IsIOError());
      return s;
    }
    // Verify compatibility of file_options_ and filesystem
    {
      std::unique_ptr<FSRandomAccessFile> idfile;
      FileOptions customized_fs(file_options_);
      customized_fs.use_direct_reads |=
          immutable_db_options_.use_direct_io_for_flush_and_compaction;
      const std::string& fname =
          manifest_path.empty() ? current_fname : manifest_path;
      s = fs_->NewRandomAccessFile(fname, customized_fs, &idfile, nullptr);
      if (!s.ok()) {
        std::string error_str = s.ToString();
        // Check if unsupported Direct I/O is the root cause
        customized_fs.use_direct_reads = false;
        s = fs_->NewRandomAccessFile(fname, customized_fs, &idfile, nullptr);
        if (s.ok()) {
          return Status::InvalidArgument(
              "Direct I/O is not supported by the specified DB.");
        } else {
          return Status::InvalidArgument(
              "Found options incompatible with filesystem", error_str.c_str());
        }
      }
    }
  } else if (immutable_db_options_.best_efforts_recovery) {
    assert(files_in_dbname.empty());
    IOOptions io_opts;
    io_opts.do_not_recurse = true;
    Status s = immutable_db_options_.fs->GetChildren(
        dbname_, io_opts, &files_in_dbname, /*IODebugContext*=*/nullptr);
    if (s.IsNotFound()) {
      return Status::InvalidArgument(dbname_,
                                     "does not exist (open for read only)");
    } else if (s.IsIOError()) {
      return s;
    }
    assert(s.ok());
  }
  assert(is_new_db || db_id_.empty());
  Status s;
  bool missing_table_file = false;
  if (!immutable_db_options_.best_efforts_recovery) {
    // Status of reading the descriptor file
    Status desc_status;
    s = versions_->Recover(column_families, read_only, &db_id_,
                           /*no_error_if_files_missing=*/false, is_retry,
                           &desc_status);
    desc_status.PermitUncheckedError();
    if (is_retry) {
      RecordTick(stats_, FILE_READ_CORRUPTION_RETRY_COUNT);
      if (desc_status.ok()) {
        RecordTick(stats_, FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT);
      }
    }
    if (can_retry) {
      // If we're opening for the first time and the failure is likely due to
      // a corrupt MANIFEST file (could result in either the log::Reader
      // detecting a corrupt record, or SST files not found error due to
      // discarding badly formed tail records)
      if (!is_retry &&
          (desc_status.IsCorruption() || s.IsNotFound() || s.IsCorruption()) &&
          CheckFSFeatureSupport(fs_.get(),
                                FSSupportedOps::kVerifyAndReconstructRead)) {
        *can_retry = true;
        ROCKS_LOG_ERROR(
            immutable_db_options_.info_log,
            "Possible corruption detected while replaying MANIFEST %s, %s. "
            "Will be retried.",
            desc_status.ToString().c_str(), s.ToString().c_str());
      } else {
        *can_retry = false;
      }
    }
  } else {
    assert(!files_in_dbname.empty());
    s = versions_->TryRecover(column_families, read_only, files_in_dbname,
                              &db_id_, &missing_table_file);
    if (s.ok()) {
      // TryRecover may delete previous column_family_set_.
      column_family_memtables_.reset(
          new ColumnFamilyMemTablesImpl(versions_->GetColumnFamilySet()));
    }
  }
  if (!s.ok()) {
    return s;
  }
  if (s.ok() && !read_only) {
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      auto& moptions = *cfd->GetLatestMutableCFOptions();
      // Try to trivially move files down the LSM tree to start from bottommost
      // level when level_compaction_dynamic_level_bytes is enabled. This should
      // only be useful when user is migrating to turning on this option.
      // If a user is migrating from Level Compaction with a smaller level
      // multiplier or from Universal Compaction, there may be too many
      // non-empty levels and the trivial moves here are not sufficed for
      // migration. Additional compactions are needed to drain unnecessary
      // levels.
      //
      // Note that this step moves files down LSM without consulting
      // SSTPartitioner. Further compactions are still needed if
      // the user wants to partition SST files.
      // Note that files moved in this step may not respect the compression
      // option in target level.
      if (cfd->ioptions()->compaction_style ==
              CompactionStyle::kCompactionStyleLevel &&
          cfd->ioptions()->level_compaction_dynamic_level_bytes &&
          !moptions.disable_auto_compactions) {
        int to_level = cfd->ioptions()->num_levels - 1;
        // last level is reserved
        // allow_ingest_behind does not support Level Compaction,
        // and per_key_placement can have infinite compaction loop for Level
        // Compaction. Adjust to_level here just to be safe.
        if (cfd->ioptions()->allow_ingest_behind ||
            moptions.preclude_last_level_data_seconds > 0) {
          to_level -= 1;
        }
        // Whether this column family has a level trivially moved
        bool moved = false;
        // Fill the LSM starting from to_level and going up one level at a time.
        // Some loop invariants (when last level is not reserved):
        // - levels in (from_level, to_level] are empty, and
        // - levels in (to_level, last_level] are non-empty.
        for (int from_level = to_level; from_level >= 0; --from_level) {
          const std::vector<FileMetaData*>& level_files =
              cfd->current()->storage_info()->LevelFiles(from_level);
          if (level_files.empty() || from_level == 0) {
            continue;
          }
          assert(from_level <= to_level);
          // Trivial move files from `from_level` to `to_level`
          if (from_level < to_level) {
            if (!moved) {
              // lsm_state will look like "[1,2,3,4,5,6,0]" for an LSM with
              // 7 levels
              std::string lsm_state = "[";
              for (int i = 0; i < cfd->ioptions()->num_levels; ++i) {
                lsm_state += std::to_string(
                    cfd->current()->storage_info()->NumLevelFiles(i));
                if (i < cfd->ioptions()->num_levels - 1) {
                  lsm_state += ",";
                }
              }
              lsm_state += "]";
              ROCKS_LOG_WARN(immutable_db_options_.info_log,
                             "[%s] Trivially move files down the LSM when open "
                             "with level_compaction_dynamic_level_bytes=true,"
                             " lsm_state: %s (Files are moved only if DB "
                             "Recovery is successful).",
                             cfd->GetName().c_str(), lsm_state.c_str());
              moved = true;
            }
            ROCKS_LOG_WARN(
                immutable_db_options_.info_log,
                "[%s] Moving %zu files from from_level-%d to from_level-%d",
                cfd->GetName().c_str(), level_files.size(), from_level,
                to_level);
            VersionEdit edit;
            edit.SetColumnFamily(cfd->GetID());
            for (const FileMetaData* f : level_files) {
              edit.DeleteFile(from_level, f->fd.GetNumber());
              edit.AddFile(to_level, f->fd.GetNumber(), f->fd.GetPathId(),
                           f->fd.GetFileSize(), f->smallest, f->largest,
                           f->fd.smallest_seqno, f->fd.largest_seqno,
                           f->marked_for_compaction,
                           f->temperature,  // this can be different from
                           // `last_level_temperature`
                           f->oldest_blob_file_number, f->oldest_ancester_time,
                           f->file_creation_time, f->epoch_number,
                           f->file_checksum, f->file_checksum_func_name,
                           f->unique_id, f->compensated_range_deletion_size,
                           f->tail_size, f->user_defined_timestamps_persisted);
              ROCKS_LOG_WARN(immutable_db_options_.info_log,
                             "[%s] Moving #%" PRIu64
                             " from from_level-%d to from_level-%d %" PRIu64
                             " bytes\n",
                             cfd->GetName().c_str(), f->fd.GetNumber(),
                             from_level, to_level, f->fd.GetFileSize());
            }
            recovery_ctx->UpdateVersionEdits(cfd, edit);
          }
          --to_level;
        }
      }
    }
  }
  if (is_new_db) {
    // Already set up DB ID in NewDB
  } else if (immutable_db_options_.write_dbid_to_manifest && recovery_ctx) {
    VersionEdit edit;
    s = SetupDBId(write_options, read_only, is_new_db, is_retry, &edit);
    recovery_ctx->UpdateVersionEdits(
        versions_->GetColumnFamilySet()->GetDefault(), edit);
  } else {
    s = SetupDBId(write_options, read_only, is_new_db, is_retry, nullptr);
  }
  assert(!s.ok() || !db_id_.empty());
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "DB ID: %s\n", db_id_.c_str());
  if (s.ok() && !read_only) {
    s = MaybeUpdateNextFileNumber(recovery_ctx);
  }

  if (immutable_db_options_.paranoid_checks && s.ok()) {
    s = CheckConsistency();
  }
  if (s.ok() && !read_only) {
    // TODO: share file descriptors (FSDirectory) with SetDirectories above
    std::map<std::string, std::shared_ptr<FSDirectory>> created_dirs;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      s = cfd->AddDirectories(&created_dirs);
      if (!s.ok()) {
        return s;
      }
    }
  }

  std::vector<std::string> files_in_wal_dir;
  if (s.ok()) {
    // Initial max_total_in_memory_state_ before recovery wals. Log recovery
    // may check this value to decide whether to flush.
    max_total_in_memory_state_ = 0;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
      max_total_in_memory_state_ += mutable_cf_options->write_buffer_size *
                                    mutable_cf_options->max_write_buffer_number;
    }

    SequenceNumber next_sequence(kMaxSequenceNumber);
    default_cf_handle_ = new ColumnFamilyHandleImpl(
        versions_->GetColumnFamilySet()->GetDefault(), this, &mutex_);
    default_cf_internal_stats_ = default_cf_handle_->cfd()->internal_stats();

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that prev_log_number() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of rocksdb.
    auto wal_dir = immutable_db_options_.GetWalDir();
    if (!immutable_db_options_.best_efforts_recovery) {
      IOOptions io_opts;
      io_opts.do_not_recurse = true;
      s = immutable_db_options_.fs->GetChildren(
          wal_dir, io_opts, &files_in_wal_dir, /*IODebugContext*=*/nullptr);
    }
    if (s.IsNotFound()) {
      return Status::InvalidArgument("wal_dir not found", wal_dir);
    } else if (!s.ok()) {
      return s;
    }

    std::unordered_map<uint64_t, std::string> wal_files;
    for (const auto& file : files_in_wal_dir) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file, &number, &type) && type == kWalFile) {
        if (is_new_db) {
          return Status::Corruption(
              "While creating a new Db, wal_dir contains "
              "existing log file: ",
              file);
        } else {
          wal_files[number] = LogFileName(wal_dir, number);
        }
      }
    }

    if (immutable_db_options_.track_and_verify_wals_in_manifest) {
      if (!immutable_db_options_.best_efforts_recovery) {
        // Verify WALs in MANIFEST.
        s = versions_->GetWalSet().CheckWals(env_, wal_files);
      }  // else since best effort recovery does not recover from WALs, no need
         // to check WALs.
    } else if (!versions_->GetWalSet().GetWals().empty()) {
      // Tracking is disabled, clear previously tracked WALs from MANIFEST,
      // otherwise, in the future, if WAL tracking is enabled again,
      // since the WALs deleted when WAL tracking is disabled are not persisted
      // into MANIFEST, WAL check may fail.
      VersionEdit edit;
      WalNumber max_wal_number =
          versions_->GetWalSet().GetWals().rbegin()->first;
      edit.DeleteWalsBefore(max_wal_number + 1);
      assert(recovery_ctx != nullptr);
      assert(versions_->GetColumnFamilySet() != nullptr);
      recovery_ctx->UpdateVersionEdits(
          versions_->GetColumnFamilySet()->GetDefault(), edit);
    }
    if (!s.ok()) {
      return s;
    }

    if (!wal_files.empty()) {
      if (error_if_wal_file_exists) {
        return Status::Corruption(
            "The db was opened in readonly mode with error_if_wal_file_exists"
            "flag but a WAL file already exists");
      } else if (error_if_data_exists_in_wals) {
        for (auto& wal_file : wal_files) {
          uint64_t bytes;
          s = env_->GetFileSize(wal_file.second, &bytes);
          if (s.ok()) {
            if (bytes > 0) {
              return Status::Corruption(
                  "error_if_data_exists_in_wals is set but there are data "
                  " in WAL files.");
            }
          }
        }
      }
    }

    if (!wal_files.empty()) {
      // Recover in the order in which the wals were generated
      std::vector<uint64_t> wals;
      wals.reserve(wal_files.size());
      for (const auto& wal_file : wal_files) {
        wals.push_back(wal_file.first);
      }
      std::sort(wals.begin(), wals.end());

      bool corrupted_wal_found = false;
      s = RecoverLogFiles(wals, &next_sequence, read_only, is_retry,
                          &corrupted_wal_found, recovery_ctx);
      if (corrupted_wal_found && recovered_seq != nullptr) {
        *recovered_seq = next_sequence;
      }
      if (!s.ok()) {
        // Clear memtables if recovery failed
        for (auto cfd : *versions_->GetColumnFamilySet()) {
          cfd->CreateNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                 kMaxSequenceNumber);
        }
      }
    }
  }

  if (read_only) {
    // If we are opening as read-only, we need to update options_file_number_
    // to reflect the most recent OPTIONS file. It does not matter for regular
    // read-write db instance because options_file_number_ will later be
    // updated to versions_->NewFileNumber() in RenameTempFileToOptionsFile.
    std::vector<std::string> filenames;
    if (s.ok()) {
      const std::string normalized_dbname = NormalizePath(dbname_);
      const std::string normalized_wal_dir =
          NormalizePath(immutable_db_options_.GetWalDir());
      if (immutable_db_options_.best_efforts_recovery) {
        filenames = std::move(files_in_dbname);
      } else if (normalized_dbname == normalized_wal_dir) {
        filenames = std::move(files_in_wal_dir);
      } else {
        IOOptions io_opts;
        io_opts.do_not_recurse = true;
        s = immutable_db_options_.fs->GetChildren(
            GetName(), io_opts, &filenames, /*IODebugContext*=*/nullptr);
      }
    }
    if (s.ok()) {
      uint64_t number = 0;
      uint64_t options_file_number = 0;
      FileType type;
      for (const auto& fname : filenames) {
        if (ParseFileName(fname, &number, &type) && type == kOptionsFile) {
          options_file_number = std::max(number, options_file_number);
        }
      }
      versions_->options_file_number_ = options_file_number;
      uint64_t options_file_size = 0;
      if (options_file_number > 0) {
        s = env_->GetFileSize(OptionsFileName(GetName(), options_file_number),
                              &options_file_size);
      }
      versions_->options_file_size_ = options_file_size;
    }
  }
  return s;
}

Status DBImpl::PersistentStatsProcessFormatVersion() {
  mutex_.AssertHeld();
  Status s;
  // persist version when stats CF doesn't exist
  bool should_persist_format_version = !persistent_stats_cfd_exists_;
  mutex_.Unlock();
  if (persistent_stats_cfd_exists_) {
    // Check persistent stats format version compatibility. Drop and recreate
    // persistent stats CF if format version is incompatible
    uint64_t format_version_recovered = 0;
    Status s_format = DecodePersistentStatsVersionNumber(
        this, StatsVersionKeyType::kFormatVersion, &format_version_recovered);
    uint64_t compatible_version_recovered = 0;
    Status s_compatible = DecodePersistentStatsVersionNumber(
        this, StatsVersionKeyType::kCompatibleVersion,
        &compatible_version_recovered);
    // abort reading from existing stats CF if any of following is true:
    // 1. failed to read format version or compatible version from disk
    // 2. sst's format version is greater than current format version, meaning
    // this sst is encoded with a newer RocksDB release, and current compatible
    // version is below the sst's compatible version
    if (!s_format.ok() || !s_compatible.ok() ||
        (kStatsCFCurrentFormatVersion < format_version_recovered &&
         kStatsCFCompatibleFormatVersion < compatible_version_recovered)) {
      if (!s_format.ok() || !s_compatible.ok()) {
        ROCKS_LOG_WARN(
            immutable_db_options_.info_log,
            "Recreating persistent stats column family since reading "
            "persistent stats version key failed. Format key: %s, compatible "
            "key: %s",
            s_format.ToString().c_str(), s_compatible.ToString().c_str());
      } else {
        ROCKS_LOG_WARN(
            immutable_db_options_.info_log,
            "Recreating persistent stats column family due to corrupted or "
            "incompatible format version. Recovered format: %" PRIu64
            "; recovered format compatible since: %" PRIu64 "\n",
            format_version_recovered, compatible_version_recovered);
      }
      s = DropColumnFamily(persist_stats_cf_handle_);
      if (s.ok()) {
        s = DestroyColumnFamilyHandle(persist_stats_cf_handle_);
      }
      ColumnFamilyHandle* handle = nullptr;
      if (s.ok()) {
        ColumnFamilyOptions cfo;
        OptimizeForPersistentStats(&cfo);
        s = CreateColumnFamilyImpl(ReadOptions(Env::IOActivity::kDBOpen),
                                   WriteOptions(Env::IOActivity::kDBOpen), cfo,
                                   kPersistentStatsColumnFamilyName, &handle);
      }
      if (s.ok()) {
        persist_stats_cf_handle_ = static_cast<ColumnFamilyHandleImpl*>(handle);
        // should also persist version here because old stats CF is discarded
        should_persist_format_version = true;
      }
    }
  }
  if (should_persist_format_version) {
    // Persistent stats CF being created for the first time, need to write
    // format version key
    WriteBatch batch;
    if (s.ok()) {
      s = batch.Put(persist_stats_cf_handle_, kFormatVersionKeyString,
                    std::to_string(kStatsCFCurrentFormatVersion));
    }
    if (s.ok()) {
      s = batch.Put(persist_stats_cf_handle_, kCompatibleVersionKeyString,
                    std::to_string(kStatsCFCompatibleFormatVersion));
    }
    if (s.ok()) {
      // TODO: plumb Env::IOActivity, Env::IOPriority
      WriteOptions wo;
      wo.low_pri = true;
      wo.no_slowdown = true;
      wo.sync = false;
      s = Write(wo, &batch);
    }
  }
  mutex_.Lock();
  return s;
}

Status DBImpl::InitPersistStatsColumnFamily() {
  mutex_.AssertHeld();
  assert(!persist_stats_cf_handle_);
  ColumnFamilyData* persistent_stats_cfd =
      versions_->GetColumnFamilySet()->GetColumnFamily(
          kPersistentStatsColumnFamilyName);
  persistent_stats_cfd_exists_ = persistent_stats_cfd != nullptr;

  Status s;
  if (persistent_stats_cfd != nullptr) {
    // We are recovering from a DB which already contains persistent stats CF,
    // the CF is already created in VersionSet::ApplyOneVersionEdit, but
    // column family handle was not. Need to explicitly create handle here.
    persist_stats_cf_handle_ =
        new ColumnFamilyHandleImpl(persistent_stats_cfd, this, &mutex_);
  } else {
    mutex_.Unlock();
    ColumnFamilyHandle* handle = nullptr;
    ColumnFamilyOptions cfo;
    OptimizeForPersistentStats(&cfo);
    s = CreateColumnFamilyImpl(ReadOptions(Env::IOActivity::kDBOpen),
                               WriteOptions(Env::IOActivity::kDBOpen), cfo,
                               kPersistentStatsColumnFamilyName, &handle);
    persist_stats_cf_handle_ = static_cast<ColumnFamilyHandleImpl*>(handle);
    mutex_.Lock();
  }
  return s;
}

Status DBImpl::LogAndApplyForRecovery(const RecoveryContext& recovery_ctx) {
  mutex_.AssertHeld();
  assert(versions_->descriptor_log_ == nullptr);
  const ReadOptions read_options(Env::IOActivity::kDBOpen);
  const WriteOptions write_options(Env::IOActivity::kDBOpen);

  Status s = versions_->LogAndApply(recovery_ctx.cfds_,
                                    recovery_ctx.mutable_cf_opts_, read_options,
                                    write_options, recovery_ctx.edit_lists_,
                                    &mutex_, directories_.GetDbDir());
  return s;
}

void DBImpl::InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap() {
  if (immutable_db_options_.wal_filter == nullptr) {
    return;
  }
  assert(immutable_db_options_.wal_filter != nullptr);
  WalFilter& wal_filter = *(immutable_db_options_.wal_filter);

  std::map<std::string, uint32_t> cf_name_id_map;
  std::map<uint32_t, uint64_t> cf_lognumber_map;
  assert(versions_);
  assert(versions_->GetColumnFamilySet());
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    assert(cfd);
    cf_name_id_map.insert(std::make_pair(cfd->GetName(), cfd->GetID()));
    cf_lognumber_map.insert(std::make_pair(cfd->GetID(), cfd->GetLogNumber()));
  }

  wal_filter.ColumnFamilyLogNumberMap(cf_lognumber_map, cf_name_id_map);
}

bool DBImpl::InvokeWalFilterIfNeededOnWalRecord(uint64_t wal_number,
                                                const std::string& wal_fname,
                                                log::Reader::Reporter& reporter,
                                                Status& status,
                                                bool& stop_replay,
                                                WriteBatch& batch) {
  if (immutable_db_options_.wal_filter == nullptr) {
    return true;
  }
  assert(immutable_db_options_.wal_filter != nullptr);
  WalFilter& wal_filter = *(immutable_db_options_.wal_filter);

  WriteBatch new_batch;
  bool batch_changed = false;

  bool process_current_record = true;

  WalFilter::WalProcessingOption wal_processing_option =
      wal_filter.LogRecordFound(wal_number, wal_fname, batch, &new_batch,
                                &batch_changed);

  switch (wal_processing_option) {
    case WalFilter::WalProcessingOption::kContinueProcessing:
      // do nothing, proceeed normally
      break;
    case WalFilter::WalProcessingOption::kIgnoreCurrentRecord:
      // skip current record
      process_current_record = false;
      break;
    case WalFilter::WalProcessingOption::kStopReplay:
      // skip current record and stop replay
      process_current_record = false;
      stop_replay = true;
      break;
    case WalFilter::WalProcessingOption::kCorruptedRecord: {
      status = Status::Corruption("Corruption reported by Wal Filter ",
                                  wal_filter.Name());
      MaybeIgnoreError(&status);
      if (!status.ok()) {
        process_current_record = false;
        reporter.Corruption(batch.GetDataSize(), status);
      }
      break;
    }
    default: {
      // logical error which should not happen. If RocksDB throws, we would
      // just do `throw std::logic_error`.
      assert(false);
      status = Status::NotSupported(
          "Unknown WalProcessingOption returned by Wal Filter ",
          wal_filter.Name());
      MaybeIgnoreError(&status);
      if (!status.ok()) {
        // Ignore the error with current record processing.
        stop_replay = true;
      }
      break;
    }
  }

  if (!process_current_record) {
    return false;
  }

  if (batch_changed) {
    // Make sure that the count in the new batch is
    // within the orignal count.
    int new_count = WriteBatchInternal::Count(&new_batch);
    int original_count = WriteBatchInternal::Count(&batch);
    if (new_count > original_count) {
      ROCKS_LOG_FATAL(
          immutable_db_options_.info_log,
          "Recovering log #%" PRIu64
          " mode %d log filter %s returned "
          "more records (%d) than original (%d) which is not allowed. "
          "Aborting recovery.",
          wal_number, static_cast<int>(immutable_db_options_.wal_recovery_mode),
          wal_filter.Name(), new_count, original_count);
      status = Status::NotSupported(
          "More than original # of records "
          "returned by Wal Filter ",
          wal_filter.Name());
      return false;
    }
    // Set the same sequence number in the new_batch
    // as the original batch.
    WriteBatchInternal::SetSequence(&new_batch,
                                    WriteBatchInternal::Sequence(&batch));
    batch = new_batch;
  }
  return true;
}

// REQUIRES: wal_numbers are sorted in ascending order
Status DBImpl::RecoverLogFiles(const std::vector<uint64_t>& wal_numbers,
                               SequenceNumber* next_sequence, bool read_only,
                               bool is_retry, bool* corrupted_wal_found,
                               RecoveryContext* recovery_ctx) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // nullptr if immutable_db_options_.paranoid_checks==false
    bool* old_log_record;
    void Corruption(size_t bytes, const Status& s) override {
      ROCKS_LOG_WARN(info_log, "%s%s: dropping %d bytes; %s",
                     (status == nullptr ? "(ignoring error) " : ""), fname,
                     static_cast<int>(bytes), s.ToString().c_str());
      if (status != nullptr && status->ok()) {
        *status = s;
      }
    }

    void OldLogRecord(size_t bytes) override {
      if (old_log_record != nullptr) {
        *old_log_record = true;
      }
      ROCKS_LOG_WARN(info_log, "%s: dropping %d bytes; possibly recycled",
                     fname, static_cast<int>(bytes));
    }
  };

  mutex_.AssertHeld();
  Status status;
  bool old_log_record = false;
  std::unordered_map<int, VersionEdit> version_edits;
  // no need to refcount because iteration is under mutex
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    version_edits.insert({cfd->GetID(), edit});
  }
  int job_id = next_job_id_.fetch_add(1);
  {
    auto stream = event_logger_.Log();
    stream << "job" << job_id << "event"
           << "recovery_started";
    stream << "wal_files";
    stream.StartArray();
    for (auto wal_number : wal_numbers) {
      stream << wal_number;
    }
    stream.EndArray();
  }

  // No-op for immutable_db_options_.wal_filter == nullptr.
  InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap();

  bool stop_replay_by_wal_filter = false;
  bool stop_replay_for_corruption = false;
  bool flushed = false;
  uint64_t corrupted_wal_number = kMaxSequenceNumber;
  uint64_t min_wal_number = MinLogNumberToKeep();
  if (!allow_2pc()) {
    // In non-2pc mode, we skip WALs that do not back unflushed data.
    min_wal_number =
        std::max(min_wal_number, versions_->MinLogNumberWithUnflushedData());
  }
  for (auto wal_number : wal_numbers) {
    if (wal_number < min_wal_number) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Skipping log #%" PRIu64
                     " since it is older than min log to keep #%" PRIu64,
                     wal_number, min_wal_number);
      continue;
    }
    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(wal_number);
    // Open the log file
    std::string fname =
        LogFileName(immutable_db_options_.GetWalDir(), wal_number);

    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Recovering log #%" PRIu64 " mode %d", wal_number,
                   static_cast<int>(immutable_db_options_.wal_recovery_mode));
    auto logFileDropped = [this, &fname]() {
      uint64_t bytes;
      if (env_->GetFileSize(fname, &bytes).ok()) {
        auto info_log = immutable_db_options_.info_log.get();
        ROCKS_LOG_WARN(info_log, "%s: dropping %d bytes", fname.c_str(),
                       static_cast<int>(bytes));
      }
    };
    if (stop_replay_by_wal_filter) {
      logFileDropped();
      continue;
    }

    std::unique_ptr<SequentialFileReader> file_reader;
    {
      std::unique_ptr<FSSequentialFile> file;
      status = fs_->NewSequentialFile(
          fname, fs_->OptimizeForLogRead(file_options_), &file, nullptr);
      if (!status.ok()) {
        MaybeIgnoreError(&status);
        if (!status.ok()) {
          return status;
        } else {
          // Fail with one log file, but that's ok.
          // Try next one.
          continue;
        }
      }
      file_reader.reset(new SequentialFileReader(
          std::move(file), fname, immutable_db_options_.log_readahead_size,
          io_tracer_, /*listeners=*/{}, /*rate_limiter=*/nullptr, is_retry));
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = immutable_db_options_.info_log.get();
    reporter.fname = fname.c_str();
    reporter.old_log_record = &old_log_record;
    if (!immutable_db_options_.paranoid_checks ||
        immutable_db_options_.wal_recovery_mode ==
            WALRecoveryMode::kSkipAnyCorruptedRecords) {
      reporter.status = nullptr;
    } else {
      reporter.status = &status;
    }
    // We intentially make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(immutable_db_options_.info_log, std::move(file_reader),
                       &reporter, true /*checksum*/, wal_number);

    // Determine if we should tolerate incomplete records at the tail end of the
    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;

    const UnorderedMap<uint32_t, size_t>& running_ts_sz =
        versions_->GetRunningColumnFamiliesTimestampSize();

    TEST_SYNC_POINT_CALLBACK("DBImpl::RecoverLogFiles:BeforeReadWal",
                             /*arg=*/nullptr);
    uint64_t record_checksum;
    while (!stop_replay_by_wal_filter &&
           reader.ReadRecord(&record, &scratch,
                             immutable_db_options_.wal_recovery_mode,
                             &record_checksum) &&
           status.ok()) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter.Corruption(record.size(),
                            Status::Corruption("log record too small"));
        continue;
      }
      // We create a new batch and initialize with a valid prot_info_ to store
      // the data checksums
      WriteBatch batch;
      std::unique_ptr<WriteBatch> new_batch;

      status = WriteBatchInternal::SetContents(&batch, record);
      if (!status.ok()) {
        return status;
      }

      const UnorderedMap<uint32_t, size_t>& record_ts_sz =
          reader.GetRecordedTimestampSize();
      status = HandleWriteBatchTimestampSizeDifference(
          &batch, running_ts_sz, record_ts_sz,
          TimestampSizeConsistencyMode::kReconcileInconsistency, seq_per_batch_,
          batch_per_txn_, &new_batch);
      if (!status.ok()) {
        return status;
      }

      bool batch_updated = new_batch != nullptr;
      WriteBatch* batch_to_use = batch_updated ? new_batch.get() : &batch;
      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::RecoverLogFiles:BeforeUpdateProtectionInfo:batch",
          batch_to_use);
      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::RecoverLogFiles:BeforeUpdateProtectionInfo:checksum",
          &record_checksum);
      status = WriteBatchInternal::UpdateProtectionInfo(
          batch_to_use, 8 /* bytes_per_key */,
          batch_updated ? nullptr : &record_checksum);
      if (!status.ok()) {
        return status;
      }

      SequenceNumber sequence = WriteBatchInternal::Sequence(batch_to_use);
      if (sequence > kMaxSequenceNumber) {
        reporter.Corruption(
            record.size(),
            Status::Corruption("sequence " + std::to_string(sequence) +
                               " is too large"));
        continue;
      }

      if (immutable_db_options_.wal_recovery_mode ==
          WALRecoveryMode::kPointInTimeRecovery) {
        // In point-in-time recovery mode, if sequence id of log files are
        // consecutive, we continue recovery despite corruption. This could
        // happen when we open and write to a corrupted DB, where sequence id
        // will start from the last sequence id we recovered.
        if (sequence == *next_sequence) {
          stop_replay_for_corruption = false;
        }
        if (stop_replay_for_corruption) {
          logFileDropped();
          break;
        }
      }

      // For the default case of wal_filter == nullptr, always performs no-op
      // and returns true.
      if (!InvokeWalFilterIfNeededOnWalRecord(wal_number, fname, reporter,
                                              status, stop_replay_by_wal_filter,
                                              *batch_to_use)) {
        continue;
      }

      // If column family was not found, it might mean that the WAL write
      // batch references to the column family that was dropped after the
      // insert. We don't want to fail the whole write batch in that case --
      // we just ignore the update.
      // That's why we set ignore missing column families to true
      bool has_valid_writes = false;
      status = WriteBatchInternal::InsertInto(
          batch_to_use, column_family_memtables_.get(), &flush_scheduler_,
          &trim_history_scheduler_, true, wal_number, this,
          false /* concurrent_memtable_writes */, next_sequence,
          &has_valid_writes, seq_per_batch_, batch_per_txn_);
      MaybeIgnoreError(&status);
      if (!status.ok()) {
        // We are treating this as a failure while reading since we read valid
        // blocks that do not form coherent data
        reporter.Corruption(record.size(), status);
        continue;
      }

      if (has_valid_writes && !read_only) {
        // we can do this because this is called before client has access to the
        // DB and there is only a single thread operating on DB
        ColumnFamilyData* cfd;

        while ((cfd = flush_scheduler_.TakeNextColumnFamily()) != nullptr) {
          cfd->UnrefAndTryDelete();
          // If this asserts, it means that InsertInto failed in
          // filtering updates to already-flushed column families
          assert(cfd->GetLogNumber() <= wal_number);
          auto iter = version_edits.find(cfd->GetID());
          assert(iter != version_edits.end());
          VersionEdit* edit = &iter->second;
          status = WriteLevel0TableForRecovery(job_id, cfd, cfd->mem(), edit);
          if (!status.ok()) {
            // Reflect errors immediately so that conditions like full
            // file-systems cause the DB::Open() to fail.
            return status;
          }
          flushed = true;

          cfd->CreateNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                 *next_sequence - 1);
        }
      }
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Recovered to log #%" PRIu64 " seq #%" PRIu64, wal_number,
                   *next_sequence);

    if (!status.ok() || old_log_record) {
      if (status.IsNotSupported()) {
        // We should not treat NotSupported as corruption. It is rather a clear
        // sign that we are processing a WAL that is produced by an incompatible
        // version of the code.
        return status;
      }
      if (immutable_db_options_.wal_recovery_mode ==
          WALRecoveryMode::kSkipAnyCorruptedRecords) {
        // We should ignore all errors unconditionally
        status = Status::OK();
      } else if (immutable_db_options_.wal_recovery_mode ==
                 WALRecoveryMode::kPointInTimeRecovery) {
        if (status.IsIOError()) {
          ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                          "IOError during point-in-time reading log #%" PRIu64
                          " seq #%" PRIu64
                          ". %s. This likely mean loss of synced WAL, "
                          "thus recovery fails.",
                          wal_number, *next_sequence,
                          status.ToString().c_str());
          return status;
        }
        // We should ignore the error but not continue replaying
        status = Status::OK();
        old_log_record = false;
        stop_replay_for_corruption = true;
        corrupted_wal_number = wal_number;
        if (corrupted_wal_found != nullptr) {
          *corrupted_wal_found = true;
        }
      } else {
        assert(immutable_db_options_.wal_recovery_mode ==
                   WALRecoveryMode::kTolerateCorruptedTailRecords ||
               immutable_db_options_.wal_recovery_mode ==
                   WALRecoveryMode::kAbsoluteConsistency);
        return status;
      }
    }

    flush_scheduler_.Clear();
    trim_history_scheduler_.Clear();
    auto last_sequence = *next_sequence - 1;
    if ((*next_sequence != kMaxSequenceNumber) &&
        (versions_->LastSequence() <= last_sequence)) {
      versions_->SetLastAllocatedSequence(last_sequence);
      versions_->SetLastPublishedSequence(last_sequence);
      versions_->SetLastSequence(last_sequence);
    }
  }
  // Compare the corrupted log number to all columnfamily's current log number.
  // Abort Open() if any column family's log number is greater than
  // the corrupted log number, which means CF contains data beyond the point of
  // corruption. This could during PIT recovery when the WAL is corrupted and
  // some (but not all) CFs are flushed
  // Exclude the PIT case where no log is dropped after the corruption point.
  // This is to cover the case for empty wals after corrupted log, in which we
  // don't reset stop_replay_for_corruption.
  if (stop_replay_for_corruption == true &&
      (immutable_db_options_.wal_recovery_mode ==
           WALRecoveryMode::kPointInTimeRecovery ||
       immutable_db_options_.wal_recovery_mode ==
           WALRecoveryMode::kTolerateCorruptedTailRecords)) {
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      // One special case cause cfd->GetLogNumber() > corrupted_wal_number but
      // the CF is still consistent: If a new column family is created during
      // the flush and the WAL sync fails at the same time, the new CF points to
      // the new WAL but the old WAL is curropted. Since the new CF is empty, it
      // is still consistent. We add the check of CF sst file size to avoid the
      // false positive alert.

      // Note that, the check of (cfd->GetLiveSstFilesSize() > 0) may leads to
      // the ignorance of a very rare inconsistency case caused in data
      // canclation. One CF is empty due to KV deletion. But those operations
      // are in the WAL. If the WAL is corrupted, the status of this CF might
      // not be consistent with others. However, the consistency check will be
      // bypassed due to empty CF.
      // TODO: a better and complete implementation is needed to ensure strict
      // consistency check in WAL recovery including hanlding the tailing
      // issues.
      if (cfd->GetLogNumber() > corrupted_wal_number &&
          cfd->GetLiveSstFilesSize() > 0) {
        ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                        "Column family inconsistency: SST file contains data"
                        " beyond the point of corruption.");
        return Status::Corruption("SST file is ahead of WALs in CF " +
                                  cfd->GetName());
      }
    }
  }

  // True if there's any data in the WALs; if not, we can skip re-processing
  // them later
  bool data_seen = false;
  if (!read_only) {
    // no need to refcount since client still doesn't have access
    // to the DB and can not drop column families while we iterate
    const WalNumber max_wal_number = wal_numbers.back();
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      auto iter = version_edits.find(cfd->GetID());
      assert(iter != version_edits.end());
      VersionEdit* edit = &iter->second;

      if (cfd->GetLogNumber() > max_wal_number) {
        // Column family cfd has already flushed the data
        // from all wals. Memtable has to be empty because
        // we filter the updates based on wal_number
        // (in WriteBatch::InsertInto)
        assert(cfd->mem()->GetFirstSequenceNumber() == 0);
        assert(edit->NumEntries() == 0);
        continue;
      }

      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::RecoverLogFiles:BeforeFlushFinalMemtable", /*arg=*/nullptr);

      // flush the final memtable (if non-empty)
      if (cfd->mem()->GetFirstSequenceNumber() != 0) {
        // If flush happened in the middle of recovery (e.g. due to memtable
        // being full), we flush at the end. Otherwise we'll need to record
        // where we were on last flush, which make the logic complicated.
        if (flushed || !immutable_db_options_.avoid_flush_during_recovery) {
          status = WriteLevel0TableForRecovery(job_id, cfd, cfd->mem(), edit);
          if (!status.ok()) {
            // Recovery failed
            break;
          }
          flushed = true;

          cfd->CreateNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                 versions_->LastSequence());
        }
        data_seen = true;
      }

      // Update the log number info in the version edit corresponding to this
      // column family. Note that the version edits will be written to MANIFEST
      // together later.
      // writing wal_number in the manifest means that any log file
      // with number strongly less than (wal_number + 1) is already
      // recovered and should be ignored on next reincarnation.
      // Since we already recovered max_wal_number, we want all wals
      // with numbers `<= max_wal_number` (includes this one) to be ignored
      if (flushed || cfd->mem()->GetFirstSequenceNumber() == 0) {
        edit->SetLogNumber(max_wal_number + 1);
      }
    }
    if (status.ok()) {
      // we must mark the next log number as used, even though it's
      // not actually used. that is because VersionSet assumes
      // VersionSet::next_file_number_ always to be strictly greater than any
      // log number
      versions_->MarkFileNumberUsed(max_wal_number + 1);
      assert(recovery_ctx != nullptr);

      for (auto* cfd : *versions_->GetColumnFamilySet()) {
        auto iter = version_edits.find(cfd->GetID());
        assert(iter != version_edits.end());
        recovery_ctx->UpdateVersionEdits(cfd, iter->second);
      }

      if (flushed || !data_seen) {
        VersionEdit wal_deletion;
        if (immutable_db_options_.track_and_verify_wals_in_manifest) {
          wal_deletion.DeleteWalsBefore(max_wal_number + 1);
        }
        if (!allow_2pc()) {
          // In non-2pc mode, flushing the memtables of the column families
          // means we can advance min_log_number_to_keep.
          wal_deletion.SetMinLogNumberToKeep(max_wal_number + 1);
        }
        assert(versions_->GetColumnFamilySet() != nullptr);
        recovery_ctx->UpdateVersionEdits(
            versions_->GetColumnFamilySet()->GetDefault(), wal_deletion);
      }
    }
  }

  if (status.ok()) {
    if (data_seen && !flushed) {
      status = RestoreAliveLogFiles(wal_numbers);
    } else if (!wal_numbers.empty()) {  // If there's no data in the WAL, or we
                                        // flushed all the data, still
      // truncate the log file. If the process goes into a crash loop before
      // the file is deleted, the preallocated space will never get freed.
      const bool truncate = !read_only;
      GetLogSizeAndMaybeTruncate(wal_numbers.back(), truncate, nullptr)
          .PermitUncheckedError();
    }
  }

  event_logger_.Log() << "job" << job_id << "event"
                      << "recovery_finished";

  return status;
}

Status DBImpl::GetLogSizeAndMaybeTruncate(uint64_t wal_number, bool truncate,
                                          LogFileNumberSize* log_ptr) {
  LogFileNumberSize log(wal_number);
  std::string fname =
      LogFileName(immutable_db_options_.GetWalDir(), wal_number);
  Status s;
  // This gets the appear size of the wals, not including preallocated space.
  s = env_->GetFileSize(fname, &log.size);
  TEST_SYNC_POINT_CALLBACK("DBImpl::GetLogSizeAndMaybeTruncate:0", /*arg=*/&s);
  if (s.ok() && truncate) {
    std::unique_ptr<FSWritableFile> last_log;
    Status truncate_status = fs_->ReopenWritableFile(
        fname,
        fs_->OptimizeForLogWrite(
            file_options_,
            BuildDBOptions(immutable_db_options_, mutable_db_options_)),
        &last_log, nullptr);
    if (truncate_status.ok()) {
      truncate_status = last_log->Truncate(log.size, IOOptions(), nullptr);
    }
    if (truncate_status.ok()) {
      truncate_status = last_log->Close(IOOptions(), nullptr);
    }
    // Not a critical error if fail to truncate.
    if (!truncate_status.ok() && !truncate_status.IsNotSupported()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to truncate log #%" PRIu64 ": %s", wal_number,
                     truncate_status.ToString().c_str());
    }
  }
  if (log_ptr) {
    *log_ptr = log;
  }
  return s;
}

Status DBImpl::RestoreAliveLogFiles(const std::vector<uint64_t>& wal_numbers) {
  if (wal_numbers.empty()) {
    return Status::OK();
  }
  Status s;
  mutex_.AssertHeld();
  assert(immutable_db_options_.avoid_flush_during_recovery);
  // Mark these as alive so they'll be considered for deletion later by
  // FindObsoleteFiles()
  total_log_size_ = 0;
  log_empty_ = false;
  uint64_t min_wal_with_unflushed_data =
      versions_->MinLogNumberWithUnflushedData();
  for (auto wal_number : wal_numbers) {
    if (!allow_2pc() && wal_number < min_wal_with_unflushed_data) {
      // In non-2pc mode, the WAL files not backing unflushed data are not
      // alive, thus should not be added to the alive_log_files_.
      continue;
    }
    // We preallocate space for wals, but then after a crash and restart, those
    // preallocated space are not needed anymore. It is likely only the last
    // log has such preallocated space, so we only truncate for the last log.
    LogFileNumberSize log;
    s = GetLogSizeAndMaybeTruncate(
        wal_number, /*truncate=*/(wal_number == wal_numbers.back()), &log);
    if (!s.ok()) {
      break;
    }
    total_log_size_ += log.size;
    alive_log_files_.push_back(log);
  }
  return s;
}

Status DBImpl::WriteLevel0TableForRecovery(int job_id, ColumnFamilyData* cfd,
                                           MemTable* mem, VersionEdit* edit) {
  mutex_.AssertHeld();
  assert(cfd);
  assert(cfd->imm());
  // The immutable memtable list must be empty.
  assert(std::numeric_limits<uint64_t>::max() ==
         cfd->imm()->GetEarliestMemTableID());

  const uint64_t start_micros = immutable_db_options_.clock->NowMicros();

  FileMetaData meta;
  std::vector<BlobFileAddition> blob_file_additions;

  std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_inserted_elem(
      new std::list<uint64_t>::iterator(
          CaptureCurrentFileNumberInPendingOutputs()));
  meta.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
  ReadOptions ro;
  ro.total_order_seek = true;
  ro.io_activity = Env::IOActivity::kDBOpen;
  Arena arena;
  Status s;
  TableProperties table_properties;
  const auto* ucmp = cfd->internal_comparator().user_comparator();
  assert(ucmp);
  const size_t ts_sz = ucmp->timestamp_size();
  const bool logical_strip_timestamp =
      ts_sz > 0 && !cfd->ioptions()->persist_user_defined_timestamps;
  {
    ScopedArenaPtr<InternalIterator> iter(
        logical_strip_timestamp
            ? mem->NewTimestampStrippingIterator(
                  ro, /*seqno_to_time_mapping=*/nullptr, &arena,
                  /*prefix_extractor=*/nullptr, ts_sz)
            : mem->NewIterator(ro, /*seqno_to_time_mapping=*/nullptr, &arena,
                               /*prefix_extractor=*/nullptr,
                               /*for_flush=*/true));
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] [WriteLevel0TableForRecovery]"
                    " Level-0 table #%" PRIu64 ": started",
                    cfd->GetName().c_str(), meta.fd.GetNumber());

    // Get the latest mutable cf options while the mutex is still locked
    const MutableCFOptions mutable_cf_options =
        *cfd->GetLatestMutableCFOptions();
    bool paranoid_file_checks =
        cfd->GetLatestMutableCFOptions()->paranoid_file_checks;

    int64_t _current_time = 0;
    immutable_db_options_.clock->GetCurrentTime(&_current_time)
        .PermitUncheckedError();  // ignore error
    const uint64_t current_time = static_cast<uint64_t>(_current_time);
    meta.oldest_ancester_time = current_time;
    meta.epoch_number = cfd->NewEpochNumber();
    {
      auto write_hint =
          cfd->current()->storage_info()->CalculateSSTWriteHint(/*level=*/0);
      mutex_.Unlock();

      SequenceNumber earliest_write_conflict_snapshot;
      std::vector<SequenceNumber> snapshot_seqs =
          snapshots_.GetAll(&earliest_write_conflict_snapshot);
      SequenceNumber earliest_snapshot =
          (snapshot_seqs.empty() ? kMaxSequenceNumber : snapshot_seqs.at(0));
      auto snapshot_checker = snapshot_checker_.get();
      if (use_custom_gc_ && snapshot_checker == nullptr) {
        snapshot_checker = DisableGCSnapshotChecker::Instance();
      }
      std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
          range_del_iters;
      auto range_del_iter =
          logical_strip_timestamp
              ? mem->NewTimestampStrippingRangeTombstoneIterator(
                    ro, kMaxSequenceNumber, ts_sz)
              // This is called during recovery, where a live memtable is
              // flushed directly. In this case, no fragmented tombstone list is
              // cached in this memtable yet.
              : mem->NewRangeTombstoneIterator(ro, kMaxSequenceNumber,
                                               false /* immutable_memtable */);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }

      IOStatus io_s;
      const ReadOptions read_option(Env::IOActivity::kDBOpen);
      const WriteOptions write_option(Env::IO_HIGH, Env::IOActivity::kDBOpen);

      TableBuilderOptions tboptions(
          *cfd->ioptions(), mutable_cf_options, read_option, write_option,
          cfd->internal_comparator(), cfd->internal_tbl_prop_coll_factories(),
          GetCompressionFlush(*cfd->ioptions(), mutable_cf_options),
          mutable_cf_options.compression_opts, cfd->GetID(), cfd->GetName(),
          0 /* level */, current_time /* newest_key_time */,
          false /* is_bottommost */, TableFileCreationReason::kRecovery,
          0 /* oldest_key_time */, 0 /* file_creation_time */, db_id_,
          db_session_id_, 0 /* target_file_size */, meta.fd.GetNumber(),
          kMaxSequenceNumber);
      Version* version = cfd->current();
      version->Ref();
      uint64_t num_input_entries = 0;
      s = BuildTable(dbname_, versions_.get(), immutable_db_options_, tboptions,
                     file_options_for_compaction_, cfd->table_cache(),
                     iter.get(), std::move(range_del_iters), &meta,
                     &blob_file_additions, snapshot_seqs, earliest_snapshot,
                     earliest_write_conflict_snapshot, kMaxSequenceNumber,
                     snapshot_checker, paranoid_file_checks,
                     cfd->internal_stats(), &io_s, io_tracer_,
                     BlobFileCreationReason::kRecovery,
                     nullptr /* seqno_to_time_mapping */, &event_logger_,
                     job_id, nullptr /* table_properties */, write_hint,
                     nullptr /*full_history_ts_low*/, &blob_callback_, version,
                     &num_input_entries);
      version->Unref();
      LogFlush(immutable_db_options_.info_log);
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] [WriteLevel0TableForRecovery]"
                      " Level-0 table #%" PRIu64 ": %" PRIu64 " bytes %s",
                      cfd->GetName().c_str(), meta.fd.GetNumber(),
                      meta.fd.GetFileSize(), s.ToString().c_str());
      mutex_.Lock();

      // TODO(AR) is this ok?
      if (!io_s.ok() && s.ok()) {
        s = io_s;
      }

      uint64_t total_num_entries = mem->NumEntries();
      if (s.ok() && total_num_entries != num_input_entries) {
        std::string msg = "Expected " + std::to_string(total_num_entries) +
                          " entries in memtable, but read " +
                          std::to_string(num_input_entries);
        ROCKS_LOG_WARN(immutable_db_options_.info_log,
                       "[%s] [JOB %d] Level-0 flush during recover: %s",
                       cfd->GetName().c_str(), job_id, msg.c_str());
        if (immutable_db_options_.flush_verify_memtable_count) {
          s = Status::Corruption(msg);
        }
      }
    }
  }
  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  const bool has_output = meta.fd.GetFileSize() > 0;

  constexpr int level = 0;

  if (s.ok() && has_output) {
    edit->AddFile(level, meta.fd.GetNumber(), meta.fd.GetPathId(),
                  meta.fd.GetFileSize(), meta.smallest, meta.largest,
                  meta.fd.smallest_seqno, meta.fd.largest_seqno,
                  meta.marked_for_compaction, meta.temperature,
                  meta.oldest_blob_file_number, meta.oldest_ancester_time,
                  meta.file_creation_time, meta.epoch_number,
                  meta.file_checksum, meta.file_checksum_func_name,
                  meta.unique_id, meta.compensated_range_deletion_size,
                  meta.tail_size, meta.user_defined_timestamps_persisted);

    for (const auto& blob : blob_file_additions) {
      edit->AddBlobFile(blob);
    }

    // For UDT in memtable only feature, move up the cutoff timestamp whenever
    // a flush happens.
    if (logical_strip_timestamp) {
      Slice mem_newest_udt = mem->GetNewestUDT();
      std::string full_history_ts_low = cfd->GetFullHistoryTsLow();
      if (full_history_ts_low.empty() ||
          ucmp->CompareTimestamp(mem_newest_udt, full_history_ts_low) >= 0) {
        std::string new_full_history_ts_low;
        GetFullHistoryTsLowFromU64CutoffTs(&mem_newest_udt,
                                           &new_full_history_ts_low);
        edit->SetFullHistoryTsLow(new_full_history_ts_low);
      }
    }
  }

  InternalStats::CompactionStats stats(CompactionReason::kFlush, 1);
  stats.micros = immutable_db_options_.clock->NowMicros() - start_micros;

  if (has_output) {
    stats.bytes_written = meta.fd.GetFileSize();
    stats.num_output_files = 1;
  }

  const auto& blobs = edit->GetBlobFileAdditions();
  for (const auto& blob : blobs) {
    stats.bytes_written_blob += blob.GetTotalBlobBytes();
  }

  stats.num_output_files_blob = static_cast<int>(blobs.size());

  cfd->internal_stats()->AddCompactionStats(level, Env::Priority::USER, stats);
  cfd->internal_stats()->AddCFStats(
      InternalStats::BYTES_FLUSHED,
      stats.bytes_written + stats.bytes_written_blob);
  RecordTick(stats_, COMPACT_WRITE_BYTES, meta.fd.GetFileSize());
  return s;
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  if (db_options.persist_stats_to_disk) {
    column_families.emplace_back(kPersistentStatsColumnFamilyName, cf_options);
  }
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    if (db_options.persist_stats_to_disk) {
      assert(handles.size() == 2);
    } else {
      assert(handles.size() == 1);
    }
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    if (db_options.persist_stats_to_disk && handles[1] != nullptr) {
      delete handles[1];
    }
    delete handles[0];
  }
  return s;
}

Status DB::Open(const DBOptions& db_options, const std::string& dbname,
                const std::vector<ColumnFamilyDescriptor>& column_families,
                std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {
  const bool kSeqPerBatch = true;
  const bool kBatchPerTxn = true;
  ThreadStatusUtil::SetEnableTracking(db_options.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_DBOPEN);
  bool can_retry = false;
  Status s;
  do {
    s = DBImpl::Open(db_options, dbname, column_families, handles, dbptr,
                     !kSeqPerBatch, kBatchPerTxn, can_retry, &can_retry);
  } while (!s.ok() && can_retry);
  ThreadStatusUtil::ResetThreadStatus();
  return s;
}

// TODO: Implement the trimming in flush code path.
// TODO: Perform trimming before inserting into memtable during recovery.
// TODO: Pick files with max_timestamp > trim_ts by each file's timestamp meta
// info, and handle only these files to reduce io.
Status DB::OpenAndTrimHistory(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
    std::string trim_ts) {
  assert(dbptr != nullptr);
  assert(handles != nullptr);
  auto validate_options = [&db_options] {
    if (db_options.avoid_flush_during_recovery) {
      return Status::InvalidArgument(
          "avoid_flush_during_recovery incompatible with "
          "OpenAndTrimHistory");
    }
    return Status::OK();
  };
  auto s = validate_options();
  if (!s.ok()) {
    return s;
  }

  DB* db = nullptr;
  s = DB::Open(db_options, dbname, column_families, handles, &db);
  if (!s.ok()) {
    return s;
  }
  assert(db);
  CompactRangeOptions options;
  options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  auto db_impl = static_cast_with_check<DBImpl>(db);
  for (auto handle : *handles) {
    assert(handle != nullptr);
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(handle);
    auto cfd = cfh->cfd();
    assert(cfd != nullptr);
    // Only compact column families with timestamp enabled
    if (cfd->user_comparator() != nullptr &&
        cfd->user_comparator()->timestamp_size() > 0) {
      s = db_impl->CompactRangeInternal(options, handle, nullptr, nullptr,
                                        trim_ts);
      if (!s.ok()) {
        break;
      }
    }
  }
  auto clean_op = [&handles, &db] {
    for (auto handle : *handles) {
      auto temp_s = db->DestroyColumnFamilyHandle(handle);
      assert(temp_s.ok());
    }
    handles->clear();
    delete db;
  };
  if (!s.ok()) {
    clean_op();
    return s;
  }

  *dbptr = db;
  return s;
}

IOStatus DBImpl::CreateWAL(const WriteOptions& write_options,
                           uint64_t log_file_num, uint64_t recycle_log_number,
                           size_t preallocate_block_size,
                           log::Writer** new_log) {
  IOStatus io_s;
  std::unique_ptr<FSWritableFile> lfile;

  DBOptions db_options =
      BuildDBOptions(immutable_db_options_, mutable_db_options_);
  FileOptions opt_file_options =
      fs_->OptimizeForLogWrite(file_options_, db_options);
  // DB option takes precedence when not kUnknown
  if (immutable_db_options_.wal_write_temperature != Temperature::kUnknown) {
    opt_file_options.temperature = immutable_db_options_.wal_write_temperature;
  }
  std::string wal_dir = immutable_db_options_.GetWalDir();
  std::string log_fname = LogFileName(wal_dir, log_file_num);

  if (recycle_log_number) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "reusing log %" PRIu64 " from recycle list\n",
                   recycle_log_number);
    std::string old_log_fname = LogFileName(wal_dir, recycle_log_number);
    TEST_SYNC_POINT("DBImpl::CreateWAL:BeforeReuseWritableFile1");
    TEST_SYNC_POINT("DBImpl::CreateWAL:BeforeReuseWritableFile2");
    io_s = fs_->ReuseWritableFile(log_fname, old_log_fname, opt_file_options,
                                  &lfile, /*dbg=*/nullptr);
  } else {
    io_s = NewWritableFile(fs_.get(), log_fname, &lfile, opt_file_options);
  }

  if (io_s.ok()) {
    lfile->SetWriteLifeTimeHint(CalculateWALWriteHint());
    lfile->SetPreallocationBlockSize(preallocate_block_size);

    const auto& listeners = immutable_db_options_.listeners;
    FileTypeSet tmp_set = immutable_db_options_.checksum_handoff_file_types;
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(lfile), log_fname, opt_file_options,
        immutable_db_options_.clock, io_tracer_, nullptr /* stats */,
        Histograms::HISTOGRAM_ENUM_MAX /* hist_type */, listeners, nullptr,
        tmp_set.Contains(FileType::kWalFile),
        tmp_set.Contains(FileType::kWalFile)));
    *new_log = new log::Writer(std::move(file_writer), log_file_num,
                               immutable_db_options_.recycle_log_file_num > 0,
                               immutable_db_options_.manual_wal_flush,
                               immutable_db_options_.wal_compression);
    io_s = (*new_log)->AddCompressionTypeRecord(write_options);
  }
  return io_s;
}

void DBImpl::TrackExistingDataFiles(
    const std::vector<std::string>& existing_data_files) {
  TrackOrUntrackFiles(existing_data_files, /*track=*/true);
}

Status DBImpl::Open(const DBOptions& db_options, const std::string& dbname,
                    const std::vector<ColumnFamilyDescriptor>& column_families,
                    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
                    const bool seq_per_batch, const bool batch_per_txn,
                    const bool is_retry, bool* can_retry) {
  const WriteOptions write_options(Env::IOActivity::kDBOpen);
  const ReadOptions read_options(Env::IOActivity::kDBOpen);

  Status s = ValidateOptionsByTable(db_options, column_families);
  if (!s.ok()) {
    return s;
  }

  s = ValidateOptions(db_options, column_families);
  if (!s.ok()) {
    return s;
  }

  *dbptr = nullptr;
  assert(handles);
  handles->clear();

  size_t max_write_buffer_size = 0;
  for (const auto& cf : column_families) {
    max_write_buffer_size =
        std::max(max_write_buffer_size, cf.options.write_buffer_size);
  }

  DBImpl* impl = new DBImpl(db_options, dbname, seq_per_batch, batch_per_txn);
  if (!impl->immutable_db_options_.info_log) {
    s = impl->init_logger_creation_s_;
    delete impl;
    return s;
  } else {
    assert(impl->init_logger_creation_s_.ok());
  }
  s = impl->env_->CreateDirIfMissing(impl->immutable_db_options_.GetWalDir());
  if (s.ok()) {
    std::vector<std::string> paths;
    for (auto& db_path : impl->immutable_db_options_.db_paths) {
      paths.emplace_back(db_path.path);
    }
    for (auto& cf : column_families) {
      for (auto& cf_path : cf.options.cf_paths) {
        paths.emplace_back(cf_path.path);
      }
    }
    for (const auto& path : paths) {
      s = impl->env_->CreateDirIfMissing(path);
      if (!s.ok()) {
        break;
      }
    }

    // For recovery from NoSpace() error, we can only handle
    // the case where the database is stored in a single path
    if (paths.size() <= 1) {
      impl->error_handler_.EnableAutoRecovery();
    }
  }
  if (s.ok()) {
    s = impl->CreateArchivalDirectory();
  }
  if (!s.ok()) {
    delete impl;
    return s;
  }

  impl->wal_in_db_path_ = impl->immutable_db_options_.IsWalDirSameAsDBPath();
  RecoveryContext recovery_ctx;
  impl->options_mutex_.Lock();
  impl->mutex_.Lock();

  // Handles create_if_missing, error_if_exists
  uint64_t recovered_seq(kMaxSequenceNumber);
  s = impl->Recover(column_families, false /* read_only */,
                    false /* error_if_wal_file_exists */,
                    false /* error_if_data_exists_in_wals */, is_retry,
                    &recovered_seq, &recovery_ctx, can_retry);
  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    log::Writer* new_log = nullptr;
    const size_t preallocate_block_size =
        impl->GetWalPreallocateBlockSize(max_write_buffer_size);
    s = impl->CreateWAL(write_options, new_log_number, 0 /*recycle_log_number*/,
                        preallocate_block_size, &new_log);
    if (s.ok()) {
      // Prevent log files created by previous instance from being recycled.
      // They might be in alive_log_file_, and might get recycled otherwise.
      impl->min_log_number_to_recycle_ = new_log_number;
    }
    if (s.ok()) {
      InstrumentedMutexLock wl(&impl->log_write_mutex_);
      impl->logfile_number_ = new_log_number;
      assert(new_log != nullptr);
      assert(impl->logs_.empty());
      impl->logs_.emplace_back(new_log_number, new_log);
    }

    if (s.ok()) {
      impl->alive_log_files_.emplace_back(impl->logfile_number_);
      // In WritePrepared there could be gap in sequence numbers. This breaks
      // the trick we use in kPointInTimeRecovery which assumes the first seq in
      // the log right after the corrupted log is one larger than the last seq
      // we read from the wals. To let this trick keep working, we add a dummy
      // entry with the expected sequence to the first log right after recovery.
      // In non-WritePrepared case also the new log after recovery could be
      // empty, and thus missing the consecutive seq hint to distinguish
      // middle-log corruption to corrupted-log-remained-after-recovery. This
      // case also will be addressed by a dummy write.
      if (recovered_seq != kMaxSequenceNumber) {
        WriteBatch empty_batch;
        WriteBatchInternal::SetSequence(&empty_batch, recovered_seq);
        uint64_t log_used, log_size;
        log::Writer* log_writer = impl->logs_.back().writer;
        LogFileNumberSize& log_file_number_size = impl->alive_log_files_.back();

        assert(log_writer->get_log_number() == log_file_number_size.number);
        impl->mutex_.AssertHeld();
        s = impl->WriteToWAL(empty_batch, write_options, log_writer, &log_used,
                             &log_size, log_file_number_size);
        if (s.ok()) {
          // Need to fsync, otherwise it might get lost after a power reset.
          s = impl->FlushWAL(write_options, false);
          TEST_SYNC_POINT_CALLBACK("DBImpl::Open::BeforeSyncWAL", /*arg=*/&s);
          IOOptions opts;
          if (s.ok()) {
            s = WritableFileWriter::PrepareIOOptions(write_options, opts);
          }
          if (s.ok()) {
            s = log_writer->file()->Sync(opts,
                                         impl->immutable_db_options_.use_fsync);
          }
        }
      }
    }
  }
  if (s.ok()) {
    s = impl->LogAndApplyForRecovery(recovery_ctx);
  }

  if (s.ok() && !impl->immutable_db_options_.write_identity_file) {
    // On successful recovery, delete an obsolete IDENTITY file to avoid DB ID
    // inconsistency
    impl->env_->DeleteFile(IdentityFileName(impl->dbname_))
        .PermitUncheckedError();
  }

  if (s.ok() && impl->immutable_db_options_.persist_stats_to_disk) {
    impl->mutex_.AssertHeld();
    s = impl->InitPersistStatsColumnFamily();
  }

  if (s.ok()) {
    // set column family handles
    for (const auto& cf : column_families) {
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (cfd != nullptr) {
        handles->push_back(
            new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
        impl->NewThreadStatusCfInfo(cfd);
      } else {
        if (db_options.create_missing_column_families) {
          // missing column family, create it
          ColumnFamilyHandle* handle = nullptr;
          impl->mutex_.Unlock();
          // NOTE: the work normally done in WrapUpCreateColumnFamilies will
          // be done separately below.
          s = impl->CreateColumnFamilyImpl(read_options, write_options,
                                           cf.options, cf.name, &handle);
          impl->mutex_.Lock();
          if (s.ok()) {
            handles->push_back(handle);
          } else {
            break;
          }
        } else {
          s = Status::InvalidArgument("Column family not found", cf.name);
          break;
        }
      }
    }
  }

  if (s.ok()) {
    SuperVersionContext sv_context(/* create_superversion */ true);
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      impl->InstallSuperVersionAndScheduleWork(
          cfd, &sv_context, *cfd->GetLatestMutableCFOptions());
    }
    sv_context.Clean();
  }

  if (s.ok() && impl->immutable_db_options_.persist_stats_to_disk) {
    // try to read format version
    s = impl->PersistentStatsProcessFormatVersion();
  }

  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      if (!cfd->mem()->IsSnapshotSupported()) {
        impl->is_snapshot_supported_ = false;
      }
      if (cfd->ioptions()->merge_operator != nullptr &&
          !cfd->mem()->IsMergeOperatorSupported()) {
        s = Status::InvalidArgument(
            "The memtable of column family %s does not support merge operator "
            "its options.merge_operator is non-null",
            cfd->GetName().c_str());
      }
      if (!s.ok()) {
        break;
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::Open:Opened");
  Status persist_options_status;
  if (s.ok()) {
    // Persist RocksDB Options before scheduling the compaction.
    // The WriteOptionsFile() will release and lock the mutex internally.
    persist_options_status =
        impl->WriteOptionsFile(write_options, true /*db_mutex_already_held*/);
    *dbptr = impl;
    impl->opened_successfully_ = true;
  } else {
    persist_options_status.PermitUncheckedError();
  }
  impl->mutex_.Unlock();

  auto sfm = static_cast<SstFileManagerImpl*>(
      impl->immutable_db_options_.sst_file_manager.get());
  if (s.ok() && sfm) {
    // Set Statistics ptr for SstFileManager to dump the stats of
    // DeleteScheduler.
    sfm->SetStatisticsPtr(impl->immutable_db_options_.statistics);
    ROCKS_LOG_INFO(impl->immutable_db_options_.info_log,
                   "SstFileManager instance %p", sfm);

    impl->TrackExistingDataFiles(recovery_ctx.existing_data_files_);

    // Reserve some disk buffer space. This is a heuristic - when we run out
    // of disk space, this ensures that there is at least write_buffer_size
    // amount of free space before we resume DB writes. In low disk space
    // conditions, we want to avoid a lot of small L0 files due to frequent
    // WAL write failures and resultant forced flushes
    sfm->ReserveDiskBuffer(max_write_buffer_size,
                           impl->immutable_db_options_.db_paths[0].path);
  }

  if (s.ok()) {
    // When the DB is stopped, it's possible that there are some .trash files
    // that were not deleted yet, when we open the DB we will find these .trash
    // files and schedule them to be deleted (or delete immediately if
    // SstFileManager was not used).
    // Note that we only start doing this and below delete obsolete file after
    // `TrackExistingDataFiles` are called, the `max_trash_db_ratio` is
    // ineffective otherwise and these files' deletion won't be rate limited
    // which can cause discard stall.
    for (const auto& path : impl->CollectAllDBPaths()) {
      DeleteScheduler::CleanupDirectory(impl->immutable_db_options_.env, sfm,
                                        path)
          .PermitUncheckedError();
    }
    impl->mutex_.Lock();
    // This will do a full scan.
    impl->DeleteObsoleteFiles();
    TEST_SYNC_POINT("DBImpl::Open:AfterDeleteFiles");
    impl->MaybeScheduleFlushOrCompaction();
    impl->mutex_.Unlock();
  }

  if (s.ok()) {
    ROCKS_LOG_HEADER(impl->immutable_db_options_.info_log, "DB pointer %p",
                     impl);
    LogFlush(impl->immutable_db_options_.info_log);
    if (!impl->WALBufferIsEmpty()) {
      s = impl->FlushWAL(write_options, false);
      if (s.ok()) {
        // Sync is needed otherwise WAL buffered data might get lost after a
        // power reset.
        log::Writer* log_writer = impl->logs_.back().writer;
        IOOptions opts;
        s = WritableFileWriter::PrepareIOOptions(write_options, opts);
        if (s.ok()) {
          s = log_writer->file()->Sync(opts,
                                       impl->immutable_db_options_.use_fsync);
        }
      }
    }
    if (s.ok() && !persist_options_status.ok()) {
      s = Status::IOError(
          "DB::Open() failed --- Unable to persist Options file",
          persist_options_status.ToString());
    }
  }
  if (!s.ok()) {
    ROCKS_LOG_WARN(impl->immutable_db_options_.info_log,
                   "DB::Open() failed: %s", s.ToString().c_str());
  }
  if (s.ok()) {
    s = impl->StartPeriodicTaskScheduler();
  }
  if (s.ok()) {
    s = impl->RegisterRecordSeqnoTimeWorker(read_options, write_options,
                                            recovery_ctx.is_new_db_);
  }
  impl->options_mutex_.Unlock();
  if (!s.ok()) {
    for (auto* h : *handles) {
      delete h;
    }
    handles->clear();
    delete impl;
    *dbptr = nullptr;
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
