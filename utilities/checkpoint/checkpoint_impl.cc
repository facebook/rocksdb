//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "utilities/checkpoint/checkpoint_impl.h"

#include <algorithm>
#include <cinttypes>
#include <future>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "db/wal_manager.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/checkpoint.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/file_checksum_helper.h"
#include "utilities/copy_engine/copy_engine.h"

namespace ROCKSDB_NAMESPACE {

Status Checkpoint::Create(DB* db, Checkpoint** checkpoint_ptr) {
  *checkpoint_ptr = new CheckpointImpl(db);
  return Status::OK();
}

namespace {
// CheckpointEngine backed by the shared CopyEngine pool; reuses
// CheckpointImpl's enumeration via CreateCheckpointImpl.
class CheckpointEngineImpl : public CheckpointEngine {
 public:
  explicit CheckpointEngineImpl(const CheckpointEngineOptions& options)
      : options_(options) {}

  // Spawn the pool once at Open() so it is reused across checkpoints.
  void Initialize() {
    // Build a rate limiter from backup_rate_limit if the caller passed a number
    // instead of a limiter (mirrors BackupEngineImpl).
    if (options_.backup_rate_limiter == nullptr &&
        options_.backup_rate_limit > 0) {
      options_.backup_rate_limiter.reset(
          NewGenericRateLimiter(options_.backup_rate_limit));
    }
    CopyEngineOptions copy_options;
    copy_options.max_background_operations =
        std::max(1, options_.max_background_operations);
    copy_options.io_buffer_size = options_.io_buffer_size;
    copy_options.info_log = options_.info_log;
    copy_options.thread_name = "rocksdb:ckpt";
    copy_engine_ = std::make_unique<CopyEngine>(std::move(copy_options));
  }

  IOStatus CreateCheckpoint(
      DB* source_db, const std::string& destination_dir,
      uint64_t* sequence_number_ptr,
      const CreateCheckpointOptions& create_options) override {
    assert(copy_engine_ != nullptr);
    if (create_options.decrease_background_thread_cpu_priority) {
      copy_engine_->MaybeDecreaseCpuPriority(
          create_options.background_thread_cpu_priority);
    }

    const bool use_link = options_.use_link_file_when_available;
    CheckpointImpl impl(source_db);
    return status_to_io_status(impl.CreateCheckpointImpl(
        destination_dir, create_options.log_size_for_flush, sequence_number_ptr,
        copy_engine_.get(), use_link, options_.backup_rate_limiter.get()));
  }

  IOStatus ExportColumnFamily(
      DB* source_db, ColumnFamilyHandle* handle,
      const std::string& destination_dir,
      ExportImportFilesMetaData** out_metadata,
      const CreateCheckpointOptions& /*options*/) override {
    // Export runs serially for now; parallelizing it is future work.
    CheckpointImpl impl(source_db);
    return status_to_io_status(
        impl.ExportColumnFamily(handle, destination_dir, out_metadata));
  }

 private:
  CheckpointEngineOptions options_;
  std::unique_ptr<CopyEngine> copy_engine_;
};

// Stages checkpoint files into the temp dir, hiding the serial-vs-parallel
// split from CreateCheckpointImpl. Finish() awaits any deferred work and
// returns the first error.
class CheckpointFileMover {
 public:
  virtual ~CheckpointFileMover() = default;

  // Returning NotSupported makes CreateCustomCheckpoint retry the file via
  // Copy. temperature is the file's temperature, preserved if a failed link
  // falls back to a copy.
  virtual Status Link(const std::string& src, const std::string& dst,
                      Temperature temperature) = 0;

  virtual Status Copy(const std::string& src, const std::string& dst,
                      uint64_t size_limit, Temperature temperature) = 0;

  virtual Status Finish() = 0;
};

// Links/copies inline (legacy Checkpoint behavior); a failed Link() lets
// CreateCustomCheckpoint fall back to copy. Finish() is a no-op.
class SerialFileMover : public CheckpointFileMover {
 public:
  SerialFileMover(FileSystem* fs, bool use_fsync, Logger* info_log)
      : fs_(fs), use_fsync_(use_fsync), info_log_(info_log) {}

  Status Link(const std::string& src, const std::string& dst,
              Temperature /*temperature*/) override {
    // A failed link falls back to copy_file_cb, which preserves temperature.
    ROCKS_LOG_INFO(info_log_, "Hard Linking %s", dst.c_str());
    return fs_->LinkFile(src, dst, IOOptions(), nullptr);
  }

  Status Copy(const std::string& src, const std::string& dst,
              uint64_t size_limit, Temperature temperature) override {
    ROCKS_LOG_INFO(info_log_, "Copying %s", dst.c_str());
    return CopyFile(fs_, src, temperature, dst, temperature, size_limit,
                    use_fsync_, /*io_tracer=*/nullptr);
  }

  Status Finish() override { return Status::OK(); }

 private:
  FileSystem* fs_;
  bool use_fsync_;
  Logger* info_log_;
};

// Enqueues link/copy work on the CopyEngine pool; Finish() awaits it and does
// the link->copy fallback for FileSystems that cannot hard-link (which costs an
// extra round-trip per file; the intended warm-storage targets support
// linking).
class ParallelFileMover : public CheckpointFileMover {
 public:
  ParallelFileMover(CopyEngine* engine, Env* env, bool use_link, bool use_fsync,
                    RateLimiter* copy_rate_limiter, Logger* info_log)
      : engine_(engine),
        env_(env),
        use_link_(use_link),
        use_fsync_(use_fsync),
        copy_rate_limiter_(copy_rate_limiter),
        info_log_(info_log) {}

  Status Link(const std::string& src, const std::string& dst,
              Temperature temperature) override {
    if (!use_link_) {
      // Force CreateCustomCheckpoint down its copy path.
      return Status::NotSupported("Linking disabled");
    }
    ROCKS_LOG_INFO(info_log_, "Hard Linking %s", dst.c_str());
    // Linking moves no data, so the WorkItem carries no temperature; it is kept
    // in PendingLink for the copy fallback in Finish().
    WorkItem w(src, dst, Temperature::kUnknown, Temperature::kUnknown,
               /*contents=*/"", env_, env_, EnvOptions(), use_fsync_,
               /*rate_limiter=*/nullptr, /*size_limit=*/0, /*stats=*/nullptr);
    w.type = WorkItemType::Link;
    link_pendings_.push_back({w.result.get_future(), src, dst, temperature});
    engine_->Submit(std::move(w));
    return Status::OK();
  }

  Status Copy(const std::string& src, const std::string& dst,
              uint64_t size_limit, Temperature temperature) override {
    ROCKS_LOG_INFO(info_log_, "Copying %s", dst.c_str());
    WorkItem w(src, dst, temperature, temperature, /*contents=*/"", env_, env_,
               EnvOptions(), use_fsync_, copy_rate_limiter_, size_limit,
               /*stats=*/nullptr);
    copy_futures_.push_back(w.result.get_future());
    engine_->Submit(std::move(w));
    return Status::OK();
  }

  Status Finish() override {
    Status result;
    auto fold = [&result](const IOStatus& io_s) {
      if (!io_s.ok() && result.ok()) {
        result = io_s;
      }
    };
    for (auto& f : copy_futures_) {
      fold(f.get().io_status);
    }
    // Await all links even after an error: every submitted WorkItem must finish
    // before we return, or pool threads could touch the staging dir during
    // cleanup. NotSupported ones fall back to a copy (still on the pool).
    std::vector<std::future<WorkItemResult>> fallback_copies;
    for (auto& p : link_pendings_) {
      IOStatus link_s = p.future.get().io_status;
      if (link_s.ok()) {
        continue;
      }
      if (link_s.IsNotSupported()) {
        ROCKS_LOG_INFO(info_log_, "Copying (link fallback) %s", p.dst.c_str());
        // size_limit 0 copies the whole file; only immutable (non-trim_to_size)
        // files are linked, so this matches the serial path's info.size bound.
        WorkItem w(p.src, p.dst, p.temperature, p.temperature,
                   /*contents=*/"", env_, env_, EnvOptions(), use_fsync_,
                   copy_rate_limiter_, /*size_limit=*/0, /*stats=*/nullptr);
        fallback_copies.push_back(w.result.get_future());
        engine_->Submit(std::move(w));
      } else {
        fold(link_s);
      }
    }
    for (auto& f : fallback_copies) {
      fold(f.get().io_status);
    }
    return result;
  }

 private:
  struct PendingLink {
    std::future<WorkItemResult> future;
    std::string src;
    std::string dst;
    Temperature temperature = Temperature::kUnknown;
  };

  CopyEngine* engine_;
  Env* env_;
  bool use_link_;
  bool use_fsync_;
  RateLimiter* copy_rate_limiter_;
  Logger* info_log_;
  std::vector<std::future<WorkItemResult>> copy_futures_;
  std::vector<PendingLink> link_pendings_;
};
}  // namespace

Status CheckpointEngine::Open(
    const CheckpointEngineOptions& options,
    std::unique_ptr<CheckpointEngine>* out_checkpoint_engine) {
  if (out_checkpoint_engine == nullptr) {
    return Status::InvalidArgument("out_checkpoint_engine must not be null");
  }
  auto engine = std::make_unique<CheckpointEngineImpl>(options);
  engine->Initialize();
  *out_checkpoint_engine = std::move(engine);
  return Status::OK();
}

Status Checkpoint::CreateCheckpoint(const std::string& /*checkpoint_dir*/,
                                    uint64_t /*log_size_for_flush*/,
                                    uint64_t* /*sequence_number_ptr*/) {
  return Status::NotSupported("");
}

Status CheckpointImpl::CleanStagingDirectory(
    const std::string& full_private_path, Logger* info_log) {
  std::vector<std::string> subchildren;
  Status s = db_->GetEnv()->FileExists(full_private_path);
  if (s.IsNotFound()) {
    // Nothing to clean
    return Status::OK();
  } else if (!s.ok()) {
    return s;
  }
  assert(s.ok());
  ROCKS_LOG_INFO(info_log, "File exists %s -- %s", full_private_path.c_str(),
                 s.ToString().c_str());

  s = db_->GetEnv()->GetChildren(full_private_path, &subchildren);
  if (s.ok()) {
    for (auto& subchild : subchildren) {
      Status del_s;
      std::string subchild_path = full_private_path + "/" + subchild;
      del_s = db_->GetEnv()->DeleteFile(subchild_path);
      ROCKS_LOG_INFO(info_log, "Delete file %s -- %s", subchild_path.c_str(),
                     del_s.ToString().c_str());
      if (!del_s.ok() && s.ok()) {
        s = del_s;
      }
    }
  }

  // Then delete the private dir
  if (s.ok()) {
    s = db_->GetEnv()->DeleteDir(full_private_path);
    ROCKS_LOG_INFO(info_log, "Delete dir %s -- %s", full_private_path.c_str(),
                   s.ToString().c_str());
  }
  return s;
}

Status Checkpoint::ExportColumnFamily(
    ColumnFamilyHandle* /*handle*/, const std::string& /*export_dir*/,
    ExportImportFilesMetaData** /*metadata*/) {
  return Status::NotSupported("");
}

// Builds an openable snapshot of RocksDB
Status CheckpointImpl::CreateCheckpoint(const std::string& checkpoint_dir,
                                        uint64_t log_size_for_flush,
                                        uint64_t* sequence_number_ptr) {
  return CreateCheckpointImpl(checkpoint_dir, log_size_for_flush,
                              sequence_number_ptr, /*engine=*/nullptr,
                              /*use_link=*/true, /*copy_rate_limiter=*/nullptr);
}

Status CheckpointImpl::CreateCheckpointImpl(const std::string& checkpoint_dir,
                                            uint64_t log_size_for_flush,
                                            uint64_t* sequence_number_ptr,
                                            CopyEngine* engine, bool use_link,
                                            RateLimiter* copy_rate_limiter) {
  DBOptions db_options = db_->GetDBOptions();
  Env* env = db_->GetEnv();
  const auto& fs = db_->GetFileSystem();

  Status file_exists_s = env->FileExists(checkpoint_dir);
  if (file_exists_s.ok()) {
    return Status::InvalidArgument("Directory exists");
  } else if (!file_exists_s.IsNotFound()) {
    assert(file_exists_s.IsIOError());
    return file_exists_s;
  } else {
    assert(file_exists_s.IsNotFound());
  };

  Status s;
  ROCKS_LOG_INFO(
      db_options.info_log,
      "Started the snapshot process -- creating snapshot in directory %s",
      checkpoint_dir.c_str());

  size_t final_nonslash_idx = checkpoint_dir.find_last_not_of('/');
  if (final_nonslash_idx == std::string::npos) {
    // npos means it's only slashes or empty. Non-empty means it's the root
    // directory, but it shouldn't be because we verified above the directory
    // doesn't exist.
    assert(checkpoint_dir.empty());
    s.PermitUncheckedError();
    return Status::InvalidArgument("invalid checkpoint directory name");
  }

  std::string full_private_path =
      checkpoint_dir.substr(0, final_nonslash_idx + 1) + ".tmp";
  ROCKS_LOG_INFO(db_options.info_log,
                 "Snapshot process -- using temporary directory %s",
                 full_private_path.c_str());

  s = CleanStagingDirectory(full_private_path, db_options.info_log.get());
  if (!s.ok()) {
    return Status::Aborted(
        "Failed to clean the temporary directory " + full_private_path +
        " needed before checkpoint creation : " + s.ToString());
  }

  std::unique_ptr<CheckpointFileMover> mover;
  if (engine == nullptr) {
    mover = std::make_unique<SerialFileMover>(fs, db_options.use_fsync,
                                              db_options.info_log.get());
  } else {
    mover = std::make_unique<ParallelFileMover>(
        engine, env, use_link, db_options.use_fsync, copy_rate_limiter,
        db_options.info_log.get());
  }

  // create snapshot directory
  s = env->CreateDir(full_private_path);
  uint64_t sequence_number = 0;
  if (s.ok()) {
    // enable file deletions
    s = db_->DisableFileDeletions();
    const bool disabled_file_deletions = s.ok();

    if (s.ok() || s.IsNotSupported()) {
      s = CreateCustomCheckpoint(
          [&](const std::string& src_dirname, const std::string& fname,
              FileType, const Temperature temperature) -> Status {
            return mover->Link(src_dirname + "/" + fname,
                               full_private_path + "/" + fname, temperature);
          } /* link_file_cb */,
          [&](const std::string& src_dirname, const std::string& fname,
              uint64_t size_limit_bytes, FileType,
              const std::string& /* checksum_func_name */,
              const std::string& /* checksum_val */,
              const Temperature temperature) -> Status {
            return mover->Copy(src_dirname + "/" + fname,
                               full_private_path + "/" + fname,
                               size_limit_bytes, temperature);
          } /* copy_file_cb */,
          [&](const std::string& fname, const std::string& contents, FileType) {
            ROCKS_LOG_INFO(db_options.info_log, "Creating %s", fname.c_str());
            return CreateFile(fs, full_private_path + "/" + fname, contents,
                              db_options.use_fsync);
          } /* create_file_cb */,
          &sequence_number, log_size_for_flush);

      // Await any deferred work and fold in the first error before committing.
      Status finish_s = mover->Finish();
      if (s.ok()) {
        s = finish_s;
      } else {
        finish_s.PermitUncheckedError();
      }

      // we copied all the files, enable file deletions
      if (disabled_file_deletions) {
        Status ss = db_->EnableFileDeletions();
        assert(ss.ok());
        ss.PermitUncheckedError();
      }
    }
  }

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    Status rename_s = env->RenameFile(full_private_path, checkpoint_dir);
    if (!rename_s.ok()) {
      s = rename_s;
    }
  }
  if (s.ok()) {
    std::unique_ptr<FSDirectory> checkpoint_directory;
    IOStatus new_dir_io_s = fs->NewDirectory(checkpoint_dir, IOOptions(),
                                             &checkpoint_directory, nullptr);
    if (new_dir_io_s.ok() && checkpoint_directory != nullptr) {
      IOStatus fsync_io_s = checkpoint_directory->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kDirRenamed));
      if (!fsync_io_s.ok()) {
        s = fsync_io_s;
      }
    } else if (!new_dir_io_s.ok()) {
      s = new_dir_io_s;
    }
  }

  if (s.ok()) {
    if (sequence_number_ptr != nullptr) {
      *sequence_number_ptr = sequence_number;
    }
    // here we know that we succeeded and installed the new snapshot
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot DONE. All is good");
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot sequence number: %" PRIu64,
                   sequence_number);
  } else {
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot failed -- %s",
                   s.ToString().c_str());
    // clean all the files and directory we might have created
    Status del_s =
        CleanStagingDirectory(full_private_path, db_options.info_log.get());
    ROCKS_LOG_INFO(db_options.info_log,
                   "Clean files or directory we might have created %s: %s",
                   full_private_path.c_str(), del_s.ToString().c_str());
    del_s.PermitUncheckedError();
  }
  return s;
}

Status CheckpointImpl::CreateCustomCheckpoint(
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname, FileType type,
                         const Temperature temperature)>
        link_file_cb,
    std::function<
        Status(const std::string& src_dirname, const std::string& src_fname,
               uint64_t size_limit_bytes, FileType type,
               const std::string& checksum_func_name,
               const std::string& checksum_val, const Temperature temperature)>
        copy_file_cb,
    std::function<Status(const std::string& fname, const std::string& contents,
                         FileType type)>
        create_file_cb,
    uint64_t* sequence_number, uint64_t log_size_for_flush,
    bool get_live_table_checksum, bool atomic_flush) {
  *sequence_number = db_->GetLatestSequenceNumber();

  LiveFilesStorageInfoOptions opts;
  opts.include_checksum_info = get_live_table_checksum;
  opts.wal_size_for_flush = log_size_for_flush;
  opts.atomic_flush = atomic_flush;

  std::vector<LiveFileStorageInfo> infos;
  {
    Status s = db_->GetLiveFilesStorageInfo(opts, &infos);
    if (!s.ok()) {
      return s;
    }
  }

  // Verify that everything except WAL files are in same directory
  // (db_paths / cf_paths not supported)
  std::unordered_set<std::string> dirs;
  for (auto& info : infos) {
    if (info.file_type != kWalFile) {
      dirs.insert(info.directory);
    }
  }
  if (dirs.size() > 1) {
    return Status::NotSupported(
        "db_paths / cf_paths not supported for Checkpoint nor BackupEngine");
  }

  bool same_fs = true;

  for (auto& info : infos) {
    Status s;
    if (!info.replacement_contents.empty()) {
      // Currently should only be used for CURRENT file.
      assert(info.file_type == kCurrentFile);

      if (info.size != info.replacement_contents.size()) {
        s = Status::Corruption("Inconsistent size metadata for " +
                               info.relative_filename);
      } else {
        s = create_file_cb(info.relative_filename, info.replacement_contents,
                           info.file_type);
      }
    } else {
      if (same_fs && !info.trim_to_size) {
        s = link_file_cb(info.directory, info.relative_filename, info.file_type,
                         info.temperature);
        if (s.IsNotSupported()) {
          same_fs = false;
          s = Status::OK();
        }
        s.MustCheck();
      }
      if (!same_fs || info.trim_to_size) {
        assert(info.file_checksum_func_name.empty() ==
               !opts.include_checksum_info);
        // no assertion on file_checksum because empty is used for both "not
        // set" and "unknown"
        if (opts.include_checksum_info) {
          s = copy_file_cb(info.directory, info.relative_filename, info.size,
                           info.file_type, info.file_checksum_func_name,
                           info.file_checksum, info.temperature);
        } else {
          s = copy_file_cb(info.directory, info.relative_filename, info.size,
                           info.file_type, kUnknownFileChecksumFuncName,
                           kUnknownFileChecksum, info.temperature);
        }
      }
    }
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

// Exports all live SST files of a specified Column Family onto export_dir,
// returning SST files information in metadata.
Status CheckpointImpl::ExportColumnFamily(
    ColumnFamilyHandle* handle, const std::string& export_dir,
    ExportImportFilesMetaData** metadata) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(handle);
  const auto cf_name = cfh->GetName();
  const auto db_options = db_->GetDBOptions();

  assert(metadata != nullptr);
  assert(*metadata == nullptr);
  auto s = db_->GetEnv()->FileExists(export_dir);
  if (s.ok()) {
    return Status::InvalidArgument("Specified export_dir exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  const auto final_nonslash_idx = export_dir.find_last_not_of('/');
  if (final_nonslash_idx == std::string::npos) {
    return Status::InvalidArgument("Specified export_dir invalid");
  }
  ROCKS_LOG_INFO(db_options.info_log,
                 "[%s] export column family onto export directory %s",
                 cf_name.c_str(), export_dir.c_str());

  // Create a temporary export directory.
  const auto tmp_export_dir =
      export_dir.substr(0, final_nonslash_idx + 1) + ".tmp";
  s = db_->GetEnv()->CreateDir(tmp_export_dir);

  if (s.ok()) {
    // FIXME: should respect atomic_flush and flush all CFs if needed.
    s = db_->Flush(ROCKSDB_NAMESPACE::FlushOptions(), handle);
  }

  ColumnFamilyMetaData db_metadata;
  if (s.ok()) {
    // Export live sst files with file deletions disabled.
    s = db_->DisableFileDeletions();
    if (s.ok()) {
      db_->GetColumnFamilyMetaData(handle, &db_metadata);

      s = ExportFilesInMetaData(
          db_options, db_metadata,
          [&](const std::string& src_dirname, const std::string& fname) {
            ROCKS_LOG_INFO(db_options.info_log, "[%s] HardLinking %s",
                           cf_name.c_str(), fname.c_str());
            return db_->GetEnv()->LinkFile(src_dirname + fname,
                                           tmp_export_dir + fname);
          } /*link_file_cb*/,
          [&](const std::string& src_dirname, const std::string& fname) {
            ROCKS_LOG_INFO(db_options.info_log, "[%s] Copying %s",
                           cf_name.c_str(), fname.c_str());
            // FIXME: temperature handling
            return CopyFile(db_->GetFileSystem(), src_dirname + fname,
                            Temperature::kUnknown, tmp_export_dir + fname,
                            Temperature::kUnknown, 0, db_options.use_fsync,
                            nullptr);
          } /*copy_file_cb*/);

      const auto enable_status = db_->EnableFileDeletions();
      if (s.ok()) {
        s = enable_status;
      }
    }
  }

  auto moved_to_user_specified_dir = false;
  if (s.ok()) {
    // Move temporary export directory to the actual export directory.
    s = db_->GetEnv()->RenameFile(tmp_export_dir, export_dir);
  }

  if (s.ok()) {
    // Fsync export directory.
    moved_to_user_specified_dir = true;
    std::unique_ptr<FSDirectory> dir_ptr;
    s = db_->GetFileSystem()->NewDirectory(export_dir, IOOptions(), &dir_ptr,
                                           nullptr);
    if (s.ok()) {
      assert(dir_ptr != nullptr);
      s = dir_ptr->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kDirRenamed));
    }
  }

  if (s.ok()) {
    // Export of files succeeded. Fill in the metadata information.
    auto result_metadata = new ExportImportFilesMetaData();
    result_metadata->db_comparator_name = handle->GetComparator()->Name();
    for (const auto& level_metadata : db_metadata.levels) {
      for (const auto& file_metadata : level_metadata.files) {
        LiveFileMetaData live_file_metadata;
        live_file_metadata.size = file_metadata.size;
        live_file_metadata.name = file_metadata.name;
        live_file_metadata.file_number = file_metadata.file_number;
        live_file_metadata.db_path = export_dir;
        live_file_metadata.smallest_seqno = file_metadata.smallest_seqno;
        live_file_metadata.largest_seqno = file_metadata.largest_seqno;
        live_file_metadata.smallestkey = file_metadata.smallestkey;
        live_file_metadata.largestkey = file_metadata.largestkey;
        live_file_metadata.oldest_blob_file_number =
            file_metadata.oldest_blob_file_number;
        live_file_metadata.epoch_number = file_metadata.epoch_number;
        live_file_metadata.level = level_metadata.level;
        live_file_metadata.smallest = file_metadata.smallest;
        live_file_metadata.largest = file_metadata.largest;
        result_metadata->files.push_back(live_file_metadata);
      }
    }
    *metadata = result_metadata;
    ROCKS_LOG_INFO(db_options.info_log, "[%s] Export succeeded.",
                   cf_name.c_str());
  } else {
    // Failure: Clean up all the files/directories created.
    ROCKS_LOG_INFO(db_options.info_log, "[%s] Export failed. %s",
                   cf_name.c_str(), s.ToString().c_str());
    std::vector<std::string> subchildren;
    const auto cleanup_dir =
        moved_to_user_specified_dir ? export_dir : tmp_export_dir;
    db_->GetEnv()->GetChildren(cleanup_dir, &subchildren);
    for (const auto& subchild : subchildren) {
      const auto subchild_path = cleanup_dir + "/" + subchild;
      const auto status = db_->GetEnv()->DeleteFile(subchild_path);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options.info_log, "Failed to cleanup file %s: %s",
                       subchild_path.c_str(), status.ToString().c_str());
      }
    }
    const auto status = db_->GetEnv()->DeleteDir(cleanup_dir);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options.info_log, "Failed to cleanup dir %s: %s",
                     cleanup_dir.c_str(), status.ToString().c_str());
    }
  }
  return s;
}

Status CheckpointImpl::ExportFilesInMetaData(
    const DBOptions& db_options, const ColumnFamilyMetaData& metadata,
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname)>
        link_file_cb,
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname)>
        copy_file_cb) {
  Status s;
  auto hardlink_file = true;

  // Copy/hard link files in metadata.
  size_t num_files = 0;
  for (const auto& level_metadata : metadata.levels) {
    for (const auto& file_metadata : level_metadata.files) {
      uint64_t number;
      FileType type;
      const auto ok = ParseFileName(file_metadata.name, &number, &type);
      if (!ok) {
        s = Status::Corruption("Could not parse file name");
        break;
      }

      // We should only get sst files here.
      assert(type == kTableFile);
      assert(file_metadata.size > 0 && file_metadata.name[0] == '/');
      const auto src_fname = file_metadata.name;
      ++num_files;

      if (hardlink_file) {
        s = link_file_cb(db_->GetName(), src_fname);
        if (num_files == 1 && s.IsNotSupported()) {
          // Fallback to copy if link failed due to cross-device directories.
          hardlink_file = false;
          s = Status::OK();
        }
      }
      if (!hardlink_file) {
        s = copy_file_cb(db_->GetName(), src_fname);
      }
      if (!s.ok()) {
        break;
      }
    }
  }
  ROCKS_LOG_INFO(db_options.info_log, "Number of table files %" ROCKSDB_PRIszt,
                 num_files);

  return s;
}
}  // namespace ROCKSDB_NAMESPACE
