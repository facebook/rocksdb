//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#include "utilities/checkpoint/checkpoint_impl.h"

#include <algorithm>
#include <cinttypes>
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
#include "rocksdb/transaction_log.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/checkpoint.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/file_checksum_helper.h"

namespace ROCKSDB_NAMESPACE {

Status Checkpoint::Create(DB* db, Checkpoint** checkpoint_ptr) {
  *checkpoint_ptr = new CheckpointImpl(db);
  return Status::OK();
}

Status Checkpoint::CreateCheckpoint(const std::string& /*checkpoint_dir*/,
                                    uint64_t /*log_size_for_flush*/,
                                    uint64_t* /*sequence_number_ptr*/) {
  return Status::NotSupported("");
}

void CheckpointImpl::CleanStagingDirectory(const std::string& full_private_path,
                                           Logger* info_log) {
  std::vector<std::string> subchildren;
  Status s = db_->GetEnv()->FileExists(full_private_path);
  if (s.IsNotFound()) {
    return;
  }
  ROCKS_LOG_INFO(info_log, "File exists %s -- %s", full_private_path.c_str(),
                 s.ToString().c_str());
  s = db_->GetEnv()->GetChildren(full_private_path, &subchildren);
  if (s.ok()) {
    for (auto& subchild : subchildren) {
      std::string subchild_path = full_private_path + "/" + subchild;
      s = db_->GetEnv()->DeleteFile(subchild_path);
      ROCKS_LOG_INFO(info_log, "Delete file %s -- %s", subchild_path.c_str(),
                     s.ToString().c_str());
    }
  }
  // finally delete the private dir
  s = db_->GetEnv()->DeleteDir(full_private_path);
  ROCKS_LOG_INFO(info_log, "Delete dir %s -- %s", full_private_path.c_str(),
                 s.ToString().c_str());
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
  DBOptions db_options = db_->GetDBOptions();

  Status s = db_->GetEnv()->FileExists(checkpoint_dir);
  if (s.ok()) {
    return Status::InvalidArgument("Directory exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

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
    return Status::InvalidArgument("invalid checkpoint directory name");
  }

  std::string full_private_path =
      checkpoint_dir.substr(0, final_nonslash_idx + 1) + ".tmp";
  ROCKS_LOG_INFO(db_options.info_log,
                 "Snapshot process -- using temporary directory %s",
                 full_private_path.c_str());
  CleanStagingDirectory(full_private_path, db_options.info_log.get());
  // create snapshot directory
  s = db_->GetEnv()->CreateDir(full_private_path);
  uint64_t sequence_number = 0;
  if (s.ok()) {
    // enable file deletions
    s = db_->DisableFileDeletions();
    const bool disabled_file_deletions = s.ok();

    if (s.ok() || s.IsNotSupported()) {
      s = CreateCustomCheckpoint(
          [&](const std::string& src_dirname, const std::string& fname,
              FileType) {
            ROCKS_LOG_INFO(db_options.info_log, "Hard Linking %s",
                           fname.c_str());
            return db_->GetFileSystem()->LinkFile(
                src_dirname + "/" + fname, full_private_path + "/" + fname,
                IOOptions(), nullptr);
          } /* link_file_cb */,
          [&](const std::string& src_dirname, const std::string& fname,
              uint64_t size_limit_bytes, FileType,
              const std::string& /* checksum_func_name */,
              const std::string& /* checksum_val */,
              const Temperature temperature) {
            ROCKS_LOG_INFO(db_options.info_log, "Copying %s", fname.c_str());
            return CopyFile(db_->GetFileSystem(), src_dirname + "/" + fname,
                            full_private_path + "/" + fname, size_limit_bytes,
                            db_options.use_fsync, nullptr, temperature);
          } /* copy_file_cb */,
          [&](const std::string& fname, const std::string& contents, FileType) {
            ROCKS_LOG_INFO(db_options.info_log, "Creating %s", fname.c_str());
            return CreateFile(db_->GetFileSystem(),
                              full_private_path + "/" + fname, contents,
                              db_options.use_fsync);
          } /* create_file_cb */,
          &sequence_number, log_size_for_flush);

      // we copied all the files, enable file deletions
      if (disabled_file_deletions) {
        Status ss = db_->EnableFileDeletions(false);
        assert(ss.ok());
        ss.PermitUncheckedError();
      }
    }
  }

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    s = db_->GetEnv()->RenameFile(full_private_path, checkpoint_dir);
  }
  if (s.ok()) {
    std::unique_ptr<FSDirectory> checkpoint_directory;
    s = db_->GetFileSystem()->NewDirectory(checkpoint_dir, IOOptions(),
                                           &checkpoint_directory, nullptr);
    if (s.ok() && checkpoint_directory != nullptr) {
      s = checkpoint_directory->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kDirRenamed));
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
    // clean all the files we might have created
    ROCKS_LOG_INFO(db_options.info_log, "Snapshot failed -- %s",
                   s.ToString().c_str());
    CleanStagingDirectory(full_private_path, db_options.info_log.get());
  }
  return s;
}

Status CheckpointImpl::CreateCustomCheckpoint(
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname, FileType type)>
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
    bool get_live_table_checksum) {
  *sequence_number = db_->GetLatestSequenceNumber();

  LiveFilesStorageInfoOptions opts;
  opts.include_checksum_info = get_live_table_checksum;
  opts.wal_size_for_flush = log_size_for_flush;

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
        s = link_file_cb(info.directory, info.relative_filename,
                         info.file_type);
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
            return CopyFile(db_->GetFileSystem(), src_dirname + fname,
                            tmp_export_dir + fname, 0, db_options.use_fsync,
                            nullptr, Temperature::kUnknown);
          } /*copy_file_cb*/);

      const auto enable_status = db_->EnableFileDeletions(false /*force*/);
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
        live_file_metadata.name = std::move(file_metadata.name);
        live_file_metadata.file_number = file_metadata.file_number;
        live_file_metadata.db_path = export_dir;
        live_file_metadata.smallest_seqno = file_metadata.smallest_seqno;
        live_file_metadata.largest_seqno = file_metadata.largest_seqno;
        live_file_metadata.smallestkey = std::move(file_metadata.smallestkey);
        live_file_metadata.largestkey = std::move(file_metadata.largestkey);
        live_file_metadata.oldest_blob_file_number =
            file_metadata.oldest_blob_file_number;
        live_file_metadata.level = level_metadata.level;
        result_metadata->files.push_back(live_file_metadata);
      }
      *metadata = result_metadata;
    }
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

#endif  // ROCKSDB_LITE
