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
#include <vector>

#include "db/wal_manager.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/utilities/checkpoint.h"
#include "test_util/sync_point.h"

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

void CheckpointImpl::CleanStagingDirectory(
    const std::string& full_private_path, Logger* info_log) {
    std::vector<std::string> subchildren;
  Status s = db_->GetEnv()->FileExists(full_private_path);
  if (s.IsNotFound()) {
    return;
  }
  ROCKS_LOG_INFO(info_log, "File exists %s -- %s",
                 full_private_path.c_str(), s.ToString().c_str());
  db_->GetEnv()->GetChildren(full_private_path, &subchildren);
  for (auto& subchild : subchildren) {
    std::string subchild_path = full_private_path + "/" + subchild;
    s = db_->GetEnv()->DeleteFile(subchild_path);
    ROCKS_LOG_INFO(info_log, "Delete file %s -- %s",
                   subchild_path.c_str(), s.ToString().c_str());
  }
  // finally delete the private dir
  s = db_->GetEnv()->DeleteDir(full_private_path);
  ROCKS_LOG_INFO(info_log, "Delete dir %s -- %s",
                 full_private_path.c_str(), s.ToString().c_str());
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
  ROCKS_LOG_INFO(
      db_options.info_log,
      "Snapshot process -- using temporary directory %s",
      full_private_path.c_str());
  CleanStagingDirectory(full_private_path, db_options.info_log.get());
  // create snapshot directory
  s = db_->GetEnv()->CreateDir(full_private_path);
  uint64_t sequence_number = 0;
  if (s.ok()) {
    db_->DisableFileDeletions();
    s = CreateCustomCheckpoint(
        db_options,
        [&](const std::string& src_dirname, const std::string& fname,
            FileType) {
          ROCKS_LOG_INFO(db_options.info_log, "Hard Linking %s", fname.c_str());
          return db_->GetFileSystem()->LinkFile(src_dirname + fname,
                                                full_private_path + fname,
                                                IOOptions(), nullptr);
        } /* link_file_cb */,
        [&](const std::string& src_dirname, const std::string& fname,
            uint64_t size_limit_bytes, FileType) {
          ROCKS_LOG_INFO(db_options.info_log, "Copying %s", fname.c_str());
          return CopyFile(db_->GetFileSystem(), src_dirname + fname,
                          full_private_path + fname, size_limit_bytes,
                          db_options.use_fsync);
        } /* copy_file_cb */,
        [&](const std::string& fname, const std::string& contents, FileType) {
          ROCKS_LOG_INFO(db_options.info_log, "Creating %s", fname.c_str());
          return CreateFile(db_->GetFileSystem(), full_private_path + fname,
                            contents, db_options.use_fsync);
        } /* create_file_cb */,
        &sequence_number, log_size_for_flush);
    // we copied all the files, enable file deletions
    db_->EnableFileDeletions(false);
  }

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    s = db_->GetEnv()->RenameFile(full_private_path, checkpoint_dir);
  }
  if (s.ok()) {
    std::unique_ptr<Directory> checkpoint_directory;
    db_->GetEnv()->NewDirectory(checkpoint_dir, &checkpoint_directory);
    if (checkpoint_directory != nullptr) {
      s = checkpoint_directory->Fsync();
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
    const DBOptions& db_options,
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname, FileType type)>
        link_file_cb,
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname,
                         uint64_t size_limit_bytes, FileType type)>
        copy_file_cb,
    std::function<Status(const std::string& fname, const std::string& contents,
                         FileType type)>
        create_file_cb,
    uint64_t* sequence_number, uint64_t log_size_for_flush) {
  Status s;
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  uint64_t min_log_num = port::kMaxUint64;
  *sequence_number = db_->GetLatestSequenceNumber();
  bool same_fs = true;
  VectorLogPtr live_wal_files;

  bool flush_memtable = true;
  if (s.ok()) {
    if (!db_options.allow_2pc) {
      if (log_size_for_flush == port::kMaxUint64) {
        flush_memtable = false;
      } else if (log_size_for_flush > 0) {
        // If out standing log files are small, we skip the flush.
        s = db_->GetSortedWalFiles(live_wal_files);

        if (!s.ok()) {
          return s;
        }

        // Don't flush column families if total log size is smaller than
        // log_size_for_flush. We copy the log files instead.
        // We may be able to cover 2PC case too.
        uint64_t total_wal_size = 0;
        for (auto& wal : live_wal_files) {
          total_wal_size += wal->SizeFileBytes();
        }
        if (total_wal_size < log_size_for_flush) {
          flush_memtable = false;
        }
        live_wal_files.clear();
      }
    }

    // this will return live_files prefixed with "/"
    s = db_->GetLiveFiles(live_files, &manifest_file_size, flush_memtable);

    if (s.ok() && db_options.allow_2pc) {
      // If 2PC is enabled, we need to get minimum log number after the flush.
      // Need to refetch the live files to recapture the snapshot.
      if (!db_->GetIntProperty(DB::Properties::kMinLogNumberToKeep,
                               &min_log_num)) {
        return Status::InvalidArgument(
            "2PC enabled but cannot fine the min log number to keep.");
      }
      // We need to refetch live files with flush to handle this case:
      // A previous 000001.log contains the prepare record of transaction tnx1.
      // The current log file is 000002.log, and sequence_number points to this
      // file.
      // After calling GetLiveFiles(), 000003.log is created.
      // Then tnx1 is committed. The commit record is written to 000003.log.
      // Now we fetch min_log_num, which will be 3.
      // Then only 000002.log and 000003.log will be copied, and 000001.log will
      // be skipped. 000003.log contains commit message of tnx1, but we don't
      // have respective prepare record for it.
      // In order to avoid this situation, we need to force flush to make sure
      // all transactions committed before getting min_log_num will be flushed
      // to SST files.
      // We cannot get min_log_num before calling the GetLiveFiles() for the
      // first time, because if we do that, all the logs files will be included,
      // far more than needed.
      s = db_->GetLiveFiles(live_files, &manifest_file_size, flush_memtable);
    }

    TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles1");
    TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles2");
    db_->FlushWAL(false /* sync */);
  }
  // if we have more than one column family, we need to also get WAL files
  if (s.ok()) {
    s = db_->GetSortedWalFiles(live_wal_files);
  }
  if (!s.ok()) {
    return s;
  }

  size_t wal_size = live_wal_files.size();

  // copy/hard link live_files
  std::string manifest_fname, current_fname;
  for (size_t i = 0; s.ok() && i < live_files.size(); ++i) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(live_files[i], &number, &type);
    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad");
      break;
    }
    // we should only get sst, options, manifest and current files here
    assert(type == kTableFile || type == kDescriptorFile ||
           type == kCurrentFile || type == kOptionsFile);
    assert(live_files[i].size() > 0 && live_files[i][0] == '/');
    if (type == kCurrentFile) {
      // We will craft the current file manually to ensure it's consistent with
      // the manifest number. This is necessary because current's file contents
      // can change during checkpoint creation.
      current_fname = live_files[i];
      continue;
    } else if (type == kDescriptorFile) {
      manifest_fname = live_files[i];
    }
    std::string src_fname = live_files[i];

    // rules:
    // * if it's kTableFile, then it's shared
    // * if it's kDescriptorFile, limit the size to manifest_file_size
    // * always copy if cross-device link
    if ((type == kTableFile) && same_fs) {
      s = link_file_cb(db_->GetName(), src_fname, type);
      if (s.IsNotSupported()) {
        same_fs = false;
        s = Status::OK();
      }
    }
    if ((type != kTableFile) || (!same_fs)) {
      s = copy_file_cb(db_->GetName(), src_fname,
                       (type == kDescriptorFile) ? manifest_file_size : 0,
                       type);
    }
  }
  if (s.ok() && !current_fname.empty() && !manifest_fname.empty()) {
    create_file_cb(current_fname, manifest_fname.substr(1) + "\n",
                   kCurrentFile);
  }
  ROCKS_LOG_INFO(db_options.info_log, "Number of log files %" ROCKSDB_PRIszt,
                 live_wal_files.size());

  // Link WAL files. Copy exact size of last one because it is the only one
  // that has changes after the last flush.
  for (size_t i = 0; s.ok() && i < wal_size; ++i) {
    if ((live_wal_files[i]->Type() == kAliveLogFile) &&
        (!flush_memtable ||
         live_wal_files[i]->StartSequence() >= *sequence_number ||
         live_wal_files[i]->LogNumber() >= min_log_num)) {
      if (i + 1 == wal_size) {
        s = copy_file_cb(db_options.wal_dir, live_wal_files[i]->PathName(),
                         live_wal_files[i]->SizeFileBytes(), kLogFile);
        break;
      }
      if (same_fs) {
        // we only care about live log files
        s = link_file_cb(db_options.wal_dir, live_wal_files[i]->PathName(),
                         kLogFile);
        if (s.IsNotSupported()) {
          same_fs = false;
          s = Status::OK();
        }
      }
      if (!same_fs) {
        s = copy_file_cb(db_options.wal_dir, live_wal_files[i]->PathName(), 0,
                         kLogFile);
      }
    }
  }

  return s;
}

// Exports all live SST files of a specified Column Family onto export_dir,
// returning SST files information in metadata.
Status CheckpointImpl::ExportColumnFamily(
    ColumnFamilyHandle* handle, const std::string& export_dir,
    ExportImportFilesMetaData** metadata) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(handle);
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
                            tmp_export_dir + fname, 0, db_options.use_fsync);
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
    std::unique_ptr<Directory> dir_ptr;
    s = db_->GetEnv()->NewDirectory(export_dir, &dir_ptr);
    if (s.ok()) {
      assert(dir_ptr != nullptr);
      s = dir_ptr->Fsync();
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
