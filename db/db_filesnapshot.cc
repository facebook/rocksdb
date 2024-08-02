//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//


#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/types.h"
#include "test_util/sync_point.h"
#include "util/file_checksum_helper.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

Status DBImpl::FlushForGetLiveFiles() {
  return DBImpl::FlushAllColumnFamilies(FlushOptions(),
                                        FlushReason::kGetLiveFiles);
}

Status DBImpl::GetLiveFiles(std::vector<std::string>& ret,
                            uint64_t* manifest_file_size, bool flush_memtable) {
  *manifest_file_size = 0;

  mutex_.Lock();

  if (flush_memtable) {
    Status status = FlushForGetLiveFiles();
    if (!status.ok()) {
      mutex_.Unlock();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log, "Cannot Flush data %s\n",
                      status.ToString().c_str());
      return status;
    }
  }

  // Make a set of all of the live table and blob files
  std::vector<uint64_t> live_table_files;
  std::vector<uint64_t> live_blob_files;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    cfd->current()->AddLiveFiles(&live_table_files, &live_blob_files);
  }

  ret.clear();
  ret.reserve(live_table_files.size() + live_blob_files.size() +
              3);  // for CURRENT + MANIFEST + OPTIONS

  // create names of the live files. The names are not absolute
  // paths, instead they are relative to dbname_.
  for (const auto& table_file_number : live_table_files) {
    ret.emplace_back(MakeTableFileName("", table_file_number));
  }

  for (const auto& blob_file_number : live_blob_files) {
    ret.emplace_back(BlobFileName("", blob_file_number));
  }

  ret.emplace_back(CurrentFileName(""));
  ret.emplace_back(DescriptorFileName("", versions_->manifest_file_number()));
  // The OPTIONS file number is zero in read-write mode when OPTIONS file
  // writing failed and the DB was configured with
  // `fail_if_options_file_error == false`. In read-only mode the OPTIONS file
  // number is zero when no OPTIONS file exist at all. In those cases we do not
  // record any OPTIONS file in the live file list.
  if (versions_->options_file_number() != 0) {
    ret.emplace_back(OptionsFileName("", versions_->options_file_number()));
  }

  // find length of manifest file while holding the mutex lock
  *manifest_file_size = versions_->manifest_file_size();

  mutex_.Unlock();
  return Status::OK();
}

Status DBImpl::GetSortedWalFiles(VectorWalPtr& files) {
  return GetSortedWalFilesImpl(files,
                               /*need_seqnos*/ true);
}

Status DBImpl::GetSortedWalFilesImpl(VectorWalPtr& files, bool need_seqnos) {
  // Record tracked WALs as a (minimum) cross-check for directory scan
  std::vector<uint64_t> required_by_manifest;

  // If caller disabled deletions, this function should return files that are
  // guaranteed not to be deleted until deletions are re-enabled. We need to
  // wait for pending purges to finish since WalManager doesn't know which
  // files are going to be purged. Additional purges won't be scheduled as
  // long as deletions are disabled (so the below loop must terminate).
  // Also note that we disable deletions anyway to avoid the case where a
  // file is deleted in the middle of the scan, causing IO error.
  Status deletions_disabled = DisableFileDeletions();
  {
    InstrumentedMutexLock l(&mutex_);
    while (pending_purge_obsolete_files_ > 0 || bg_purge_scheduled_ > 0) {
      bg_cv_.Wait();
    }

    // Record tracked WALs as a (minimum) cross-check for directory scan
    const auto& manifest_wals = versions_->GetWalSet().GetWals();
    required_by_manifest.reserve(manifest_wals.size());
    for (const auto& wal : manifest_wals) {
      required_by_manifest.push_back(wal.first);
    }
  }

  // NOTE: need to include archived WALs because needed WALs might have been
  // archived since getting required_by_manifest set
  Status s = wal_manager_.GetSortedWalFiles(files, need_seqnos,
                                            /*include_archived*/ true);

  // DisableFileDeletions / EnableFileDeletions not supported in read-only DB
  if (deletions_disabled.ok()) {
    Status s2 = EnableFileDeletions();
    assert(s2.ok());
    s2.PermitUncheckedError();
  } else {
    assert(deletions_disabled.IsNotSupported());
  }

  if (s.ok()) {
    // Verify includes those required by manifest (one sorted list is superset
    // of the other)
    auto required = required_by_manifest.begin();
    auto included = files.begin();

    while (required != required_by_manifest.end()) {
      if (included == files.end() || *required < (*included)->LogNumber()) {
        // FAIL - did not find
        return Status::Corruption(
            "WAL file " + std::to_string(*required) +
            " required by manifest but not in directory list");
      }
      if (*required == (*included)->LogNumber()) {
        ++required;
        ++included;
      } else {
        assert(*required > (*included)->LogNumber());
        ++included;
      }
    }
  }

  if (s.ok()) {
    size_t wal_count = files.size();
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Number of WAL files %" ROCKSDB_PRIszt " (%" ROCKSDB_PRIszt
                   " required by manifest)",
                   wal_count, required_by_manifest.size());
#ifndef NDEBUG
    std::ostringstream wal_names;
    for (const auto& wal : files) {
      wal_names << wal->PathName() << " ";
    }

    std::ostringstream wal_required_by_manifest_names;
    for (const auto& wal : required_by_manifest) {
      wal_required_by_manifest_names << wal << ".log ";
    }

    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Log files : %s .Log files required by manifest: %s.",
                   wal_names.str().c_str(),
                   wal_required_by_manifest_names.str().c_str());
#endif  // NDEBUG
  }
  return s;
}

Status DBImpl::GetCurrentWalFile(std::unique_ptr<WalFile>* current_log_file) {
  uint64_t current_logfile_number;
  {
    InstrumentedMutexLock l(&mutex_);
    current_logfile_number = logfile_number_;
  }

  return wal_manager_.GetLiveWalFile(current_logfile_number, current_log_file);
}

Status DBImpl::GetLiveFilesStorageInfo(
    const LiveFilesStorageInfoOptions& opts,
    std::vector<LiveFileStorageInfo>* files) {
  // To avoid returning partial results, only move results to files on success.
  assert(files);
  files->clear();
  std::vector<LiveFileStorageInfo> results;

  // NOTE: This implementation was largely migrated from Checkpoint.

  Status s;
  VectorWalPtr live_wal_files;
  bool flush_memtable = true;
  if (!immutable_db_options_.allow_2pc) {
    if (opts.wal_size_for_flush == std::numeric_limits<uint64_t>::max()) {
      flush_memtable = false;
    } else if (opts.wal_size_for_flush > 0) {
      // FIXME: avoid querying the filesystem for current WAL state
      // If the outstanding WAL files are small, we skip the flush.
      // Don't take archived log size into account when calculating wal
      // size for flush, and don't need to verify consistency with manifest
      // here & now.
      s = wal_manager_.GetSortedWalFiles(live_wal_files,
                                         /* need_seqnos */ false,
                                         /*include_archived*/ false);

      if (!s.ok()) {
        return s;
      }

      // Don't flush column families if total log size is smaller than
      // log_size_for_flush. We copy the log files instead.
      // We may be able to cover 2PC case too.
      uint64_t total_wal_size = 0;
      for (auto& wal : live_wal_files) {
        assert(wal->Type() == kAliveLogFile);
        total_wal_size += wal->SizeFileBytes();
      }
      if (total_wal_size < opts.wal_size_for_flush) {
        flush_memtable = false;
      }
      live_wal_files.clear();
    }
  }

  // This is a modified version of GetLiveFiles, to get access to more
  // metadata.
  mutex_.Lock();
  if (flush_memtable) {
    bool wal_locked = lock_wal_count_ > 0;
    if (wal_locked) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Can't FlushForGetLiveFiles while WAL is locked");
    } else {
      Status status = FlushForGetLiveFiles();
      if (!status.ok()) {
        mutex_.Unlock();
        ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                        "Cannot Flush data %s\n", status.ToString().c_str());
        return status;
      }
    }
  }

  // Make a set of all of the live table and blob files
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    VersionStorageInfo& vsi = *cfd->current()->storage_info();
    auto& cf_paths = cfd->ioptions()->cf_paths;

    auto GetDir = [&](size_t path_id) {
      // Matching TableFileName() behavior
      if (path_id >= cf_paths.size()) {
        assert(false);
        return cf_paths.back().path;
      } else {
        return cf_paths[path_id].path;
      }
    };

    for (int level = 0; level < vsi.num_levels(); ++level) {
      const auto& level_files = vsi.LevelFiles(level);
      for (const auto& meta : level_files) {
        assert(meta);

        results.emplace_back();
        LiveFileStorageInfo& info = results.back();

        info.relative_filename = MakeTableFileName(meta->fd.GetNumber());
        info.directory = GetDir(meta->fd.GetPathId());
        info.file_number = meta->fd.GetNumber();
        info.file_type = kTableFile;
        info.size = meta->fd.GetFileSize();
        if (opts.include_checksum_info) {
          info.file_checksum_func_name = meta->file_checksum_func_name;
          info.file_checksum = meta->file_checksum;
          if (info.file_checksum_func_name.empty()) {
            info.file_checksum_func_name = kUnknownFileChecksumFuncName;
            info.file_checksum = kUnknownFileChecksum;
          }
        }
        info.temperature = meta->temperature;
      }
    }
    const auto& blob_files = vsi.GetBlobFiles();
    for (const auto& meta : blob_files) {
      assert(meta);

      results.emplace_back();
      LiveFileStorageInfo& info = results.back();

      info.relative_filename = BlobFileName(meta->GetBlobFileNumber());
      info.directory = GetDir(/* path_id */ 0);
      info.file_number = meta->GetBlobFileNumber();
      info.file_type = kBlobFile;
      info.size = meta->GetBlobFileSize();
      if (opts.include_checksum_info) {
        info.file_checksum_func_name = meta->GetChecksumMethod();
        info.file_checksum = meta->GetChecksumValue();
        if (info.file_checksum_func_name.empty()) {
          info.file_checksum_func_name = kUnknownFileChecksumFuncName;
          info.file_checksum = kUnknownFileChecksum;
        }
      }
      // TODO?: info.temperature
    }
  }

  // Capture some final info before releasing mutex
  const uint64_t manifest_number = versions_->manifest_file_number();
  const uint64_t manifest_size = versions_->manifest_file_size();
  const uint64_t options_number = versions_->options_file_number();
  const uint64_t options_size = versions_->options_file_size_;
  const uint64_t min_log_num = MinLogNumberToKeep();
  // Ensure consistency with manifest for track_and_verify_wals_in_manifest
  const uint64_t max_log_num = logfile_number_;

  mutex_.Unlock();

  std::string manifest_fname = DescriptorFileName(manifest_number);
  {  // MANIFEST
    results.emplace_back();
    LiveFileStorageInfo& info = results.back();

    info.relative_filename = manifest_fname;
    info.directory = GetName();
    info.file_number = manifest_number;
    info.file_type = kDescriptorFile;
    info.size = manifest_size;
    info.trim_to_size = true;
    if (opts.include_checksum_info) {
      info.file_checksum_func_name = kUnknownFileChecksumFuncName;
      info.file_checksum = kUnknownFileChecksum;
    }
  }

  {  // CURRENT
    results.emplace_back();
    LiveFileStorageInfo& info = results.back();

    info.relative_filename = kCurrentFileName;
    info.directory = GetName();
    info.file_type = kCurrentFile;
    // CURRENT could be replaced so we have to record the contents as needed.
    info.replacement_contents = manifest_fname + "\n";
    info.size = manifest_fname.size() + 1;
    if (opts.include_checksum_info) {
      info.file_checksum_func_name = kUnknownFileChecksumFuncName;
      info.file_checksum = kUnknownFileChecksum;
    }
  }

  // The OPTIONS file number is zero in read-write mode when OPTIONS file
  // writing failed and the DB was configured with
  // `fail_if_options_file_error == false`. In read-only mode the OPTIONS file
  // number is zero when no OPTIONS file exist at all. In those cases we do not
  // record any OPTIONS file in the live file list.
  if (options_number != 0) {
    results.emplace_back();
    LiveFileStorageInfo& info = results.back();

    info.relative_filename = OptionsFileName(options_number);
    info.directory = GetName();
    info.file_number = options_number;
    info.file_type = kOptionsFile;
    info.size = options_size;
    if (opts.include_checksum_info) {
      info.file_checksum_func_name = kUnknownFileChecksumFuncName;
      info.file_checksum = kUnknownFileChecksum;
    }
  }

  // Some legacy testing stuff  TODO: carefully clean up obsolete parts
  TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:FlushDone");

  TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles1");
  TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles2");

  if (s.ok()) {
    // FlushWAL is required to ensure we can physically copy everything
    // logically written to the WAL. (Sync not strictly required for
    // active WAL to be copied rather than hard linked, even when
    // Checkpoint guarantees that the copied-to file is sync-ed. Plus we can't
    // help track_and_verify_wals_in_manifest after manifest_size is
    // already determined.)
    s = FlushWAL(/*sync=*/false);
    if (s.IsNotSupported()) {  // read-only DB or similar
      s = Status::OK();
    }
  }

  TEST_SYNC_POINT("CheckpointImpl::CreateCustomCheckpoint:AfterGetLive1");
  TEST_SYNC_POINT("CheckpointImpl::CreateCustomCheckpoint:AfterGetLive2");

  // Even after WAL flush, there could be multiple WALs that are not
  // fully synced. Although the output DB of a Checkpoint or Backup needs
  // to be fully synced on return, we don't strictly need to sync this
  // DB (the input DB). If we allow Checkpoint to hard link an inactive
  // WAL that isn't fully synced, that could result in an insufficiently
  // sync-ed Checkpoint. Here we get the set of WALs that are potentially
  // unsynced or still being written to, to prevent them from being hard
  // linked. Enforcing max_log_num from above ensures any new WALs after
  // GetOpenWalSizes() and before GetSortedWalFiles() are not included in
  // the results.
  // NOTE: we might still hard link a file that is open for writing, even
  // if we don't do any more writes to it.
  //
  // In a step toward reducing unnecessary file metadata queries, we also
  // get and use our known flushed sizes for those WALs.
  // FIXME: eventually we should not be using filesystem queries at all for
  // the required set of WAL files.
  //
  // However for recycled log files, we just copy the whole file,
  // for better or worse.
  //
  std::map<uint64_t, uint64_t> open_wal_number_to_size;
  bool recycling_log_files = immutable_db_options_.recycle_log_file_num > 0;
  if (s.ok() && !recycling_log_files) {
    s = GetOpenWalSizes(open_wal_number_to_size);
  }

  // [old comment] If we have more than one column family, we also need to get
  // WAL files.
  if (s.ok()) {
    // FIXME: avoid querying the filesystem for current WAL state
    s = GetSortedWalFilesImpl(live_wal_files,
                              /* need_seqnos */ false);
  }
  if (!s.ok()) {
    return s;
  }

  size_t wal_count = live_wal_files.size();
  // Link WAL files. Copy exact size of last one because it is the only one
  // that has changes after the last flush.
  auto wal_dir = immutable_db_options_.GetWalDir();
  for (size_t i = 0; s.ok() && i < wal_count; ++i) {
    if ((live_wal_files[i]->Type() == kAliveLogFile) &&
        (!flush_memtable || live_wal_files[i]->LogNumber() >= min_log_num) &&
        live_wal_files[i]->LogNumber() <= max_log_num) {
      results.emplace_back();
      LiveFileStorageInfo& info = results.back();
      auto f = live_wal_files[i]->PathName();
      assert(!f.empty() && f[0] == '/');
      info.relative_filename = f.substr(1);
      info.directory = wal_dir;
      info.file_number = live_wal_files[i]->LogNumber();
      info.file_type = kWalFile;
      if (recycling_log_files) {
        info.size = live_wal_files[i]->SizeFileBytes();
        // Recyclable WAL files must be copied instead of hard linked
        info.trim_to_size = true;
      } else {
        auto it = open_wal_number_to_size.find(info.file_number);
        if (it == open_wal_number_to_size.end()) {
          // Known fully synced and no future writes (in part from
          // max_log_num check). Ok to hard link
          info.size = live_wal_files[i]->SizeFileBytes();
          assert(!info.trim_to_size);
        } else {
          // Marked as (possibly) still open -> use our known flushed size
          // and force file copy instead of hard link
          info.size = it->second;
          info.trim_to_size = true;
          // FIXME: this is needed as long as db_stress uses
          // SetReadUnsyncedData(false), because it will only be able to
          // copy the synced portion of the WAL, which under
          // SetReadUnsyncedData(false) is given by the reported file size.
          info.size = std::min(info.size, live_wal_files[i]->SizeFileBytes());
        }
      }
      if (opts.include_checksum_info) {
        info.file_checksum_func_name = kUnknownFileChecksumFuncName;
        info.file_checksum = kUnknownFileChecksum;
      }
    }
  }

  if (s.ok()) {
    // Only move results to output on success.
    *files = std::move(results);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
