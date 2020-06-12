//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#ifndef ROCKSDB_LITE

#include <stdint.h>
#include <algorithm>
#include <cinttypes>
#include <string>
#include "db/db_impl/db_impl.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

Status DBImpl::GetLiveFiles(std::vector<std::string>& ret,
                            uint64_t* manifest_file_size,
                            bool flush_memtable) {
  *manifest_file_size = 0;

  mutex_.Lock();

  if (flush_memtable) {
    // flush all dirty data to disk.
    Status status;
    if (immutable_db_options_.atomic_flush) {
      autovector<ColumnFamilyData*> cfds;
      SelectColumnFamiliesForAtomicFlush(&cfds);
      mutex_.Unlock();
      status = AtomicFlushMemTables(cfds, FlushOptions(),
                                    FlushReason::kGetLiveFiles);
      if (status.IsColumnFamilyDropped()) {
        status = Status::OK();
      }
      mutex_.Lock();
    } else {
      for (auto cfd : *versions_->GetColumnFamilySet()) {
        if (cfd->IsDropped()) {
          continue;
        }
        cfd->Ref();
        mutex_.Unlock();
        status = FlushMemTable(cfd, FlushOptions(), FlushReason::kGetLiveFiles);
        TEST_SYNC_POINT("DBImpl::GetLiveFiles:1");
        TEST_SYNC_POINT("DBImpl::GetLiveFiles:2");
        mutex_.Lock();
        cfd->UnrefAndTryDelete();
        if (!status.ok() && !status.IsColumnFamilyDropped()) {
          break;
        } else if (status.IsColumnFamilyDropped()) {
          status = Status::OK();
        }
      }
    }
    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();

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
  // paths, instead they are relative to dbname_;
  for (const auto& table_file_number : live_table_files) {
    ret.emplace_back(MakeTableFileName("", table_file_number));
  }

  for (const auto& blob_file_number : live_blob_files) {
    ret.emplace_back(BlobFileName("", blob_file_number));
  }

  ret.emplace_back(CurrentFileName(""));
  ret.emplace_back(DescriptorFileName("", versions_->manifest_file_number()));
  ret.emplace_back(OptionsFileName("", versions_->options_file_number()));

  // find length of manifest file while holding the mutex lock
  *manifest_file_size = versions_->manifest_file_size();

  mutex_.Unlock();
  return Status::OK();
}

Status DBImpl::GetSortedWalFiles(VectorLogPtr& files) {
  {
    // If caller disabled deletions, this function should return files that are
    // guaranteed not to be deleted until deletions are re-enabled. We need to
    // wait for pending purges to finish since WalManager doesn't know which
    // files are going to be purged. Additional purges won't be scheduled as
    // long as deletions are disabled (so the below loop must terminate).
    InstrumentedMutexLock l(&mutex_);
    while (disable_delete_obsolete_files_ > 0 &&
           pending_purge_obsolete_files_ > 0) {
      bg_cv_.Wait();
    }
  }
  return wal_manager_.GetSortedWalFiles(files);
}

Status DBImpl::GetCurrentWalFile(std::unique_ptr<LogFile>* current_log_file) {
  uint64_t current_logfile_number;
  {
    InstrumentedMutexLock l(&mutex_);
    current_logfile_number = logfile_number_;
  }

  return wal_manager_.GetLiveWalFile(current_logfile_number, current_log_file);
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
