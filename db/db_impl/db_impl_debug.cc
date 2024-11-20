//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef NDEBUG
#include <iostream>

#include "db/blob/blob_file_cache.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/periodic_task_scheduler.h"
#include "monitoring/thread_status_updater.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {
uint64_t DBImpl::TEST_GetLevel0TotalSize() {
  InstrumentedMutexLock l(&mutex_);
  return default_cf_handle_->cfd()->current()->storage_info()->NumLevelBytes(0);
}

Status DBImpl::TEST_SwitchWAL() {
  WriteContext write_context;
  InstrumentedMutexLock l(&mutex_);
  void* writer = TEST_BeginWrite();
  auto s = SwitchWAL(&write_context);
  TEST_EndWrite(writer);
  return s;
}

uint64_t DBImpl::TEST_MaxNextLevelOverlappingBytes(
    ColumnFamilyHandle* column_family) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    cfd = cfh->cfd();
  }
  InstrumentedMutexLock l(&mutex_);
  return cfd->current()->storage_info()->MaxNextLevelOverlappingBytes();
}

void DBImpl::TEST_GetFilesMetaData(
    ColumnFamilyHandle* column_family,
    std::vector<std::vector<FileMetaData>>* metadata,
    std::vector<std::shared_ptr<BlobFileMetaData>>* blob_metadata) {
  assert(metadata);

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(cfh);

  auto cfd = cfh->cfd();
  assert(cfd);

  InstrumentedMutexLock l(&mutex_);

  const auto* current = cfd->current();
  assert(current);

  const auto* vstorage = current->storage_info();
  assert(vstorage);

  metadata->resize(NumberLevels());

  for (int level = 0; level < NumberLevels(); ++level) {
    const std::vector<FileMetaData*>& files = vstorage->LevelFiles(level);

    (*metadata)[level].clear();
    (*metadata)[level].reserve(files.size());

    for (const auto& f : files) {
      (*metadata)[level].push_back(*f);
    }
  }

  if (blob_metadata) {
    *blob_metadata = vstorage->GetBlobFiles();
  }
}

uint64_t DBImpl::TEST_Current_Manifest_FileNo() {
  return versions_->manifest_file_number();
}

uint64_t DBImpl::TEST_Current_Next_FileNo() {
  return versions_->current_next_file_number();
}

Status DBImpl::TEST_CompactRange(int level, const Slice* begin,
                                 const Slice* end,
                                 ColumnFamilyHandle* column_family,
                                 bool disallow_trivial_move) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    cfd = cfh->cfd();
  }
  int output_level =
      (cfd->ioptions()->compaction_style == kCompactionStyleUniversal ||
       cfd->ioptions()->compaction_style == kCompactionStyleFIFO)
          ? level
          : level + 1;
  return RunManualCompaction(
      cfd, level, output_level, CompactRangeOptions(), begin, end, true,
      disallow_trivial_move,
      std::numeric_limits<uint64_t>::max() /*max_file_num_to_ignore*/,
      "" /*trim_ts*/);
}

Status DBImpl::TEST_SwitchMemtable(ColumnFamilyData* cfd) {
  WriteContext write_context;
  InstrumentedMutexLock l(&mutex_);
  if (cfd == nullptr) {
    cfd = default_cf_handle_->cfd();
  }

  Status s;
  void* writer = TEST_BeginWrite();
  if (two_write_queues_) {
    WriteThread::Writer nonmem_w;
    nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
    s = SwitchMemtable(cfd, &write_context);
    nonmem_write_thread_.ExitUnbatched(&nonmem_w);
  } else {
    s = SwitchMemtable(cfd, &write_context);
  }
  TEST_EndWrite(writer);
  return s;
}

Status DBImpl::TEST_FlushMemTable(bool wait, bool allow_write_stall,
                                  ColumnFamilyHandle* cfh) {
  FlushOptions fo;
  fo.wait = wait;
  fo.allow_write_stall = allow_write_stall;
  ColumnFamilyData* cfd;
  if (cfh == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfhi = static_cast_with_check<ColumnFamilyHandleImpl>(cfh);
    cfd = cfhi->cfd();
  }
  return FlushMemTable(cfd, fo, FlushReason::kTest);
}

Status DBImpl::TEST_FlushMemTable(ColumnFamilyData* cfd,
                                  const FlushOptions& flush_opts) {
  return FlushMemTable(cfd, flush_opts, FlushReason::kTest);
}

Status DBImpl::TEST_AtomicFlushMemTables(
    const autovector<ColumnFamilyData*>& provided_candidate_cfds,
    const FlushOptions& flush_opts) {
  return AtomicFlushMemTables(flush_opts, FlushReason::kTest,
                              provided_candidate_cfds);
}

Status DBImpl::TEST_WaitForBackgroundWork() {
  InstrumentedMutexLock l(&mutex_);
  WaitForBackgroundWork();
  return error_handler_.GetBGError();
}

Status DBImpl::TEST_WaitForFlushMemTable(ColumnFamilyHandle* column_family) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    cfd = cfh->cfd();
  }
  return WaitForFlushMemTable(cfd, nullptr, false);
}

Status DBImpl::TEST_WaitForCompact() {
  return WaitForCompact(WaitForCompactOptions());
}
Status DBImpl::TEST_WaitForCompact(
    const WaitForCompactOptions& wait_for_compact_options) {
  return WaitForCompact(wait_for_compact_options);
}

Status DBImpl::TEST_WaitForPurge() {
  InstrumentedMutexLock l(&mutex_);
  while (bg_purge_scheduled_ && error_handler_.GetBGError().ok()) {
    bg_cv_.Wait();
  }
  return error_handler_.GetBGError();
}

Status DBImpl::TEST_GetBGError() {
  InstrumentedMutexLock l(&mutex_);
  return error_handler_.GetBGError();
}

bool DBImpl::TEST_IsRecoveryInProgress() {
  InstrumentedMutexLock l(&mutex_);
  return error_handler_.IsRecoveryInProgress();
}

void DBImpl::TEST_LockMutex() { mutex_.Lock(); }

void DBImpl::TEST_UnlockMutex() { mutex_.Unlock(); }

void DBImpl::TEST_SignalAllBgCv() { bg_cv_.SignalAll(); }

void* DBImpl::TEST_BeginWrite() {
  auto w = new WriteThread::Writer();
  write_thread_.EnterUnbatched(w, &mutex_);
  return static_cast<void*>(w);
}

void DBImpl::TEST_EndWrite(void* w) {
  auto writer = static_cast<WriteThread::Writer*>(w);
  write_thread_.ExitUnbatched(writer);
  delete writer;
}

size_t DBImpl::TEST_LogsToFreeSize() {
  InstrumentedMutexLock l(&log_write_mutex_);
  return logs_to_free_.size();
}

uint64_t DBImpl::TEST_LogfileNumber() {
  InstrumentedMutexLock l(&mutex_);
  return logfile_number_;
}

void DBImpl::TEST_GetAllBlockCaches(
    std::unordered_set<const Cache*>* cache_set) {
  InstrumentedMutexLock l(&mutex_);
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (const auto bbto =
            cfd->GetCurrentMutableCFOptions()
                ->table_factory->GetOptions<BlockBasedTableOptions>()) {
      cache_set->insert(bbto->block_cache.get());
    }
  }
}

uint64_t DBImpl::TEST_FindMinLogContainingOutstandingPrep() {
  return logs_with_prep_tracker_.FindMinLogContainingOutstandingPrep();
}

size_t DBImpl::TEST_PreparedSectionCompletedSize() {
  return logs_with_prep_tracker_.TEST_PreparedSectionCompletedSize();
}

size_t DBImpl::TEST_LogsWithPrepSize() {
  return logs_with_prep_tracker_.TEST_LogsWithPrepSize();
}

uint64_t DBImpl::TEST_FindMinPrepLogReferencedByMemTable() {
  autovector<ReadOnlyMemTable*> empty_list;
  return FindMinPrepLogReferencedByMemTable(versions_.get(), empty_list);
}

Status DBImpl::TEST_GetLatestMutableCFOptions(
    ColumnFamilyHandle* column_family, MutableCFOptions* mutable_cf_options) {
  InstrumentedMutexLock l(&mutex_);

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  *mutable_cf_options = *cfh->cfd()->GetLatestMutableCFOptions();
  return Status::OK();
}

int DBImpl::TEST_BGCompactionsAllowed() const {
  InstrumentedMutexLock l(&mutex_);
  return GetBGJobLimits().max_compactions;
}

int DBImpl::TEST_BGFlushesAllowed() const {
  InstrumentedMutexLock l(&mutex_);
  return GetBGJobLimits().max_flushes;
}

SequenceNumber DBImpl::TEST_GetLastVisibleSequence() const {
  if (last_seq_same_as_publish_seq_) {
    return versions_->LastSequence();
  } else {
    return versions_->LastAllocatedSequence();
  }
}

size_t DBImpl::TEST_GetWalPreallocateBlockSize(
    uint64_t write_buffer_size) const {
  InstrumentedMutexLock l(&mutex_);
  return GetWalPreallocateBlockSize(write_buffer_size);
}

void DBImpl::TEST_WaitForPeriodicTaskRun(std::function<void()> callback) const {
  periodic_task_scheduler_.TEST_WaitForRun(callback);
}

const PeriodicTaskScheduler& DBImpl::TEST_GetPeriodicTaskScheduler() const {
  return periodic_task_scheduler_;
}

SeqnoToTimeMapping DBImpl::TEST_GetSeqnoToTimeMapping() const {
  InstrumentedMutexLock l(&mutex_);
  return seqno_to_time_mapping_;
}

const autovector<uint64_t>& DBImpl::TEST_GetFilesToQuarantine() const {
  InstrumentedMutexLock l(&mutex_);
  return error_handler_.GetFilesToQuarantine();
}

void DBImpl::TEST_DeleteObsoleteFiles() {
  InstrumentedMutexLock l(&mutex_);
  DeleteObsoleteFiles();
}

size_t DBImpl::TEST_EstimateInMemoryStatsHistorySize() const {
  InstrumentedMutexLock l(&const_cast<DBImpl*>(this)->stats_history_mutex_);
  return EstimateInMemoryStatsHistorySize();
}

void DBImpl::TEST_VerifyNoObsoleteFilesCached(
    bool db_mutex_already_held) const {
  // This check is somewhat expensive and obscure to make a part of every
  // unit test in every build variety. Thus, we only enable it for ASAN builds.
  if (!kMustFreeHeapAllocations) {
    return;
  }

  std::optional<InstrumentedMutexLock> l;
  if (db_mutex_already_held) {
    mutex_.AssertHeld();
  } else {
    l.emplace(&mutex_);
  }

  if (!opened_successfully_) {
    // We don't need to pro-actively clean up open files during DB::Open()
    // if we know we are about to fail and clean up in Close().
    return;
  }
  if (disable_delete_obsolete_files_ > 0) {
    // For better or worse, DB::Close() is allowed with deletions disabled.
    // Since we generally associate clean-up of open files with deleting them,
    // we allow "obsolete" open files when deletions are disabled.
    return;
  }

  // Live and "quarantined" files are allowed to be open in table cache
  std::set<uint64_t> live_and_quar_files;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    // Iterate over live versions
    Version* current = cfd->current();
    Version* ver = current;
    do {
      // Sneakily add both SST and blob files to the same list
      std::vector<uint64_t> live_files_vec;
      ver->AddLiveFiles(&live_files_vec, &live_files_vec);
      live_and_quar_files.insert(live_files_vec.begin(), live_files_vec.end());

      ver = ver->Next();
    } while (ver != current);
  }
  {
    const auto& quar_files = error_handler_.GetFilesToQuarantine();
    live_and_quar_files.insert(quar_files.begin(), quar_files.end());
  }
  auto fn = [&live_and_quar_files](const Slice& key, Cache::ObjectPtr, size_t,
                                   const Cache::CacheItemHelper*) {
    // See TableCache and BlobFileCache
    assert(key.size() == sizeof(uint64_t));
    uint64_t file_number;
    GetUnaligned(reinterpret_cast<const uint64_t*>(key.data()), &file_number);
    // Assert file is in live/quarantined set
    if (live_and_quar_files.find(file_number) == live_and_quar_files.end()) {
      std::cerr << "File " << file_number << " is not live nor quarantined"
                << std::endl;
      assert(false);
    }
  };
  table_cache_->ApplyToAllEntries(fn, {});
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // NDEBUG
