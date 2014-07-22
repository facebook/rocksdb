//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include "db/db_impl.h"

namespace rocksdb {

void DBImpl::TEST_PurgeObsoleteteWAL() { PurgeObsoleteWALFiles(); }

uint64_t DBImpl::TEST_GetLevel0TotalSize() {
  MutexLock l(&mutex_);
  return default_cf_handle_->cfd()->current()->NumLevelBytes(0);
}

Iterator* DBImpl::TEST_NewInternalIterator(ColumnFamilyHandle* column_family) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    cfd = cfh->cfd();
  }

  mutex_.Lock();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  mutex_.Unlock();
  ReadOptions roptions;
  return NewInternalIterator(roptions, cfd, super_version);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes(
    ColumnFamilyHandle* column_family) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    cfd = cfh->cfd();
  }
  MutexLock l(&mutex_);
  return cfd->current()->MaxNextLevelOverlappingBytes();
}

void DBImpl::TEST_GetFilesMetaData(
    ColumnFamilyHandle* column_family,
    std::vector<std::vector<FileMetaData>>* metadata) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  MutexLock l(&mutex_);
  metadata->resize(NumberLevels());
  for (int level = 0; level < NumberLevels(); level++) {
    const std::vector<FileMetaData*>& files = cfd->current()->files_[level];

    (*metadata)[level].clear();
    for (const auto& f : files) {
      (*metadata)[level].push_back(*f);
    }
  }
}

uint64_t DBImpl::TEST_Current_Manifest_FileNo() {
  return versions_->ManifestFileNumber();
}

Status DBImpl::TEST_CompactRange(int level, const Slice* begin,
                                 const Slice* end,
                                 ColumnFamilyHandle* column_family) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    cfd = cfh->cfd();
  }
  int output_level =
      (cfd->options()->compaction_style == kCompactionStyleUniversal ||
       cfd->options()->compaction_style == kCompactionStyleFIFO)
          ? level
          : level + 1;
  return RunManualCompaction(cfd, level, output_level, 0, begin, end);
}

Status DBImpl::TEST_FlushMemTable(bool wait) {
  FlushOptions fo;
  fo.wait = wait;
  return FlushMemTable(default_cf_handle_->cfd(), fo);
}

Status DBImpl::TEST_WaitForFlushMemTable(ColumnFamilyHandle* column_family) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    cfd = cfh->cfd();
  }
  return WaitForFlushMemTable(cfd);
}

Status DBImpl::TEST_WaitForCompact() {
  // Wait until the compaction completes

  // TODO: a bug here. This function actually does not necessarily
  // wait for compact. It actually waits for scheduled compaction
  // OR flush to finish.

  MutexLock l(&mutex_);
  while ((bg_compaction_scheduled_ || bg_flush_scheduled_) && bg_error_.ok()) {
    bg_cv_.Wait();
  }
  return bg_error_;
}

Status DBImpl::TEST_ReadFirstRecord(const WalFileType type,
                                    const uint64_t number,
                                    SequenceNumber* sequence) {
  return ReadFirstRecord(type, number, sequence);
}

Status DBImpl::TEST_ReadFirstLine(const std::string& fname,
                                  SequenceNumber* sequence) {
  return ReadFirstLine(fname, sequence);
}
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
