//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/trim_history_scheduler.h"

#include <cassert>

#include "db/column_family.h"

namespace ROCKSDB_NAMESPACE {

void TrimHistoryScheduler::ScheduleWork(ColumnFamilyData* cfd) {
  std::lock_guard<std::mutex> lock(checking_mutex_);
  cfd->Ref();
  cfds_.push_back(cfd);
  is_empty_.store(false, std::memory_order_relaxed);
}

ColumnFamilyData* TrimHistoryScheduler::TakeNextColumnFamily() {
  std::lock_guard<std::mutex> lock(checking_mutex_);
  while (true) {
    if (cfds_.empty()) {
      return nullptr;
    }
    ColumnFamilyData* cfd = cfds_.back();
    cfds_.pop_back();
    if (cfds_.empty()) {
      is_empty_.store(true, std::memory_order_relaxed);
    }

    if (!cfd->IsDropped()) {
      // success
      return cfd;
    }
    cfd->UnrefAndTryDelete();
  }
}

bool TrimHistoryScheduler::Empty() {
  bool is_empty = is_empty_.load(std::memory_order_relaxed);
  return is_empty;
}

void TrimHistoryScheduler::Clear() {
  ColumnFamilyData* cfd;
  while ((cfd = TakeNextColumnFamily()) != nullptr) {
    cfd->UnrefAndTryDelete();
  }
  assert(Empty());
}

}  // namespace ROCKSDB_NAMESPACE
