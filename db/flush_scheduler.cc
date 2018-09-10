//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/flush_scheduler.h"

#include <cassert>

#include "db/column_family.h"

namespace rocksdb {

void FlushScheduler::ScheduleFlush(ColumnFamilyData* cfd) {
#ifndef NDEBUG
  std::lock_guard<std::mutex> lock(checking_mutex_);
  assert(checking_set_.count(cfd) == 0);
  checking_set_.insert(cfd);
#endif  // NDEBUG
  cfd->Ref();
// Suppress false positive clang analyzer warnings.
#ifndef __clang_analyzer__
  Node* node = new Node{cfd, head_.load(std::memory_order_relaxed)};
  while (!head_.compare_exchange_strong(
      node->next, node, std::memory_order_relaxed, std::memory_order_relaxed)) {
    // failing CAS updates the first param, so we are already set for
    // retry.  TakeNextColumnFamily won't happen until after another
    // inter-thread synchronization, so we don't even need release
    // semantics for this CAS
  }
#endif  // __clang_analyzer__
}

ColumnFamilyData* FlushScheduler::TakeNextColumnFamily() {
#ifndef NDEBUG
  std::lock_guard<std::mutex> lock(checking_mutex_);
#endif  // NDEBUG
  while (true) {
    if (head_.load(std::memory_order_relaxed) == nullptr) {
      return nullptr;
    }

    // dequeue the head
    Node* node = head_.load(std::memory_order_relaxed);
    head_.store(node->next, std::memory_order_relaxed);
    ColumnFamilyData* cfd = node->column_family;
    delete node;

#ifndef NDEBUG
    auto iter = checking_set_.find(cfd);
    assert(iter != checking_set_.end());
    checking_set_.erase(iter);
#endif  // NDEBUG

    if (!cfd->IsDropped()) {
      // success
      return cfd;
    }

    // no longer relevant, retry
    if (cfd->Unref()) {
      delete cfd;
    }
  }
}

bool FlushScheduler::Empty() {
#ifndef NDEBUG
  std::lock_guard<std::mutex> lock(checking_mutex_);
#endif  // NDEBUG
  auto rv = head_.load(std::memory_order_relaxed) == nullptr;
#ifndef NDEBUG
  assert(rv == checking_set_.empty());
#endif  // NDEBUG
  return rv;
}

void FlushScheduler::Clear() {
  ColumnFamilyData* cfd;
  while ((cfd = TakeNextColumnFamily()) != nullptr) {
    if (cfd->Unref()) {
      delete cfd;
    }
  }
  assert(head_.load(std::memory_order_relaxed) == nullptr);
}

void DefaultFlushManager::OnManualFlush(
    ColumnFamilySet& /* column_family_set */, ColumnFamilyData* cfd,
    std::atomic<bool>& cached_recoverable_state_empty,
    autovector<ColumnFamilyData*>* cfds_picked) {
  if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
      !cached_recoverable_state_empty.load()) {
    cfds_picked->emplace_back(cfd);
  }
}

void DefaultFlushManager::OnSwitchWAL(
    ColumnFamilySet& column_family_set, uint64_t oldest_alive_log,
    autovector<ColumnFamilyData*>* cfds_picked) {
  for (ColumnFamilyData* cfd : column_family_set) {
    if (cfd->IsDropped()) {
      continue;
    }
    if (cfd->OldestLogToKeep() <= oldest_alive_log) {
      cfds_picked->emplace_back(cfd);
    }
  }
}

void DefaultFlushManager::OnHandleWriteBufferFull(
    ColumnFamilySet& column_family_set,
    autovector<ColumnFamilyData*>* cfds_picked) {
  ColumnFamilyData* cfd_picked = nullptr;
  SequenceNumber seqno_for_cf_picked = kMaxSequenceNumber;
  for (ColumnFamilyData* cfd : column_family_set) {
    if (cfd->IsDropped()) {
      continue;
    }
    if (!cfd->mem()->IsEmpty()) {
      // We only consider active mem table, hoping immutable memtable is
      // already in the process of flushing.
      uint64_t seq = cfd->mem()->GetCreationSeq();
      if (cfd_picked == nullptr || seq < seqno_for_cf_picked) {
        cfd_picked = cfd;
        seqno_for_cf_picked = seq;
      }
    }
  }
  if (cfd_picked != nullptr) {
    cfds_picked->emplace_back(cfd_picked);
  }
}

void DefaultFlushManager::OnScheduleFlushes(
    ColumnFamilySet& /* column_family_set */, FlushScheduler& scheduler,
    autovector<ColumnFamilyData*>* cfds_picked) {
  ColumnFamilyData* cfd = nullptr;
  while ((cfd = scheduler.TakeNextColumnFamily()) != nullptr) {
    cfds_picked->emplace_back(cfd);
  }
}

FlushManager* NewDefaultFlushManager() { return new DefaultFlushManager(); }

}  // namespace rocksdb
