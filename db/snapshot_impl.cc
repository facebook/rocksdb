//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/snapshot_impl.h"

#include <algorithm>
#include <queue>

#include "rocksdb/db.h"
#include "rocksdb/snapshot.h"

namespace ROCKSDB_NAMESPACE {

ManagedSnapshot::ManagedSnapshot(DB* db)
    : db_(db), snapshot_(db->GetSnapshot()) {}

ManagedSnapshot::ManagedSnapshot(DB* db, const Snapshot* _snapshot)
    : db_(db), snapshot_(_snapshot) {}

ManagedSnapshot::~ManagedSnapshot() {
  if (snapshot_) {
    db_->ReleaseSnapshot(snapshot_);
  }
}

const Snapshot* ManagedSnapshot::snapshot() { return snapshot_; }

void ShardedSnapshotList::GetAll(std::vector<SequenceNumber>* snap_vector,
                                 SequenceNumber* oldest_write_conflict_snapshot,
                                 SequenceNumber max_seq,
                                 SequenceNumber snapshot_creation_lower_bound,
                                 OldestSnapshotInfo* oldest_snapshot) const {
  assert(oldest_snapshot != nullptr);
  std::vector<SequenceNumber>& ret = *snap_vector;
  // So far we have no use case that would pass a non-empty vector
  assert(ret.empty());
  SequenceNumber oldest_ww = kMaxSequenceNumber;
  SequenceNumber oldest = snapshot_creation_lower_bound;
  uint64_t release_count = 0;
  std::array<std::vector<SequenceNumber>, kNumShards> shard_seqs;
  size_t total = 0;
  for (uint32_t i = 0; i < kNumShards; ++i) {
    const Shard& shard = shards_[i];
    SequenceNumber shard_ww = kMaxSequenceNumber;
    {
      MutexLock l(&shard.mu);
      if (!shard.list.empty()) {
        oldest = std::min(oldest, shard.list.oldest()->number_);
      }
      release_count += shard.release_count;
      shard.list.GetAll(&shard_seqs[i], &shard_ww, max_seq);
    }
    oldest_ww = std::min(oldest_ww, shard_ww);
    total += shard_seqs[i].size();
    TEST_SYNC_POINT_CALLBACK("ShardedSnapshotList::GetAll:AfterShard", &i);
  }

  struct Cursor {
    SequenceNumber seq;
    uint32_t shard;
    size_t pos;
  };
  struct Greater {
    bool operator()(const Cursor& lhs, const Cursor& rhs) const {
      return lhs.seq > rhs.seq;
    }
  };
  std::vector<Cursor> heap_storage;
  heap_storage.reserve(kNumShards);
  std::priority_queue<Cursor, std::vector<Cursor>, Greater> heap(
      Greater(), std::move(heap_storage));
  for (uint32_t i = 0; i < kNumShards; ++i) {
    if (!shard_seqs[i].empty()) {
      heap.push({shard_seqs[i][0], i, 0});
    }
  }
  ret.reserve(total);
  while (!heap.empty()) {
    Cursor cursor = heap.top();
    heap.pop();
    if (ret.empty() || ret.back() != cursor.seq) {
      ret.push_back(cursor.seq);
    }
    ++cursor.pos;
    if (cursor.pos < shard_seqs[cursor.shard].size()) {
      cursor.seq = shard_seqs[cursor.shard][cursor.pos];
      heap.push(cursor);
    }
  }
  if (oldest_write_conflict_snapshot != nullptr) {
    *oldest_write_conflict_snapshot = oldest_ww;
  }
  *oldest_snapshot = {oldest, release_count, snapshot_creation_lower_bound};
}

bool ShardedSnapshotList::empty() const { return count() == 0; }

uint64_t ShardedSnapshotList::count() const {
  LockAll();
  uint64_t total = 0;
  for (const Shard& shard : shards_) {
    total += shard.list.count();
  }
  UnlockAll();
  return total;
}

ShardedSnapshotList::OldestSnapshotInfo
ShardedSnapshotList::GetOldestSnapshotSequenceLowerBound(
    SequenceNumber snapshot_creation_lower_bound) const {
  SequenceNumber oldest = snapshot_creation_lower_bound;
  uint64_t release_count = 0;
  for (const Shard& shard : shards_) {
    MutexLock l(&shard.mu);
    if (!shard.list.empty()) {
      oldest = std::min(oldest, shard.list.oldest()->number_);
    }
    release_count += shard.release_count;
  }
  return {oldest, release_count, snapshot_creation_lower_bound};
}

int64_t ShardedSnapshotList::GetOldestSnapshotTime() const {
  LockAll();
  const SnapshotImpl* oldest = OldestLocked();
  const int64_t result = oldest != nullptr ? oldest->GetUnixTime() : 0;
  UnlockAll();
  return result;
}

int64_t ShardedSnapshotList::GetOldestSnapshotSequence() const {
  return static_cast<int64_t>(OldestOr([]() { return SequenceNumber{0}; }));
}

void ShardedSnapshotList::BlockNew() {
  for (Shard& shard : shards_) {
    MutexLock l(&shard.mu);
    ++shard.new_blocked;
  }
}

void ShardedSnapshotList::UnblockNew() {
  for (Shard& shard : shards_) {
    MutexLock l(&shard.mu);
    assert(shard.new_blocked > 0);
    --shard.new_blocked;
  }
}

void ShardedSnapshotList::LockAll() const {
  for (const Shard& shard : shards_) {
    shard.mu.Lock();
  }
}

void ShardedSnapshotList::UnlockAll() const {
  for (const Shard& shard : shards_) {
    shard.mu.Unlock();
  }
}

const SnapshotImpl* ShardedSnapshotList::OldestLocked() const {
  const SnapshotImpl* oldest = nullptr;
  for (const Shard& shard : shards_) {
    if (shard.list.empty()) {
      continue;
    }
    const SnapshotImpl* head = shard.list.oldest();
    // On equal sequence numbers, prefer the earlier created snapshot, as the
    // single unsharded list did.
    if (oldest == nullptr || head->number_ < oldest->number_ ||
        (head->number_ == oldest->number_ &&
         head->creation_epoch_ < oldest->creation_epoch_)) {
      oldest = head;
    }
  }
  return oldest;
}

uint64_t ShardedSnapshotList::GetCreationEpoch(int64_t unix_time) {
  CreationEpochState& state = creation_epoch_state_;
  const uint64_t version = state.version.load(std::memory_order_acquire);
  if ((version & 1) == 0 &&
      state.unix_time.load(std::memory_order_acquire) == unix_time) {
    const uint64_t epoch = state.epoch.load(std::memory_order_acquire);
    if (state.version.load(std::memory_order_acquire) == version) {
      return epoch;
    }
  }

  MutexLock l(&state.mu);
  if (state.unix_time.load(std::memory_order_relaxed) != unix_time) {
    const uint64_t old_version =
        state.version.fetch_add(1, std::memory_order_acq_rel);
    assert((old_version & 1) == 0);
    (void)old_version;
    state.epoch.fetch_add(1, std::memory_order_release);
    state.unix_time.store(unix_time, std::memory_order_release);
    state.version.fetch_add(1, std::memory_order_release);
  }
  return state.epoch.load(std::memory_order_relaxed);
}

uint32_t ShardedSnapshotList::FallbackShard() {
  static std::atomic<uint32_t> next_shard{0};
  static thread_local const uint32_t shard =
      next_shard.fetch_add(1, std::memory_order_relaxed) & (kNumShards - 1);
  return shard;
}

}  // namespace ROCKSDB_NAMESPACE
