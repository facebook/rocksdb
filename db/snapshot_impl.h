//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/db.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class SnapshotList;

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
class SnapshotImpl : public Snapshot {
 public:
  SequenceNumber number_;  // const after creation
  // It indicates the smallest uncommitted data at the time the snapshot was
  // taken. This is currently used by WritePrepared transactions to limit the
  // scope of queries to IsInSnapshot.
  SequenceNumber min_uncommitted_ = kMinUnCommittedSeq;

  SequenceNumber GetSequenceNumber() const override { return number_; }

  int64_t GetUnixTime() const override { return unix_time_; }

  uint64_t GetTimestamp() const override { return timestamp_; }

 private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked circular list
  SnapshotImpl* prev_;
  SnapshotImpl* next_;

  SnapshotList* list_;  // just for sanity checks

  int64_t unix_time_;

  uint64_t timestamp_;

  // Will this snapshot be used by a Transaction to do write-conflict checking?
  bool is_write_conflict_boundary_;
};

class SnapshotList {
 public:
  SnapshotList() {
    list_.prev_ = &list_;
    list_.next_ = &list_;
    list_.number_ = 0xFFFFFFFFL;  // placeholder marker, for debugging
    // Set all the variables to make UBSAN happy.
    list_.list_ = nullptr;
    list_.unix_time_ = 0;
    list_.timestamp_ = 0;
    list_.is_write_conflict_boundary_ = false;
    count_ = 0;
  }

  // No copy-construct.
  SnapshotList(const SnapshotList&) = delete;

  bool empty() const {
    assert(list_.next_ != &list_ || 0 == count_);
    return list_.next_ == &list_;
  }
  SnapshotImpl* oldest() const {
    assert(!empty());
    return list_.next_;
  }
  SnapshotImpl* newest() const {
    assert(!empty());
    return list_.prev_;
  }

  SnapshotImpl* New(SnapshotImpl* s, SequenceNumber seq, uint64_t unix_time,
                    bool is_write_conflict_boundary,
                    uint64_t ts = std::numeric_limits<uint64_t>::max()) {
    s->number_ = seq;
    s->unix_time_ = unix_time;
    s->timestamp_ = ts;
    s->is_write_conflict_boundary_ = is_write_conflict_boundary;
    s->list_ = this;
    s->next_ = &list_;
    s->prev_ = list_.prev_;
    s->prev_->next_ = s;
    s->next_->prev_ = s;
    count_++;
    return s;
  }

  // Do not responsible to free the object.
  void Delete(const SnapshotImpl* s) {
    assert(s->list_ == this);
    s->prev_->next_ = s->next_;
    s->next_->prev_ = s->prev_;
    count_--;
  }

  // retrieve all snapshot numbers up until max_seq. They are sorted in
  // ascending order (with no duplicates).
  std::vector<SequenceNumber> GetAll(
      SequenceNumber* oldest_write_conflict_snapshot = nullptr,
      const SequenceNumber& max_seq = kMaxSequenceNumber) const {
    std::vector<SequenceNumber> ret;
    GetAll(&ret, oldest_write_conflict_snapshot, max_seq);
    return ret;
  }

  void GetAll(std::vector<SequenceNumber>* snap_vector,
              SequenceNumber* oldest_write_conflict_snapshot = nullptr,
              const SequenceNumber& max_seq = kMaxSequenceNumber) const {
    std::vector<SequenceNumber>& ret = *snap_vector;
    // So far we have no use case that would pass a non-empty vector
    assert(ret.size() == 0);

    if (oldest_write_conflict_snapshot != nullptr) {
      *oldest_write_conflict_snapshot = kMaxSequenceNumber;
    }

    if (empty()) {
      return;
    }
    const SnapshotImpl* s = &list_;
    while (s->next_ != &list_) {
      if (s->next_->number_ > max_seq) {
        break;
      }
      // Avoid duplicates
      if (ret.empty() || ret.back() != s->next_->number_) {
        ret.push_back(s->next_->number_);
      }

      if (oldest_write_conflict_snapshot != nullptr &&
          *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
          s->next_->is_write_conflict_boundary_) {
        // If this is the first write-conflict boundary snapshot in the list,
        // it is the oldest
        *oldest_write_conflict_snapshot = s->next_->number_;
      }

      s = s->next_;
    }
    return;
  }

  // get the sequence number of the most recent snapshot
  SequenceNumber GetNewest() {
    if (empty()) {
      return 0;
    }
    return newest()->number_;
  }

  int64_t GetOldestSnapshotTime() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->unix_time_;
    }
  }

  int64_t GetOldestSnapshotSequence() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->GetSequenceNumber();
    }
  }

  uint64_t count() const { return count_; }

 private:
  // Dummy head of doubly-linked list of snapshots
  SnapshotImpl list_;
  uint64_t count_;
};

// All operations on TimestampedSnapshotList must be protected by db mutex.
class TimestampedSnapshotList {
 public:
  explicit TimestampedSnapshotList() = default;

  std::shared_ptr<const SnapshotImpl> GetSnapshot(uint64_t ts) const {
    if (ts == std::numeric_limits<uint64_t>::max() && !snapshots_.empty()) {
      auto it = snapshots_.rbegin();
      assert(it != snapshots_.rend());
      return it->second;
    }
    auto it = snapshots_.find(ts);
    if (it == snapshots_.end()) {
      return std::shared_ptr<const SnapshotImpl>();
    }
    return it->second;
  }

  void GetSnapshots(
      uint64_t ts_lb, uint64_t ts_ub,
      std::vector<std::shared_ptr<const Snapshot>>& snapshots) const {
    assert(ts_lb < ts_ub);
    auto it_low = snapshots_.lower_bound(ts_lb);
    auto it_high = snapshots_.lower_bound(ts_ub);
    for (auto it = it_low; it != it_high; ++it) {
      snapshots.emplace_back(it->second);
    }
  }

  void AddSnapshot(const std::shared_ptr<const SnapshotImpl>& snapshot) {
    assert(snapshot);
    snapshots_.try_emplace(snapshot->GetTimestamp(), snapshot);
  }

  // snapshots_to_release: the container to where the timestamped snapshots will
  // be moved so that it retains the last reference to the snapshots and the
  // snapshots won't be actually released which requires db mutex. The
  // snapshots will be released by caller of ReleaseSnapshotsOlderThan().
  void ReleaseSnapshotsOlderThan(
      uint64_t ts,
      autovector<std::shared_ptr<const SnapshotImpl>>& snapshots_to_release) {
    auto ub = snapshots_.lower_bound(ts);
    for (auto it = snapshots_.begin(); it != ub; ++it) {
      snapshots_to_release.emplace_back(it->second);
    }
    snapshots_.erase(snapshots_.begin(), ub);
  }

 private:
  std::map<uint64_t, std::shared_ptr<const SnapshotImpl>> snapshots_;
};

}  // namespace ROCKSDB_NAMESPACE
