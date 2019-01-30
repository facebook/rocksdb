// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
#include "db/in_memory_stats_history.h"


namespace rocksdb {

InMemoryStatsHistoryIterator::~InMemoryStatsHistoryIterator() {

}

bool InMemoryStatsHistoryIterator::Valid() const { return valid_; }

Status InMemoryStatsHistoryIterator::status() const { return status_; }

void InMemoryStatsHistoryIterator::SeekToFirst() {
  AdvanceIteratorByTime(start_time_, end_time_);
}

void InMemoryStatsHistoryIterator::Next() {
  // increment start_time by 1 to avoid infinite loop
  AdvanceIteratorByTime(key()+1, end_time_);
}

uint64_t InMemoryStatsHistoryIterator::key() const {
  return key_;
}

const std::map<std::string, std::string>& InMemoryStatsHistoryIterator::value() const {
  return value_;
}

// advance the iterator to the next time between [start_time, end_time)
// if success, update key_ and value_ with new_time and stats_map
void InMemoryStatsHistoryIterator::AdvanceIteratorByTime(uint64_t start_time, uint64_t end_time) {
  // try to find next entry in stats_history_ map
  std::map<std::string, std::string> stats_map;
  uint64_t new_time = 0;
  bool found = false;
  if (db_impl_ != nullptr) {
    found = db_impl_->FindStatsByTime(start_time, end_time, &new_time, &stats_map);
  }
  if (found) {
    value_.swap(stats_map);
    key_ = new_time;
    valid_ = true;
  } else {
    valid_ = false;
  }
}


}  // namespace rocksdb
