// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/persistent_stats_history.h"
#include <string>
#include <utility>
#include "db/db_impl.h"
#include "util/string_util.h"

namespace rocksdb {

PersistentStatsHistoryIterator::~PersistentStatsHistoryIterator() {}

bool PersistentStatsHistoryIterator::Valid() const { return valid_; }

Status PersistentStatsHistoryIterator::status() const { return status_; }

void PersistentStatsHistoryIterator::Next() {
  // increment start_time by 1 to avoid infinite loop
  AdvanceIteratorByTime(GetStatsTime() + 1, end_time_);
}

uint64_t PersistentStatsHistoryIterator::GetStatsTime() const { return time_; }

const std::map<std::string, uint64_t>&
PersistentStatsHistoryIterator::GetStatsMap() const {
  return stats_map_;
}

std::pair<uint64_t, std::string> parseKey(const Slice& key,
                                          uint64_t start_time) {
  std::pair<uint64_t, std::string> result;
  std::string key_str = key.ToString();
  std::string::size_type pos = key_str.find("#");
  // TODO(Zhongyi): add counters to track parse failures?
  if (pos == std::string::npos) {
    result.first = 0;
    result.second = "";
  } else {
    uint64_t parsed_time = ParseUint64(key_str.substr(0, pos));
    // skip entries with timestamp smaller than start_time
    if (parsed_time < start_time) {
      result.first = 0;
      result.second = "";
    } else {
      result.first = parsed_time;
      std::string key_resize = key_str.substr(pos + 1);
      key_resize.erase(std::find(key_resize.begin(), key_resize.end(), '\0'), key_resize.end());
      result.second = key_resize;
    }
  }
  return result;
}

// advance the iterator to the next time between [start_time, end_time)
// if success, update time_ and stats_map_ with new_time and stats_map
void PersistentStatsHistoryIterator::AdvanceIteratorByTime(uint64_t start_time,
                                                           uint64_t end_time) {
  // try to find next entry in stats_history_ map
  if (db_impl_ != nullptr) {
    ReadOptions ro;
    Iterator* iter =
        db_impl_->NewIterator(ro, db_impl_->PersistentStatsColumnFamily());
    const int kNowSecondsStringLength = 10;
    char timestamp[kNowSecondsStringLength + 1];
    snprintf(timestamp, sizeof(timestamp), "%010d",
             static_cast<int>(std::max(time_, start_time)));
    timestamp[kNowSecondsStringLength] = '\0';

    iter->Seek(timestamp);
    // no more entries with timestamp >= start_time is found
    if (!iter->Valid()) {
      valid_ = false;
      delete iter;
      return;
    }
    time_ = parseKey(iter->key(), start_time).first;
    valid_ = true;
    if (time_ > end_time) {
      valid_ = false;
      delete iter;
      return;
    }
    // find all entries with timestamp equal to time_
    std::map<std::string, uint64_t> new_stats_map;
    std::pair<uint64_t, std::string> parsed_key =
        parseKey(iter->key(), start_time);
    for (; iter->Valid() &&
           parseKey(iter->key(), start_time).first == time_;
         iter->Next()) {
      new_stats_map[parseKey(iter->key(), start_time).second] =
          std::stoull(iter->value().ToString());
    }
    stats_map_.swap(new_stats_map);
    delete iter;
  } else {
    valid_ = false;
  }
}

}  // namespace rocksdb
