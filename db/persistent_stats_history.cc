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
#include "db/db_impl/db_impl.h"
#include "port/likely.h"
#include "util/string_util.h"

namespace rocksdb {
// 10 digit seconds timestamp => [Sep 9, 2001 ~ Nov 20, 2286]
const int kNowSecondsStringLength = 10;
const int kMicrosInSecond = 1000 * 1000;
const char* kVersionKeyString = "__persistent_stats_version__";
const int kPersistentStatsVersion = 1;

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

int PersistentStatsHistoryIterator::EncodeVersionKey(uint64_t now_micros,
                                                     int size, char* buf) {
  // make time stamp string equal in length to allow sorting by time
  int length = snprintf(buf, size, "%010d#%s",
                        static_cast<int>(now_micros / kMicrosInSecond),
                        kVersionKeyString);
  return length;
}

int PersistentStatsHistoryIterator::DecodeVersionNumber(
    const char* timestamp_str, Iterator* iter) {
  char version_key[100];
  snprintf(version_key, 100, "%s#%s", timestamp_str, kVersionKeyString);
  iter->Seek(version_key);
  // cannot find version_number, could be a user-defined CF or internal error
  // skip all with the same timestamp
  if (!iter->Valid()) {
    return -1;
  } else {
    // read version_number but do nothing in current version
    int version_number =
        static_cast<int>(std::stoull(iter->value().ToString()));
    // reset iter to beginning
    iter->Seek(timestamp_str);
    return version_number;
  }
}

int PersistentStatsHistoryIterator::EncodeKey(uint64_t now_micros,
                                              const std::string& key, int size,
                                              char* buf) {
  char timestamp[kNowSecondsStringLength + 1];
  // make time stamp string equal in length to allow sorting by time
  snprintf(timestamp, sizeof(timestamp), "%010d",
           static_cast<int>(now_micros / kMicrosInSecond));
  timestamp[kNowSecondsStringLength] = '\0';
  return snprintf(buf, size, "%s#%s", timestamp, key.c_str());
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
      auto it = std::find(key_resize.begin(), key_resize.end(), '\0');
      assert(it == key_resize.end());
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
    char timestamp[kNowSecondsStringLength + 1];
    snprintf(timestamp, sizeof(timestamp), "%010d",
             static_cast<int>(std::max(time_, start_time)));
    timestamp[kNowSecondsStringLength] = '\0';

    iter->Seek(timestamp);
    // no more entries with timestamp >= start_time is found or version key
    // is found to be incompatible
    if (!iter->Valid() ||
        (version_ = DecodeVersionNumber(timestamp, iter)) < 0) {
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
    std::pair<uint64_t, std::string> kv(time_, "");
    for (; iter->Valid() && kv.first == time_; iter->Next()) {
      kv = parseKey(iter->key(), start_time);
      // skip version key and first dummpy entry
      if (LIKELY(kv.second.compare(kVersionKeyString) == 0 ||
                 kv.first != time_)) {
        continue;
      }
      new_stats_map[kv.second] = std::stoull(iter->value().ToString());
    }
    stats_map_.swap(new_stats_map);
    delete iter;
  } else {
    valid_ = false;
  }
}

}  // namespace rocksdb
