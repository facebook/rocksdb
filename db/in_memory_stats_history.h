// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/stats_history.h"

namespace rocksdb {

class InMemoryStatsHistoryIterator final: public StatsHistoryIterator {
public:
  InMemoryStatsHistoryIterator(uint64_t start_time, uint64_t end_time,
                               bool in_memory, GetStatsOptions& stats_opts,
                               DBImpl* db_impl)
    : start_time_(start_time),
      end_time_(end_time),
      in_memory_(in_memory),
      valid_(true),
      stats_opts_(stats_opts),
      db_impl_(db_impl) {
  }
  virtual ~InMemoryStatsHistoryIterator();
  virtual bool Valid() const override;
  virtual Status status() const override;

  virtual void SeekToFirst() override;

  virtual void Next() override;
  virtual uint64_t key() const override;

  virtual const std::map<std::string, std::string>& value() const override;

private:
  // advance the iterator to the next time between [start_time, end_time)
  void AdvanceIteratorByTime(uint64_t start_time, uint64_t end_time);

  uint64_t key_;
  uint64_t start_time_;
  uint64_t end_time_;
  std::map<std::string, std::string> value_;
  Status status_;
  bool in_memory_;
  bool valid_;
  GetStatsOptions stats_opts_;
  DBImpl* db_impl_;
};

}  // namespace rocksdb
