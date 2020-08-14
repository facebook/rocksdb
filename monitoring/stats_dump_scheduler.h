//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include "db/db_impl/db_impl.h"
#include "util/timer.h"

namespace ROCKSDB_NAMESPACE {

// StatsDumpScheduler is a singleton object, which is scheduling/running
// DumpStats() and PersistStats() for all DB instances. All DB instances uses
// the same object from `Default()`.
// Internally, it uses a single threaded timer object to run the stats dump
// functions. Timer thread won't be started if there's no function needs to run,
// for example, option.stats_dump_period_sec and option.stats_persist_period_sec
// are set to 0.
class StatsDumpScheduler {
 public:
  static StatsDumpScheduler* Default();

  StatsDumpScheduler() = delete;
  StatsDumpScheduler(const StatsDumpScheduler&) = delete;
  StatsDumpScheduler(StatsDumpScheduler&&) = delete;
  StatsDumpScheduler& operator=(const StatsDumpScheduler&) = delete;
  StatsDumpScheduler& operator=(StatsDumpScheduler&&) = delete;

  void Register(DBImpl* dbi, unsigned int stats_dump_period_sec,
                unsigned int stats_persist_period_sec);

  void Unregister(DBImpl* dbi);

 protected:
  std::unique_ptr<Timer> timer;

  explicit StatsDumpScheduler(Env* env);

 private:
  std::string GetTaskName(DBImpl* dbi, const std::string& func_name);
};

#ifndef NDEBUG
// StatsDumpTestScheduler is for unittest, which can specify the Env like
// SafeMockTimeEnv. It also contains functions for unittest.
class StatsDumpTestScheduler : public StatsDumpScheduler {
 public:
  static StatsDumpTestScheduler* Default(Env* env);

  void TEST_WaitForRun(std::function<void()> callback) const;

  size_t TEST_GetValidTaskNum() const;

 private:
  explicit StatsDumpTestScheduler(Env* env);
};
#endif  // !NDEBUG

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
