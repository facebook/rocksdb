//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/stats_dump_scheduler.h"

#include "db/db_impl/db_impl.h"
#include "util/cast_util.h"

#ifndef ROCKSDB_LITE
namespace ROCKSDB_NAMESPACE {

StatsDumpScheduler::StatsDumpScheduler(Env* env) {
  timer = std::unique_ptr<Timer>(new Timer(env));
}

void StatsDumpScheduler::Register(DBImpl* dbi,
                                  unsigned int stats_dump_period_sec,
                                  unsigned int stats_persist_period_sec) {
  static std::atomic<uint64_t> initial_delay(0);
  if (stats_dump_period_sec > 0) {
    timer->Start();
    timer->Add([dbi]() { dbi->DumpStats(); }, GetTaskName(dbi, "dump_st"),
               initial_delay.fetch_add(1) %
                   static_cast<uint64_t>(stats_dump_period_sec) *
                   kMicrosInSecond,
               static_cast<uint64_t>(stats_dump_period_sec) * kMicrosInSecond);
  }
  if (stats_persist_period_sec > 0) {
    timer->Start();
    timer->Add([dbi]() { dbi->PersistStats(); }, GetTaskName(dbi, "pst_st"),
               initial_delay.fetch_add(1) %
                   static_cast<uint64_t>(stats_persist_period_sec) *
                   kMicrosInSecond,
               static_cast<uint64_t>(stats_persist_period_sec) * kMicrosInSecond);
  }
}

void StatsDumpScheduler::Unregister(DBImpl* dbi) {
  timer->Cancel(GetTaskName(dbi, "dump_st"));
  timer->Cancel(GetTaskName(dbi, "pst_st"));
  if (!timer->HasPendingTask()) {
    timer->Shutdown();
  }
}

StatsDumpScheduler* StatsDumpScheduler::Default() {
  // Always use the default Env for the scheduler, as we only use the NowMicros
  // which is the same for all env.
  // The Env could only be overridden in test.
  static StatsDumpScheduler scheduler(Env::Default());
  return &scheduler;
}

std::string StatsDumpScheduler::GetTaskName(DBImpl* dbi, const std::string& func_name) {
  std::string db_session_id;
  dbi->GetDbSessionId(db_session_id);
  return db_session_id + ":" + func_name;
}

#ifndef NDEBUG
bool StatsDumpScheduler::TEST_UpdateEnv(Env* env) {
  auto scheduler = Default();
  if (scheduler->timer == nullptr ||
      scheduler->timer->TEST_GetPendingTaskNum() != 0) {
    return false;
  }
  scheduler->timer.reset(new Timer(env));
  return true;
}

void StatsDumpScheduler::TEST_WaitForRun(std::function<void()> callback) const {
  if (timer != nullptr) {
    timer->TEST_WaitForRun(callback);
  }
}

size_t StatsDumpScheduler::TEST_GetValidTaskNum() const {
  if (timer != nullptr) {
    return timer->TEST_GetPendingTaskNum();
  }
  return 0;
}
#endif  // !NDEBUG

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
