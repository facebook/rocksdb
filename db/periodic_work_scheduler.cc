//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_work_scheduler.h"

#include "db/db_impl/db_impl.h"

#ifndef ROCKSDB_LITE
namespace ROCKSDB_NAMESPACE {

PeriodicWorkScheduler::PeriodicWorkScheduler(Env* env) : timer_mu_(env) {
  timer = std::unique_ptr<Timer>(new Timer(env));
}

void PeriodicWorkScheduler::Register(DBImpl* dbi,
                                     unsigned int stats_dump_period_sec,
                                     unsigned int stats_persist_period_sec) {
  MutexLock l(&timer_mu_);
  static std::atomic<uint64_t> initial_delay(0);
  timer->Start();
  if (stats_dump_period_sec > 0) {
    timer->Add([dbi]() { dbi->DumpStats(); }, GetTaskName(dbi, "dump_st"),
               initial_delay.fetch_add(1) %
                   static_cast<uint64_t>(stats_dump_period_sec) *
                   kMicrosInSecond,
               static_cast<uint64_t>(stats_dump_period_sec) * kMicrosInSecond);
  }
  if (stats_persist_period_sec > 0) {
    timer->Add(
        [dbi]() { dbi->PersistStats(); }, GetTaskName(dbi, "pst_st"),
        initial_delay.fetch_add(1) %
            static_cast<uint64_t>(stats_persist_period_sec) * kMicrosInSecond,
        static_cast<uint64_t>(stats_persist_period_sec) * kMicrosInSecond);
  }
  timer->Add([dbi]() { dbi->FlushInfoLog(); },
             GetTaskName(dbi, "flush_info_log"),
             initial_delay.fetch_add(1) % kDefaultFlushInfoLogPeriodSec *
                 kMicrosInSecond,
             kDefaultFlushInfoLogPeriodSec * kMicrosInSecond);
}

void PeriodicWorkScheduler::Unregister(DBImpl* dbi) {
  MutexLock l(&timer_mu_);
  timer->Cancel(GetTaskName(dbi, "dump_st"));
  timer->Cancel(GetTaskName(dbi, "pst_st"));
  timer->Cancel(GetTaskName(dbi, "flush_info_log"));
  if (!timer->HasPendingTask()) {
    timer->Shutdown();
  }
}

PeriodicWorkScheduler* PeriodicWorkScheduler::Default() {
  // Always use the default Env for the scheduler, as we only use the NowMicros
  // which is the same for all env.
  // The Env could only be overridden in test.
  static PeriodicWorkScheduler scheduler(Env::Default());
  return &scheduler;
}

std::string PeriodicWorkScheduler::GetTaskName(DBImpl* dbi,
                                               const std::string& func_name) {
  std::string db_session_id;
  // TODO: Should this error be ignored?
  dbi->GetDbSessionId(db_session_id).PermitUncheckedError();
  return db_session_id + ":" + func_name;
}

#ifndef NDEBUG

// Get the static scheduler. For a new env, it needs to re-create the internal
// timer, so only re-create it when there's no running task. Otherwise, return
// the existing scheduler. Which means if the unittest needs to update MockEnv,
// Close all db instances and then re-open them.
PeriodicWorkTestScheduler* PeriodicWorkTestScheduler::Default(Env* env) {
  static PeriodicWorkTestScheduler scheduler(env);
  static port::Mutex mutex;
  {
    MutexLock l(&mutex);
    if (scheduler.timer.get() != nullptr &&
        scheduler.timer->TEST_GetPendingTaskNum() == 0) {
      {
        MutexLock timer_mu_guard(&scheduler.timer_mu_);
        scheduler.timer->Shutdown();
      }
      scheduler.timer.reset(new Timer(env));
    }
  }
  return &scheduler;
}

void PeriodicWorkTestScheduler::TEST_WaitForRun(
    std::function<void()> callback) const {
  if (timer != nullptr) {
    timer->TEST_WaitForRun(callback);
  }
}

size_t PeriodicWorkTestScheduler::TEST_GetValidTaskNum() const {
  if (timer != nullptr) {
    return timer->TEST_GetPendingTaskNum();
  }
  return 0;
}

PeriodicWorkTestScheduler::PeriodicWorkTestScheduler(Env* env)
    : PeriodicWorkScheduler(env) {}

#endif  // !NDEBUG
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
