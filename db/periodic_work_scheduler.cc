//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_work_scheduler.h"

#include "db/db_impl/db_impl.h"
#include "rocksdb/system_clock.h"

#ifndef ROCKSDB_LITE
namespace ROCKSDB_NAMESPACE {

const std::string PeriodicWorkTaskNames::kDumpStats = "dump_st";
const std::string PeriodicWorkTaskNames::kPersistStats = "pst_st";
const std::string PeriodicWorkTaskNames::kFlushInfoLog = "flush_info_log";
const std::string PeriodicWorkTaskNames::kRecordSeqnoTime = "record_seq_time";

PeriodicWorkScheduler::PeriodicWorkScheduler(
    const std::shared_ptr<SystemClock>& clock) {
  timer = std::unique_ptr<Timer>(new Timer(clock.get()));
}

Status PeriodicWorkScheduler::Register(DBImpl* dbi,
                                       unsigned int stats_dump_period_sec,
                                       unsigned int stats_persist_period_sec) {
  MutexLock l(&timer_mu_);
  static std::atomic<uint64_t> initial_delay(0);
  timer->Start();
  if (stats_dump_period_sec > 0) {
    bool succeeded = timer->Add(
        [dbi]() { dbi->DumpStats(); },
        GetTaskName(dbi, PeriodicWorkTaskNames::kDumpStats),
        initial_delay.fetch_add(1) %
            static_cast<uint64_t>(stats_dump_period_sec) * kMicrosInSecond,
        static_cast<uint64_t>(stats_dump_period_sec) * kMicrosInSecond);
    if (!succeeded) {
      return Status::Aborted("Unable to add periodic task DumpStats");
    }
  }
  if (stats_persist_period_sec > 0) {
    bool succeeded = timer->Add(
        [dbi]() { dbi->PersistStats(); },
        GetTaskName(dbi, PeriodicWorkTaskNames::kPersistStats),
        initial_delay.fetch_add(1) %
            static_cast<uint64_t>(stats_persist_period_sec) * kMicrosInSecond,
        static_cast<uint64_t>(stats_persist_period_sec) * kMicrosInSecond);
    if (!succeeded) {
      return Status::Aborted("Unable to add periodic task PersistStats");
    }
  }
  bool succeeded =
      timer->Add([dbi]() { dbi->FlushInfoLog(); },
                 GetTaskName(dbi, PeriodicWorkTaskNames::kFlushInfoLog),
                 initial_delay.fetch_add(1) % kDefaultFlushInfoLogPeriodSec *
                     kMicrosInSecond,
                 kDefaultFlushInfoLogPeriodSec * kMicrosInSecond);
  if (!succeeded) {
    return Status::Aborted("Unable to add periodic task FlushInfoLog");
  }
  return Status::OK();
}

Status PeriodicWorkScheduler::RegisterRecordSeqnoTimeWorker(
    DBImpl* dbi, uint64_t record_cadence_sec) {
  MutexLock l(&timer_mu_);
  timer->Start();
  static std::atomic_uint64_t initial_delay(0);
  bool succeeded = timer->Add(
      [dbi]() { dbi->RecordSeqnoToTimeMapping(); },
      GetTaskName(dbi, PeriodicWorkTaskNames::kRecordSeqnoTime),
      initial_delay.fetch_add(1) % record_cadence_sec * kMicrosInSecond,
      record_cadence_sec * kMicrosInSecond);
  if (!succeeded) {
    return Status::NotSupported(
        "Updating seqno to time worker cadence is not supported yet");
  }
  return Status::OK();
}

void PeriodicWorkScheduler::UnregisterRecordSeqnoTimeWorker(DBImpl* dbi) {
  MutexLock l(&timer_mu_);
  timer->Cancel(GetTaskName(dbi, PeriodicWorkTaskNames::kRecordSeqnoTime));
  if (!timer->HasPendingTask()) {
    timer->Shutdown();
  }
}

void PeriodicWorkScheduler::Unregister(DBImpl* dbi) {
  MutexLock l(&timer_mu_);
  timer->Cancel(GetTaskName(dbi, PeriodicWorkTaskNames::kDumpStats));
  timer->Cancel(GetTaskName(dbi, PeriodicWorkTaskNames::kPersistStats));
  timer->Cancel(GetTaskName(dbi, PeriodicWorkTaskNames::kFlushInfoLog));
  if (!timer->HasPendingTask()) {
    timer->Shutdown();
  }
}

PeriodicWorkScheduler* PeriodicWorkScheduler::Default() {
  // Always use the default SystemClock for the scheduler, as we only use the
  // NowMicros which is the same for all clocks. The Env could only be
  // overridden in test.
  static PeriodicWorkScheduler scheduler(SystemClock::Default());
  return &scheduler;
}

std::string PeriodicWorkScheduler::GetTaskName(
    const DBImpl* dbi, const std::string& func_name) const {
  std::string db_session_id;
  // TODO: Should this error be ignored?
  dbi->GetDbSessionId(db_session_id).PermitUncheckedError();
  return db_session_id + ":" + func_name;
}

#ifndef NDEBUG

// Get the static scheduler. For a new SystemClock, it needs to re-create the
// internal timer, so only re-create it when there's no running task. Otherwise,
// return the existing scheduler. Which means if the unittest needs to update
// MockClock, Close all db instances and then re-open them.
PeriodicWorkTestScheduler* PeriodicWorkTestScheduler::Default(
    const std::shared_ptr<SystemClock>& clock) {
  static PeriodicWorkTestScheduler scheduler(clock);
  static port::Mutex mutex;
  {
    MutexLock l(&mutex);
    if (scheduler.timer.get() != nullptr &&
        scheduler.timer->TEST_GetPendingTaskNum() == 0) {
      {
        MutexLock timer_mu_guard(&scheduler.timer_mu_);
        scheduler.timer->Shutdown();
      }
      scheduler.timer.reset(new Timer(clock.get()));
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

bool PeriodicWorkTestScheduler::TEST_HasValidTask(
    const DBImpl* dbi, const std::string& func_name) const {
  if (timer == nullptr) {
    return false;
  }
  return timer->TEST_HasVaildTask(GetTaskName(dbi, func_name));
}

PeriodicWorkTestScheduler::PeriodicWorkTestScheduler(
    const std::shared_ptr<SystemClock>& clock)
    : PeriodicWorkScheduler(clock) {}

#endif  // !NDEBUG
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
