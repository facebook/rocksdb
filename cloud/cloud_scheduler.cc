// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE
#include "cloud/cloud_scheduler.h"

#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace ROCKSDB_NAMESPACE {

struct ScheduledJob {
  ScheduledJob(long _id, std::chrono::steady_clock::time_point _when,
               std::chrono::microseconds _frequency,
               std::function<void(void*)> _callback, void* _arg)
      : id(_id),
        when(_when),
        frequency(_frequency),
        callback(_callback),
        arg(_arg) {}

  long id;
  std::chrono::steady_clock::time_point when;
  std::chrono::microseconds frequency;
  std::function<void(void*)> callback;
  void* arg;
};

struct Comp {
  bool operator()(const ScheduledJob& a, const ScheduledJob& b) const {
    return a.when < b.when;
  }
};

class CloudSchedulerImpl : public CloudScheduler {
 public:
  CloudSchedulerImpl();
  ~CloudSchedulerImpl();
  long ScheduleJob(std::chrono::microseconds when,
                   std::function<void(void*)> callback, void* arg) override;
  long ScheduleRecurringJob(std::chrono::microseconds when,
                            std::chrono::microseconds frequency,
                            std::function<void(void*)> callback,
                            void* arg) override;
  bool CancelJob(long handle) override;

 private:
  void DoWork();
  long next_id_;

  std::mutex mutex_;
  // Notified when the earliest job to be scheduled has changed.
  std::condition_variable jobs_changed_cv_;
  std::multiset<ScheduledJob, Comp> scheduled_jobs_;
  bool shutting_down_{false};

  std::unique_ptr<std::thread> thread_;
};

// Implementation of a CloudScheduler that keeps track of the jobs
// it scheduled.  Only cleans up those jobs on exit or cancel.
class LocalCloudScheduler : public CloudScheduler {
 public:
  LocalCloudScheduler(const std::shared_ptr<CloudScheduler>& scheduler,
                      long local_id)
      : scheduler_(scheduler), next_local_id_(local_id) {}
  ~LocalCloudScheduler() override {
    for (const auto job : jobs_) {
      scheduler_->CancelJob(job.second);
    }
    jobs_.clear();
  }

  long ScheduleJob(std::chrono::microseconds when,
                   std::function<void(void*)> callback, void* arg) override {
    std::lock_guard<std::mutex> lk(job_mutex_);
    long local_id = next_local_id_++;
    auto job = [this, local_id, callback](void* a) {
      callback(a);
      std::lock_guard<std::mutex> cblk(job_mutex_);
      jobs_.erase(local_id);
    };
    jobs_[local_id] = scheduler_->ScheduleJob(when, job, arg);
    return local_id;
  }

  long ScheduleRecurringJob(std::chrono::microseconds when,
                            std::chrono::microseconds frequency,
                            std::function<void(void*)> callback,
                            void* arg) override {
    auto job = scheduler_->ScheduleRecurringJob(when, frequency, callback, arg);
    std::lock_guard<std::mutex> lk(job_mutex_);
    long local_id = next_local_id_++;
    jobs_[local_id] = job;
    return local_id;
  }
  // Cancels the job referred to by handle if it is active and associated with
  // this scheduler
  bool CancelJob(long handle) override {
    std::lock_guard<std::mutex> lk(job_mutex_);
    const auto& it = jobs_.find(handle);
    if (it != jobs_.end()) {
      jobs_.erase(it);
      return scheduler_->CancelJob(it->second);
    } else {
      return false;
    }
  }

 private:
  std::mutex job_mutex_;
  std::shared_ptr<CloudScheduler> scheduler_;
  long next_local_id_;
  std::unordered_map<long, long> jobs_;
};

std::shared_ptr<CloudScheduler> CloudScheduler::Get() {
  static std::shared_ptr<CloudSchedulerImpl> scheduler =
      std::make_shared<CloudSchedulerImpl>();
  static long local_scheduler_id = 0;

  std::shared_ptr<CloudScheduler> result =
      std::make_shared<LocalCloudScheduler>(scheduler, local_scheduler_id);
  local_scheduler_id += 10000;
  return result;
}

CloudSchedulerImpl::CloudSchedulerImpl() {
  next_id_ = 1;
  auto lambda = [this]() { DoWork(); };
  thread_.reset(new std::thread(lambda));
}

CloudSchedulerImpl::~CloudSchedulerImpl() {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    shutting_down_ = true;
    scheduled_jobs_.clear();
    jobs_changed_cv_.notify_all();
  }
  if (thread_ && thread_->joinable()) {
    thread_->join();
  }
  thread_.reset();
}

long CloudSchedulerImpl::ScheduleJob(std::chrono::microseconds when,
                                     std::function<void(void*)> callback,
                                     void* arg) {
  std::lock_guard<std::mutex> lk(mutex_);
  long id = next_id_++;

  auto time = std::chrono::steady_clock::now() + when;
  auto itr = scheduled_jobs_.emplace(id, time, std::chrono::microseconds(0),
                                     callback, arg);

  if (itr == scheduled_jobs_.begin()) {
    jobs_changed_cv_.notify_all();
  }
  return id;
}

long CloudSchedulerImpl::ScheduleRecurringJob(
    std::chrono::microseconds when, std::chrono::microseconds frequency,
    std::function<void(void*)> callback, void* arg) {
  std::lock_guard<std::mutex> lk(mutex_);
  long id = next_id_++;

  auto time = std::chrono::steady_clock::now() + when;
  auto itr = scheduled_jobs_.emplace(id, time, frequency, callback, arg);
  if (itr == scheduled_jobs_.begin()) {
    jobs_changed_cv_.notify_all();
  }
  return id;
}

bool CloudSchedulerImpl::CancelJob(long id) {
  std::lock_guard<std::mutex> lk(mutex_);
  for (auto it = scheduled_jobs_.begin(); it != scheduled_jobs_.end(); ++it) {
    if (it->id == id) {
      bool is_first = (it == scheduled_jobs_.begin());
      scheduled_jobs_.erase(it);
      if (is_first) {
        jobs_changed_cv_.notify_all();
      }
      return true;
    }
  }
  return false;
}

void CloudSchedulerImpl::DoWork() {
  while (true) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (shutting_down_) {
      break;
    }
    if (scheduled_jobs_.empty()) {
      jobs_changed_cv_.wait(lk);
      continue;
    }
    auto earliest_job = scheduled_jobs_.begin();
    auto earliest_job_time = earliest_job->when;
    if (earliest_job_time >= std::chrono::steady_clock::now()) {
      jobs_changed_cv_.wait_until(lk, earliest_job_time);
      continue;
    }
    // invoke the function
    lk.unlock();

    earliest_job->callback(earliest_job->arg);

    lk.lock();
    if (earliest_job->frequency.count() > 0) {
      ScheduledJob new_job = *earliest_job;
      new_job.when = std::chrono::steady_clock::now() + new_job.frequency;
      scheduled_jobs_.emplace(new_job);
    }
    scheduled_jobs_.erase(earliest_job);
  }
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
