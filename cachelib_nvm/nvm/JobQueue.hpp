#pragma once

#include <cstdint>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <utility>

namespace facebook {
namespace cachelib {
enum class JobExitCode {
  Done,
  Reschedule,
};

// std::function requires CopyConstructable for captures, we need movable.
class Job {
 public:
  virtual ~Job() = default;
  virtual JobExitCode run() = 0;
};

template <typename F>
class GenericJob : public Job {
 public:
  explicit GenericJob(F f): f_{std::move(f)} {}
  GenericJob(const GenericJob&) = delete;
  GenericJob operator=(const GenericJob&) = delete;
  GenericJob(GenericJob&&) = default;
  GenericJob& operator=(GenericJob&&) = default;

  JobExitCode run() override {
    return f_();
  }

 private:
  F f_;
};

template <typename F>
std::unique_ptr<Job> makeJob(F f) {
  return std::make_unique<GenericJob<F>>(std::move(f));
}

class JobQueue {
 public:
  // 0 for unimited queue
  JobQueue(uint32_t numThreads, uint32_t maxQueueSize);
  JobQueue(const JobQueue&) = delete;
  JobQueue& operator=(const JobQueue&) = delete;
  ~JobQueue() {
    stop();
  }

  // TODO: Stats
  // TODO: Broadcast messages: this is how in general we can do stop and pause

  bool enqueue(std::unique_ptr<Job> job, const char* tag);
  void stop();
  void pause(bool stop);
  void drain();

  uint32_t queueSize() const {
    std::lock_guard<std::mutex> lock{mutex_};
    return queue_.size();
  }

 private:
  struct QueueEntry {
    std::unique_ptr<Job> job;
    uint32_t rescheduleCount{};
    const char* tag{};

    QueueEntry(std::unique_ptr<Job> j, const char* t)
        : job{std::move(j)}, tag{t} {}

    QueueEntry(QueueEntry&&) = default;
    QueueEntry& operator=(QueueEntry&&) = default;
    QueueEntry(const QueueEntry&) = delete;
    QueueEntry& operator=(const QueueEntry&) = delete;
  };

  void workerThread();

  const uint32_t maxQueueSize_{};
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  // Can have several queues and round robin between them to reduce contention
  std::deque<QueueEntry> queue_;
  uint32_t processing_{};
  bool stop_{false};
  bool pause_{false};
  std::vector<std::thread> workers_;
};
}
}
