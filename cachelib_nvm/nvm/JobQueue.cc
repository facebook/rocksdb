#include "JobQueue.hpp"

#include <cassert>
#include <algorithm>
#include <utility>

#include "cachelib_nvm/common/Logging.hpp"

namespace facebook {
namespace cachelib {
JobQueue::JobQueue(uint32_t numThreads, uint32_t maxQueueSize)
    : maxQueueSize_{maxQueueSize} {
  assert(numThreads > 0);
  numThreads = std::max(numThreads, 1u);
  workers_.reserve(numThreads);
  for (uint32_t i = 0; i < numThreads; i++) {
    workers_.emplace_back([this]() { workerThread(); });
  }
}

bool JobQueue::enqueue(std::unique_ptr<Job> job, const char* tag) {
  assert(tag);
  assert(!workers_.empty());
  auto queued = false;
  auto overflow = false;
  {
    std::lock_guard<std::mutex> lock{mutex_};
    if (!stop_) {
      overflow = maxQueueSize_ > 0 && queue_.size() >= maxQueueSize_;
      if (!overflow) {
        queue_.emplace_back(std::move(job), tag);
        queued = true;
        cv_.notify_one();
      }
    }
  }
  assert(!overflow);
#if CACHELIB_LOGGING
  if (overflow) {
    VLOG_EVERY_N(2, 100) << "Queue overflow";
  }
#endif
  return queued;
}

void JobQueue::stop() {
  {
    std::lock_guard<std::mutex> lock{mutex_};
    if (stop_) {
      return;
    }
    stop_ = true;
    cv_.notify_all();
  }
#if CACHELIB_LOGGING
  VLOG(1) << "Joining threads";
#endif
  for (auto& t : workers_) {
    t.join();
  }
  workers_.clear();
#if CACHELIB_LOGGING
  VLOG(1) << "Joined";
#endif
}

void JobQueue::pause(bool stop) {
#if CACHELIB_LOGGING
  VLOG(1) << "Pause message queue: " << stop;
#endif
  std::lock_guard<std::mutex> lock{mutex_};
  if (pause_ != stop) {
    pause_ = stop;
    if (!pause_) {
      cv_.notify_all();
    }
  }
}

void JobQueue::workerThread() {
  std::unique_lock<std::mutex> lock{mutex_};
  for (;;) {
    while (!stop_ && !pause_ && !queue_.empty()) {
      auto entry = std::move(queue_.front());
      queue_.pop_front();
      processing_++;
      lock.unlock();
      auto exitCode = entry.job->run();
      if (exitCode == JobExitCode::Reschedule) {
        entry.rescheduleCount++;
#if CACHELIB_LOGGING
        if (entry.rescheduleCount >= 100 && entry.rescheduleCount % 100 == 0) {
          VLOG(3) << "High job reschedule count " << entry.rescheduleCount
                  << " '" << entry.tag << "'";
        }
#endif
      } else {
        // Deallocate outside the lock:
        entry.job.reset();
#if CACHELIB_LOGGING
        if (entry.rescheduleCount > 0) {
          VLOG(3) << "Job '" << entry.tag << "' reschedule count: "
                  << entry.rescheduleCount;
        }
#endif
      }
      lock.lock();
      processing_--;
      if (exitCode == JobExitCode::Reschedule) {
        queue_.emplace_back(std::move(entry));
        if (queue_.size() == 1) {
          // In really rare case let's be a little better than busy wait
          std::this_thread::yield();
        }
      }
    }
    if (stop_) {
      break;
    }
    cv_.wait(lock);
  }
}

void JobQueue::drain() {
  // Busy wait, but used only in tests
  std::unique_lock<std::mutex> lock{mutex_};
  assert(!pause_);
  while (processing_ != 0 || !queue_.empty()) {
    lock.unlock();
    std::this_thread::yield();
    lock.lock();
  }
}
}
}
