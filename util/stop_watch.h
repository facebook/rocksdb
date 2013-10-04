#ifndef STORAGE_LEVELDB_UTIL_STOP_WATCH_H_
#define STORAGE_LEVELDB_UTIL_STOP_WATCH_H_

#include "rocksdb/env.h"
#include "rocksdb/statistics.h"

namespace rocksdb {
// Auto-scoped.
// Records the statistic into the corresponding histogram.
class StopWatch {
 public:
  StopWatch(
    Env * const env,
    std::shared_ptr<Statistics> statistics = nullptr,
    const Histograms histogram_name = DB_GET) :
      env_(env),
      start_time_(env->NowMicros()),
      statistics_(statistics),
      histogram_name_(histogram_name) {}



  uint64_t ElapsedMicros() {
    return env_->NowMicros() - start_time_;
  }

  ~StopWatch() {
    if (statistics_) {
      statistics_->measureTime(histogram_name_, ElapsedMicros());
    }
  }

 private:
  Env* const env_;
  const uint64_t start_time_;
  std::shared_ptr<Statistics> statistics_;
  const Histograms histogram_name_;

};

// a nano second precision stopwatch
class StopWatchNano {
 public:
  StopWatchNano(Env* const env, bool auto_start = false)
      : env_(env), start_(0) {
    if (auto_start) {
      Start();
    }
  }

  void Start() { start_ = env_->NowNanos(); }

  uint64_t ElapsedNanos(bool reset = false) {
    auto now = env_->NowNanos();
    auto elapsed = now - start_;
    if (reset) {
      start_ = now;
    }
    return elapsed;
  }

 private:
  Env* const env_;
  uint64_t start_;
};

} // namespace rocksdb
#endif // STORAGE_LEVELDB_UTIL_STOP_WATCH_H_
