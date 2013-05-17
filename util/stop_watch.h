#ifndef STORAGE_LEVELDB_UTIL_STOP_WATCH_H_
#define STORAGE_LEVELDB_UTIL_STOP_WATCH_H_

#include "leveldb/env.h"
#include "leveldb/statistics.h"

namespace leveldb {
// Auto-scoped.
// Records the statistic into the corresponding histogram.
class StopWatch {
 public:
  StopWatch(
    Env * const env,
    std::shared_ptr<Statistics> statistics,
    const Histograms histogram_name) :
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
} // namespace leveldb
#endif // STORAGE_LEVELDB_UTIL_STOP_WATCH_H_
