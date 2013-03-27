#ifndef STORAGE_LEVELDB_UTIL_STOP_WATCH_H_
#define STORAGE_LEVELDB_UTIL_STOP_WATCH_H_

#include "leveldb/env.h"
#include "leveldb/statistics.h"
#include <iostream>
namespace leveldb {

class StopWatch {
 public:
  virtual uint64_t ElapsedMicros() = 0;
  virtual ~StopWatch() {}
};

class DoNothingStopWatch : public StopWatch {
 public:
  virtual uint64_t ElapsedMicros() {
    return 0;
  }
};

// Auto-scoped.
// Records the statistic into the corresponding histogram.
class ScopedRecordingStopWatch : public StopWatch {
 public:
  ScopedRecordingStopWatch(Env * const env,
                           std::shared_ptr<Statistics> statistics,
                           const Histograms histogram_name) :
                            env_(env),
                            start_time_(env->NowMicros()),
                            statistics_(statistics),
                            histogram_name_(histogram_name) {}

  virtual uint64_t ElapsedMicros() {
    return env_->NowMicros() - start_time_;
  }

  virtual ~ScopedRecordingStopWatch() {
    uint64_t elapsed_time = env_->NowMicros() - start_time_;
    statistics_->measureTime(histogram_name_, elapsed_time);
  }

 private:
  Env* const env_;
  const uint64_t start_time_;
  std::shared_ptr<Statistics> statistics_;
  const Histograms histogram_name_;

};

namespace stats {
// Helper method
std::unique_ptr<StopWatch> StartStopWatch(Env * const env,
                                          std::shared_ptr<Statistics> stats,
                                          Histograms histogram_name) {
  assert(env);
  if (stats) {
    return std::unique_ptr<StopWatch>(new ScopedRecordingStopWatch(
                                           env,
                                           stats,
                                           histogram_name));
  } else {
    return std::unique_ptr<StopWatch>(new DoNothingStopWatch());
  }
};
} // namespace stats
} // namespace leveldb
#endif // STORAGE_LEVELDB_UTIL_STOP_WATCH_H_
