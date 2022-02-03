//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else

#include <sys/types.h>

#include <cinttypes>
#include <cstdio>
#include <limits>

#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/system_clock.h"
#include "util/coding.h"
#include "util/gflags_compat.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/random.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

static constexpr uint32_t KiB = uint32_t{1} << 10;
static constexpr uint32_t MiB = KiB << 10;
static constexpr uint64_t GiB = MiB << 10;

DEFINE_uint32(threads, 16, "Number of concurrent threads to run.");
DEFINE_uint64(cache_size, 1 * GiB,
              "Number of bytes to use as a cache of uncompressed data.");
DEFINE_uint32(num_shard_bits, 6, "shard_bits.");

DEFINE_double(resident_ratio, 0.25,
              "Ratio of keys fitting in cache to keyspace.");
DEFINE_uint64(ops_per_thread, 0,
              "Number of operations per thread. (Default: 5 * keyspace size)");
DEFINE_uint32(value_bytes, 8 * KiB, "Size of each value added.");

DEFINE_uint32(skew, 5, "Degree of skew in key selection");
DEFINE_bool(populate_cache, true, "Populate cache before operations");

DEFINE_uint32(lookup_insert_percent, 87,
              "Ratio of lookup (+ insert on not found) to total workload "
              "(expressed as a percentage)");
DEFINE_uint32(insert_percent, 2,
              "Ratio of insert to total workload (expressed as a percentage)");
DEFINE_uint32(lookup_percent, 10,
              "Ratio of lookup to total workload (expressed as a percentage)");
DEFINE_uint32(erase_percent, 1,
              "Ratio of erase to total workload (expressed as a percentage)");

DEFINE_bool(use_clock_cache, false, "");

namespace ROCKSDB_NAMESPACE {

class CacheBench;
namespace {
// State shared by all concurrent executions of the same benchmark.
class SharedState {
 public:
  explicit SharedState(CacheBench* cache_bench)
      : cv_(&mu_),
        num_initialized_(0),
        start_(false),
        num_done_(0),
        cache_bench_(cache_bench) {}

  ~SharedState() {}

  port::Mutex* GetMutex() {
    return &mu_;
  }

  port::CondVar* GetCondVar() {
    return &cv_;
  }

  CacheBench* GetCacheBench() const {
    return cache_bench_;
  }

  void IncInitialized() {
    num_initialized_++;
  }

  void IncDone() {
    num_done_++;
  }

  bool AllInitialized() const { return num_initialized_ >= FLAGS_threads; }

  bool AllDone() const { return num_done_ >= FLAGS_threads; }

  void SetStart() {
    start_ = true;
  }

  bool Started() const {
    return start_;
  }

 private:
  port::Mutex mu_;
  port::CondVar cv_;

  uint64_t num_initialized_;
  bool start_;
  uint64_t num_done_;

  CacheBench* cache_bench_;
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  uint32_t tid;
  Random64 rnd;
  SharedState* shared;

  ThreadState(uint32_t index, SharedState* _shared)
      : tid(index), rnd(1000 + index), shared(_shared) {}
};

struct KeyGen {
  char key_data[27];

  Slice GetRand(Random64& rnd, uint64_t max_key) {
    uint64_t raw = rnd.Next();
    // Skew according to setting
    for (uint32_t i = 0; i < FLAGS_skew; ++i) {
      raw = std::min(raw, rnd.Next());
    }
    uint64_t key = FastRange64(raw, max_key);
    // Variable size and alignment
    size_t off = key % 8;
    key_data[0] = char{42};
    EncodeFixed64(key_data + 1, key);
    key_data[9] = char{11};
    EncodeFixed64(key_data + 10, key);
    key_data[18] = char{4};
    EncodeFixed64(key_data + 19, key);
    return Slice(&key_data[off], sizeof(key_data) - off);
  }
};

char* createValue(Random64& rnd) {
  char* rv = new char[FLAGS_value_bytes];
  // Fill with some filler data, and take some CPU time
  for (uint32_t i = 0; i < FLAGS_value_bytes; i += 8) {
    EncodeFixed64(rv + i, rnd.Next());
  }
  return rv;
}

void deleter(const Slice& /*key*/, void* value) {
  delete[] static_cast<char*>(value);
}
}  // namespace

class CacheBench {
  static constexpr uint64_t kHundredthUint64 =
      std::numeric_limits<uint64_t>::max() / 100U;

 public:
  CacheBench()
      : max_key_(static_cast<uint64_t>(FLAGS_cache_size / FLAGS_resident_ratio /
                                       FLAGS_value_bytes)),
        lookup_insert_threshold_(kHundredthUint64 *
                                 FLAGS_lookup_insert_percent),
        insert_threshold_(lookup_insert_threshold_ +
                          kHundredthUint64 * FLAGS_insert_percent),
        lookup_threshold_(insert_threshold_ +
                          kHundredthUint64 * FLAGS_lookup_percent),
        erase_threshold_(lookup_threshold_ +
                         kHundredthUint64 * FLAGS_erase_percent) {
    if (erase_threshold_ != 100U * kHundredthUint64) {
      fprintf(stderr, "Percentages must add to 100.\n");
      exit(1);
    }
    if (FLAGS_use_clock_cache) {
      cache_ = NewClockCache(FLAGS_cache_size, FLAGS_num_shard_bits);
      if (!cache_) {
        fprintf(stderr, "Clock cache not supported.\n");
        exit(1);
      }
    } else {
      cache_ = NewLRUCache(FLAGS_cache_size, FLAGS_num_shard_bits);
    }
    if (FLAGS_ops_per_thread == 0) {
      FLAGS_ops_per_thread = 5 * max_key_;
    }
  }

  ~CacheBench() {}

  void PopulateCache() {
    Random64 rnd(1);
    KeyGen keygen;
    for (uint64_t i = 0; i < 2 * FLAGS_cache_size; i += FLAGS_value_bytes) {
      cache_->Insert(keygen.GetRand(rnd, max_key_), createValue(rnd),
                     FLAGS_value_bytes, &deleter);
    }
  }

  bool Run() {
    ROCKSDB_NAMESPACE::Env* env = ROCKSDB_NAMESPACE::Env::Default();
    const auto& clock = env->GetSystemClock();

    PrintEnv();
    SharedState shared(this);
    std::vector<std::unique_ptr<ThreadState> > threads(FLAGS_threads);
    for (uint32_t i = 0; i < FLAGS_threads; i++) {
      threads[i].reset(new ThreadState(i, &shared));
      env->StartThread(ThreadBody, threads[i].get());
    }
    {
      MutexLock l(shared.GetMutex());
      while (!shared.AllInitialized()) {
        shared.GetCondVar()->Wait();
      }
      // Record start time
      uint64_t start_time = clock->NowMicros();

      // Start all threads
      shared.SetStart();
      shared.GetCondVar()->SignalAll();

      // Wait threads to complete
      while (!shared.AllDone()) {
        shared.GetCondVar()->Wait();
      }

      // Record end time
      uint64_t end_time = clock->NowMicros();
      double elapsed = static_cast<double>(end_time - start_time) * 1e-6;
      uint32_t qps = static_cast<uint32_t>(
          static_cast<double>(FLAGS_threads * FLAGS_ops_per_thread) / elapsed);
      fprintf(stdout, "Complete in %.3f s; QPS = %u\n", elapsed, qps);
    }
    return true;
  }

 private:
  std::shared_ptr<Cache> cache_;
  const uint64_t max_key_;
  // Cumulative thresholds in the space of a random uint64_t
  const uint64_t lookup_insert_threshold_;
  const uint64_t insert_threshold_;
  const uint64_t lookup_threshold_;
  const uint64_t erase_threshold_;

  static void ThreadBody(void* v) {
    ThreadState* thread = static_cast<ThreadState*>(v);
    SharedState* shared = thread->shared;

    {
      MutexLock l(shared->GetMutex());
      shared->IncInitialized();
      if (shared->AllInitialized()) {
        shared->GetCondVar()->SignalAll();
      }
      while (!shared->Started()) {
        shared->GetCondVar()->Wait();
      }
    }
    thread->shared->GetCacheBench()->OperateCache(thread);

    {
      MutexLock l(shared->GetMutex());
      shared->IncDone();
      if (shared->AllDone()) {
        shared->GetCondVar()->SignalAll();
      }
    }
  }

  void OperateCache(ThreadState* thread) {
    // To use looked-up values
    uint64_t result = 0;
    // To hold handles for a non-trivial amount of time
    Cache::Handle* handle = nullptr;
    KeyGen gen;
    for (uint64_t i = 0; i < FLAGS_ops_per_thread; i++) {
      Slice key = gen.GetRand(thread->rnd, max_key_);
      uint64_t random_op = thread->rnd.Next();
      if (random_op < lookup_insert_threshold_) {
        if (handle) {
          cache_->Release(handle);
          handle = nullptr;
        }
        // do lookup
        handle = cache_->Lookup(key);
        if (handle) {
          // do something with the data
          result += NPHash64(static_cast<char*>(cache_->Value(handle)),
                             FLAGS_value_bytes);
        } else {
          // do insert
          cache_->Insert(key, createValue(thread->rnd), FLAGS_value_bytes,
                         &deleter, &handle);
        }
      } else if (random_op < insert_threshold_) {
        if (handle) {
          cache_->Release(handle);
          handle = nullptr;
        }
        // do insert
        cache_->Insert(key, createValue(thread->rnd), FLAGS_value_bytes,
                       &deleter, &handle);
      } else if (random_op < lookup_threshold_) {
        if (handle) {
          cache_->Release(handle);
          handle = nullptr;
        }
        // do lookup
        handle = cache_->Lookup(key);
        if (handle) {
          // do something with the data
          result += NPHash64(static_cast<char*>(cache_->Value(handle)),
                             FLAGS_value_bytes);
        }
      } else if (random_op < erase_threshold_) {
        // do erase
        cache_->Erase(key);
      } else {
        // Should be extremely unlikely (noop)
        assert(random_op >= kHundredthUint64 * 100U);
      }
    }
    if (handle) {
      cache_->Release(handle);
      handle = nullptr;
    }
  }

  void PrintEnv() const {
    printf("RocksDB version     : %d.%d\n", kMajorVersion, kMinorVersion);
    printf("Number of threads   : %u\n", FLAGS_threads);
    printf("Ops per thread      : %" PRIu64 "\n", FLAGS_ops_per_thread);
    printf("Cache size          : %" PRIu64 "\n", FLAGS_cache_size);
    printf("Num shard bits      : %u\n", FLAGS_num_shard_bits);
    printf("Max key             : %" PRIu64 "\n", max_key_);
    printf("Resident ratio      : %g\n", FLAGS_resident_ratio);
    printf("Skew degree         : %u\n", FLAGS_skew);
    printf("Populate cache      : %d\n", int{FLAGS_populate_cache});
    printf("Lookup+Insert pct   : %u%%\n", FLAGS_lookup_insert_percent);
    printf("Insert percentage   : %u%%\n", FLAGS_insert_percent);
    printf("Lookup percentage   : %u%%\n", FLAGS_lookup_percent);
    printf("Erase percentage    : %u%%\n", FLAGS_erase_percent);
    printf("----------------------------\n");
  }
};
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_threads <= 0) {
    fprintf(stderr, "threads number <= 0\n");
    exit(1);
  }

  ROCKSDB_NAMESPACE::CacheBench bench;
  if (FLAGS_populate_cache) {
    bench.PopulateCache();
    printf("Population complete\n");
    printf("----------------------------\n");
  }
  if (bench.Run()) {
    return 0;
  } else {
    return 1;
  }
}

#endif  // GFLAGS
