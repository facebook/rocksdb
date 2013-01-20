// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "db/db_statistics.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "leveldb/statistics.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include "hdfs/env_hdfs.h"

static const long KB = 1024;

// Seed for PRNG
static uint32_t FLAGS_seed = 2341234;

// Max number of key/values to place in database
static long FLAGS_max_key = 2 * KB * KB * KB;

// Number of concurrent threads to run.
static int FLAGS_threads = 32;

// Size of each value will be this number times rand_int(1,3) bytes
static int FLAGS_value_size_mult = 8;

static bool FLAGS_verify_before_write = false;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

static bool FLAGS_verbose = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// The number of in-memory memtables.
// Each memtable is of size FLAGS_write_buffer_size.
// This is initialized to default value of 2 in "main" function.
static int FLAGS_max_write_buffer_number = 0;

// The maximum number of concurrent background compactions
// that can occur in parallel.
// This is initialized to default value of 1 in "main" function.
static int FLAGS_max_background_compactions = 0;

// Number of bytes to use as a cache of uncompressed data.
static long FLAGS_cache_size = 2 * KB * KB * KB;

// Number of bytes in a block.
static int FLAGS_block_size = 4 * KB;

// Number of times database reopens
static int FLAGS_reopen = 10;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = 10;

// Use the db with the following name.
static const char* FLAGS_db = NULL;

// Verify checksum for every block read from storage
static bool FLAGS_verify_checksum = false;

// Database statistics
static class leveldb::DBStatistics* dbstats;

// Sync all writes to disk
static bool FLAGS_sync = false;

// If true, do not wait until data is synced to disk.
static bool FLAGS_disable_data_sync = false;

// If true, issue fsync instead of fdatasync
static bool FLAGS_use_fsync = false;

// If true, do not write WAL for write.
static bool FLAGS_disable_wal = false;

// Target level-0 file size for compaction
static int FLAGS_target_file_size_base = 64 * KB;

// A multiplier to compute targe level-N file size
static int FLAGS_target_file_size_multiplier = 1;

// Max bytes for level-0
static uint64_t FLAGS_max_bytes_for_level_base = 256 * KB;

// A multiplier to compute max bytes for level-N
static int FLAGS_max_bytes_for_level_multiplier = 2;

// Number of files in level-0 that will trigger put stop.
static int FLAGS_level0_stop_writes_trigger = 12;

// Number of files in level-0 that will slow down writes.
static int FLAGS_level0_slowdown_writes_trigger = 8;

// Ratio of reads to total workload (expressed as a percentage)
static unsigned int FLAGS_readpercent = 10;

// Ratio of deletes to total workload (expressed as a percentage)
static unsigned int FLAGS_delpercent = 30;

// Option to disable compation triggered by read.
static int FLAGS_disable_seek_compaction = false;

// Option to delete obsolete files periodically
// Default: 0 which means that obsolete files are
// deleted after every compaction run.
 static uint64_t FLAGS_delete_obsolete_files_period_micros = 0;

// Algorithm to use to compress the database
static enum leveldb::CompressionType FLAGS_compression_type =
    leveldb::kSnappyCompression;

// posix or hdfs environment
static leveldb::Env* FLAGS_env = leveldb::Env::Default();

// Number of operations per thread.
static uint32_t FLAGS_ops_per_thread = 600000;

// Log2 of number of keys per lock
static uint32_t FLAGS_log2_keys_per_lock = 2; // implies 2^2 keys per lock

extern bool useOsBuffer;
extern bool useFsReadAhead;
extern bool useMmapRead;

namespace leveldb {

class StressTest;
namespace {

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  long done_;
  long writes_;
  long deletes_;
  int next_report_;
  size_t bytes_;
  double last_op_finish_;
  Histogram hist_;

 public:
  Stats() { }

  void Start() {
    next_report_ = 100;
    hist_.Clear();
    done_ = 0;
    writes_ = 0;
    deletes_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = FLAGS_env->NowMicros();
    last_op_finish_ = start_;
    finish_ = start_;
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    writes_ += other.writes_;
    deletes_ += other.deletes_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;
  }

  void Stop() {
    finish_ = FLAGS_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = FLAGS_env->NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
      if      (next_report_ < 1000)   next_report_ += 100;
      else if (next_report_ < 5000)   next_report_ += 500;
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      fprintf(stderr, "... finished %ld ops%30s\r", done_, "");
      fflush(stderr);
    }
  }

  void AddBytesForOneWrite(size_t n) {
    writes_ ++;
    bytes_ += n;
  }

  void AddOneDelete() {
    deletes_ ++;
  }

  void Report(const char* name) {
    std::string extra;
    if (bytes_ < 1 || done_ < 1) {
      fprintf(stderr, "No writes or ops?\n");
      return;
    }

    double elapsed = (finish_ - start_) * 1e-6;
    double bytes_mb = bytes_ / 1048576.0;
    double rate = bytes_mb / elapsed;
    double throughput = (double)done_/elapsed;

    fprintf(stdout, "%-12s: ", name);
    fprintf(stdout, "%.3f micros/op %ld ops/sec\n",
            seconds_ * 1e6 / done_, (long)throughput);
    fprintf(stdout, "%-12s: Wrote %.2f MB (%.2f MB/sec) (%ld%% of %ld ops)\n",
            "", bytes_mb, rate, (100*writes_)/done_, done_);
    fprintf(stdout, "%-12s: Deleted %ld times\n", "", deletes_);

    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
class SharedState {
 public:
  static const uint32_t SENTINEL = 0xffffffff;

  SharedState(StressTest* stress_test) :
      cv_(&mu_),
      seed_(FLAGS_seed),
      max_key_(FLAGS_max_key),
      log2_keys_per_lock_(FLAGS_log2_keys_per_lock),
      num_threads_(FLAGS_threads),
      num_initialized_(0),
      num_populated_(0),
      vote_reopen_(0),
      num_done_(0),
      start_(false),
      start_verify_(false),
      stress_test_(stress_test) {
    values_ = new uint32_t[max_key_];
    for (long i = 0; i < max_key_; i++) {
      values_[i] = SENTINEL;
    }
    long num_locks = (max_key_ >> log2_keys_per_lock_);
    if (max_key_ & ((1 << log2_keys_per_lock_) - 1)) {
      num_locks ++;
    }
    fprintf(stdout, "Creating %ld locks\n", num_locks);
    key_locks_ = new port::Mutex[num_locks];
  }

  ~SharedState() {
    delete[] values_;
    delete[] key_locks_;
  }

  port::Mutex* GetMutex() {
    return &mu_;
  }

  port::CondVar* GetCondVar() {
    return &cv_;
  }

  StressTest* GetStressTest() const {
    return stress_test_;
  }

  long GetMaxKey() const {
    return max_key_;
  }

  uint32_t GetNumThreads() const {
    return num_threads_;
  }

  void IncInitialized() {
    num_initialized_++;
  }

  void IncOperated() {
    num_populated_++;
  }

  void IncDone() {
    num_done_++;
  }

  void IncVotedReopen() {
    vote_reopen_ = (vote_reopen_ + 1) % num_threads_;
  }

  bool AllInitialized() const {
    return num_initialized_ >= num_threads_;
  }

  bool AllOperated() const {
    return num_populated_ >= num_threads_;
  }

  bool AllDone() const {
    return num_done_ >= num_threads_;
  }

  bool AllVotedReopen() {
    return (vote_reopen_ == 0);
  }

  void SetStart() {
    start_ = true;
  }

  void SetStartVerify() {
    start_verify_ = true;
  }

  bool Started() const {
    return start_;
  }

  bool VerifyStarted() const {
    return start_verify_;
  }

  port::Mutex* GetMutexForKey(long key) {
    return &key_locks_[key >> log2_keys_per_lock_];
  }

  void Put(long key, uint32_t value_base) {
    values_[key] = value_base;
  }

  uint32_t Get(long key) const {
    return values_[key];
  }

  void Delete(long key) const {
    values_[key] = SENTINEL;
  }

  uint32_t GetSeed() const {
    return seed_;
  }

 private:
  port::Mutex mu_;
  port::CondVar cv_;
  const uint32_t seed_;
  const long max_key_;
  const uint32_t log2_keys_per_lock_;
  const int num_threads_;
  long num_initialized_;
  long num_populated_;
  long vote_reopen_;
  long num_done_;
  bool start_;
  bool start_verify_;
  StressTest* stress_test_;

  uint32_t *values_;
  port::Mutex *key_locks_;

};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  uint32_t tid; // 0..n-1
  Random rand;  // Has different seeds for different threads
  SharedState* shared;
  Stats stats;

  ThreadState(uint32_t index, SharedState *shared)
      : tid(index),
        rand(1000 + index + shared->GetSeed()),
        shared(shared) {
  }
};

}  // namespace

class StressTest {
 public:
  StressTest()
      : cache_(NewLRUCache(FLAGS_cache_size)),
        filter_policy_(FLAGS_bloom_bits >= 0
                       ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                       : NULL),
        db_(NULL),
        num_times_reopened_(0) {
    std::vector<std::string> files;
    FLAGS_env->GetChildren(FLAGS_db, &files);
    for (unsigned int i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        FLAGS_env->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
      }
    }
    DestroyDB(FLAGS_db, Options());
  }

  ~StressTest() {
    delete db_;
    delete filter_policy_;
  }

  void Run() {
    PrintEnv();
    Open();
    SharedState shared(this);
    uint32_t n = shared.GetNumThreads();

    std::vector<ThreadState*> threads(n);
    for (uint32_t i = 0; i < n; i++) {
      threads[i] = new ThreadState(i, &shared);
      FLAGS_env->StartThread(ThreadBody, threads[i]);
    }
    // Each thread goes through the following states:
    // initializing -> wait for others to init -> read/populate/depopulate
    // wait for others to operate -> verify -> done

    {
      MutexLock l(shared.GetMutex());
      while (!shared.AllInitialized()) {
        shared.GetCondVar()->Wait();
      }

      double now = FLAGS_env->NowMicros();
      fprintf(stdout, "%s Starting database operations\n",
              FLAGS_env->TimeToString((uint64_t) now/1000000).c_str());

      shared.SetStart();
      shared.GetCondVar()->SignalAll();
      while (!shared.AllOperated()) {
        shared.GetCondVar()->Wait();
      }

      now = FLAGS_env->NowMicros();
      fprintf(stdout, "%s Starting verification\n",
              FLAGS_env->TimeToString((uint64_t) now/1000000).c_str());

      shared.SetStartVerify();
      shared.GetCondVar()->SignalAll();
      while (!shared.AllDone()) {
        shared.GetCondVar()->Wait();
      }
    }

    for (unsigned int i = 1; i < n; i++) {
      threads[0]->stats.Merge(threads[i]->stats);
    }
    threads[0]->stats.Report("Stress Test");

    for (unsigned int i = 0; i < n; i++) {
      delete threads[i];
      threads[i] = NULL;
    }
    double now = FLAGS_env->NowMicros();
    fprintf(stdout, "%s Verification successful\n",
            FLAGS_env->TimeToString((uint64_t) now/1000000).c_str());

    PrintStatistics();
  }

 private:

  static void ThreadBody(void* v) {
    ThreadState* thread = reinterpret_cast<ThreadState*>(v);
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
    thread->shared->GetStressTest()->OperateDb(thread);

    {
      MutexLock l(shared->GetMutex());
      shared->IncOperated();
      if (shared->AllOperated()) {
        shared->GetCondVar()->SignalAll();
      }
      while (!shared->VerifyStarted()) {
        shared->GetCondVar()->Wait();
      }
    }

    thread->shared->GetStressTest()->VerifyDb(*(thread->shared), thread->tid);

    {
      MutexLock l(shared->GetMutex());
      shared->IncDone();
      if (shared->AllDone()) {
        shared->GetCondVar()->SignalAll();
      }
    }

  }

  void OperateDb(ThreadState* thread) {
    ReadOptions read_opts(FLAGS_verify_checksum, true);
    WriteOptions write_opts;
    char value[100];
    long max_key = thread->shared->GetMaxKey();
    std::string from_db;
    if (FLAGS_sync) {
      write_opts.sync = true;
    }
    write_opts.disableWAL = FLAGS_disable_wal;

    thread->stats.Start();
    for (long i = 0; i < FLAGS_ops_per_thread; i++) {
      if(i != 0 && (i % (FLAGS_ops_per_thread / (FLAGS_reopen + 1))) == 0) {
        {
          thread->stats.FinishedSingleOp();
          MutexLock l(thread->shared->GetMutex());
          thread->shared->IncVotedReopen();
          if (thread->shared->AllVotedReopen()) {
            thread->shared->GetStressTest()->Reopen();
            thread->shared->GetCondVar()->SignalAll();
          }
          else {
            thread->shared->GetCondVar()->Wait();
          }
          thread->stats.Start();
        }
      }
      long rand_key = thread->rand.Next() % max_key;
      Slice key((char*)&rand_key, sizeof(rand_key));
      //Read:10%;Delete:30%;Write:60%
      unsigned int probability_operation = thread->rand.Uniform(100);
      if (probability_operation < FLAGS_readpercent) {
        // read load
        db_->Get(read_opts, key, &from_db);
      } else if (probability_operation < FLAGS_delpercent + FLAGS_readpercent) {
        //introduce delete load
        {
          MutexLock l(thread->shared->GetMutexForKey(rand_key));
          thread->shared->Delete(rand_key);
          db_->Delete(write_opts, key);
        }
        thread->stats.AddOneDelete();
      } else {
        // write load
        uint32_t value_base = thread->rand.Next();
        size_t sz = GenerateValue(value_base, value, sizeof(value));
        Slice v(value, sz);
        {
          MutexLock l(thread->shared->GetMutexForKey(rand_key));
          if (FLAGS_verify_before_write) {
            VerifyValue(rand_key, read_opts, *(thread->shared), &from_db, true);
          }
          thread->shared->Put(rand_key, value_base);
          db_->Put(write_opts, key, v);
        }
        PrintKeyValue(rand_key, value, sz);
        thread->stats.AddBytesForOneWrite(sz);
      }
      thread->stats.FinishedSingleOp();
    }
    thread->stats.Stop();
  }

  void VerifyDb(const SharedState &shared, long start) const {
    ReadOptions options(FLAGS_verify_checksum, true);
    long max_key = shared.GetMaxKey();
    long step = shared.GetNumThreads();
    for (long i = start; i < max_key; i+= step) {
      std::string from_db;
      VerifyValue(i, options, shared, &from_db, true);
      if (from_db.length()) {
        PrintKeyValue(i, from_db.data(), from_db.length());
      }
    }
  }

  void VerificationAbort(std::string msg, long key) const {
    fprintf(stderr, "Verification failed for key %ld: %s\n",
            key, msg.c_str());
    exit(1);
  }

  void VerifyValue(long key, const ReadOptions &opts, const SharedState &shared,
                   std::string *value_from_db, bool strict=false) const {
    Slice k((char*)&key, sizeof(key));
    char value[100];
    size_t value_sz = 0;
    uint32_t value_base = shared.Get(key);
    if (value_base == SharedState::SENTINEL && !strict) {
      return;
    }

    if (db_->Get(opts, k, value_from_db).ok()) {
      if (value_base == SharedState::SENTINEL) {
        VerificationAbort("Unexpected value found", key);
      }
      size_t sz = GenerateValue(value_base, value, value_sz);
      if (value_from_db->length() != sz) {
        VerificationAbort("Length of value read is not equal", key);
      }
      if (memcmp(value_from_db->data(), value, sz) != 0) {
        VerificationAbort("Contents of value read don't match", key);
      }
    } else {
      if (value_base != SharedState::SENTINEL) {
        VerificationAbort("Value not found", key);
      }
    }
  }

  static void PrintKeyValue(uint32_t key, const char *value, size_t sz) {
    if (!FLAGS_verbose) return;
    fprintf(stdout, "%u ==> (%u) ", key, (unsigned int)sz);
    for (size_t i=0; i<sz; i++) {
      fprintf(stdout, "%X", value[i]);
    }
    fprintf(stdout, "\n");
  }

  static size_t GenerateValue(uint32_t rand, char *v, size_t max_sz) {
    size_t value_sz = ((rand % 3) + 1) * FLAGS_value_size_mult;
    assert(value_sz <= max_sz && value_sz >= sizeof(uint32_t));
    *((uint32_t*)v) = rand;
    for (size_t i=sizeof(uint32_t); i < value_sz; i++) {
      v[i] = (char)(rand ^ i);
    }
    return value_sz; // the size of the value set.
  }

  void PrintEnv() const {
    fprintf(stdout, "LevelDB version     : %d.%d\n",
            kMajorVersion, kMinorVersion);
    fprintf(stdout, "Number of threads   : %d\n", FLAGS_threads);
    fprintf(stdout, "Ops per thread      : %d\n", FLAGS_ops_per_thread);
    fprintf(stdout, "Read percentage     : %d\n", FLAGS_readpercent);
    fprintf(stdout, "Delete percentage   : %d\n", FLAGS_delpercent);
    fprintf(stdout, "Max key             : %ld\n", FLAGS_max_key);
    fprintf(stdout, "Num times DB reopens: %d\n", FLAGS_reopen);
    fprintf(stdout, "Num keys per lock   : %d\n",
            1 << FLAGS_log2_keys_per_lock);

    const char* compression = "";
    switch (FLAGS_compression_type) {
      case leveldb::kNoCompression:
        compression = "none";
        break;
      case leveldb::kSnappyCompression:
        compression = "snappy";
        break;
      case leveldb::kZlibCompression:
        compression = "zlib";
        break;
      case leveldb::kBZip2Compression:
        compression = "bzip2";
        break;
    }

    fprintf(stdout, "Compression         : %s\n", compression);
    fprintf(stdout, "------------------------------------------------\n");
  }

  void Open() {
    assert(db_ == NULL);
    Options options;
    options.block_cache = cache_;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_write_buffer_number = FLAGS_max_write_buffer_number;
    options.max_background_compactions = FLAGS_max_background_compactions;
    options.block_size = FLAGS_block_size;
    options.filter_policy = filter_policy_;
    options.max_open_files = FLAGS_open_files;
    options.statistics = dbstats;
    options.env = FLAGS_env;
    options.disableDataSync = FLAGS_disable_data_sync;
    options.use_fsync = FLAGS_use_fsync;
    options.target_file_size_base = FLAGS_target_file_size_base;
    options.target_file_size_multiplier = FLAGS_target_file_size_multiplier;
    options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
    options.max_bytes_for_level_multiplier =
        FLAGS_max_bytes_for_level_multiplier;
    options.level0_stop_writes_trigger = FLAGS_level0_stop_writes_trigger;
    options.level0_slowdown_writes_trigger =
      FLAGS_level0_slowdown_writes_trigger;
    options.compression = FLAGS_compression_type;
    options.create_if_missing = true;
    options.disable_seek_compaction = FLAGS_disable_seek_compaction;
    options.delete_obsolete_files_period_micros =
      FLAGS_delete_obsolete_files_period_micros;
    options.max_manifest_file_size = 1024;
    Status s = DB::Open(options, FLAGS_db, &db_);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  void Reopen() {
    // do not close the db. Just delete the lock file. This
    // simulates a crash-recovery kind of situation.
    ((DBImpl*) db_)->TEST_Destroy_DBImpl();
    db_ = NULL;

    num_times_reopened_++;
    double now = FLAGS_env->NowMicros();
    fprintf(stdout, "%s Reopening database for the %dth time\n",
            FLAGS_env->TimeToString((uint64_t) now/1000000).c_str(),
            num_times_reopened_);
    Open();
  }

  void PrintStatistics() {
    if (dbstats) {
      fprintf(stdout, "File opened:%ld closed:%ld errors:%ld\n",
              dbstats->getNumFileOpens(),
              dbstats->getNumFileCloses(),
              dbstats->getNumFileErrors());
    }
  }

 private:
  shared_ptr<Cache> cache_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  int num_times_reopened_;
};

}  // namespace leveldb

int main(int argc, char** argv) {
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_max_write_buffer_number = leveldb::Options().max_write_buffer_number;
  FLAGS_open_files = leveldb::Options().max_open_files;
  FLAGS_max_background_compactions =
    leveldb::Options().max_background_compactions;
  // Compression test code above refers to FLAGS_block_size
  FLAGS_block_size = leveldb::Options().block_size;
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    int n;
    uint32_t u;
    long l;
    char junk;
    char hdfsname[2048];

    if (sscanf(argv[i], "--seed=%uf%c", &u, &junk) == 1) {
      FLAGS_seed = u;
    } else if (sscanf(argv[i], "--max_key=%ld%c", &l, &junk) == 1) {
      FLAGS_max_key = l;
    } else if (sscanf(argv[i], "--log2_keys_per_lock=%u%c", &u, &junk) == 1) {
      FLAGS_log2_keys_per_lock = u;
    } else if (sscanf(argv[i], "--ops_per_thread=%u%c", &u, &junk) == 1) {
      FLAGS_ops_per_thread = u;
    } else if (sscanf(argv[i], "--verbose=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_verbose = n;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--verify_before_write=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_verify_before_write = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--value_size_mult=%d%c", &n, &junk) == 1) {
      FLAGS_value_size_mult = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--max_write_buffer_number=%d%c", &n, &junk) == 1) {
      FLAGS_max_write_buffer_number = n;
    } else if (sscanf(argv[i], "--max_background_compactions=%d%c", &n, &junk) == 1) {
      FLAGS_max_background_compactions = n;
    } else if (sscanf(argv[i], "--cache_size=%ld%c", &l, &junk) == 1) {
      FLAGS_cache_size = l;
    } else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
      FLAGS_block_size = n;
    } else if (sscanf(argv[i], "--reopen=%d%c", &n, &junk) == 1 && n >= 0) {
      FLAGS_reopen = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    } else if (sscanf(argv[i], "--verify_checksum=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_verify_checksum = n;
    } else if (sscanf(argv[i], "--bufferedio=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      useOsBuffer = n;
    } else if (sscanf(argv[i], "--mmap_read=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      useMmapRead = n;
    } else if (sscanf(argv[i], "--readhead=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      useFsReadAhead = n;
    } else if (sscanf(argv[i], "--statistics=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      if (n == 1) {
        dbstats =  new leveldb::DBStatistics();
      }
    } else if (sscanf(argv[i], "--sync=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_sync = n;
    } else if (sscanf(argv[i], "--readpercent=%d%c", &n, &junk) == 1 &&
               (n >= 0 && n <= 100)) {
      FLAGS_readpercent = n;
    } else if (sscanf(argv[i], "--delpercent=%d%c", &n, &junk) == 1 &&
               (n >= 0 && n <= 100)) {
      FLAGS_delpercent = n;
    } else if (sscanf(argv[i], "--disable_data_sync=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_disable_data_sync = n;
    } else if (sscanf(argv[i], "--use_fsync=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_use_fsync = n;
    } else if (sscanf(argv[i], "--disable_wal=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_disable_wal = n;
    } else if (sscanf(argv[i], "--hdfs=%s", hdfsname) == 1) {
      FLAGS_env  = new leveldb::HdfsEnv(hdfsname);
    } else if (sscanf(argv[i], "--target_file_size_base=%d%c",
        &n, &junk) == 1) {
      FLAGS_target_file_size_base = n;
    } else if ( sscanf(argv[i], "--target_file_size_multiplier=%d%c",
        &n, &junk) == 1) {
      FLAGS_target_file_size_multiplier = n;
    } else if (
        sscanf(argv[i], "--max_bytes_for_level_base=%ld%c", &l, &junk) == 1) {
      FLAGS_max_bytes_for_level_base = l;
    } else if (sscanf(argv[i], "--max_bytes_for_level_multiplier=%d%c",
        &n, &junk) == 1) {
      FLAGS_max_bytes_for_level_multiplier = n;
    } else if (sscanf(argv[i],"--level0_stop_writes_trigger=%d%c",
        &n, &junk) == 1) {
      FLAGS_level0_stop_writes_trigger = n;
    } else if (sscanf(argv[i],"--level0_slowdown_writes_trigger=%d%c",
        &n, &junk) == 1) {
      FLAGS_level0_slowdown_writes_trigger = n;
    } else if (strncmp(argv[i], "--compression_type=", 19) == 0) {
      const char* ctype = argv[i] + 19;
      if (!strcasecmp(ctype, "none"))
        FLAGS_compression_type = leveldb::kNoCompression;
      else if (!strcasecmp(ctype, "snappy"))
        FLAGS_compression_type = leveldb::kSnappyCompression;
      else if (!strcasecmp(ctype, "zlib"))
        FLAGS_compression_type = leveldb::kZlibCompression;
      else if (!strcasecmp(ctype, "bzip2"))
        FLAGS_compression_type = leveldb::kBZip2Compression;
      else {
        fprintf(stdout, "Cannot parse %s\n", argv[i]);
      }
    } else if (sscanf(argv[i], "--disable_seek_compaction=%d%c", &n, &junk) == 1
        && (n == 0 || n == 1)) {
      FLAGS_disable_seek_compaction = n;
    } else if (sscanf(argv[i], "--delete_obsolete_files_period_micros=%ld%c",
                      &l, &junk) == 1) {
      FLAGS_delete_obsolete_files_period_micros = n;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  // The number of background threads should be at least as much the
  // max number of concurrent compactions.
  FLAGS_env->SetBackgroundThreads(FLAGS_max_background_compactions);

  if ((FLAGS_readpercent + FLAGS_delpercent) > 100) {
      fprintf(stderr, "Error: Read + Delete percents > 100!\n");
      exit(1);
  }

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      leveldb::Env::Default()->GetTestDirectory(&default_db_path);
      default_db_path += "/dbstress";
      FLAGS_db = default_db_path.c_str();
  }

  leveldb::StressTest stress;
  stress.Run();
  if (dbstats) {
    delete dbstats;
  }
  return 0;
}
