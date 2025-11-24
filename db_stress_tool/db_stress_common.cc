//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"

#include <cmath>

#include "file/file_util.h"
#include "rocksdb/secondary_cache.h"
#include "util/file_checksum_helper.h"
#include "util/xxhash.h"

ROCKSDB_NAMESPACE::Env* db_stress_listener_env = nullptr;
ROCKSDB_NAMESPACE::Env* db_stress_env = nullptr;
std::shared_ptr<ROCKSDB_NAMESPACE::FaultInjectionTestFS> fault_fs_guard;
std::shared_ptr<ROCKSDB_NAMESPACE::SecondaryCache> compressed_secondary_cache;
std::shared_ptr<ROCKSDB_NAMESPACE::Cache> block_cache;
enum ROCKSDB_NAMESPACE::CompressionType compression_type_e =
    ROCKSDB_NAMESPACE::kSnappyCompression;
enum ROCKSDB_NAMESPACE::CompressionType bottommost_compression_type_e =
    ROCKSDB_NAMESPACE::kSnappyCompression;
enum ROCKSDB_NAMESPACE::ChecksumType checksum_type_e =
    ROCKSDB_NAMESPACE::kCRC32c;
enum RepFactory FLAGS_rep_factory = kSkipList;
std::vector<double> sum_probs(100001);
constexpr int64_t zipf_sum_size = 100000;

namespace ROCKSDB_NAMESPACE {

// Zipfian distribution is generated based on a pre-calculated array.
// It should be used before start the stress test.
// First, the probability distribution function (PDF) of this Zipfian follows
// power low. P(x) = 1/(x^alpha).
// So we calculate the PDF when x is from 0 to zipf_sum_size in first for loop
// and add the PDF value togetger as c. So we get the total probability in c.
// Next, we calculate inverse CDF of Zipfian and store the value of each in
// an array (sum_probs). The rank is from 0 to zipf_sum_size. For example, for
// integer k, its Zipfian CDF value is sum_probs[k].
// Third, when we need to get an integer whose probability follows Zipfian
// distribution, we use a rand_seed [0,1] which follows uniform distribution
// as a seed and search it in the sum_probs via binary search. When we find
// the closest sum_probs[i] of rand_seed, i is the integer that in
// [0, zipf_sum_size] following Zipfian distribution with parameter alpha.
// Finally, we can scale i to [0, max_key] scale.
// In order to avoid that hot keys are close to each other and skew towards 0,
// we use Rando64 to shuffle it.
void InitializeHotKeyGenerator(double alpha) {
  double c = 0;
  for (int64_t i = 1; i <= zipf_sum_size; i++) {
    c = c + (1.0 / std::pow(static_cast<double>(i), alpha));
  }
  c = 1.0 / c;

  sum_probs[0] = 0;
  for (int64_t i = 1; i <= zipf_sum_size; i++) {
    sum_probs[i] =
        sum_probs[i - 1] + c / std::pow(static_cast<double>(i), alpha);
  }
}

// Generate one key that follows the Zipfian distribution. The skewness
// is decided by the parameter alpha. Input is the rand_seed [0,1] and
// the max of the key to be generated. If we directly return tmp_zipf_seed,
// the closer to 0, the higher probability will be. To randomly distribute
// the hot keys in [0, max_key], we use Random64 to shuffle it.
int64_t GetOneHotKeyID(double rand_seed, int64_t max_key) {
  int64_t low = 1, mid, high = zipf_sum_size, zipf = 0;
  while (low <= high) {
    mid = (low + high) / 2;
    if (sum_probs[mid] >= rand_seed && sum_probs[mid - 1] < rand_seed) {
      zipf = mid;
      break;
    } else if (sum_probs[mid] >= rand_seed) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  int64_t tmp_zipf_seed = zipf * max_key / zipf_sum_size;
  Random64 rand_local(tmp_zipf_seed);
  return rand_local.Next() % max_key;
}

void PoolSizeChangeThread(void* v) {
  assert(FLAGS_compaction_thread_pool_adjust_interval > 0);
  ThreadState* thread = static_cast<ThreadState*>(v);
  SharedState* shared = thread->shared;

  while (true) {
    {
      MutexLock l(shared->GetMutex());
      if (shared->ShouldStopBgThread()) {
        shared->IncBgThreadsFinished();
        if (shared->BgThreadsFinished()) {
          shared->GetCondVar()->SignalAll();
        }
        return;
      }
    }

    auto thread_pool_size_base = FLAGS_max_background_compactions;
    auto thread_pool_size_var = FLAGS_compaction_thread_pool_variations;
    int new_thread_pool_size =
        thread_pool_size_base - thread_pool_size_var +
        thread->rand.Next() % (thread_pool_size_var * 2 + 1);
    if (new_thread_pool_size < 1) {
      new_thread_pool_size = 1;
    }
    db_stress_env->SetBackgroundThreads(new_thread_pool_size,
                                        ROCKSDB_NAMESPACE::Env::Priority::LOW);
    // Sleep up to 3 seconds
    db_stress_env->SleepForMicroseconds(
        thread->rand.Next() % FLAGS_compaction_thread_pool_adjust_interval *
            1000 +
        1);
  }
}

void DbVerificationThread(void* v) {
  assert(FLAGS_continuous_verification_interval > 0);
  auto* thread = static_cast<ThreadState*>(v);
  SharedState* shared = thread->shared;
  StressTest* stress_test = shared->GetStressTest();
  assert(stress_test != nullptr);
  while (true) {
    {
      MutexLock l(shared->GetMutex());
      if (shared->ShouldStopBgThread()) {
        shared->IncBgThreadsFinished();
        if (shared->BgThreadsFinished()) {
          shared->GetCondVar()->SignalAll();
        }
        return;
      }
    }
    if (!shared->HasVerificationFailedYet()) {
      stress_test->ContinuouslyVerifyDb(thread);
    }
    db_stress_env->SleepForMicroseconds(
        thread->rand.Next() % FLAGS_continuous_verification_interval * 1000 +
        1);
  }
}

void CompressedCacheSetCapacityThread(void* v) {
  assert(FLAGS_compressed_secondary_cache_size > 0 ||
         FLAGS_compressed_secondary_cache_ratio > 0.0);
  auto* thread = static_cast<ThreadState*>(v);
  SharedState* shared = thread->shared;
  while (true) {
    {
      MutexLock l(shared->GetMutex());
      if (shared->ShouldStopBgThread()) {
        shared->IncBgThreadsFinished();
        if (shared->BgThreadsFinished()) {
          shared->GetCondVar()->SignalAll();
        }
        return;
      }
    }
    db_stress_env->SleepForMicroseconds(FLAGS_secondary_cache_update_interval);
    if (FLAGS_compressed_secondary_cache_size > 0) {
      Status s = compressed_secondary_cache->SetCapacity(0);
      size_t capacity;
      if (s.ok()) {
        s = compressed_secondary_cache->GetCapacity(capacity);
        assert(capacity == 0);
      }
      db_stress_env->SleepForMicroseconds(10 * 1000 * 1000);
      if (s.ok()) {
        s = compressed_secondary_cache->SetCapacity(
            FLAGS_compressed_secondary_cache_size);
      }
      if (s.ok()) {
        s = compressed_secondary_cache->GetCapacity(capacity);
        assert(capacity == FLAGS_compressed_secondary_cache_size);
      }
      if (!s.ok()) {
        fprintf(stderr, "Compressed cache Set/GetCapacity returned error: %s\n",
                s.ToString().c_str());
      }
    } else if (FLAGS_compressed_secondary_cache_ratio > 0.0) {
      if (thread->rand.OneIn(2)) {  // if (thread->rand.OneIn(2)) {
        size_t capacity = block_cache->GetCapacity();
        size_t adjustment;
        if (FLAGS_use_write_buffer_manager && FLAGS_db_write_buffer_size > 0) {
          adjustment = (capacity - FLAGS_db_write_buffer_size);
        } else {
          adjustment = capacity;
        }
        // Lower by upto 50% of usable block cache capacity
        adjustment = (adjustment * thread->rand.Uniform(50)) / 100;
        block_cache->SetCapacity(capacity - adjustment);
        fprintf(stdout, "New cache capacity = %zu\n",
                block_cache->GetCapacity());
        db_stress_env->SleepForMicroseconds(10 * 1000 * 1000);
        block_cache->SetCapacity(capacity);
      } else {
        Status s;
        double new_comp_cache_ratio =
            (double)thread->rand.Uniform(
                FLAGS_compressed_secondary_cache_ratio * 100) /
            100;
        fprintf(stdout, "New comp cache ratio = %f\n", new_comp_cache_ratio);

        s = UpdateTieredCache(block_cache, /*capacity*/ -1,
                              new_comp_cache_ratio);
        if (s.ok()) {
          db_stress_env->SleepForMicroseconds(10 * 1000 * 1000);
        }
        if (s.ok()) {
          s = UpdateTieredCache(block_cache, /*capacity*/ -1,
                                FLAGS_compressed_secondary_cache_ratio);
        }
        if (!s.ok()) {
          fprintf(stderr, "UpdateTieredCache returned error: %s\n",
                  s.ToString().c_str());
        }
      }
    }
  }
}

#ifndef NDEBUG
static void SetupFaultInjectionForRemoteCompaction(SharedState* shared) {
  if (!fault_fs_guard) {
    return;
  }

  fault_fs_guard->SetThreadLocalErrorContext(
      FaultInjectionIOType::kRead, shared->GetSeed(), FLAGS_read_fault_one_in,
      FLAGS_inject_error_severity == 1 /* retryable */,
      FLAGS_inject_error_severity == 2 /* has_data_loss*/);
  fault_fs_guard->EnableThreadLocalErrorInjection(FaultInjectionIOType::kRead);

  fault_fs_guard->SetThreadLocalErrorContext(
      FaultInjectionIOType::kWrite, shared->GetSeed(), FLAGS_write_fault_one_in,
      FLAGS_inject_error_severity == 1 /* retryable */,
      FLAGS_inject_error_severity == 2 /* has_data_loss*/);
  fault_fs_guard->EnableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);

  fault_fs_guard->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataRead, shared->GetSeed(),
      FLAGS_metadata_read_fault_one_in,
      FLAGS_inject_error_severity == 1 /* retryable */,
      FLAGS_inject_error_severity == 2 /* has_data_loss*/);
  fault_fs_guard->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataRead);

  fault_fs_guard->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataWrite, shared->GetSeed(),
      FLAGS_metadata_write_fault_one_in,
      FLAGS_inject_error_severity == 1 /* retryable */,
      FLAGS_inject_error_severity == 2 /* has_data_loss*/);
  fault_fs_guard->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataWrite);
}
#endif  // NDEBUG

static CompactionServiceOptionsOverride CreateOverrideOptions(
    const Options& options, const CompactionServiceJobInfo& job_info) {
  CompactionServiceOptionsOverride override_options{
      .env = db_stress_env,
      .file_checksum_gen_factory = options.file_checksum_gen_factory,
      .merge_operator = options.merge_operator,
      .compaction_filter = options.compaction_filter,
      .compaction_filter_factory = options.compaction_filter_factory,
      .prefix_extractor = options.prefix_extractor,
      .sst_partitioner_factory = options.sst_partitioner_factory,
      .listeners = options.listeners,
      .statistics = options.statistics,
      .table_properties_collector_factories =
          options.table_properties_collector_factories};

  // TODO(jaykorean) - create a new compaction filter / merge operator and
  // others for remote compactions
  //
  // Create a new Table Factory
  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.ignore_unsupported_options = false;

  Status s = TableFactory::CreateFromString(config_options,
                                            options.table_factory->Name(),
                                            &override_options.table_factory);

  if (s.ok()) {
    std::string options_str;
    s = options.table_factory->GetOptionString(config_options, &options_str);
    if (s.ok()) {
      s = override_options.table_factory->ConfigureFromString(config_options,
                                                              options_str);
    }
  }

  if (!s.ok()) {
    fprintf(stdout,
            "Failed to set up TableFactory for remote compaction - (%s): %s\n",
            job_info.db_name.c_str(), s.ToString().c_str());
  }

  return override_options;
}

static Status CleanupOutputDirectory(const std::string& output_directory) {
#ifndef NDEBUG
  // Temporarily disable fault injection to ensure deletion always succeeds
  if (fault_fs_guard) {
    fault_fs_guard->DisableAllThreadLocalErrorInjection();
  }
#endif  // NDEBUG

  Status s = DestroyDir(db_stress_env, output_directory);
  if (!s.ok()) {
    fprintf(stderr,
            "Failed to destroy output directory %s when allow_resumption is "
            "false: %s\n",
            output_directory.c_str(), s.ToString().c_str());
  }

  if (s.ok()) {
    s = db_stress_env->CreateDir(output_directory);
    if (!s.ok()) {
      fprintf(stderr,
              "Failed to recreate output directory %s when allow_resumption is "
              "false: %s\n",
              output_directory.c_str(), s.ToString().c_str());
    }
  }

#ifndef NDEBUG
  // Re-enable fault injection after deletion
  if (fault_fs_guard) {
    fault_fs_guard->EnableAllThreadLocalErrorInjection();
  }
#endif  // NDEBUG

  return s;
}

// Set up cancellation mechanism for testing resumable remote compactions.
// Spawns a detached thread to trigger cancellation after a delay (50ms
// initially, or 2/3 of the previous successful compaction time for adaptive
// timing). First-time jobs are always canceled; retries have a 10% chance
// to test consecutive cancellation scenarios.
static std::shared_ptr<std::atomic<bool>> SetupCancellation(
    OpenAndCompactOptions& open_compact_options, bool was_canceled,
    Random& rand, uint64_t successful_compaction_end_to_end_micros) {
  auto canceled = std::make_shared<std::atomic<bool>>(false);
  open_compact_options.canceled = canceled.get();

  bool should_cancel = !was_canceled || rand.OneIn(10);

  if (should_cancel) {
    std::thread interruption_thread(
        [canceled, successful_compaction_end_to_end_micros]() {
          uint64_t sleep_micros =
              successful_compaction_end_to_end_micros == 0
                  ? 50000
                  : successful_compaction_end_to_end_micros * 2 / 3;
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_micros));
          canceled->store(true);
        });
    interruption_thread.detach();
  }

  return canceled;
}

// Process the result of OpenAndCompact operation
static void ProcessCompactionResult(
    const Status& s, const std::string& job_id,
    const CompactionServiceJobInfo& job_info,
    const std::string& serialized_input, const std::string& output_directory,
    const std::string& serialized_output, SharedState* shared,
    uint64_t& successful_compaction_end_to_end_micros, uint64_t start_micros,
    Env* env) {
  if (s.IsManualCompactionPaused() && FLAGS_allow_resumption_one_in > 0) {
    // Re-enqueue for retry
    shared->EnqueueRemoteCompaction(job_id, job_info, serialized_input,
                                    output_directory, true /* was_cancelled */);
    return;
  }

  if (!s.ok()) {
    if (!StressTest::IsErrorInjectedAndRetryable(s)) {
      // Print in stdout instead of stderr to avoid stress test failure,
      // because OpenAndCompact() failure doesn't necessarily mean
      // primary db instance failure.
      fprintf(stdout, "Failed to run OpenAndCompact(%s): %s\n",
              job_info.db_name.c_str(), s.ToString().c_str());
    }
  } else {
    // Track successful completion time
    successful_compaction_end_to_end_micros = env->NowMicros() - start_micros;
  }

  // Add the output regardless of status, so that primary DB doesn't rely
  // on the timeout to finish waiting. The actual failure from the
  // deserialization can fail the compaction properly
  shared->AddRemoteCompactionResult(job_id, s, serialized_output);
}

static void ProcessRemoteCompactionJob(
    const std::string& job_id, const CompactionServiceJobInfo& job_info,
    const std::string& serialized_input, const std::string& output_directory,
    bool was_canceled, SharedState* shared, StressTest* stress_test,
    Random& rand, uint64_t& successful_compaction_end_to_end_micros) {
  auto options = stress_test->GetOptions(job_info.cf_id);
  assert(options.env != nullptr);

  auto override_options = CreateOverrideOptions(options, job_info);

  OpenAndCompactOptions open_compact_options;
  if (FLAGS_allow_resumption_one_in > 0) {
    open_compact_options.allow_resumption =
        rand.OneIn(FLAGS_allow_resumption_one_in);
  } else {
    open_compact_options.allow_resumption = false;
  }

  if (!open_compact_options.allow_resumption) {
    CleanupOutputDirectory(output_directory);
  }

  std::shared_ptr<std::atomic<bool>> canceled = nullptr;
  if (FLAGS_allow_resumption_one_in > 0) {
    canceled = SetupCancellation(open_compact_options, was_canceled, rand,
                                 successful_compaction_end_to_end_micros);
  }

  std::string serialized_output;
  uint64_t start_micros = options.env->NowMicros();

  Status s = DB::OpenAndCompact(open_compact_options, job_info.db_name,
                                output_directory, serialized_input,
                                &serialized_output, override_options);

  ProcessCompactionResult(s, job_id, job_info, serialized_input,
                          output_directory, serialized_output, shared,
                          successful_compaction_end_to_end_micros, start_micros,
                          options.env);
}

void RemoteCompactionWorkerThread(void* v) {
  assert(FLAGS_remote_compaction_worker_threads > 0);
  assert(FLAGS_remote_compaction_worker_interval > 0);

  auto* thread = static_cast<ThreadState*>(v);
  SharedState* shared = thread->shared;
  StressTest* stress_test = shared->GetStressTest();
  assert(stress_test != nullptr);

#ifndef NDEBUG
  SetupFaultInjectionForRemoteCompaction(shared);
#endif  // NDEBUG

  // Tracks the duration (in microseconds) of the most recent successfully
  // completed compaction from start to finish. This value is used in
  // SetupCancellation() to adaptively set up cancellation point for a
  // compaction
  uint64_t successful_compaction_end_to_end_micros = 0;
  Random rand(static_cast<uint32_t>(FLAGS_seed));

  // Main worker loop
  while (true) {
    // Check if we should stop
    {
      MutexLock l(shared->GetMutex());
      if (shared->ShouldStopBgThread()) {
        shared->IncBgThreadsFinished();
        if (shared->BgThreadsFinished()) {
          shared->GetCondVar()->SignalAll();
        }
        return;
      }
    }

    std::string job_id;
    CompactionServiceJobInfo job_info;
    std::string serialized_input;
    std::string output_directory;
    bool was_canceled;

    if (shared->DequeueRemoteCompaction(&job_id, &job_info, &serialized_input,
                                        &output_directory, &was_canceled)) {
      ProcessRemoteCompactionJob(
          job_id, job_info, serialized_input, output_directory, was_canceled,
          shared, stress_test, rand, successful_compaction_end_to_end_micros);
    }

    db_stress_env->SleepForMicroseconds(
        thread->rand.Next() % FLAGS_remote_compaction_worker_interval * 1000 +
        1);
  }
}

void PrintKeyValue(int cf, uint64_t key, const char* value, size_t sz) {
  if (!FLAGS_verbose) {
    return;
  }
  std::string tmp;
  tmp.reserve(sz * 2 + 16);
  char buf[4];
  for (size_t i = 0; i < sz; i++) {
    snprintf(buf, 4, "%X", value[i]);
    tmp.append(buf);
  }
  auto key_str = Key(key);
  Slice key_slice = key_str;
  fprintf(stdout, "[CF %d] %s (%" PRIi64 ") == > (%" ROCKSDB_PRIszt ") %s\n",
          cf, key_slice.ToString(true).c_str(), key, sz, tmp.c_str());
}

// Note that if hot_key_alpha != 0, it generates the key based on Zipfian
// distribution. Keys are randomly scattered to [0, FLAGS_max_key]. It does
// not ensure the order of the keys being generated and the keys does not have
// the active range which is related to FLAGS_active_width.
int64_t GenerateOneKey(ThreadState* thread, uint64_t iteration) {
  const double completed_ratio =
      static_cast<double>(iteration) / FLAGS_ops_per_thread;
  const int64_t base_key = static_cast<int64_t>(
      completed_ratio * (FLAGS_max_key - FLAGS_active_width));
  int64_t rand_seed = base_key + thread->rand.Next() % FLAGS_active_width;
  int64_t cur_key = rand_seed;
  if (FLAGS_hot_key_alpha != 0) {
    // If set the Zipfian distribution Alpha to non 0, use Zipfian
    double float_rand =
        (static_cast<double>(thread->rand.Next() % FLAGS_max_key)) /
        FLAGS_max_key;
    cur_key = GetOneHotKeyID(float_rand, FLAGS_max_key);
  }
  return cur_key;
}

// Note that if hot_key_alpha != 0, it generates the key based on Zipfian
// distribution. Keys being generated are in random order.
// If user want to generate keys based on uniform distribution, user needs to
// set hot_key_alpha == 0. It will generate the random keys in increasing
// order in the key array (ensure key[i] >= key[i+1]) and constrained in a
// range related to FLAGS_active_width.
std::vector<int64_t> GenerateNKeys(ThreadState* thread, int num_keys,
                                   uint64_t iteration) {
  const double completed_ratio =
      static_cast<double>(iteration) / FLAGS_ops_per_thread;
  const int64_t base_key = static_cast<int64_t>(
      completed_ratio * (FLAGS_max_key - FLAGS_active_width));
  std::vector<int64_t> keys;
  keys.reserve(num_keys);
  int64_t next_key = base_key + thread->rand.Next() % FLAGS_active_width;
  keys.push_back(next_key);
  for (int i = 1; i < num_keys; ++i) {
    // Generate the key follows zipfian distribution
    if (FLAGS_hot_key_alpha != 0) {
      double float_rand =
          (static_cast<double>(thread->rand.Next() % FLAGS_max_key)) /
          FLAGS_max_key;
      next_key = GetOneHotKeyID(float_rand, FLAGS_max_key);
    } else {
      // This may result in some duplicate keys
      next_key = next_key + thread->rand.Next() %
                                (FLAGS_active_width - (next_key - base_key));
    }
    keys.push_back(next_key);
  }
  return keys;
}

size_t GenerateValue(uint32_t rand, char* v, size_t max_sz) {
  size_t value_sz =
      ((rand % kRandomValueMaxFactor) + 1) * FLAGS_value_size_mult;
  assert(value_sz <= max_sz && value_sz >= sizeof(uint32_t));
  (void)max_sz;
  PutUnaligned(reinterpret_cast<uint32_t*>(v), rand);
  for (size_t i = sizeof(uint32_t); i < value_sz; i++) {
    v[i] = (char)(rand ^ i);
  }
  v[value_sz] = '\0';
  return value_sz;  // the size of the value set.
}

uint32_t GetValueBase(Slice s) {
  assert(s.size() >= sizeof(uint32_t));
  uint32_t res;
  GetUnaligned(reinterpret_cast<const uint32_t*>(s.data()), &res);
  return res;
}

AttributeGroups GenerateAttributeGroups(
    const std::vector<ColumnFamilyHandle*>& cfhs, uint32_t value_base,
    const Slice& slice) {
  WideColumns columns = GenerateWideColumns(value_base, slice);
  AttributeGroups attribute_groups;
  for (auto* cfh : cfhs) {
    attribute_groups.emplace_back(cfh, columns);
  }
  return attribute_groups;
}

WideColumns GenerateWideColumns(uint32_t value_base, const Slice& slice) {
  WideColumns columns;

  constexpr size_t max_columns = 4;
  const size_t num_columns = (value_base % max_columns) + 1;

  columns.reserve(num_columns);

  assert(slice.size() >= num_columns);

  columns.emplace_back(kDefaultWideColumnName, slice);

  for (size_t i = 1; i < num_columns; ++i) {
    const Slice name(slice.data(), i);
    const Slice value(slice.data() + i, slice.size() - i);

    columns.emplace_back(name, value);
  }

  return columns;
}

WideColumns GenerateExpectedWideColumns(uint32_t value_base,
                                        const Slice& slice) {
  if (FLAGS_use_put_entity_one_in == 0 ||
      (value_base % FLAGS_use_put_entity_one_in) != 0) {
    return WideColumns{{kDefaultWideColumnName, slice}};
  }

  WideColumns columns = GenerateWideColumns(value_base, slice);

  WideColumnsHelper::SortColumns(columns);

  return columns;
}

bool VerifyWideColumns(const Slice& value, const WideColumns& columns) {
  if (value.size() < sizeof(uint32_t)) {
    return false;
  }

  const uint32_t value_base = GetValueBase(value);

  const WideColumns expected_columns =
      GenerateExpectedWideColumns(value_base, value);

  if (columns != expected_columns) {
    return false;
  }

  return true;
}

bool VerifyWideColumns(const WideColumns& columns) {
  if (!WideColumnsHelper::HasDefaultColumn(columns)) {
    return false;
  }

  const Slice& value_of_default = WideColumnsHelper::GetDefaultColumn(columns);

  return VerifyWideColumns(value_of_default, columns);
}

bool VerifyIteratorAttributeGroups(
    const IteratorAttributeGroups& attribute_groups) {
  for (const auto& attribute_group : attribute_groups) {
    if (!VerifyWideColumns(attribute_group.columns())) {
      return false;
    }
  }
  return true;
}

std::string GetNowNanos() {
  uint64_t t = db_stress_env->NowNanos();
  std::string ret;
  PutFixed64(&ret, t);
  return ret;
}

uint64_t GetWriteUnixTime(ThreadState* thread) {
  static uint64_t kPreserveSeconds =
      std::max(FLAGS_preserve_internal_time_seconds,
               FLAGS_preclude_last_level_data_seconds);
  static uint64_t kFallbackTime = std::numeric_limits<uint64_t>::max();
  int64_t write_time = 0;
  Status s = db_stress_env->GetCurrentTime(&write_time);
  uint32_t write_time_mode = thread->rand.Uniform(3);
  if (write_time_mode == 0 || !s.ok()) {
    return kFallbackTime;
  } else if (write_time_mode == 1) {
    uint64_t delta = kPreserveSeconds > 0
                         ? static_cast<uint64_t>(thread->rand.Uniform(
                               static_cast<int>(kPreserveSeconds)))
                         : 0;
    return static_cast<uint64_t>(write_time) - delta;
  } else {
    return static_cast<uint64_t>(write_time) - kPreserveSeconds;
  }
}

namespace {

class MyXXH64Checksum : public FileChecksumGenerator {
 public:
  explicit MyXXH64Checksum(bool big) : big_(big) {
    state_ = XXH64_createState();
    XXH64_reset(state_, 0);
  }

  ~MyXXH64Checksum() override { XXH64_freeState(state_); }

  void Update(const char* data, size_t n) override {
    XXH64_update(state_, data, n);
  }

  void Finalize() override {
    assert(str_.empty());
    uint64_t digest = XXH64_digest(state_);
    // Store as little endian raw bytes
    PutFixed64(&str_, digest);
    if (big_) {
      // Throw in some more data for stress testing (448 bits total)
      PutFixed64(&str_, GetSliceHash64(str_));
      PutFixed64(&str_, GetSliceHash64(str_));
      PutFixed64(&str_, GetSliceHash64(str_));
      PutFixed64(&str_, GetSliceHash64(str_));
      PutFixed64(&str_, GetSliceHash64(str_));
      PutFixed64(&str_, GetSliceHash64(str_));
    }
  }

  std::string GetChecksum() const override {
    assert(!str_.empty());
    return str_;
  }

  const char* Name() const override {
    return big_ ? "MyBigChecksum" : "MyXXH64Checksum";
  }

 private:
  bool big_;
  XXH64_state_t* state_;
  std::string str_;
};

class DbStressChecksumGenFactory : public FileChecksumGenFactory {
  std::string default_func_name_;

  std::unique_ptr<FileChecksumGenerator> CreateFromFuncName(
      const std::string& func_name) {
    std::unique_ptr<FileChecksumGenerator> rv;
    if (func_name == "FileChecksumCrc32c") {
      rv.reset(new FileChecksumGenCrc32c(FileChecksumGenContext()));
    } else if (func_name == "MyXXH64Checksum") {
      rv.reset(new MyXXH64Checksum(false /* big */));
    } else if (func_name == "MyBigChecksum") {
      rv.reset(new MyXXH64Checksum(true /* big */));
    } else {
      // Should be a recognized function when we get here
      assert(false);
    }
    return rv;
  }

 public:
  explicit DbStressChecksumGenFactory(const std::string& default_func_name)
      : default_func_name_(default_func_name) {}

  std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& context) override {
    if (context.requested_checksum_func_name.empty()) {
      return CreateFromFuncName(default_func_name_);
    } else {
      return CreateFromFuncName(context.requested_checksum_func_name);
    }
  }

  const char* Name() const override { return "FileChecksumGenCrc32cFactory"; }
};

}  // namespace

std::shared_ptr<FileChecksumGenFactory> GetFileChecksumImpl(
    const std::string& name) {
  // Translate from friendly names to internal names
  std::string internal_name;
  if (name == "crc32c") {
    internal_name = "FileChecksumCrc32c";
  } else if (name == "xxh64") {
    internal_name = "MyXXH64Checksum";
  } else if (name == "big") {
    internal_name = "MyBigChecksum";
  } else {
    assert(name.empty() || name == "none");
    return nullptr;
  }
  return std::make_shared<DbStressChecksumGenFactory>(internal_name);
}

Status DeleteFilesInDirectory(const std::string& dirname) {
  std::vector<std::string> filenames;
  Status s = Env::Default()->GetChildren(dirname, &filenames);
  for (size_t i = 0; s.ok() && i < filenames.size(); ++i) {
    s = Env::Default()->DeleteFile(dirname + "/" + filenames[i]);
  }
  return s;
}

Status SaveFilesInDirectory(const std::string& src_dirname,
                            const std::string& dst_dirname) {
  std::vector<std::string> filenames;
  Status s = Env::Default()->GetChildren(src_dirname, &filenames);
  for (size_t i = 0; s.ok() && i < filenames.size(); ++i) {
    bool is_dir = false;
    s = Env::Default()->IsDirectory(src_dirname + "/" + filenames[i], &is_dir);
    if (s.ok()) {
      if (is_dir) {
        continue;
      }
      s = Env::Default()->LinkFile(src_dirname + "/" + filenames[i],
                                   dst_dirname + "/" + filenames[i]);
    }
  }
  return s;
}

Status InitUnverifiedSubdir(const std::string& dirname) {
  Status s = Env::Default()->FileExists(dirname);
  if (s.IsNotFound()) {
    return Status::OK();
  }

  const std::string kUnverifiedDirname = dirname + "/unverified";
  if (s.ok()) {
    s = Env::Default()->CreateDirIfMissing(kUnverifiedDirname);
  }
  if (s.ok()) {
    // It might already exist with some stale contents. Delete any such
    // contents.
    s = DeleteFilesInDirectory(kUnverifiedDirname);
  }
  if (s.ok()) {
    s = SaveFilesInDirectory(dirname, kUnverifiedDirname);
  }
  return s;
}

Status DestroyUnverifiedSubdir(const std::string& dirname) {
  Status s = Env::Default()->FileExists(dirname);
  if (s.IsNotFound()) {
    return Status::OK();
  }

  const std::string kUnverifiedDirname = dirname + "/unverified";
  if (s.ok()) {
    s = Env::Default()->FileExists(kUnverifiedDirname);
  }
  if (s.IsNotFound()) {
    return Status::OK();
  }

  if (s.ok()) {
    s = DeleteFilesInDirectory(kUnverifiedDirname);
  }
  if (s.ok()) {
    s = Env::Default()->DeleteDir(kUnverifiedDirname);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
