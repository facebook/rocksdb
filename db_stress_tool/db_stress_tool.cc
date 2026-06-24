//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The test uses an array to compare against values written to the database.
// Keys written to the array are in 1:1 correspondence to the actual values in
// the database according to the formula in the function GenerateValue.

// Space is reserved in the array from 0 to FLAGS_max_key and values are
// randomly written/deleted/read from those positions. During verification we
// compare all the positions in the array. To shorten/elongate the running
// time, you could change the settings: FLAGS_max_key, FLAGS_ops_per_thread,
// (sometimes also FLAGS_threads).
//
// NOTE that if FLAGS_test_batches_snapshots is set, the test will have
// different behavior. See comment of the flag for details.

#ifdef GFLAGS
#include <thread>

#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_driver.h"
#include "db_stress_tool/db_stress_flag_validator.h"
#include "db_stress_tool/db_stress_shared_state.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static std::shared_ptr<ROCKSDB_NAMESPACE::Env> env_guard;
static std::shared_ptr<ROCKSDB_NAMESPACE::Env> legacy_env_wrapper_guard;
// Raw pointers for signal-safe crash callback. Signal handlers can only
// access file-static/global variables; can't capture StressTest instances.
static std::vector<ROCKSDB_NAMESPACE::FaultInjectionTestFS*>
    fault_fs_for_crash_flush;

int DestroyAllDbs() {
  bool all_ok = true;
  const int num_dbs = FLAGS_num_dbs;
  auto destroy_one = [&](const std::string& db_path) {
    Status s = DbStressDestroyDb(db_path);
    if (s.ok()) {
      fprintf(stdout, "Successfully destroyed db at %s\n", db_path.c_str());
    } else {
      fprintf(stderr, "Failed to destroy db at %s: %s\n", db_path.c_str(),
              s.ToString().c_str());
      all_ok = false;
    }
  };
  if (num_dbs == 1) {
    destroy_one(FLAGS_db);
  } else {
    for (int i = 0; i < num_dbs; i++) {
      destroy_one(FLAGS_db + "/db_" + std::to_string(i));
    }
    DestroyDir(raw_env, FLAGS_db);
  }
  return all_ok ? 0 : 1;
}

void RegisterCrashCallbacks(
    const std::vector<std::unique_ptr<StressTest>>& stress_tests, int num_dbs) {
  fault_fs_for_crash_flush.resize(num_dbs, nullptr);
  bool any_fault_fs = false;
  for (int i = 0; i < num_dbs; i++) {
    fault_fs_for_crash_flush[i] =
        stress_tests[i]->GetDbFaultInjectionFs().get();
    if (fault_fs_for_crash_flush[i]) {
      any_fault_fs = true;
    }
  }
  if (any_fault_fs) {
    port::RegisterCrashCallback([]() {
      for (auto* fs : fault_fs_for_crash_flush) {
        if (fs) {
          fs->FlushRecentInjectedErrors();
        }
      }
    });
  }
}
}  // namespace

KeyGenContext key_gen_ctx;

int db_stress_tool(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  RegisterDbStressBdwFlagValidators();
  ParseCommandLineFlags(&argc, &argv, true);

  SanitizeDoubleParam(&FLAGS_bloom_bits);
  SanitizeDoubleParam(&FLAGS_memtable_prefix_bloom_size_ratio);
  SanitizeDoubleParam(&FLAGS_max_bytes_for_level_multiplier);

#ifndef NDEBUG
  if (FLAGS_mock_direct_io) {
    SetupSyncPointsToMockDirectIO();
  }
#endif
  compression_type_e = StringToCompressionType(FLAGS_compression_type.c_str());
  bottommost_compression_type_e =
      StringToCompressionType(FLAGS_bottommost_compression_type.c_str());
  checksum_type_e = StringToChecksumType(FLAGS_checksum_type.c_str());

  int env_opts = !FLAGS_env_uri.empty() + !FLAGS_fs_uri.empty();
  if (env_opts > 1) {
    fprintf(stderr, "Error: --env_uri and --fs_uri are mutually exclusive\n");
    exit(1);
  }

  Status s = Env::CreateFromUri(ConfigOptions(), FLAGS_env_uri, FLAGS_fs_uri,
                                &raw_env, &env_guard);
  if (!s.ok()) {
    fprintf(stderr, "Error Creating Env URI: %s: %s\n", FLAGS_env_uri.c_str(),
            s.ToString().c_str());
    exit(1);
  }

  auto validate_flags = [&]() {
    FLAGS_rep_factory = StringToRepFactory(FLAGS_memtablerep.c_str());

    // The number of background threads should be at least as much the
    // max number of concurrent compactions.
    raw_env->SetBackgroundThreads(FLAGS_max_background_compactions,
                                  ROCKSDB_NAMESPACE::Env::Priority::LOW);
    raw_env->SetBackgroundThreads(FLAGS_num_bottom_pri_threads,
                                  ROCKSDB_NAMESPACE::Env::Priority::BOTTOM);
    int rc = ValidateDbStressFlags();
    if (rc != 0) {
      return rc;
    }
    if (FLAGS_validate_db_stress_flags_only) {
      return ValidateDbStressCoreOptionCompatibility();
    }
    return 0;
  };
  if (FLAGS_validate_db_stress_flags_only) {
    return validate_flags();
  }

  // Handle --destroy_db_and_exit early
  if (FLAGS_destroy_db_and_exit) {
    return DestroyAllDbs();
  }

  // Handle --delete_dir_and_exit early, before other option validation
  if (!FLAGS_delete_dir_and_exit.empty()) {
    s = DestroyDir(raw_env, FLAGS_delete_dir_and_exit);
    if (s.ok()) {
      fprintf(stdout, "Successfully deleted directory %s\n",
              FLAGS_delete_dir_and_exit.c_str());
      return 0;
    } else {
      fprintf(stderr, "Failed to delete directory %s: %s\n",
              FLAGS_delete_dir_and_exit.c_str(), s.ToString().c_str());
      return 1;
    }
  }

  int rc = validate_flags();
  if (rc != 0) {
    return rc;
  }

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db.empty()) {
    std::string default_db_path;
    raw_env->GetTestDirectory(&default_db_path);
    default_db_path += "/dbstress";
    FLAGS_db = default_db_path;
  }

  // For num_dbs=1: --db and --expected_values_dir are paths used as-is.
  // For num_dbs>1: they are parent directories; C++ creates db_0/, db_1/, ...
  // subdirs underneath. DB::Open(create_if_missing) creates each DB dir.
  // Python (db_crashtest.py) also creates EV dirs; C++ creates them here as a
  // fallback for direct CLI usage.
  const int num_dbs = FLAGS_num_dbs;
  if (num_dbs > 1) {
    s = raw_env->CreateDirIfMissing(FLAGS_db);
    if (!s.ok()) {
      fprintf(stderr, "Failed to create directory %s: %s\n", FLAGS_db.c_str(),
              s.ToString().c_str());
      exit(1);
    }
  }
  std::vector<std::string> db_paths;
  std::vector<std::string> ev_paths;
  for (int i = 0; i < num_dbs; i++) {
    std::string suffix = (num_dbs == 1) ? "" : "/db_" + std::to_string(i);
    db_paths.push_back(FLAGS_db + suffix);
    if (!FLAGS_expected_values_dir.empty()) {
      std::string ep = FLAGS_expected_values_dir + suffix;
      s = Env::Default()->CreateDirIfMissing(ep);
      if (!s.ok()) {
        fprintf(stderr, "Failed to create directory %s: %s\n", ep.c_str(),
                s.ToString().c_str());
        exit(1);
      }
      ev_paths.push_back(std::move(ep));
    }
  }
  if (ev_paths.empty()) {
    ev_paths.resize(num_dbs);
  }

  // Secondary paths: C++ owns creation entirely.
  std::vector<std::string> sec_paths;
  if (FLAGS_test_secondary || FLAGS_continuous_verification_interval > 0) {
    std::string sec_parent;
    if (!FLAGS_secondaries_base.empty()) {
      sec_parent = FLAGS_secondaries_base;
    } else {
      raw_env->GetTestDirectory(&sec_parent);
      sec_parent += "/dbstress_secondaries";
    }
    s = raw_env->CreateDirIfMissing(sec_parent);
    if (!s.ok()) {
      fprintf(stderr, "Failed to create directory %s: %s\n", sec_parent.c_str(),
              s.ToString().c_str());
      exit(1);
    }
    for (int i = 0; i < num_dbs; i++) {
      std::string suffix = (num_dbs == 1) ? "" : "/db_" + std::to_string(i);
      std::string sec_path = sec_parent + suffix;
      s = raw_env->CreateDirIfMissing(sec_path);
      if (!s.ok()) {
        fprintf(stderr, "Failed to create directory %s: %s\n", sec_path.c_str(),
                s.ToString().c_str());
        exit(1);
      }
      sec_paths.push_back(std::move(sec_path));
    }
  }
  if (sec_paths.empty()) {
    sec_paths.resize(num_dbs);
  }

#ifndef NDEBUG
  KillPoint* kp = KillPoint::GetInstance();
  kp->rocksdb_kill_odds = FLAGS_kill_random_test;
  kp->rocksdb_kill_exclude_prefixes = SplitString(FLAGS_kill_exclude_prefixes);
#endif

  unsigned int levels = FLAGS_max_key_len;
  std::vector<std::string> weights;
  uint64_t scale_factor = FLAGS_key_window_scale_factor;
  key_gen_ctx.window = scale_factor * 100;
  if (!FLAGS_key_len_percent_dist.empty()) {
    weights = SplitString(FLAGS_key_len_percent_dist);
    if (weights.size() != levels) {
      fprintf(stderr,
              "Number of weights in key_len_dist should be equal to"
              " max_key_len");
      exit(1);
    }

    uint64_t total_weight = 0;
    for (std::string& weight : weights) {
      uint64_t val = std::stoull(weight);
      key_gen_ctx.weights.emplace_back(val * scale_factor);
      total_weight += val;
    }
    if (total_weight != 100) {
      fprintf(stderr, "Sum of all weights in key_len_dist should be 100");
      exit(1);
    }
  } else {
    uint64_t keys_per_level = key_gen_ctx.window / levels;
    for (unsigned int level = 0; level + 1 < levels; ++level) {
      key_gen_ctx.weights.emplace_back(keys_per_level);
    }
    key_gen_ctx.weights.emplace_back(key_gen_ctx.window -
                                     keys_per_level * (levels - 1));
  }
  // Initialize shared resources (hot-key generator, block cache, WBM,
  // rate limiter) once so that all DB instances share them.
  InitializeHotKeyGenerator(FLAGS_hot_key_alpha);
  block_cache =
      StressTest::NewCache(FLAGS_cache_size, FLAGS_cache_numshardbits);
  if (FLAGS_use_write_buffer_manager) {
    wbm = std::make_shared<WriteBufferManager>(FLAGS_db_write_buffer_size,
                                               block_cache);
  }
  if (FLAGS_rate_limiter_bytes_per_sec > 0) {
    rate_limiter.reset(NewGenericRateLimiter(
        FLAGS_rate_limiter_bytes_per_sec, 1000 /* refill_period_us */,
        10 /* fairness */,
        FLAGS_rate_limit_bg_reads ? RateLimiter::Mode::kReadsOnly
                                  : RateLimiter::Mode::kWritesOnly));
  }

  // Phase 1: Create StressTest instances (one per DB).
  std::vector<std::unique_ptr<StressTest>> stress_tests(num_dbs);
  for (int i = 0; i < num_dbs; i++) {
    if (FLAGS_test_cf_consistency) {
      stress_tests[i].reset(CreateCfConsistencyStressTest(
          i, db_paths[i], ev_paths[i], sec_paths[i]));
    } else if (FLAGS_test_batches_snapshots) {
      stress_tests[i].reset(CreateBatchedOpsStressTest(
          i, db_paths[i], ev_paths[i], sec_paths[i]));
    } else if (FLAGS_test_multi_ops_txns) {
      stress_tests[i].reset(CreateMultiOpsTxnsStressTest(
          i, db_paths[i], ev_paths[i], sec_paths[i]));
    } else {
      stress_tests[i].reset(CreateNonBatchedOpsStressTest(
          i, db_paths[i], ev_paths[i], sec_paths[i]));
    }
  }

  RegisterCrashCallbacks(stress_tests, num_dbs);

  // Phase 2: Create SharedState for every DB before any worker thread starts.
  std::vector<std::unique_ptr<SharedState>> shared_states(num_dbs);
  for (int i = 0; i < num_dbs; i++) {
    shared_states[i].reset(new SharedState(raw_env, stress_tests[i].get()));
  }

  // Phase 3: Launch each DB's stress test on its own thread.
  std::vector<int> results(num_dbs, 0);
  std::vector<std::thread> stress_test_runners;
  stress_test_runners.reserve(num_dbs);
  for (int i = 0; i < num_dbs; i++) {
    stress_test_runners.emplace_back([i, &results, &shared_states]() {
      results[i] = RunStressTest(shared_states[i].get()) ? 1 : 0;
    });
  }

  // Phase 4: Wait for all DB threads, collect results, clean up.
  bool all_passed = true;
  for (int i = 0; i < num_dbs; i++) {
    stress_test_runners[i].join();
    if (!results[i]) {
      all_passed = false;
    }
    stress_tests[i]->CleanUp();
  }
  for (auto& fs : fault_fs_for_crash_flush) {
    fs = nullptr;
  }
  return all_passed ? 0 : 1;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
