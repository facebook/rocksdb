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
#include <iostream>
#include <thread>

#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_driver.h"
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
    fault_fs_for_crash_report;

int ValidateNumDbsFlags() {
  if (FLAGS_num_dbs < 1) {
    fprintf(stderr, "Error: --num_dbs must be >= 1\n");
    return 1;
  }
  if (FLAGS_num_dbs > 1) {
    if (FLAGS_clear_column_family_one_in > 0) {
      fprintf(stderr,
              "Error: --num_dbs > 1 incompatible with "
              "--clear_column_family_one_in\n");
      return 1;
    }
    if (FLAGS_test_multi_ops_txns) {
      fprintf(stderr,
              "Error: --num_dbs > 1 incompatible with "
              "--test_multi_ops_txns\n");
      return 1;
    }
  }
  return 0;
}

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
  fault_fs_for_crash_report.resize(num_dbs, nullptr);
  bool any_fault_fs = false;
  for (int i = 0; i < num_dbs; i++) {
    fault_fs_for_crash_report[i] =
        stress_tests[i]->GetDbFaultInjectionFs().get();
    if (fault_fs_for_crash_report[i]) {
      any_fault_fs = true;
    }
  }
  if (any_fault_fs) {
    port::RegisterCrashCallback([]() {
      for (auto* fs : fault_fs_for_crash_report) {
        if (fs) {
          fs->PrintRecentInjectedErrors();
        }
      }
    });
  }
}

int ReturnFlagValidationError(const char* message) {
  std::cerr << "Error: " << message << '\n';
  return 1;
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

  {
    int rc = ValidateNumDbsFlags();
    if (rc != 0) {
      return rc;
    }
  }

  FLAGS_rep_factory = StringToRepFactory(FLAGS_memtablerep.c_str());

  // The number of background threads should be at least as much the
  // max number of concurrent compactions.
  raw_env->SetBackgroundThreads(FLAGS_max_background_compactions,
                                ROCKSDB_NAMESPACE::Env::Priority::LOW);
  raw_env->SetBackgroundThreads(FLAGS_num_bottom_pri_threads,
                                ROCKSDB_NAMESPACE::Env::Priority::BOTTOM);
  if (FLAGS_prefixpercent > 0 && FLAGS_prefix_size < 0) {
    fprintf(stderr,
            "Error: prefixpercent is non-zero while prefix_size is "
            "not positive!\n");
    exit(1);
  }
  if (FLAGS_test_batches_snapshots && FLAGS_prefix_size <= 0) {
    fprintf(stderr,
            "Error: please specify prefix_size for "
            "test_batches_snapshots test!\n");
    exit(1);
  }
  if (FLAGS_memtable_prefix_bloom_size_ratio > 0.0 && FLAGS_prefix_size < 0 &&
      !FLAGS_memtable_whole_key_filtering) {
    fprintf(stderr,
            "Error: please specify positive prefix_size or enable whole key "
            "filtering in order to use memtable_prefix_bloom_size_ratio\n");
    exit(1);
  }
  if ((FLAGS_readpercent + FLAGS_prefixpercent + FLAGS_writepercent +
       FLAGS_delpercent + FLAGS_delrangepercent + FLAGS_iterpercent +
       FLAGS_customopspercent) != 100) {
    fprintf(
        stderr,
        "Error: "
        "Read(-readpercent=%d)+Prefix(-prefixpercent=%d)+Write(-writepercent=%"
        "d)+Delete(-delpercent=%d)+DeleteRange(-delrangepercent=%d)"
        "+Iterate(-iterpercent=%d)+CustomOps(-customopspercent=%d) percents != "
        "100!\n",
        FLAGS_readpercent, FLAGS_prefixpercent, FLAGS_writepercent,
        FLAGS_delpercent, FLAGS_delrangepercent, FLAGS_iterpercent,
        FLAGS_customopspercent);
    exit(1);
  }
  if (FLAGS_disable_wal == 1 && FLAGS_reopen > 0) {
    fprintf(stderr, "Error: Db cannot reopen safely with disable_wal set!\n");
    exit(1);
  }
  if ((unsigned)FLAGS_reopen >= FLAGS_ops_per_thread) {
    fprintf(stderr,
            "Error: #DB-reopens should be < ops_per_thread\n"
            "Provided reopens = %d and ops_per_thread = %lu\n",
            FLAGS_reopen, (unsigned long)FLAGS_ops_per_thread);
    exit(1);
  }
  if (FLAGS_test_batches_snapshots && FLAGS_delrangepercent > 0) {
    fprintf(stderr,
            "Error: nonzero delrangepercent unsupported in "
            "test_batches_snapshots mode\n");
    exit(1);
  }
  if (FLAGS_active_width > FLAGS_max_key) {
    fprintf(stderr, "Error: active_width can be at most max_key\n");
    exit(1);
  } else if (FLAGS_active_width == 0) {
    FLAGS_active_width = FLAGS_max_key;
  }
  if (FLAGS_value_size_mult * kRandomValueMaxFactor > kValueMaxLen) {
    fprintf(stderr, "Error: value_size_mult can be at most %d\n",
            kValueMaxLen / kRandomValueMaxFactor);
    exit(1);
  }
  if (FLAGS_use_merge && FLAGS_nooverwritepercent == 100) {
    fprintf(
        stderr,
        "Error: nooverwritepercent must not be 100 when using merge operands");
    exit(1);
  }
  if (FLAGS_enable_blob_direct_write) {
    // Blob direct write is intentionally validated as a reduced-scope stress
    // feature. We allow the WAL-disabled crash-test profile, including
    // wide-column PutEntity/GetEntity coverage, but reject best-efforts
    // recovery, parallel memtable/write-queue variants, transactions, remote
    // compaction, and APIs/features that depend on active-file snapshotting or
    // unsupported blob option transitions.
    if (!FLAGS_enable_blob_files) {
      return ReturnFlagValidationError(
          "enable_blob_direct_write requires enable_blob_files");
    }
    if (FLAGS_allow_concurrent_memtable_write) {
      return ReturnFlagValidationError(
          "blob direct write stress requires "
          "allow_concurrent_memtable_write=0");
    }
    if (FLAGS_enable_pipelined_write) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support "
          "enable_pipelined_write");
    }
    if (FLAGS_unordered_write) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support unordered_write");
    }
    if (FLAGS_two_write_queues) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support two_write_queues");
    }
    if (FLAGS_use_blob_db) {
      return ReturnFlagValidationError(
          "blob direct write is only supported with integrated BlobDB");
    }
    if (FLAGS_use_merge || FLAGS_use_full_merge_v1) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support merge");
    }
    if (FLAGS_experimental_mempurge_threshold > 0.0) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support MemPurge");
    }
    if (FLAGS_user_timestamp_size > 0) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support user-defined timestamps");
    }
    if (FLAGS_allow_setting_blob_options_dynamically ||
        FLAGS_enable_blob_garbage_collection) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support dynamic blob options or "
          "blob GC");
    }
    if (FLAGS_best_efforts_recovery) {
      return ReturnFlagValidationError(
          "blob direct write stress supports disable_wal-based crash "
          "testing, not best-efforts recovery");
    }
    if (FLAGS_remote_compaction_worker_threads > 0) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support remote compaction");
    }
    if (FLAGS_use_txn || FLAGS_txn_write_policy != 0 ||
        FLAGS_use_optimistic_txn || FLAGS_test_multi_ops_txns ||
        FLAGS_commit_bypass_memtable_one_in > 0) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support TransactionDB modes");
    }
    if (FLAGS_test_secondary || FLAGS_backup_one_in > 0 ||
        FLAGS_checkpoint_one_in > 0 || FLAGS_get_live_files_apis_one_in > 0 ||
        FLAGS_ingest_external_file_one_in > 0) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support secondary, backup, "
          "checkpoint, get_live_files, or ingest_external_file modes");
    }
    if (FLAGS_ingest_wbwi_one_in > 0) {
      return ReturnFlagValidationError(
          "blob direct write stress does not support "
          "IngestWriteBatchWithIndex");
    }
  }
  if (FLAGS_ingest_external_file_one_in > 0 &&
      FLAGS_nooverwritepercent == 100) {
    fprintf(
        stderr,
        "Error: nooverwritepercent must not be 100 when using file ingestion");
    exit(1);
  }
  if (FLAGS_clear_column_family_one_in > 0 && FLAGS_backup_one_in > 0) {
    fprintf(stderr,
            "Error: clear_column_family_one_in must be 0 when using backup\n");
    exit(1);
  }
  if (FLAGS_test_cf_consistency && FLAGS_disable_wal) {
    FLAGS_atomic_flush = true;
  }

  // Trie UDI uses zero-copy pointers into block data, which is
  // incompatible with mmap_read.
  if (FLAGS_use_trie_index && FLAGS_mmap_read) {
    fprintf(stderr,
            "Error: use_trie_index is incompatible with mmap_read. "
            "The trie index uses zero-copy pointers into block data "
            "which is unsafe with mmap'd reads.\n");
    exit(1);
  }

  // TrieIndexFactory requires plain BytewiseComparator, but timestamps use
  // BytewiseComparator.u64ts.
  if (FLAGS_use_trie_index && FLAGS_user_timestamp_size > 0) {
    fprintf(stderr,
            "Error: use_trie_index is incompatible with user-defined "
            "timestamps. TrieIndexFactory requires BytewiseComparator "
            "but timestamps use BytewiseComparator.u64ts.\n");
    exit(1);
  }

  if (FLAGS_read_only) {
    if (FLAGS_writepercent != 0 || FLAGS_delpercent != 0 ||
        FLAGS_delrangepercent != 0) {
      fprintf(stderr, "Error: updates are not supported in read only mode\n");
      exit(1);
    } else if (FLAGS_checkpoint_one_in > 0 &&
               FLAGS_clear_column_family_one_in > 0) {
      fprintf(stdout,
              "Warn: checkpoint won't be validated since column families may "
              "be dropped.\n");
    }
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

  if (FLAGS_best_efforts_recovery &&
      !(FLAGS_skip_verifydb && FLAGS_disable_wal)) {
    fprintf(stderr,
            "With best-efforts recovery, skip_verifydb and disable_wal "
            "should be set to true.\n");
    exit(1);
  }
  if (FLAGS_skip_verifydb) {
    if (FLAGS_verify_db_one_in > 0) {
      fprintf(stderr,
              "Must set -verify_db_one_in=0 if skip_verifydb is true.\n");
      exit(1);
    }
    if (FLAGS_continuous_verification_interval > 0) {
      fprintf(stderr,
              "Must set -continuous_verification_interval=0 if skip_verifydb "
              "is true.\n");
      exit(1);
    }
  }
  if ((FLAGS_enable_compaction_filter || FLAGS_inplace_update_support) &&
      (FLAGS_acquire_snapshot_one_in > 0 || FLAGS_compact_range_one_in > 0 ||
       FLAGS_iterpercent > 0 || FLAGS_prefixpercent > 0 ||
       FLAGS_test_batches_snapshots || FLAGS_test_cf_consistency ||
       FLAGS_check_multiget_consistency ||
       FLAGS_check_multiget_entity_consistency)) {
    fprintf(
        stderr,
        "Error: acquire_snapshot_one_in, compact_range_one_in, iterpercent, "
        "prefixpercent, test_batches_snapshots, test_cf_consistency, "
        "check_multiget_consistency, check_multiget_entity_consistency must "
        "all be 0 when using compaction filter or inplace update support\n");
    exit(1);
  }
  if (FLAGS_test_multi_ops_txns) {
    CheckAndSetOptionsForMultiOpsTxnStressTest();
  }

  if (!FLAGS_use_txn && FLAGS_use_optimistic_txn) {
    fprintf(
        stderr,
        "You cannot set use_optimistic_txn true while use_txn is false. Please "
        "set use_txn true if you want to use OptimisticTransactionDB\n");
    exit(1);
  }

  if (FLAGS_create_timestamped_snapshot_one_in > 0) {
    if (!FLAGS_use_txn) {
      fprintf(stderr, "timestamped snapshot supported only in TransactionDB\n");
      exit(1);
    } else if (FLAGS_txn_write_policy != 0) {
      fprintf(stderr,
              "timestamped snapshot supported only in write-committed\n");
      exit(1);
    }
  }

  if (FLAGS_preserve_unverified_changes && FLAGS_reopen != 0) {
    fprintf(stderr,
            "Reopen DB is incompatible with preserving unverified changes\n");
    exit(1);
  }

  if (FLAGS_use_txn && !FLAGS_use_optimistic_txn &&
      FLAGS_sync_fault_injection && FLAGS_txn_write_policy != 0) {
    fprintf(stderr,
            "For TransactionDB, correctness testing with unsync data loss is "
            "currently compatible with only write committed policy\n");
    exit(1);
  }

  if (FLAGS_use_put_entity_one_in > 0 &&
      (FLAGS_use_full_merge_v1 || FLAGS_test_multi_ops_txns ||
       FLAGS_user_timestamp_size > 0)) {
    fprintf(stderr,
            "Wide columns are incompatible with V1 Merge, the multi-op "
            "transaction test, and user-defined timestamps\n");
    exit(1);
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
  for (auto& fs : fault_fs_for_crash_report) {
    fs = nullptr;
  }
  return all_passed ? 0 : 1;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
