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
#include "db_stress_tool/db_stress_shared_state.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static std::shared_ptr<ROCKSDB_NAMESPACE::Env> env_guard;
static std::shared_ptr<ROCKSDB_NAMESPACE::Env> env_wrapper_guard;
static std::shared_ptr<ROCKSDB_NAMESPACE::Env> legacy_env_wrapper_guard;
static std::shared_ptr<ROCKSDB_NAMESPACE::CompositeEnvWrapper>
    dbsl_env_wrapper_guard;
// Global state for crash callback — must be accessible from a plain function
// pointer (CrashCallback = void(*)()) since lambdas with captures cannot
// convert to function pointers. Uses a fixed-size C array of raw pointers
// so the callback is async-signal-safe (no vector walks, shared_ptr copies,
// or std::string access).
static constexpr int kMaxDbsForCrash = 256;
static ROCKSDB_NAMESPACE::FaultInjectionTestFS*
    g_fault_fs_for_crash[kMaxDbsForCrash] = {};
static int g_num_dbs_for_crash = 0;
}  // namespace

KeyGenContext key_gen_ctx;

struct DbPaths {
  std::string db_path;
  std::string ev_dir;
  std::string sec_base;
};

void ResolveDefaultDbPathIfEmpty() {
  if (!FLAGS_db.empty()) {
    return;
  }

  std::string default_db_path;
  db_stress_env->GetTestDirectory(&default_db_path);
  default_db_path += "/dbstress";
  FLAGS_db = default_db_path;
}

bool IsMultiDbRoot(const std::string& db_root) {
  return db_stress_env->FileExists(db_root + "/db_0").ok();
}

DbPaths ComputeDbPaths(int i, int num_dbs) {
  DbPaths p;
  if (num_dbs > 1) {
    std::string suffix = "/db_" + std::to_string(i);
    p.db_path = FLAGS_db + suffix;
    p.ev_dir = FLAGS_expected_values_dir.empty()
                   ? ""
                   : FLAGS_expected_values_dir + suffix;
    p.sec_base =
        FLAGS_secondaries_base.empty() ? "" : FLAGS_secondaries_base + suffix;
  } else {
    p.db_path = FLAGS_db;
    p.ev_dir = FLAGS_expected_values_dir;
    p.sec_base = FLAGS_secondaries_base;
  }
  return p;
}

void EnsureDirsExist(const DbPaths& paths) {
  auto check = [](Env* env, const std::string& dir) {
    if (dir.empty()) {
      return;
    }
    Status s = env->CreateDirIfMissing(dir);
    if (!s.ok()) {
      fprintf(stderr, "Error creating dir %s: %s\n", dir.c_str(),
              s.ToString().c_str());
      exit(1);
    }
  };
  check(db_stress_env, paths.db_path);
  // expected_values_dir is always on the local filesystem (the Python driver
  // materializes it via tempfile.mkdtemp), even for remote-DB runs.
  check(Env::Default(), paths.ev_dir);
  check(db_stress_env, paths.sec_base);
}

StressTest* CreateStressTestByFlags(const DbPaths& paths) {
  if (FLAGS_test_cf_consistency) {
    return CreateCfConsistencyStressTest(paths.db_path, paths.ev_dir,
                                         paths.sec_base);
  } else if (FLAGS_test_batches_snapshots) {
    return CreateBatchedOpsStressTest(paths.db_path, paths.ev_dir,
                                      paths.sec_base);
  } else if (FLAGS_test_multi_ops_txns) {
    return CreateMultiOpsTxnsStressTest(paths.db_path, paths.ev_dir,
                                        paths.sec_base);
  } else {
    return CreateNonBatchedOpsStressTest(paths.db_path, paths.ev_dir,
                                         paths.sec_base);
  }
}

int db_stress_tool(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
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

  Env* raw_env;

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
  dbsl_env_wrapper_guard = std::make_shared<CompositeEnvWrapper>(raw_env);
  db_stress_listener_env = dbsl_env_wrapper_guard.get();

  auto db_stress_fs =
      std::make_shared<DbStressFSWrapper>(raw_env->GetFileSystem());
  env_wrapper_guard =
      std::make_shared<CompositeEnvWrapper>(raw_env, db_stress_fs);
  db_stress_env = env_wrapper_guard.get();

  ResolveDefaultDbPathIfEmpty();

  // Handle --destroy_db_and_exit early, before other option validation
  if (FLAGS_destroy_db_and_exit) {
    s = (FLAGS_num_dbs > 1 || IsMultiDbRoot(FLAGS_db))
            ? DestroyDir(raw_env, FLAGS_db)
            : DbStressDestroyDb(FLAGS_db);
    // Note: expected_values_dir and secondaries_base cleanup is handled
    // by the crash test framework (db_crashtest.py) after the test passes.
    // Do NOT clean them here to avoid double-removal race.
    if (s.ok()) {
      fprintf(stdout, "Successfully destroyed db at %s\n", FLAGS_db.c_str());
      return 0;
    } else {
      fprintf(stderr, "Failed to destroy db at %s: %s\n", FLAGS_db.c_str(),
              s.ToString().c_str());
      return 1;
    }
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

  FLAGS_rep_factory = StringToRepFactory(FLAGS_memtablerep.c_str());

  // The number of background threads should be at least as much the
  // max number of concurrent compactions.
  db_stress_env->SetBackgroundThreads(FLAGS_max_background_compactions,
                                      ROCKSDB_NAMESPACE::Env::Priority::LOW);
  db_stress_env->SetBackgroundThreads(FLAGS_num_bottom_pri_threads,
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

  if ((FLAGS_test_secondary || FLAGS_continuous_verification_interval > 0) &&
      FLAGS_secondaries_base.empty()) {
    std::string default_secondaries_path;
    db_stress_env->GetTestDirectory(&default_secondaries_path);
    default_secondaries_path += "/dbstress_secondaries";
    s = db_stress_env->CreateDirIfMissing(default_secondaries_path);
    if (!s.ok()) {
      fprintf(stderr, "Failed to create directory %s: %s\n",
              default_secondaries_path.c_str(), s.ToString().c_str());
      exit(1);
    }
    FLAGS_secondaries_base = default_secondaries_path;
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
  // Initialize the Zipfian pre-calculated array
  InitializeHotKeyGenerator(FLAGS_hot_key_alpha);

  if (FLAGS_num_dbs < 1) {
    fprintf(stderr, "Error: --num_dbs must be >= 1\n");
    exit(1);
  }
  if (FLAGS_num_dbs > kMaxDbsForCrash) {
    fprintf(stderr, "Error: --num_dbs=%d exceeds maximum %d\n", FLAGS_num_dbs,
            kMaxDbsForCrash);
    exit(1);
  }

  const int num_dbs = FLAGS_num_dbs;

  // Multi-DB mode: run N independent StressTest instances sharing one Env.
  if (num_dbs > 1) {
    // Column family clearing has a pre-existing race condition where a CF
    // handle can be accessed by one thread (e.g. NewIterator) after another
    // thread drops and recreates it. This is more likely to trigger with the
    // increased thread count in multi-DB mode. Disable it to avoid spurious
    // ASAN heap-use-after-free failures.
    if (FLAGS_clear_column_family_one_in > 0) {
      fprintf(stderr,
              "Warning: --num_dbs > 1 disables --clear_column_family_one_in "
              "due to a pre-existing CF handle race condition.\n");
      FLAGS_clear_column_family_one_in = 0;
    }
    // MultiOpsTxnsStressTest uses a single global key_spaces_path file.
    // Multiple DB instances would overwrite each other's range descriptors,
    // causing key-space layout corruption on reopen.
    if (FLAGS_test_multi_ops_txns) {
      fprintf(stderr,
              "Error: --num_dbs > 1 is incompatible with "
              "--test_multi_ops_txns (shared key_spaces_path).\n");
      exit(1);
    }

    // CompressedCacheSetCapacityThread mutates and asserts on the shared
    // compressed_secondary_cache. Multiple per-DB threads racing on
    // SetCapacity(0)/SetCapacity(size) cause spurious assertion failures.
    if (FLAGS_compressed_secondary_cache_size > 0 ||
        FLAGS_compressed_secondary_cache_ratio > 0.0) {
      fprintf(stderr,
              "Warning: --num_dbs > 1 disables compressed secondary cache "
              "capacity stress to avoid races on shared cache state.\n");
      FLAGS_compressed_secondary_cache_size = 0;
      FLAGS_compressed_secondary_cache_ratio = 0.0;
    }

    // Share WriteBufferManager and RateLimiter across all DBs so total
    // memory and I/O are bounded globally, not per-DB.
    block_cache =
        StressTest::NewCache(FLAGS_cache_size, FLAGS_cache_numshardbits);
    if (FLAGS_use_write_buffer_manager) {
      shared_wbm = std::make_shared<WriteBufferManager>(
          FLAGS_db_write_buffer_size, block_cache);
    }
    if (FLAGS_rate_limiter_bytes_per_sec > 0) {
      shared_rate_limiter.reset(NewGenericRateLimiter(
          FLAGS_rate_limiter_bytes_per_sec, 1000 /* refill_period_us */,
          10 /* fairness */,
          FLAGS_rate_limit_bg_reads ? RateLimiter::Mode::kReadsOnly
                                    : RateLimiter::Mode::kWritesOnly));
    }
  }

  // Save and clear the destroy flag before any threads start to avoid a
  // data race on this non-atomic global. Destruction is done on the main
  // thread below.
  bool destroy_initially = FLAGS_destroy_db_initially;
  FLAGS_destroy_db_initially = false;

  // Create parent directories for multi-DB paths.
  if (num_dbs > 1) {
    EnsureDirsExist(
        {FLAGS_db, FLAGS_expected_values_dir, FLAGS_secondaries_base});
  }

  std::vector<std::unique_ptr<StressTest>> stresses(num_dbs);
  std::vector<std::unique_ptr<SharedState>> shareds(num_dbs);
  // Use int instead of bool to avoid std::vector<bool> bitset packing
  // which causes data races on concurrent writes to different indices.
  std::vector<int> results(num_dbs, 0);
  std::vector<std::thread> threads(num_dbs);

  // Determine log directory for fault injection error logs.
  std::string log_dir;
  const char* test_tmpdir = getenv("TEST_TMPDIR");
  if (test_tmpdir && test_tmpdir[0] != '\0') {
    log_dir = test_tmpdir;
  } else {
    log_dir = "/tmp";
  }

  for (int i = 0; i < num_dbs; i++) {
    DbPaths paths = ComputeDbPaths(i, num_dbs);
    if (num_dbs > 1) {
      EnsureDirsExist(paths);
    }
    if (destroy_initially) {
      Status exists = db_stress_env->FileExists(paths.db_path);
      if (!exists.IsNotFound()) {
        Status ds = DbStressDestroyDb(paths.db_path);
        if (!ds.ok()) {
          fprintf(stderr, "Cannot destroy db %s: %s\n", paths.db_path.c_str(),
                  ds.ToString().c_str());
          exit(1);
        }
      }
    }

    stresses[i].reset(CreateStressTestByFlags(paths));

    // Set up per-DB fault injection error log path so that PrintAll()
    // writes to a file instead of stderr (signal-safe). Include the DB
    // index in multi-DB mode for clear identification.
    auto fault_fs = stresses[i]->GetFaultFs();
    if (fault_fs) {
      std::string log_path =
          log_dir + "/fault_injection_" + std::to_string(getpid());
      if (num_dbs > 1) {
        log_path += "_db" + std::to_string(i);
      }
      log_path += "_" + std::to_string(time(nullptr)) + ".log";
      fault_fs->SetInjectedErrorLogPath(log_path);
    }

    shareds[i].reset(
        new SharedState(db_stress_env, stresses[i].get(), paths.ev_dir));
    threads[i] = std::thread([i, &results, &shareds]() {
      results[i] = RunStressTest(shareds[i].get());
    });
  }

  // Register a crash callback so that recently injected errors are
  // printed when the process crashes (SIGABRT, SIGSEGV, etc.).
  // Use a fixed-size C array of raw pointers so the callback is
  // async-signal-safe (no vector walks, shared_ptr copies, or
  // std::string access).
  assert(num_dbs <= kMaxDbsForCrash);
  for (int i = 0; i < num_dbs; i++) {
    g_fault_fs_for_crash[i] = stresses[i]->GetFaultFs().get();
  }
  g_num_dbs_for_crash = num_dbs;
  port::RegisterCrashCallback([]() {
    for (int i = 0; i < g_num_dbs_for_crash; i++) {
      if (g_fault_fs_for_crash[i]) {
        g_fault_fs_for_crash[i]->PrintRecentInjectedErrors();
      }
    }
  });

  // Join all threads and report results.
  bool all_passed = true;
  for (int i = 0; i < num_dbs; i++) {
    threads[i].join();
    if (num_dbs > 1) {
      fprintf(stdout, "[multi-db] DB %d (%s): %s\n", i,
              stresses[i]->GetDbPath().c_str(),
              results[i] ? "PASSED" : "FAILED");
    }
    if (!results[i]) {
      all_passed = false;
    }
    // Close DB in CleanUp() before destructor to prevent race between
    // destructor and operations in listener callbacks.
    stresses[i]->CleanUp();
  }

  // Clear crash callback and its references before they go out of scope.
  port::RegisterCrashCallback(nullptr);
  for (int i = 0; i < num_dbs; i++) {
    g_fault_fs_for_crash[i] = nullptr;
  }
  g_num_dbs_for_crash = 0;

  // Reset per-invocation shared resources so a second call to
  // db_stress_tool() in the same process uses fresh state from its own flags.
  shared_wbm.reset();
  shared_rate_limiter.reset();
  block_cache.reset();

  return all_passed ? 0 : 1;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
