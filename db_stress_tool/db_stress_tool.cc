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
#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_driver.h"
#include "rocksdb/convenience.h"
#ifndef NDEBUG
#include "utilities/fault_injection_fs.h"
#endif

namespace ROCKSDB_NAMESPACE {
namespace {
static std::shared_ptr<ROCKSDB_NAMESPACE::Env> env_guard;
static std::shared_ptr<ROCKSDB_NAMESPACE::DbStressEnvWrapper> env_wrapper_guard;
static std::shared_ptr<CompositeEnvWrapper> fault_env_guard;
}  // namespace

KeyGenContext key_gen_ctx;

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
  if (FLAGS_statistics) {
    dbstats = ROCKSDB_NAMESPACE::CreateDBStatistics();
    if (FLAGS_test_secondary) {
      dbstats_secondaries = ROCKSDB_NAMESPACE::CreateDBStatistics();
    }
  }
  compression_type_e = StringToCompressionType(FLAGS_compression_type.c_str());
  bottommost_compression_type_e =
      StringToCompressionType(FLAGS_bottommost_compression_type.c_str());
  checksum_type_e = StringToChecksumType(FLAGS_checksum_type.c_str());

  Env* raw_env;

  int env_opts =
      !FLAGS_hdfs.empty() + !FLAGS_env_uri.empty() + !FLAGS_fs_uri.empty();
  if (env_opts > 1) {
    fprintf(stderr,
            "Error: --hdfs, --env_uri and --fs_uri are mutually exclusive\n");
    exit(1);
  }

  if (!FLAGS_hdfs.empty()) {
    raw_env = new ROCKSDB_NAMESPACE::HdfsEnv(FLAGS_hdfs);
  } else {
    Status s = Env::CreateFromUri(ConfigOptions(), FLAGS_env_uri, FLAGS_fs_uri,
                                  &raw_env, &env_guard);
    if (!s.ok()) {
      fprintf(stderr, "Error Creating Env URI: %s: %s\n", FLAGS_env_uri.c_str(),
              s.ToString().c_str());
      exit(1);
    }
  }

#ifndef NDEBUG
  if (FLAGS_read_fault_one_in || FLAGS_sync_fault_injection ||
      FLAGS_write_fault_one_in || FLAGS_open_metadata_write_fault_one_in) {
    FaultInjectionTestFS* fs =
        new FaultInjectionTestFS(raw_env->GetFileSystem());
    fault_fs_guard.reset(fs);
    if (FLAGS_write_fault_one_in) {
      fault_fs_guard->SetFilesystemDirectWritable(false);
    } else {
      fault_fs_guard->SetFilesystemDirectWritable(true);
    }
    fault_env_guard =
        std::make_shared<CompositeEnvWrapper>(raw_env, fault_fs_guard);
    raw_env = fault_env_guard.get();
  }
  if (FLAGS_write_fault_one_in) {
    SyncPoint::GetInstance()->SetCallBack(
        "BuildTable:BeforeFinishBuildTable",
        [&](void*) { fault_fs_guard->EnableWriteErrorInjection(); });
    SyncPoint::GetInstance()->EnableProcessing();
  }
#endif

  env_wrapper_guard = std::make_shared<DbStressEnvWrapper>(raw_env);
  db_stress_env = env_wrapper_guard.get();

#ifndef NDEBUG
  if (FLAGS_write_fault_one_in) {
    // In the write injection case, we need to use the FS interface and returns
    // the IOStatus with different error and flags. Therefore,
    // DbStressEnvWrapper cannot be used which will swallow the FS
    // implementations. We should directly use the raw_env which is the
    // CompositeEnvWrapper of env and fault_fs.
    db_stress_env = raw_env;
  }
#endif

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
       FLAGS_delpercent + FLAGS_delrangepercent + FLAGS_iterpercent) != 100) {
    fprintf(stderr,
            "Error: "
            "Read(%d)+Prefix(%d)+Write(%d)+Delete(%d)+DeleteRange(%d)"
            "+Iterate(%d) percents != "
            "100!\n",
            FLAGS_readpercent, FLAGS_prefixpercent, FLAGS_writepercent,
            FLAGS_delpercent, FLAGS_delrangepercent, FLAGS_iterpercent);
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
  if (FLAGS_ingest_external_file_one_in > 0 && FLAGS_nooverwritepercent > 0) {
    fprintf(stderr,
            "Error: nooverwritepercent must be 0 when using file ingestion\n");
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
    db_stress_env->GetTestDirectory(&default_db_path);
    default_db_path += "/dbstress";
    FLAGS_db = default_db_path;
  }

  if ((FLAGS_test_secondary || FLAGS_continuous_verification_interval > 0) &&
      FLAGS_secondaries_base.empty()) {
    std::string default_secondaries_path;
    db_stress_env->GetTestDirectory(&default_secondaries_path);
    default_secondaries_path += "/dbstress_secondaries";
    ROCKSDB_NAMESPACE::Status s =
        db_stress_env->CreateDirIfMissing(default_secondaries_path);
    if (!s.ok()) {
      fprintf(stderr, "Failed to create directory %s: %s\n",
              default_secondaries_path.c_str(), s.ToString().c_str());
      exit(1);
    }
    FLAGS_secondaries_base = default_secondaries_path;
  }

  if (!FLAGS_test_secondary && FLAGS_secondary_catch_up_one_in > 0) {
    fprintf(
        stderr,
        "Must set -test_secondary=true if secondary_catch_up_one_in > 0.\n");
    exit(1);
  }
  if (FLAGS_best_efforts_recovery && !FLAGS_skip_verifydb &&
      !FLAGS_disable_wal) {
    fprintf(stderr,
            "With best-efforts recovery, either skip_verifydb or disable_wal "
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
  if (FLAGS_enable_compaction_filter &&
      (FLAGS_acquire_snapshot_one_in > 0 || FLAGS_compact_range_one_in > 0 ||
       FLAGS_iterpercent > 0 || FLAGS_test_batches_snapshots ||
       FLAGS_test_cf_consistency)) {
    fprintf(
        stderr,
        "Error: acquire_snapshot_one_in, compact_range_one_in, iterpercent, "
        "test_batches_snapshots  must all be 0 when using compaction filter\n");
    exit(1);
  }
  if (FLAGS_batch_protection_bytes_per_key > 0 &&
      !FLAGS_test_batches_snapshots) {
    fprintf(stderr,
            "Error: test_batches_snapshots must be enabled when "
            "batch_protection_bytes_per_key > 0\n");
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

  std::unique_ptr<ROCKSDB_NAMESPACE::StressTest> stress;
  if (FLAGS_test_cf_consistency) {
    stress.reset(CreateCfConsistencyStressTest());
  } else if (FLAGS_test_batches_snapshots) {
    stress.reset(CreateBatchedOpsStressTest());
  } else {
    stress.reset(CreateNonBatchedOpsStressTest());
  }
  // Initialize the Zipfian pre-calculated array
  InitializeHotKeyGenerator(FLAGS_hot_key_alpha);
  if (RunStressTest(stress.get())) {
    return 0;
  } else {
    return 1;
  }
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
