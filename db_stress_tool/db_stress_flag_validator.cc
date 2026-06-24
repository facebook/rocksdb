//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS

#include "db_stress_tool/db_stress_flag_validator.h"

#include <iostream>

#include "db_stress_tool/db_stress_common.h"
#include "options/options_helper.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {
namespace {

int ReturnFlagValidationError(const char* message) {
  std::cerr << "Error: " << message << '\n';
  return 1;
}

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

}  // namespace

Status ParseDbStressOptionCompatibilityCheckLevel(
    OptionCompatibilityCheckLevel* level) {
  if (FLAGS_option_compatibility_check_level == "skip" ||
      FLAGS_option_compatibility_check_level == "kSkip") {
    *level = OptionCompatibilityCheckLevel::kSkip;
    return Status::OK();
  }
  if (FLAGS_option_compatibility_check_level == "warn" ||
      FLAGS_option_compatibility_check_level == "kWarn") {
    *level = OptionCompatibilityCheckLevel::kWarn;
    return Status::OK();
  }
  if (FLAGS_option_compatibility_check_level == "reject" ||
      FLAGS_option_compatibility_check_level == "kReject") {
    *level = OptionCompatibilityCheckLevel::kReject;
    return Status::OK();
  }
  return Status::InvalidArgument(
      "option_compatibility_check_level must be skip, warn, or reject");
}

int ValidateDbStressCoreOptionCompatibility() {
  OptionCompatibilityCheckLevel level;
  Status s = ParseDbStressOptionCompatibilityCheckLevel(&level);
  if (!s.ok()) {
    std::cerr << "Error: " << s.ToString() << '\n';
    return 1;
  }

  DBOptions db_options;
  db_options.allow_mmap_reads = FLAGS_mmap_read;
  db_options.allow_mmap_writes = FLAGS_mmap_write;
  db_options.use_direct_reads = FLAGS_use_direct_reads;
  db_options.use_direct_io_for_compaction_reads =
      FLAGS_use_direct_io_for_compaction_reads;
  db_options.use_direct_io_for_flush_and_compaction =
      FLAGS_use_direct_io_for_flush_and_compaction;
  db_options.option_compatibility_check_level = level;

  ColumnFamilyOptions cf_options;
  cf_options.inplace_update_support = FLAGS_inplace_update_support;
  cf_options.min_tombstones_for_range_conversion =
      FLAGS_min_tombstones_for_range_conversion;

  s = ValidateOptionCompatibility(db_options, cf_options, level);
  if (!s.ok()) {
    std::cerr << "Error: " << s.ToString() << '\n';
    return 1;
  }
  return 0;
}

int ValidateDbStressFlags() {
  {
    OptionCompatibilityCheckLevel level;
    Status s = ParseDbStressOptionCompatibilityCheckLevel(&level);
    if (!s.ok()) {
      std::cerr << "Error: " << s.ToString() << '\n';
      return 1;
    }
  }

  int rc = ValidateNumDbsFlags();
  if (rc != 0) {
    return rc;
  }
  if (FLAGS_prefixpercent > 0 && FLAGS_prefix_size < 0) {
    fprintf(stderr,
            "Error: prefixpercent is non-zero while prefix_size is "
            "not positive!\n");
    return 1;
  }
  if (FLAGS_test_batches_snapshots && FLAGS_prefix_size <= 0) {
    fprintf(stderr,
            "Error: please specify prefix_size for "
            "test_batches_snapshots test!\n");
    return 1;
  }
  if (FLAGS_memtable_prefix_bloom_size_ratio > 0.0 && FLAGS_prefix_size < 0 &&
      !FLAGS_memtable_whole_key_filtering) {
    fprintf(stderr,
            "Error: please specify positive prefix_size or enable whole key "
            "filtering in order to use memtable_prefix_bloom_size_ratio\n");
    return 1;
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
    return 1;
  }
  if (FLAGS_disable_wal == 1 && FLAGS_reopen > 0) {
    fprintf(stderr, "Error: Db cannot reopen safely with disable_wal set!\n");
    return 1;
  }
  if ((unsigned)FLAGS_reopen >= FLAGS_ops_per_thread) {
    fprintf(stderr,
            "Error: #DB-reopens should be < ops_per_thread\n"
            "Provided reopens = %d and ops_per_thread = %lu\n",
            FLAGS_reopen, (unsigned long)FLAGS_ops_per_thread);
    return 1;
  }
  if (FLAGS_test_batches_snapshots && FLAGS_delrangepercent > 0) {
    fprintf(stderr,
            "Error: nonzero delrangepercent unsupported in "
            "test_batches_snapshots mode\n");
    return 1;
  }
  if (FLAGS_active_width > FLAGS_max_key) {
    fprintf(stderr, "Error: active_width can be at most max_key\n");
    return 1;
  } else if (FLAGS_active_width == 0) {
    FLAGS_active_width = FLAGS_max_key;
  }
  if (FLAGS_value_size_mult * kRandomValueMaxFactor > kValueMaxLen) {
    fprintf(stderr, "Error: value_size_mult can be at most %d\n",
            kValueMaxLen / kRandomValueMaxFactor);
    return 1;
  }
  if (FLAGS_use_merge && FLAGS_nooverwritepercent == 100) {
    fprintf(
        stderr,
        "Error: nooverwritepercent must not be 100 when using merge operands");
    return 1;
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
    return 1;
  }
  if (FLAGS_clear_column_family_one_in > 0 && FLAGS_backup_one_in > 0) {
    fprintf(stderr,
            "Error: clear_column_family_one_in must be 0 when using backup\n");
    return 1;
  }
  if (FLAGS_test_cf_consistency && FLAGS_disable_wal) {
    FLAGS_atomic_flush = true;
  }

  // Trie UDI uses zero-copy pointers into block data, which is incompatible
  // with mmap_read.
  if (FLAGS_use_trie_index && FLAGS_mmap_read) {
    fprintf(stderr,
            "Error: use_trie_index is incompatible with mmap_read. "
            "The trie index uses zero-copy pointers into block data "
            "which is unsafe with mmap'd reads.\n");
    return 1;
  }

  // TrieIndexFactory requires plain BytewiseComparator, but timestamps use
  // BytewiseComparator.u64ts.
  if (FLAGS_use_trie_index && FLAGS_user_timestamp_size > 0) {
    fprintf(stderr,
            "Error: use_trie_index is incompatible with user-defined "
            "timestamps. TrieIndexFactory requires BytewiseComparator "
            "but timestamps use BytewiseComparator.u64ts.\n");
    return 1;
  }

  if (FLAGS_read_only) {
    if (FLAGS_writepercent != 0 || FLAGS_delpercent != 0 ||
        FLAGS_delrangepercent != 0) {
      fprintf(stderr, "Error: updates are not supported in read only mode\n");
      return 1;
    } else if (FLAGS_checkpoint_one_in > 0 &&
               FLAGS_clear_column_family_one_in > 0) {
      fprintf(stdout,
              "Warn: checkpoint won't be validated since column families may "
              "be dropped.\n");
    }
  }

  if (FLAGS_best_efforts_recovery &&
      !(FLAGS_skip_verifydb && FLAGS_disable_wal)) {
    fprintf(stderr,
            "With best-efforts recovery, skip_verifydb and disable_wal "
            "should be set to true.\n");
    return 1;
  }
  if (FLAGS_skip_verifydb) {
    if (FLAGS_verify_db_one_in > 0) {
      fprintf(stderr,
              "Must set -verify_db_one_in=0 if skip_verifydb is true.\n");
      return 1;
    }
    if (FLAGS_continuous_verification_interval > 0) {
      fprintf(stderr,
              "Must set -continuous_verification_interval=0 if skip_verifydb "
              "is true.\n");
      return 1;
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
    return 1;
  }
  if (FLAGS_test_multi_ops_txns) {
    CheckAndSetOptionsForMultiOpsTxnStressTest();
  }

  if (!FLAGS_use_txn && FLAGS_use_optimistic_txn) {
    fprintf(
        stderr,
        "You cannot set use_optimistic_txn true while use_txn is false. Please "
        "set use_txn true if you want to use OptimisticTransactionDB\n");
    return 1;
  }

  if (FLAGS_create_timestamped_snapshot_one_in > 0) {
    if (!FLAGS_use_txn) {
      fprintf(stderr, "timestamped snapshot supported only in TransactionDB\n");
      return 1;
    } else if (FLAGS_txn_write_policy != 0) {
      fprintf(stderr,
              "timestamped snapshot supported only in write-committed\n");
      return 1;
    }
  }

  if (FLAGS_preserve_unverified_changes && FLAGS_reopen != 0) {
    fprintf(stderr,
            "Reopen DB is incompatible with preserving unverified changes\n");
    return 1;
  }

  if (FLAGS_use_txn && !FLAGS_use_optimistic_txn &&
      FLAGS_sync_fault_injection && FLAGS_txn_write_policy != 0) {
    fprintf(stderr,
            "For TransactionDB, correctness testing with unsync data loss is "
            "currently compatible with only write committed policy\n");
    return 1;
  }

  if (FLAGS_use_put_entity_one_in > 0 &&
      (FLAGS_use_full_merge_v1 || FLAGS_test_multi_ops_txns ||
       FLAGS_user_timestamp_size > 0)) {
    fprintf(stderr,
            "Wide columns are incompatible with V1 Merge, the multi-op "
            "transaction test, and user-defined timestamps\n");
    return 1;
  }
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
