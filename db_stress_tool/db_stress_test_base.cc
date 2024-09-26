//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include <ios>
#include <thread>

#include "db_stress_tool/db_stress_listener.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "util/compression.h"
#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_compaction_filter.h"
#include "db_stress_tool/db_stress_driver.h"
#include "db_stress_tool/db_stress_filters.h"
#include "db_stress_tool/db_stress_table_properties_collector.h"
#include "db_stress_tool/db_stress_wide_merge_operator.h"
#include "options/options_parser.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "utilities/backup/backup_engine_impl.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/fault_injection_secondary_cache.h"

namespace ROCKSDB_NAMESPACE {

namespace {

std::shared_ptr<const FilterPolicy> CreateFilterPolicy() {
  if (FLAGS_bloom_bits < 0) {
    return BlockBasedTableOptions().filter_policy;
  }
  const FilterPolicy* new_policy;
  if (FLAGS_bloom_before_level == INT_MAX) {
    // Use Bloom API
    new_policy = NewBloomFilterPolicy(FLAGS_bloom_bits, false);
  } else {
    new_policy =
        NewRibbonFilterPolicy(FLAGS_bloom_bits, FLAGS_bloom_before_level);
  }
  return std::shared_ptr<const FilterPolicy>(new_policy);
}

}  // namespace

StressTest::StressTest()
    : cache_(NewCache(FLAGS_cache_size, FLAGS_cache_numshardbits)),
      filter_policy_(CreateFilterPolicy()),
      db_(nullptr),
      txn_db_(nullptr),
      optimistic_txn_db_(nullptr),
      db_aptr_(nullptr),
      clock_(db_stress_env->GetSystemClock().get()),
      new_column_family_name_(1),
      num_times_reopened_(0),
      db_preload_finished_(false),
      cmp_db_(nullptr),
      is_db_stopped_(false) {
  if (FLAGS_destroy_db_initially) {
    std::vector<std::string> files;
    db_stress_env->GetChildren(FLAGS_db, &files);
    for (unsigned int i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        db_stress_env->DeleteFile(FLAGS_db + "/" + files[i]);
      }
    }

    Options options;
    options.env = db_stress_env;
    // Remove files without preserving manfiest files
    const Status s = !FLAGS_use_blob_db
                         ? DestroyDB(FLAGS_db, options)
                         : blob_db::DestroyBlobDB(FLAGS_db, options,
                                                  blob_db::BlobDBOptions());

    if (!s.ok()) {
      fprintf(stderr, "Cannot destroy original db: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  Status s = DbStressSqfcManager().MakeSharedFactory(
      FLAGS_sqfc_name, FLAGS_sqfc_version, &sqfc_factory_);
  if (!s.ok()) {
    fprintf(stderr, "Error initializing SstQueryFilterConfig: %s\n",
            s.ToString().c_str());
    exit(1);
  }
}

void StressTest::CleanUp() {
  for (auto cf : column_families_) {
    delete cf;
  }
  column_families_.clear();
  if (db_) {
    db_->Close();
  }
  delete db_;
  db_ = nullptr;

  for (auto* cf : cmp_cfhs_) {
    delete cf;
  }
  cmp_cfhs_.clear();
  delete cmp_db_;
}

std::shared_ptr<Cache> StressTest::NewCache(size_t capacity,
                                            int32_t num_shard_bits) {
  ConfigOptions config_options;
  if (capacity <= 0) {
    return nullptr;
  }

  std::shared_ptr<SecondaryCache> secondary_cache;
  if (!FLAGS_secondary_cache_uri.empty()) {
    assert(!strstr(FLAGS_secondary_cache_uri.c_str(),
                   "compressed_secondary_cache") ||
           (FLAGS_compressed_secondary_cache_size == 0 &&
            FLAGS_compressed_secondary_cache_ratio == 0.0 &&
            !StartsWith(FLAGS_cache_type, "tiered_")));
    Status s = SecondaryCache::CreateFromString(
        config_options, FLAGS_secondary_cache_uri, &secondary_cache);
    if (secondary_cache == nullptr) {
      fprintf(stderr,
              "No secondary cache registered matching string: %s status=%s\n",
              FLAGS_secondary_cache_uri.c_str(), s.ToString().c_str());
      exit(1);
    }
    if (FLAGS_secondary_cache_fault_one_in > 0) {
      secondary_cache = std::make_shared<FaultInjectionSecondaryCache>(
          secondary_cache, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_secondary_cache_fault_one_in);
    }
  } else if (FLAGS_compressed_secondary_cache_size > 0) {
    if (StartsWith(FLAGS_cache_type, "tiered_")) {
      fprintf(stderr,
              "Cannot specify both compressed_secondary_cache_size and %s\n",
              FLAGS_cache_type.c_str());
      exit(1);
    }
    CompressedSecondaryCacheOptions opts;
    opts.capacity = FLAGS_compressed_secondary_cache_size;
    opts.compress_format_version = FLAGS_compress_format_version;
    if (FLAGS_enable_do_not_compress_roles) {
      opts.do_not_compress_roles = {CacheEntryRoleSet::All()};
    }
    opts.enable_custom_split_merge = FLAGS_enable_custom_split_merge;
    secondary_cache = NewCompressedSecondaryCache(opts);
    if (secondary_cache == nullptr) {
      fprintf(stderr, "Failed to allocate compressed secondary cache\n");
      exit(1);
    }
    compressed_secondary_cache = secondary_cache;
  }

  std::string cache_type = FLAGS_cache_type;
  size_t cache_size = FLAGS_cache_size;
  bool tiered = false;
  if (StartsWith(cache_type, "tiered_")) {
    tiered = true;
    cache_type.erase(0, strlen("tiered_"));
  }
  if (FLAGS_use_write_buffer_manager) {
    cache_size += FLAGS_db_write_buffer_size;
  }
  if (cache_type == "clock_cache") {
    fprintf(stderr, "Old clock cache implementation has been removed.\n");
    exit(1);
  } else if (EndsWith(cache_type, "hyper_clock_cache")) {
    size_t estimated_entry_charge;
    if (cache_type == "fixed_hyper_clock_cache" ||
        cache_type == "hyper_clock_cache") {
      estimated_entry_charge = FLAGS_block_size;
    } else if (cache_type == "auto_hyper_clock_cache") {
      estimated_entry_charge = 0;
    } else {
      fprintf(stderr, "Cache type not supported.");
      exit(1);
    }
    HyperClockCacheOptions opts(cache_size, estimated_entry_charge,
                                num_shard_bits);
    opts.hash_seed = BitwiseAnd(FLAGS_seed, INT32_MAX);
    if (tiered) {
      TieredCacheOptions tiered_opts;
      tiered_opts.cache_opts = &opts;
      tiered_opts.cache_type = PrimaryCacheType::kCacheTypeHCC;
      tiered_opts.total_capacity = cache_size;
      tiered_opts.compressed_secondary_ratio = 0.5;
      tiered_opts.adm_policy =
          static_cast<TieredAdmissionPolicy>(FLAGS_adm_policy);
      if (tiered_opts.adm_policy ==
          TieredAdmissionPolicy::kAdmPolicyThreeQueue) {
        CompressedSecondaryCacheOptions nvm_sec_cache_opts;
        nvm_sec_cache_opts.capacity = cache_size;
        tiered_opts.nvm_sec_cache =
            NewCompressedSecondaryCache(nvm_sec_cache_opts);
      }
      block_cache = NewTieredCache(tiered_opts);
    } else {
      opts.secondary_cache = std::move(secondary_cache);
      block_cache = opts.MakeSharedCache();
    }
  } else if (EndsWith(cache_type, "lru_cache")) {
    LRUCacheOptions opts;
    opts.capacity = capacity;
    opts.num_shard_bits = num_shard_bits;
    opts.metadata_charge_policy =
        static_cast<CacheMetadataChargePolicy>(FLAGS_metadata_charge_policy);
    opts.use_adaptive_mutex = FLAGS_use_adaptive_mutex_lru;
    opts.high_pri_pool_ratio = FLAGS_high_pri_pool_ratio;
    opts.low_pri_pool_ratio = FLAGS_low_pri_pool_ratio;
    if (tiered) {
      TieredCacheOptions tiered_opts;
      tiered_opts.cache_opts = &opts;
      tiered_opts.cache_type = PrimaryCacheType::kCacheTypeLRU;
      tiered_opts.total_capacity = cache_size;
      tiered_opts.compressed_secondary_ratio = 0.5;
      tiered_opts.adm_policy =
          static_cast<TieredAdmissionPolicy>(FLAGS_adm_policy);
      if (tiered_opts.adm_policy ==
          TieredAdmissionPolicy::kAdmPolicyThreeQueue) {
        CompressedSecondaryCacheOptions nvm_sec_cache_opts;
        nvm_sec_cache_opts.capacity = cache_size;
        tiered_opts.nvm_sec_cache =
            NewCompressedSecondaryCache(nvm_sec_cache_opts);
      }
      block_cache = NewTieredCache(tiered_opts);
    } else {
      opts.secondary_cache = std::move(secondary_cache);
      block_cache = NewLRUCache(opts);
    }
  } else {
    fprintf(stderr, "Cache type not supported.");
    exit(1);
  }
  return block_cache;
}

std::vector<std::string> StressTest::GetBlobCompressionTags() {
  std::vector<std::string> compression_tags{"kNoCompression"};

  if (Snappy_Supported()) {
    compression_tags.emplace_back("kSnappyCompression");
  }
  if (LZ4_Supported()) {
    compression_tags.emplace_back("kLZ4Compression");
  }
  if (ZSTD_Supported()) {
    compression_tags.emplace_back("kZSTD");
  }

  return compression_tags;
}

bool StressTest::BuildOptionsTable() {
  if (FLAGS_set_options_one_in <= 0) {
    return true;
  }

  std::unordered_map<std::string, std::vector<std::string>> options_tbl = {
      {"write_buffer_size",
       {std::to_string(options_.write_buffer_size),
        std::to_string(options_.write_buffer_size * 2),
        std::to_string(options_.write_buffer_size * 4)}},
      {"max_write_buffer_number",
       {std::to_string(options_.max_write_buffer_number),
        std::to_string(options_.max_write_buffer_number * 2),
        std::to_string(options_.max_write_buffer_number * 4)}},
      {"arena_block_size",
       {
           std::to_string(options_.arena_block_size),
           std::to_string(options_.write_buffer_size / 4),
           std::to_string(options_.write_buffer_size / 8),
       }},
      {"memtable_huge_page_size", {"0", std::to_string(2 * 1024 * 1024)}},
      {"strict_max_successive_merges", {"false", "true"}},
      {"inplace_update_num_locks", {"100", "200", "300"}},
      // TODO: re-enable once internal task T124324915 is fixed.
      // {"experimental_mempurge_threshold", {"0.0", "1.0"}},
      // TODO(ljin): enable test for this option
      // {"disable_auto_compactions", {"100", "200", "300"}},
      {"level0_slowdown_writes_trigger",
       {
           std::to_string(options_.level0_slowdown_writes_trigger),
           std::to_string(options_.level0_slowdown_writes_trigger + 2),
           std::to_string(options_.level0_slowdown_writes_trigger + 4),
       }},
      {"level0_stop_writes_trigger",
       {
           std::to_string(options_.level0_stop_writes_trigger),
           std::to_string(options_.level0_stop_writes_trigger + 2),
           std::to_string(options_.level0_stop_writes_trigger + 4),
       }},
      {"max_compaction_bytes",
       {
           std::to_string(options_.target_file_size_base * 5),
           std::to_string(options_.target_file_size_base * 15),
           std::to_string(options_.target_file_size_base * 100),
       }},
      {"target_file_size_base",
       {
           std::to_string(options_.target_file_size_base),
           std::to_string(options_.target_file_size_base * 2),
           std::to_string(options_.target_file_size_base * 4),
       }},
      {"target_file_size_multiplier",
       {
           std::to_string(options_.target_file_size_multiplier),
           "1",
           "2",
       }},
      {"max_bytes_for_level_base",
       {
           std::to_string(options_.max_bytes_for_level_base / 2),
           std::to_string(options_.max_bytes_for_level_base),
           std::to_string(options_.max_bytes_for_level_base * 2),
       }},
      {"max_bytes_for_level_multiplier",
       {
           std::to_string(options_.max_bytes_for_level_multiplier),
           "1",
           "2",
       }},
      {"max_sequential_skip_in_iterations", {"4", "8", "12"}},
  };
  if (FLAGS_compaction_style == kCompactionStyleUniversal &&
      FLAGS_universal_max_read_amp > 0) {
    // level0_file_num_compaction_trigger needs to be at most max_read_amp
    options_tbl.emplace(
        "level0_file_num_compaction_trigger",
        std::vector<std::string>{
            std::to_string(options_.level0_file_num_compaction_trigger),
            std::to_string(
                std::min(options_.level0_file_num_compaction_trigger + 2,
                         FLAGS_universal_max_read_amp)),
            std::to_string(
                std::min(options_.level0_file_num_compaction_trigger + 4,
                         FLAGS_universal_max_read_amp)),
        });
  } else {
    options_tbl.emplace(
        "level0_file_num_compaction_trigger",
        std::vector<std::string>{
            std::to_string(options_.level0_file_num_compaction_trigger),
            std::to_string(options_.level0_file_num_compaction_trigger + 2),
            std::to_string(options_.level0_file_num_compaction_trigger + 4),
        });
  }
  if (FLAGS_unordered_write) {
    options_tbl.emplace("max_successive_merges", std::vector<std::string>{"0"});
  } else {
    options_tbl.emplace("max_successive_merges",
                        std::vector<std::string>{"0", "2", "4"});
  }

  if (FLAGS_allow_setting_blob_options_dynamically) {
    options_tbl.emplace("enable_blob_files",
                        std::vector<std::string>{"false", "true"});
    options_tbl.emplace("min_blob_size",
                        std::vector<std::string>{"0", "8", "16"});
    options_tbl.emplace("blob_file_size",
                        std::vector<std::string>{"1M", "16M", "256M", "1G"});
    options_tbl.emplace("blob_compression_type", GetBlobCompressionTags());
    options_tbl.emplace("enable_blob_garbage_collection",
                        std::vector<std::string>{"false", "true"});
    options_tbl.emplace(
        "blob_garbage_collection_age_cutoff",
        std::vector<std::string>{"0.0", "0.25", "0.5", "0.75", "1.0"});
    options_tbl.emplace("blob_garbage_collection_force_threshold",
                        std::vector<std::string>{"0.5", "0.75", "1.0"});
    options_tbl.emplace("blob_compaction_readahead_size",
                        std::vector<std::string>{"0", "1M", "4M"});
    options_tbl.emplace("blob_file_starting_level",
                        std::vector<std::string>{"0", "1", "2"});
    options_tbl.emplace("prepopulate_blob_cache",
                        std::vector<std::string>{"kDisable", "kFlushOnly"});
  }

  if (FLAGS_bloom_before_level != INT_MAX) {
    // Can modify RibbonFilterPolicy field
    options_tbl.emplace("table_factory.filter_policy.bloom_before_level",
                        std::vector<std::string>{"-1", "0", "1", "2",
                                                 "2147483646", "2147483647"});
  }

  options_table_ = std::move(options_tbl);

  for (const auto& iter : options_table_) {
    options_index_.push_back(iter.first);
  }
  return true;
}

void StressTest::InitDb(SharedState* shared) {
  uint64_t now = clock_->NowMicros();
  fprintf(stdout, "%s Initializing db_stress\n",
          clock_->TimeToString(now / 1000000).c_str());
  PrintEnv();
  Open(shared);
  BuildOptionsTable();
}

void StressTest::FinishInitDb(SharedState* shared) {
  if (FLAGS_read_only) {
    uint64_t now = clock_->NowMicros();
    fprintf(stdout, "%s Preloading db with %" PRIu64 " KVs\n",
            clock_->TimeToString(now / 1000000).c_str(), FLAGS_max_key);
    PreloadDbAndReopenAsReadOnly(FLAGS_max_key, shared);
  }

  if (shared->HasHistory()) {
    // The way it works right now is, if there's any history, that means the
    // previous run mutating the DB had all its operations traced, in which case
    // we should always be able to `Restore()` the expected values to match the
    // `db_`'s current seqno.
    Status s = shared->Restore(db_);
    if (!s.ok()) {
      fprintf(stderr, "Error restoring historical expected values: %s\n",
              s.ToString().c_str());
      exit(1);
    }
  }
  if (FLAGS_use_txn && !FLAGS_use_optimistic_txn) {
    // It's OK here without sync because unsynced data cannot be lost at this
    // point
    // - even with sync_fault_injection=1 as the
    // file is still directly writable until after FinishInitDb()
    ProcessRecoveredPreparedTxns(shared);
  }

  if (FLAGS_enable_compaction_filter) {
    auto* compaction_filter_factory =
        static_cast<DbStressCompactionFilterFactory*>(
            options_.compaction_filter_factory.get());
    assert(compaction_filter_factory);
    // This must be called only after any potential `SharedState::Restore()` has
    // completed in order for the `compaction_filter_factory` to operate on the
    // correct latest values file.
    compaction_filter_factory->SetSharedState(shared);
    fprintf(stdout, "Compaction filter factory: %s\n",
            compaction_filter_factory->Name());
  }
}

void StressTest::TrackExpectedState(SharedState* shared) {
  // When data loss is simulated, recovery from potential data loss is a prefix
  // recovery that requires tracing
  if (MightHaveUnsyncedDataLoss() && IsStateTracked()) {
    Status s = shared->SaveAtAndAfter(db_);
    if (!s.ok()) {
      fprintf(stderr, "Error enabling history tracing: %s\n",
              s.ToString().c_str());
      exit(1);
    }
  }
}

Status StressTest::AssertSame(DB* db, ColumnFamilyHandle* cf,
                              ThreadState::SnapshotState& snap_state) {
  Status s;
  if (cf->GetName() != snap_state.cf_at_name) {
    return s;
  }
  // This `ReadOptions` is for validation purposes. Ignore
  // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
  ReadOptions ropt;
  ropt.snapshot = snap_state.snapshot;
  Slice ts;
  if (!snap_state.timestamp.empty()) {
    ts = snap_state.timestamp;
    ropt.timestamp = &ts;
  }
  PinnableSlice exp_v(&snap_state.value);
  exp_v.PinSelf();
  PinnableSlice v;
  s = db->Get(ropt, cf, snap_state.key, &v);
  if (!s.ok() && !s.IsNotFound()) {
    // When `persist_user_defined_timestamps` is false, a repeated read with
    // both a read timestamp and an explicitly taken snapshot cannot guarantee
    // consistent result all the time. When it cannot return consistent result,
    // it will return an `InvalidArgument` status.
    if (s.IsInvalidArgument() && !FLAGS_persist_user_defined_timestamps) {
      return Status::OK();
    }
    return s;
  }
  if (snap_state.status != s) {
    return Status::Corruption(
        "The snapshot gave inconsistent results for key " +
        std::to_string(Hash(snap_state.key.c_str(), snap_state.key.size(), 0)) +
        " in cf " + cf->GetName() + ": (" + snap_state.status.ToString() +
        ") vs. (" + s.ToString() + ")");
  }
  if (s.ok()) {
    if (exp_v != v) {
      return Status::Corruption("The snapshot gave inconsistent values: (" +
                                exp_v.ToString() + ") vs. (" + v.ToString() +
                                ")");
    }
  }
  if (snap_state.key_vec != nullptr) {
    // When `prefix_extractor` is set, seeking to beginning and scanning
    // across prefixes are only supported with `total_order_seek` set.
    ropt.total_order_seek = true;
    std::unique_ptr<Iterator> iterator(db->NewIterator(ropt));
    std::unique_ptr<std::vector<bool>> tmp_bitvec(
        new std::vector<bool>(FLAGS_max_key));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      uint64_t key_val;
      if (GetIntVal(iterator->key().ToString(), &key_val)) {
        (*tmp_bitvec.get())[key_val] = true;
      }
    }
    if (!std::equal(snap_state.key_vec->begin(), snap_state.key_vec->end(),
                    tmp_bitvec.get()->begin())) {
      return Status::Corruption("Found inconsistent keys at this snapshot");
    }
  }
  return Status::OK();
}

void StressTest::ProcessStatus(SharedState* shared, std::string opname,
                               const Status& s,
                               bool ignore_injected_error) const {
  if (s.ok()) {
    return;
  }
  if (!ignore_injected_error || !IsErrorInjectedAndRetryable(s)) {
    std::ostringstream oss;
    oss << opname << " failed: " << s.ToString();
    VerificationAbort(shared, oss.str());
    assert(false);
  }
}

void StressTest::VerificationAbort(SharedState* shared, std::string msg) const {
  fprintf(stderr, "Verification failed: %s\n", msg.c_str());
  shared->SetVerificationFailure();
}

void StressTest::VerificationAbort(SharedState* shared, std::string msg, int cf,
                                   int64_t key) const {
  auto key_str = Key(key);
  Slice key_slice = key_str;
  fprintf(stderr,
          "Verification failed for column family %d key %s (%" PRIi64 "): %s\n",
          cf, key_slice.ToString(true).c_str(), key, msg.c_str());
  shared->SetVerificationFailure();
}

void StressTest::VerificationAbort(SharedState* shared, std::string msg, int cf,
                                   int64_t key, Slice value_from_db,
                                   Slice value_from_expected) const {
  auto key_str = Key(key);
  fprintf(stderr,
          "Verification failed for column family %d key %s (%" PRIi64
          "): value_from_db: %s, value_from_expected: %s, msg: %s\n",
          cf, Slice(key_str).ToString(true).c_str(), key,
          value_from_db.ToString(true).c_str(),
          value_from_expected.ToString(true).c_str(), msg.c_str());
  shared->SetVerificationFailure();
}

void StressTest::VerificationAbort(SharedState* shared, int cf, int64_t key,
                                   const Slice& value,
                                   const WideColumns& columns) const {
  assert(shared);

  auto key_str = Key(key);

  fprintf(stderr,
          "Verification failed for column family %d key %s (%" PRIi64
          "): Value and columns inconsistent: value: %s, columns: %s\n",
          cf, Slice(key_str).ToString(/* hex */ true).c_str(), key,
          value.ToString(/* hex */ true).c_str(),
          WideColumnsToHex(columns).c_str());

  shared->SetVerificationFailure();
}

std::string StressTest::DebugString(const Slice& value,
                                    const WideColumns& columns) {
  std::ostringstream oss;

  oss << "value: " << value.ToString(/* hex */ true)
      << ", columns: " << WideColumnsToHex(columns);

  return oss.str();
}

void StressTest::PrintStatistics() {
  if (dbstats) {
    fprintf(stdout, "STATISTICS:\n%s\n", dbstats->ToString().c_str());
  }
  if (dbstats_secondaries) {
    fprintf(stdout, "Secondary instances STATISTICS:\n%s\n",
            dbstats_secondaries->ToString().c_str());
  }
}

// Currently PreloadDb has to be single-threaded.
void StressTest::PreloadDbAndReopenAsReadOnly(int64_t number_of_keys,
                                              SharedState* shared) {
  WriteOptions write_opts;
  write_opts.disableWAL = FLAGS_disable_wal;
  if (FLAGS_sync) {
    write_opts.sync = true;
  }
  if (FLAGS_rate_limit_auto_wal_flush) {
    write_opts.rate_limiter_priority = Env::IO_USER;
  }
  char value[100];
  int cf_idx = 0;
  Status s;
  for (auto cfh : column_families_) {
    for (int64_t k = 0; k != number_of_keys; ++k) {
      const std::string key = Key(k);
      PendingExpectedValue pending_expected_value =
          shared->PreparePut(cf_idx, k);
      const uint32_t value_base = pending_expected_value.GetFinalValueBase();
      const size_t sz = GenerateValue(value_base, value, sizeof(value));

      const Slice v(value, sz);

      std::string ts;
      if (FLAGS_user_timestamp_size > 0) {
        ts = GetNowNanos();
      }

      if (FLAGS_use_put_entity_one_in > 0 &&
          (value_base % FLAGS_use_put_entity_one_in) == 0) {
        if (!FLAGS_use_txn) {
          if (FLAGS_use_attribute_group) {
            s = db_->PutEntity(write_opts, key,
                               GenerateAttributeGroups({cfh}, value_base, v));
          } else {
            s = db_->PutEntity(write_opts, cfh, key,
                               GenerateWideColumns(value_base, v));
          }
        } else {
          s = ExecuteTransaction(
              write_opts, /*thread=*/nullptr, [&](Transaction& txn) {
                return txn.PutEntity(cfh, key,
                                     GenerateWideColumns(value_base, v));
              });
        }
      } else if (FLAGS_use_merge) {
        if (!FLAGS_use_txn) {
          if (FLAGS_user_timestamp_size > 0) {
            s = db_->Merge(write_opts, cfh, key, ts, v);
          } else {
            s = db_->Merge(write_opts, cfh, key, v);
          }
        } else {
          s = ExecuteTransaction(
              write_opts, /*thread=*/nullptr,
              [&](Transaction& txn) { return txn.Merge(cfh, key, v); });
        }
      } else {
        if (!FLAGS_use_txn) {
          if (FLAGS_user_timestamp_size > 0) {
            s = db_->Put(write_opts, cfh, key, ts, v);
          } else {
            s = db_->Put(write_opts, cfh, key, v);
          }
        } else {
          s = ExecuteTransaction(
              write_opts, /*thread=*/nullptr,
              [&](Transaction& txn) { return txn.Put(cfh, key, v); });
        }
      }

      if (!s.ok()) {
        pending_expected_value.Rollback();
        break;
      }
      pending_expected_value.Commit();
    }
    if (!s.ok()) {
      break;
    }
    ++cf_idx;
  }
  if (s.ok()) {
    s = db_->Flush(FlushOptions(), column_families_);
  }
  if (s.ok()) {
    for (auto cf : column_families_) {
      delete cf;
    }
    column_families_.clear();
    delete db_;
    db_ = nullptr;
    txn_db_ = nullptr;
    optimistic_txn_db_ = nullptr;

    db_preload_finished_.store(true);
    auto now = clock_->NowMicros();
    fprintf(stdout, "%s Reopening database in read-only\n",
            clock_->TimeToString(now / 1000000).c_str());
    // Reopen as read-only, can ignore all options related to updates
    Open(shared);
  } else {
    fprintf(stderr, "Failed to preload db");
    exit(1);
  }
}

Status StressTest::SetOptions(ThreadState* thread) {
  assert(FLAGS_set_options_one_in > 0);
  std::unordered_map<std::string, std::string> opts;
  std::string name =
      options_index_[thread->rand.Next() % options_index_.size()];
  int value_idx = thread->rand.Next() % options_table_[name].size();
  if (name == "level0_file_num_compaction_trigger" ||
      name == "level0_slowdown_writes_trigger" ||
      name == "level0_stop_writes_trigger") {
    opts["level0_file_num_compaction_trigger"] =
        options_table_["level0_file_num_compaction_trigger"][value_idx];
    opts["level0_slowdown_writes_trigger"] =
        options_table_["level0_slowdown_writes_trigger"][value_idx];
    opts["level0_stop_writes_trigger"] =
        options_table_["level0_stop_writes_trigger"][value_idx];
  } else {
    opts[name] = options_table_[name][value_idx];
  }

  int rand_cf_idx = thread->rand.Next() % FLAGS_column_families;
  auto cfh = column_families_[rand_cf_idx];
  return db_->SetOptions(cfh, opts);
}

void StressTest::ProcessRecoveredPreparedTxns(SharedState* shared) {
  assert(txn_db_);
  std::vector<Transaction*> recovered_prepared_trans;
  txn_db_->GetAllPreparedTransactions(&recovered_prepared_trans);
  for (Transaction* txn : recovered_prepared_trans) {
    ProcessRecoveredPreparedTxnsHelper(txn, shared);
    delete txn;
  }
  recovered_prepared_trans.clear();
  txn_db_->GetAllPreparedTransactions(&recovered_prepared_trans);
  assert(recovered_prepared_trans.size() == 0);
}

void StressTest::ProcessRecoveredPreparedTxnsHelper(Transaction* txn,
                                                    SharedState* shared) {
  thread_local Random rand(static_cast<uint32_t>(FLAGS_seed));
  for (size_t i = 0; i < column_families_.size(); ++i) {
    std::unique_ptr<WBWIIterator> wbwi_iter(
        txn->GetWriteBatch()->NewIterator(column_families_[i]));
    for (wbwi_iter->SeekToFirst(); wbwi_iter->Valid(); wbwi_iter->Next()) {
      uint64_t key_val;
      if (GetIntVal(wbwi_iter->Entry().key.ToString(), &key_val)) {
        shared->SyncPendingPut(static_cast<int>(i) /* cf_idx */, key_val);
      }
    }
  }
  if (rand.OneIn(2)) {
    Status s = txn->Commit();
    assert(s.ok());
  } else {
    Status s = txn->Rollback();
    assert(s.ok());
  }
}

Status StressTest::NewTxn(WriteOptions& write_opts,
                          std::unique_ptr<Transaction>* out_txn) {
  if (!FLAGS_use_txn) {
    return Status::InvalidArgument("NewTxn when FLAGS_use_txn is not set");
  }
  write_opts.disableWAL = FLAGS_disable_wal;
  static std::atomic<uint64_t> txn_id = {0};
  if (FLAGS_use_optimistic_txn) {
    out_txn->reset(optimistic_txn_db_->BeginTransaction(write_opts));
    return Status::OK();
  } else {
    TransactionOptions txn_options;
    txn_options.use_only_the_last_commit_time_batch_for_recovery =
        FLAGS_use_only_the_last_commit_time_batch_for_recovery;
    txn_options.lock_timeout = 600000;  // 10 min
    txn_options.deadlock_detect = true;
    out_txn->reset(txn_db_->BeginTransaction(write_opts, txn_options));
    auto istr = std::to_string(txn_id.fetch_add(1));
    Status s = (*out_txn)->SetName("xid" + istr);
    return s;
  }
}

Status StressTest::CommitTxn(Transaction& txn, ThreadState* thread) {
  if (!FLAGS_use_txn) {
    return Status::InvalidArgument("CommitTxn when FLAGS_use_txn is not set");
  }
  Status s = Status::OK();
  if (FLAGS_use_optimistic_txn) {
    assert(optimistic_txn_db_);
    s = txn.Commit();
  } else {
    assert(txn_db_);
    s = txn.Prepare();
    std::shared_ptr<const Snapshot> timestamped_snapshot;
    if (s.ok()) {
      if (thread && FLAGS_create_timestamped_snapshot_one_in &&
          thread->rand.OneIn(FLAGS_create_timestamped_snapshot_one_in)) {
        uint64_t ts = db_stress_env->NowNanos();
        s = txn.CommitAndTryCreateSnapshot(/*notifier=*/nullptr, ts,
                                           &timestamped_snapshot);

        std::pair<Status, std::shared_ptr<const Snapshot>> res;
        if (thread->tid == 0) {
          uint64_t now = db_stress_env->NowNanos();
          res = txn_db_->CreateTimestampedSnapshot(now);
          if (res.first.ok()) {
            assert(res.second);
            assert(res.second->GetTimestamp() == now);
            if (timestamped_snapshot) {
              assert(res.second->GetTimestamp() >
                     timestamped_snapshot->GetTimestamp());
            }
          } else {
            assert(!res.second);
          }
        }
      } else {
        s = txn.Commit();
      }
    }
    if (thread && FLAGS_create_timestamped_snapshot_one_in > 0 &&
        thread->rand.OneInOpt(50000)) {
      uint64_t now = db_stress_env->NowNanos();
      constexpr uint64_t time_diff = static_cast<uint64_t>(1000) * 1000 * 1000;
      txn_db_->ReleaseTimestampedSnapshotsOlderThan(now - time_diff);
    }
  }
  return s;
}

Status StressTest::ExecuteTransaction(
    WriteOptions& write_opts, ThreadState* thread,
    std::function<Status(Transaction&)>&& ops) {
  std::unique_ptr<Transaction> txn;
  Status s = NewTxn(write_opts, &txn);
  std::string try_again_messages;
  if (s.ok()) {
    for (int tries = 1;; ++tries) {
      s = ops(*txn);
      if (s.ok()) {
        s = CommitTxn(*txn, thread);
        if (s.ok()) {
          break;
        }
      }
      // Optimistic txn might return TryAgain, in which case rollback
      // and try again.
      if (!s.IsTryAgain() || !FLAGS_use_optimistic_txn) {
        break;
      }
      // Record and report historical TryAgain messages for debugging
      try_again_messages +=
          std::to_string(SystemClock::Default()->NowMicros() / 1000);
      try_again_messages += "ms ";
      try_again_messages += s.getState();
      try_again_messages += "\n";
      // In theory, each Rollback after TryAgain should have an independent
      // chance of success, so too many retries could indicate something is
      // not working properly.
      if (tries >= 10) {
        s = Status::TryAgain(try_again_messages);
        break;
      }
      s = txn->Rollback();
      if (!s.ok()) {
        break;
      }
    }
  }
  return s;
}

void StressTest::OperateDb(ThreadState* thread) {
  ReadOptions read_opts(FLAGS_verify_checksum, true);
  read_opts.rate_limiter_priority =
      FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
  read_opts.async_io = FLAGS_async_io;
  read_opts.adaptive_readahead = FLAGS_adaptive_readahead;
  read_opts.readahead_size = FLAGS_readahead_size;
  read_opts.auto_readahead_size = FLAGS_auto_readahead_size;
  read_opts.fill_cache = FLAGS_fill_cache;
  read_opts.optimize_multiget_for_io = FLAGS_optimize_multiget_for_io;
  WriteOptions write_opts;
  if (FLAGS_rate_limit_auto_wal_flush) {
    write_opts.rate_limiter_priority = Env::IO_USER;
  }
  write_opts.memtable_insert_hint_per_batch =
      FLAGS_memtable_insert_hint_per_batch;
  auto shared = thread->shared;
  char value[100];
  std::string from_db;
  if (FLAGS_sync) {
    write_opts.sync = true;
  }
  write_opts.disableWAL = FLAGS_disable_wal;
  write_opts.protection_bytes_per_key = FLAGS_batch_protection_bytes_per_key;
  const int prefix_bound = static_cast<int>(FLAGS_readpercent) +
                           static_cast<int>(FLAGS_prefixpercent);
  const int write_bound = prefix_bound + static_cast<int>(FLAGS_writepercent);
  const int del_bound = write_bound + static_cast<int>(FLAGS_delpercent);
  const int delrange_bound =
      del_bound + static_cast<int>(FLAGS_delrangepercent);
  const int iterate_bound =
      delrange_bound + static_cast<int>(FLAGS_iterpercent);

  const uint64_t ops_per_open = FLAGS_ops_per_thread / (FLAGS_reopen + 1);

  thread->stats.Start();
  for (int open_cnt = 0; open_cnt <= FLAGS_reopen; ++open_cnt) {
    if (thread->shared->HasVerificationFailedYet() ||
        thread->shared->ShouldStopTest()) {
      break;
    }
    if (open_cnt != 0) {
      thread->stats.FinishedSingleOp();
      MutexLock l(thread->shared->GetMutex());
      while (!thread->snapshot_queue.empty()) {
        db_->ReleaseSnapshot(thread->snapshot_queue.front().second.snapshot);
        delete thread->snapshot_queue.front().second.key_vec;
        thread->snapshot_queue.pop();
      }
      thread->shared->IncVotedReopen();
      if (thread->shared->AllVotedReopen()) {
        thread->shared->GetStressTest()->Reopen(thread);
        thread->shared->GetCondVar()->SignalAll();
      } else {
        thread->shared->GetCondVar()->Wait();
      }
      // Commenting this out as we don't want to reset stats on each open.
      // thread->stats.Start();
    }

#ifndef NDEBUG
    if (fault_fs_guard) {
      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kRead, thread->shared->GetSeed(),
          FLAGS_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kRead);

      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kWrite, thread->shared->GetSeed(),
          FLAGS_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kWrite);

      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataRead, thread->shared->GetSeed(),
          FLAGS_metadata_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);

      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataWrite, thread->shared->GetSeed(),
          FLAGS_metadata_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataWrite);
    }
#endif  // NDEBUG

    for (uint64_t i = 0; i < ops_per_open; i++) {
      if (thread->shared->HasVerificationFailedYet()) {
        break;
      }

      // Change Options
      if (thread->rand.OneInOpt(FLAGS_set_options_one_in)) {
        Status s = SetOptions(thread);
        ProcessStatus(shared, "SetOptions", s);
      }

      if (thread->rand.OneInOpt(FLAGS_set_in_place_one_in)) {
        options_.inplace_update_support ^= options_.inplace_update_support;
      }

      if (thread->tid == 0 && FLAGS_verify_db_one_in > 0 &&
          thread->rand.OneIn(FLAGS_verify_db_one_in)) {
        //  Temporarily disable error injection for verification
        if (fault_fs_guard) {
          fault_fs_guard->DisableAllThreadLocalErrorInjection();
        }
        ContinuouslyVerifyDb(thread);
        //  Enable back error injection disabled for verification
        if (fault_fs_guard) {
          fault_fs_guard->EnableAllThreadLocalErrorInjection();
        }
        if (thread->shared->ShouldStopTest()) {
          break;
        }
      }

      MaybeClearOneColumnFamily(thread);

      if (thread->rand.OneInOpt(FLAGS_manual_wal_flush_one_in)) {
        bool sync = thread->rand.OneIn(2) ? true : false;
        Status s = db_->FlushWAL(sync);
        if (!s.ok() && !IsErrorInjectedAndRetryable(s) &&
            !(sync && s.IsNotSupported())) {
          fprintf(stderr, "FlushWAL(sync=%s) failed: %s\n",
                  (sync ? "true" : "false"), s.ToString().c_str());
        }
      }

      if (thread->rand.OneInOpt(FLAGS_lock_wal_one_in)) {
        Status s = db_->LockWAL();
        if (!s.ok() && !IsErrorInjectedAndRetryable(s)) {
          fprintf(stderr, "LockWAL() failed: %s\n", s.ToString().c_str());
        } else if (s.ok()) {
          //  Temporarily disable error injection for verification
          if (fault_fs_guard) {
            fault_fs_guard->DisableAllThreadLocalErrorInjection();
          }

          // Verify no writes during LockWAL
          auto old_seqno = db_->GetLatestSequenceNumber();
          // And also that WAL is not changed during LockWAL()
          std::unique_ptr<WalFile> old_wal;
          s = db_->GetCurrentWalFile(&old_wal);
          if (!s.ok()) {
            fprintf(stderr, "GetCurrentWalFile() failed: %s\n",
                    s.ToString().c_str());
          } else {
            // Yield for a while
            do {
              std::this_thread::yield();
            } while (thread->rand.OneIn(2));
            // Current WAL and size should not have changed
            std::unique_ptr<WalFile> new_wal;
            s = db_->GetCurrentWalFile(&new_wal);
            if (!s.ok()) {
              fprintf(stderr, "GetCurrentWalFile() failed: %s\n",
                      s.ToString().c_str());
            } else {
              if (old_wal->LogNumber() != new_wal->LogNumber()) {
                fprintf(stderr,
                        "Failed: WAL number changed during LockWAL(): %" PRIu64
                        " to %" PRIu64 "\n",
                        old_wal->LogNumber(), new_wal->LogNumber());
              }
              if (old_wal->SizeFileBytes() != new_wal->SizeFileBytes()) {
                fprintf(stderr,
                        "Failed: WAL %" PRIu64
                        " size changed during LockWAL(): %" PRIu64
                        " to %" PRIu64 "\n",
                        old_wal->LogNumber(), old_wal->SizeFileBytes(),
                        new_wal->SizeFileBytes());
              }
            }
          }
          // Verify no writes during LockWAL
          auto new_seqno = db_->GetLatestSequenceNumber();
          if (old_seqno != new_seqno) {
            fprintf(
                stderr,
                "Failure: latest seqno changed from %u to %u with WAL locked\n",
                (unsigned)old_seqno, (unsigned)new_seqno);
          }
          // Verification done. Now unlock WAL
          s = db_->UnlockWAL();
          if (!s.ok()) {
            fprintf(stderr, "UnlockWAL() failed: %s\n", s.ToString().c_str());
          }

          //  Enable back error injection disabled for verification
          if (fault_fs_guard) {
            fault_fs_guard->EnableAllThreadLocalErrorInjection();
          }
        }
      }

      if (thread->rand.OneInOpt(FLAGS_sync_wal_one_in)) {
        Status s = db_->SyncWAL();
        if (!s.ok() && !s.IsNotSupported() && !IsErrorInjectedAndRetryable(s)) {
          fprintf(stderr, "SyncWAL() failed: %s\n", s.ToString().c_str());
        }
      }

      int rand_column_family = thread->rand.Next() % FLAGS_column_families;
      ColumnFamilyHandle* column_family = column_families_[rand_column_family];

      if (thread->rand.OneInOpt(FLAGS_compact_files_one_in)) {
        TestCompactFiles(thread, column_family);
      }

      int64_t rand_key = GenerateOneKey(thread, i);
      std::string keystr = Key(rand_key);
      Slice key = keystr;

      if (thread->rand.OneInOpt(FLAGS_compact_range_one_in)) {
        TestCompactRange(thread, rand_key, key, column_family);
        if (thread->shared->HasVerificationFailedYet()) {
          break;
        }
      }

      if (thread->rand.OneInOpt(FLAGS_promote_l0_one_in)) {
        TestPromoteL0(thread, column_family);
      }

      std::vector<int> rand_column_families =
          GenerateColumnFamilies(FLAGS_column_families, rand_column_family);

      if (thread->rand.OneInOpt(FLAGS_flush_one_in)) {
        Status status = TestFlush(rand_column_families);
        ProcessStatus(shared, "Flush", status);
      }

      if (thread->rand.OneInOpt(FLAGS_get_live_files_apis_one_in)) {
        Status s_1 = TestGetLiveFiles();
        ProcessStatus(shared, "GetLiveFiles", s_1);
        Status s_2 = TestGetLiveFilesMetaData();
        ProcessStatus(shared, "GetLiveFilesMetaData", s_2);
        // TODO: enable again after making `GetLiveFilesStorageInfo()`
        // compatible with `Options::recycle_log_file_num`
        if (FLAGS_recycle_log_file_num == 0) {
          Status s_3 = TestGetLiveFilesStorageInfo();
          ProcessStatus(shared, "GetLiveFilesStorageInfo", s_3);
        }
      }

      if (thread->rand.OneInOpt(FLAGS_get_all_column_family_metadata_one_in)) {
        Status status = TestGetAllColumnFamilyMetaData();
        ProcessStatus(shared, "GetAllColumnFamilyMetaData", status);
      }

      if (thread->rand.OneInOpt(FLAGS_get_sorted_wal_files_one_in)) {
        Status status = TestGetSortedWalFiles();
        ProcessStatus(shared, "GetSortedWalFiles", status);
      }

      if (thread->rand.OneInOpt(FLAGS_get_current_wal_file_one_in)) {
        Status status = TestGetCurrentWalFile();
        ProcessStatus(shared, "GetCurrentWalFile", status);
      }

      if (thread->rand.OneInOpt(FLAGS_reset_stats_one_in)) {
        Status status = TestResetStats();
        ProcessStatus(shared, "ResetStats", status);
      }

      if (thread->rand.OneInOpt(FLAGS_pause_background_one_in)) {
        Status status = TestPauseBackground(thread);
        ProcessStatus(shared, "Pause/ContinueBackgroundWork", status);
      }

      if (thread->rand.OneInOpt(FLAGS_disable_file_deletions_one_in)) {
        Status status = TestDisableFileDeletions(thread);
        ProcessStatus(shared, "TestDisableFileDeletions", status);
      }

      if (thread->rand.OneInOpt(FLAGS_disable_manual_compaction_one_in)) {
        Status status = TestDisableManualCompaction(thread);
        ProcessStatus(shared, "TestDisableManualCompaction", status);
      }

      if (thread->rand.OneInOpt(FLAGS_verify_checksum_one_in)) {
        ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
        ThreadStatusUtil::SetThreadOperation(
            ThreadStatus::OperationType::OP_VERIFY_DB_CHECKSUM);
        Status status = db_->VerifyChecksum();
        ThreadStatusUtil::ResetThreadStatus();
        ProcessStatus(shared, "VerifyChecksum", status);
      }

      if (thread->rand.OneInOpt(FLAGS_verify_file_checksums_one_in)) {
        ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
        ThreadStatusUtil::SetThreadOperation(
            ThreadStatus::OperationType::OP_VERIFY_FILE_CHECKSUMS);
        Status status = db_->VerifyFileChecksums(read_opts);
        ThreadStatusUtil::ResetThreadStatus();
        ProcessStatus(shared, "VerifyFileChecksums", status);
      }

      if (thread->rand.OneInOpt(FLAGS_get_property_one_in)) {
        // TestGetProperty doesn't return status for us to tell whether it has
        // failed due to injected error. So we disable fault injection to avoid
        // false positive
        if (fault_fs_guard) {
          fault_fs_guard->DisableAllThreadLocalErrorInjection();
        }

        TestGetProperty(thread);

        if (fault_fs_guard) {
          fault_fs_guard->EnableAllThreadLocalErrorInjection();
        }
      }

      if (thread->rand.OneInOpt(FLAGS_get_properties_of_all_tables_one_in)) {
        Status status = TestGetPropertiesOfAllTables();
        ProcessStatus(shared, "TestGetPropertiesOfAllTables", status);
      }

      std::vector<int64_t> rand_keys = GenerateKeys(rand_key);

      if (thread->rand.OneInOpt(FLAGS_ingest_external_file_one_in)) {
        TestIngestExternalFile(thread, rand_column_families, rand_keys);
      }

      if (thread->rand.OneInOpt(FLAGS_backup_one_in)) {
        // Beyond a certain DB size threshold, this test becomes heavier than
        // it's worth.
        uint64_t total_size = 0;
        if (FLAGS_backup_max_size > 0) {
          std::vector<FileAttributes> files;
          db_stress_env->GetChildrenFileAttributes(FLAGS_db, &files);
          for (auto& file : files) {
            total_size += file.size_bytes;
          }
        }

        if (total_size <= FLAGS_backup_max_size) {
          // TODO(hx235): enable error injection with
          // backup/restore after fixing the various issues it surfaces
          if (fault_fs_guard) {
            fault_fs_guard->DisableAllThreadLocalErrorInjection();
          }
          Status s = TestBackupRestore(thread, rand_column_families, rand_keys);
          if (fault_fs_guard) {
            fault_fs_guard->EnableAllThreadLocalErrorInjection();
          }
          ProcessStatus(shared, "Backup/restore", s);
        }
      }

      if (thread->rand.OneInOpt(FLAGS_checkpoint_one_in)) {
        Status s = TestCheckpoint(thread, rand_column_families, rand_keys);
        ProcessStatus(shared, "Checkpoint", s);
      }

      if (thread->rand.OneInOpt(FLAGS_approximate_size_one_in)) {
        Status s =
            TestApproximateSize(thread, i, rand_column_families, rand_keys);
        ProcessStatus(shared, "ApproximateSize", s);
      }
      if (thread->rand.OneInOpt(FLAGS_acquire_snapshot_one_in)) {
        TestAcquireSnapshot(thread, rand_column_family, keystr, i);
      }

      /*always*/ {
        Status s = MaybeReleaseSnapshots(thread, i);
        ProcessStatus(shared, "Snapshot", s);
      }

      // Assign timestamps if necessary.
      std::string read_ts_str;
      Slice read_ts;
      if (FLAGS_user_timestamp_size > 0) {
        read_ts_str = GetNowNanos();
        read_ts = read_ts_str;
        read_opts.timestamp = &read_ts;
      }

      if (thread->rand.OneInOpt(FLAGS_key_may_exist_one_in)) {
        TestKeyMayExist(thread, read_opts, rand_column_families, rand_keys);
      }
      // Prefix-recoverability relies on tracing successful user writes.
      // Currently we trace all user writes regardless of whether it later
      // succeeds or not. To simplify, we disable any fault injection during
      // user write.
      // TODO(hx235): support tracing user writes with fault injection.
      bool disable_fault_injection_during_user_write =
          fault_fs_guard && MightHaveUnsyncedDataLoss();
      int prob_op = thread->rand.Uniform(100);
      // Reset this in case we pick something other than a read op. We don't
      // want to use a stale value when deciding at the beginning of the loop
      // whether to vote to reopen
      if (prob_op >= 0 && prob_op < static_cast<int>(FLAGS_readpercent)) {
        assert(0 <= prob_op);
        // OPERATION read
        ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
        if (FLAGS_use_multi_get_entity) {
          constexpr uint64_t max_batch_size = 64;
          const uint64_t batch_size = std::min(
              static_cast<uint64_t>(thread->rand.Uniform(max_batch_size)) + 1,
              ops_per_open - i);
          assert(batch_size >= 1);
          assert(batch_size <= max_batch_size);
          assert(i + batch_size <= ops_per_open);

          rand_keys = GenerateNKeys(thread, static_cast<int>(batch_size), i);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_MULTIGETENTITY);
          TestMultiGetEntity(thread, read_opts, rand_column_families,
                             rand_keys);
          i += batch_size - 1;
        } else if (FLAGS_use_get_entity) {
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_GETENTITY);
          TestGetEntity(thread, read_opts, rand_column_families, rand_keys);
        } else if (FLAGS_use_multiget) {
          // Leave room for one more iteration of the loop with a single key
          // batch. This is to ensure that each thread does exactly the same
          // number of ops
          int multiget_batch_size = static_cast<int>(
              std::min(static_cast<uint64_t>(thread->rand.Uniform(64)),
                       FLAGS_ops_per_thread - i - 1));
          // If its the last iteration, ensure that multiget_batch_size is 1
          multiget_batch_size = std::max(multiget_batch_size, 1);
          rand_keys = GenerateNKeys(thread, multiget_batch_size, i);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_MULTIGET);
          TestMultiGet(thread, read_opts, rand_column_families, rand_keys);
          i += multiget_batch_size - 1;
        } else {
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_GET);
          TestGet(thread, read_opts, rand_column_families, rand_keys);
        }
        ThreadStatusUtil::ResetThreadStatus();
      } else if (prob_op < prefix_bound) {
        assert(static_cast<int>(FLAGS_readpercent) <= prob_op);
        // OPERATION prefix scan
        // keys are 8 bytes long, prefix size is FLAGS_prefix_size. There are
        // (8 - FLAGS_prefix_size) bytes besides the prefix. So there will
        // be 2 ^ ((8 - FLAGS_prefix_size) * 8) possible keys with the same
        // prefix
        TestPrefixScan(thread, read_opts, rand_column_families, rand_keys);
      } else if (prob_op < write_bound) {
        assert(prefix_bound <= prob_op);
        // OPERATION write
        if (disable_fault_injection_during_user_write) {
          fault_fs_guard->DisableAllThreadLocalErrorInjection();
        }
        TestPut(thread, write_opts, read_opts, rand_column_families, rand_keys,
                value);
        if (disable_fault_injection_during_user_write) {
          fault_fs_guard->EnableAllThreadLocalErrorInjection();
        }
      } else if (prob_op < del_bound) {
        assert(write_bound <= prob_op);
        // OPERATION delete
        if (disable_fault_injection_during_user_write) {
          fault_fs_guard->DisableAllThreadLocalErrorInjection();
        }
        TestDelete(thread, write_opts, rand_column_families, rand_keys);
        if (disable_fault_injection_during_user_write) {
          fault_fs_guard->EnableAllThreadLocalErrorInjection();
        }
      } else if (prob_op < delrange_bound) {
        assert(del_bound <= prob_op);
        // OPERATION delete range
        if (disable_fault_injection_during_user_write) {
          fault_fs_guard->DisableAllThreadLocalErrorInjection();
        }
        TestDeleteRange(thread, write_opts, rand_column_families, rand_keys);
        if (disable_fault_injection_during_user_write) {
          fault_fs_guard->EnableAllThreadLocalErrorInjection();
        }
      } else if (prob_op < iterate_bound) {
        assert(delrange_bound <= prob_op);
        // OPERATION iterate
        if (!FLAGS_skip_verifydb &&
            thread->rand.OneInOpt(
                FLAGS_verify_iterator_with_expected_state_one_in)) {
          ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_DBITERATOR);
          TestIterateAgainstExpected(thread, read_opts, rand_column_families,
                                     rand_keys);
          ThreadStatusUtil::ResetThreadStatus();
        } else {
          int num_seeks = static_cast<int>(std::min(
              std::max(static_cast<uint64_t>(thread->rand.Uniform(4)),
                       static_cast<uint64_t>(1)),
              std::max(static_cast<uint64_t>(FLAGS_ops_per_thread - i - 1),
                       static_cast<uint64_t>(1))));
          rand_keys = GenerateNKeys(thread, num_seeks, i);
          i += num_seeks - 1;
          ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_DBITERATOR);
          Status s;
          if (FLAGS_use_multi_cf_iterator && FLAGS_use_attribute_group) {
            s = TestIterateAttributeGroups(thread, read_opts,
                                           rand_column_families, rand_keys);
            ProcessStatus(shared, "IterateAttributeGroups", s);
          } else {
            s = TestIterate(thread, read_opts, rand_column_families, rand_keys);
            ProcessStatus(shared, "Iterate", s);
          }
          ThreadStatusUtil::ResetThreadStatus();
        }
      } else {
        assert(iterate_bound <= prob_op);
        TestCustomOperations(thread, rand_column_families);
      }
      thread->stats.FinishedSingleOp();
    }

#ifndef NDEBUG
    if (fault_fs_guard) {
      fault_fs_guard->DisableAllThreadLocalErrorInjection();
    }
#endif  // NDEBUG
  }
  while (!thread->snapshot_queue.empty()) {
    db_->ReleaseSnapshot(thread->snapshot_queue.front().second.snapshot);
    delete thread->snapshot_queue.front().second.key_vec;
    thread->snapshot_queue.pop();
  }

  thread->stats.Stop();
}

// Generated a list of keys that close to boundaries of SST keys.
// If there isn't any SST file in the DB, return empty list.
std::vector<std::string> StressTest::GetWhiteBoxKeys(ThreadState* thread,
                                                     DB* db,
                                                     ColumnFamilyHandle* cfh,
                                                     size_t num_keys) {
  ColumnFamilyMetaData cfmd;
  db->GetColumnFamilyMetaData(cfh, &cfmd);
  std::vector<std::string> boundaries;
  for (const LevelMetaData& lmd : cfmd.levels) {
    for (const SstFileMetaData& sfmd : lmd.files) {
      // If FLAGS_user_timestamp_size > 0, then both smallestkey and largestkey
      // have timestamps.
      const auto& skey = sfmd.smallestkey;
      const auto& lkey = sfmd.largestkey;
      assert(skey.size() >= FLAGS_user_timestamp_size);
      assert(lkey.size() >= FLAGS_user_timestamp_size);
      boundaries.push_back(
          skey.substr(0, skey.size() - FLAGS_user_timestamp_size));
      boundaries.push_back(
          lkey.substr(0, lkey.size() - FLAGS_user_timestamp_size));
    }
  }
  if (boundaries.empty()) {
    return {};
  }

  std::vector<std::string> ret;
  for (size_t j = 0; j < num_keys; j++) {
    std::string k =
        boundaries[thread->rand.Uniform(static_cast<int>(boundaries.size()))];
    if (thread->rand.OneIn(3)) {
      // Reduce one byte from the string
      for (int i = static_cast<int>(k.length()) - 1; i >= 0; i--) {
        uint8_t cur = k[i];
        if (cur > 0) {
          k[i] = static_cast<char>(cur - 1);
          break;
        } else if (i > 0) {
          k[i] = 0xFFu;
        }
      }
    } else if (thread->rand.OneIn(2)) {
      // Add one byte to the string
      for (int i = static_cast<int>(k.length()) - 1; i >= 0; i--) {
        uint8_t cur = k[i];
        if (cur < 255) {
          k[i] = static_cast<char>(cur + 1);
          break;
        } else if (i > 0) {
          k[i] = 0x00;
        }
      }
    }
    ret.push_back(k);
  }
  return ret;
}

// Given a key K, this creates an iterator which scans to K and then
// does a random sequence of Next/Prev operations.
Status StressTest::TestIterate(ThreadState* thread,
                               const ReadOptions& read_opts,
                               const std::vector<int>& rand_column_families,
                               const std::vector<int64_t>& rand_keys) {
  auto new_iter_func = [&rand_column_families, this](const ReadOptions& ro) {
    if (FLAGS_use_multi_cf_iterator) {
      std::vector<ColumnFamilyHandle*> cfhs;
      cfhs.reserve(rand_column_families.size());
      for (auto cf_index : rand_column_families) {
        cfhs.emplace_back(column_families_[cf_index]);
      }
      assert(!cfhs.empty());
      return db_->NewCoalescingIterator(ro, cfhs);
    } else {
      ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
      assert(cfh);
      return std::unique_ptr<Iterator>(db_->NewIterator(ro, cfh));
    }
  };

  auto verify_func = [](Iterator* iter) {
    if (!VerifyWideColumns(iter->value(), iter->columns())) {
      fprintf(stderr,
              "Value and columns inconsistent for iterator: value: %s, "
              "columns: %s\n",
              iter->value().ToString(/* hex */ true).c_str(),
              WideColumnsToHex(iter->columns()).c_str());
      return false;
    }
    return true;
  };

  return TestIterateImpl<Iterator>(thread, read_opts, rand_column_families,
                                   rand_keys, new_iter_func, verify_func);
}

Status StressTest::TestIterateAttributeGroups(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  auto new_iter_func = [&rand_column_families, this](const ReadOptions& ro) {
    assert(FLAGS_use_multi_cf_iterator);
    std::vector<ColumnFamilyHandle*> cfhs;
    cfhs.reserve(rand_column_families.size());
    for (auto cf_index : rand_column_families) {
      cfhs.emplace_back(column_families_[cf_index]);
    }
    assert(!cfhs.empty());
    return db_->NewAttributeGroupIterator(ro, cfhs);
  };
  auto verify_func = [](AttributeGroupIterator* iter) {
    if (!VerifyIteratorAttributeGroups(iter->attribute_groups())) {
      // TODO - print out attribute group values
      fprintf(stderr,
              "one of the columns in the attribute groups inconsistent for "
              "iterator\n");
      return false;
    }
    return true;
  };

  return TestIterateImpl<AttributeGroupIterator>(
      thread, read_opts, rand_column_families, rand_keys, new_iter_func,
      verify_func);
}

template <typename IterType, typename NewIterFunc, typename VerifyFunc>
Status StressTest::TestIterateImpl(ThreadState* thread,
                                   const ReadOptions& read_opts,
                                   const std::vector<int>& rand_column_families,
                                   const std::vector<int64_t>& rand_keys,
                                   NewIterFunc new_iter_func,
                                   VerifyFunc verify_func) {
  assert(!rand_column_families.empty());
  assert(!rand_keys.empty());

  ManagedSnapshot snapshot_guard(db_);

  ReadOptions ro = read_opts;
  ro.snapshot = snapshot_guard.snapshot();

  std::string read_ts_str;
  Slice read_ts_slice;
  MaybeUseOlderTimestampForRangeScan(thread, read_ts_str, read_ts_slice, ro);

  std::string op_logs;
  ro.pin_data = thread->rand.OneIn(2);
  ro.background_purge_on_iterator_cleanup = thread->rand.OneIn(2);

  bool expect_total_order = false;
  if (thread->rand.OneIn(16)) {
    // When prefix extractor is used, it's useful to cover total order seek.
    ro.total_order_seek = true;
    expect_total_order = true;
  } else if (thread->rand.OneIn(4)) {
    ro.total_order_seek = thread->rand.OneIn(2);
    ro.auto_prefix_mode = true;
    expect_total_order = true;
  } else if (options_.prefix_extractor.get() == nullptr) {
    expect_total_order = true;
  }
  std::string upper_bound_str;
  Slice upper_bound;
  // Prefer no bound with no range query filtering; prefer bound with it
  if (FLAGS_use_sqfc_for_range_queries ^ thread->rand.OneIn(16)) {
    // Note: upper_bound can be smaller than the seek key.
    const int64_t rand_upper_key = GenerateOneKey(thread, FLAGS_ops_per_thread);
    upper_bound_str = Key(rand_upper_key);
    upper_bound = Slice(upper_bound_str);
    ro.iterate_upper_bound = &upper_bound;
  }

  std::string lower_bound_str;
  Slice lower_bound;
  if (FLAGS_use_sqfc_for_range_queries ^ thread->rand.OneIn(16)) {
    // Note: lower_bound can be greater than the seek key.
    const int64_t rand_lower_key = GenerateOneKey(thread, FLAGS_ops_per_thread);
    lower_bound_str = Key(rand_lower_key);
    lower_bound = Slice(lower_bound_str);
    ro.iterate_lower_bound = &lower_bound;
  }

  if (FLAGS_use_sqfc_for_range_queries && ro.iterate_upper_bound &&
      ro.iterate_lower_bound) {
    ro.table_filter = sqfc_factory_->GetTableFilterForRangeQuery(
        *ro.iterate_lower_bound, *ro.iterate_upper_bound);
  }

  std::unique_ptr<IterType> iter = new_iter_func(ro);

  std::vector<std::string> key_strs;
  if (thread->rand.OneIn(16)) {
    // Generate keys close to lower or upper bound of SST files.
    key_strs =
        GetWhiteBoxKeys(thread, db_, column_families_[rand_column_families[0]],
                        rand_keys.size());
  }
  if (key_strs.empty()) {
    // Use the random keys passed in.
    for (int64_t rkey : rand_keys) {
      key_strs.push_back(Key(rkey));
    }
  }

  constexpr size_t kOpLogsLimit = 10000;

  for (const std::string& key_str : key_strs) {
    if (op_logs.size() > kOpLogsLimit) {
      // Shouldn't take too much memory for the history log. Clear it.
      op_logs = "(cleared...)\n";
    }

    if (!FLAGS_use_sqfc_for_range_queries &&
        ro.iterate_upper_bound != nullptr && thread->rand.OneIn(2)) {
      // With a 1/2 chance, change the upper bound.
      // Not compatible with sqfc range filter.
      // It is possible that it is changed before first use, but there is no
      // problem with that.
      const int64_t rand_upper_key =
          GenerateOneKey(thread, FLAGS_ops_per_thread);
      upper_bound_str = Key(rand_upper_key);
      upper_bound = Slice(upper_bound_str);
    }
    if (!FLAGS_use_sqfc_for_range_queries &&
        ro.iterate_lower_bound != nullptr && thread->rand.OneIn(4)) {
      // With a 1/4 chance, change the lower bound.
      // Not compatible with sqfc range filter.
      // It is possible that it is changed before first use, but there is no
      // problem with that.
      const int64_t rand_lower_key =
          GenerateOneKey(thread, FLAGS_ops_per_thread);
      lower_bound_str = Key(rand_lower_key);
      lower_bound = Slice(lower_bound_str);
    }

    // Set up an iterator, perform the same operations without bounds and with
    // total order seek, and compare the results. This is to identify bugs
    // related to bounds, prefix extractor, or reseeking. Sometimes we are
    // comparing iterators with the same set-up, and it doesn't hurt to check
    // them to be equal.
    //
    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions cmp_ro;
    cmp_ro.timestamp = ro.timestamp;
    cmp_ro.iter_start_ts = ro.iter_start_ts;
    cmp_ro.snapshot = snapshot_guard.snapshot();
    cmp_ro.total_order_seek = true;

    ColumnFamilyHandle* const cmp_cfh =
        GetControlCfh(thread, rand_column_families[0]);
    assert(cmp_cfh);

    std::unique_ptr<Iterator> cmp_iter(db_->NewIterator(cmp_ro, cmp_cfh));

    bool diverged = false;

    Slice key(key_str);

    const bool support_seek_first_or_last = expect_total_order;

    // Write-prepared and Write-unprepared and multi-cf-iterator do not support
    // Refresh() yet.
    if (!(FLAGS_use_txn && FLAGS_txn_write_policy != 0 /* write committed */) &&
        !FLAGS_use_multi_cf_iterator && thread->rand.OneIn(4)) {
      Status s = iter->Refresh(snapshot_guard.snapshot());
      if (!s.ok() && IsErrorInjectedAndRetryable(s)) {
        return s;
      }
      assert(s.ok());
      op_logs += "Refresh ";
    }

    LastIterateOp last_op;
    if (support_seek_first_or_last && thread->rand.OneIn(100)) {
      iter->SeekToFirst();
      cmp_iter->SeekToFirst();
      last_op = kLastOpSeekToFirst;
      op_logs += "STF ";
    } else if (support_seek_first_or_last && thread->rand.OneIn(100)) {
      iter->SeekToLast();
      cmp_iter->SeekToLast();
      last_op = kLastOpSeekToLast;
      op_logs += "STL ";
    } else if (thread->rand.OneIn(8)) {
      iter->SeekForPrev(key);
      cmp_iter->SeekForPrev(key);
      last_op = kLastOpSeekForPrev;
      op_logs += "SFP " + key.ToString(true) + " ";
    } else {
      iter->Seek(key);
      cmp_iter->Seek(key);
      last_op = kLastOpSeek;
      op_logs += "S " + key.ToString(true) + " ";
    }

    if (!iter->status().ok() && IsErrorInjectedAndRetryable(iter->status())) {
      return iter->status();
    } else if (!cmp_iter->status().ok() &&
               IsErrorInjectedAndRetryable(cmp_iter->status())) {
      return cmp_iter->status();
    }

    VerifyIterator(thread, cmp_cfh, ro, iter.get(), cmp_iter.get(), last_op,
                   key, op_logs, verify_func, &diverged);

    const bool no_reverse =
        (FLAGS_memtablerep == "prefix_hash" && !expect_total_order);
    for (uint64_t i = 0; i < FLAGS_num_iterations && iter->Valid(); ++i) {
      if (no_reverse || thread->rand.OneIn(2)) {
        iter->Next();
        if (!diverged) {
          assert(cmp_iter->Valid());
          cmp_iter->Next();
        }
        op_logs += "N";
      } else {
        iter->Prev();
        if (!diverged) {
          assert(cmp_iter->Valid());
          cmp_iter->Prev();
        }
        op_logs += "P";
      }

      last_op = kLastOpNextOrPrev;

      if (!iter->status().ok() && IsErrorInjectedAndRetryable(iter->status())) {
        return iter->status();
      } else if (!cmp_iter->status().ok() &&
                 IsErrorInjectedAndRetryable(cmp_iter->status())) {
        return cmp_iter->status();
      }

      VerifyIterator(thread, cmp_cfh, ro, iter.get(), cmp_iter.get(), last_op,
                     key, op_logs, verify_func, &diverged);
    }

    thread->stats.AddIterations(1);

    op_logs += "; ";
  }

  return Status::OK();
}

Status StressTest::TestGetLiveFiles() const {
  std::vector<std::string> live_file;
  uint64_t manifest_size = 0;
  return db_->GetLiveFiles(live_file, &manifest_size);
}

Status StressTest::TestGetLiveFilesMetaData() const {
  std::vector<LiveFileMetaData> live_file_metadata;
  db_->GetLiveFilesMetaData(&live_file_metadata);
  return Status::OK();
}

Status StressTest::TestGetLiveFilesStorageInfo() const {
  std::vector<LiveFileStorageInfo> live_file_storage_info;
  return db_->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(),
                                      &live_file_storage_info);
}

Status StressTest::TestGetAllColumnFamilyMetaData() const {
  std::vector<ColumnFamilyMetaData> all_cf_metadata;
  db_->GetAllColumnFamilyMetaData(&all_cf_metadata);
  return Status::OK();
}

Status StressTest::TestGetSortedWalFiles() const {
  VectorWalPtr log_ptr;
  return db_->GetSortedWalFiles(log_ptr);
}

Status StressTest::TestGetCurrentWalFile() const {
  std::unique_ptr<WalFile> cur_wal_file;
  return db_->GetCurrentWalFile(&cur_wal_file);
}

// Compare the two iterator, iter and cmp_iter are in the same position,
// unless iter might be made invalidate or undefined because of
// upper or lower bounds, or prefix extractor.
// Will flag failure if the verification fails.
// diverged = true if the two iterator is already diverged.
// True if verification passed, false if not.
template <typename IterType, typename VerifyFuncType>
void StressTest::VerifyIterator(
    ThreadState* thread, ColumnFamilyHandle* cmp_cfh, const ReadOptions& ro,
    IterType* iter, Iterator* cmp_iter, LastIterateOp op, const Slice& seek_key,
    const std::string& op_logs, VerifyFuncType verify_func, bool* diverged) {
  assert(diverged);

  if (*diverged) {
    return;
  }

  if (ro.iter_start_ts != nullptr) {
    assert(FLAGS_user_timestamp_size > 0);
    // We currently do not verify iterator when dumping history of internal
    // keys.
    *diverged = true;
    return;
  }

  if (op == kLastOpSeekToFirst && ro.iterate_lower_bound != nullptr) {
    // SeekToFirst() with lower bound is not well-defined.
    *diverged = true;
    return;
  } else if (op == kLastOpSeekToLast && ro.iterate_upper_bound != nullptr) {
    // SeekToLast() with higher bound is not well-defined.
    *diverged = true;
    return;
  } else if (op == kLastOpSeek && ro.iterate_lower_bound != nullptr &&
             (options_.comparator->CompareWithoutTimestamp(
                  *ro.iterate_lower_bound, /*a_has_ts=*/false, seek_key,
                  /*b_has_ts=*/false) >= 0 ||
              (ro.iterate_upper_bound != nullptr &&
               options_.comparator->CompareWithoutTimestamp(
                   *ro.iterate_lower_bound, /*a_has_ts=*/false,
                   *ro.iterate_upper_bound, /*b_has_ts*/ false) >= 0))) {
    // Lower bound behavior is not well-defined if it is larger than
    // seek key or upper bound. Disable the check for now.
    *diverged = true;
    return;
  } else if (op == kLastOpSeekForPrev && ro.iterate_upper_bound != nullptr &&
             (options_.comparator->CompareWithoutTimestamp(
                  *ro.iterate_upper_bound, /*a_has_ts=*/false, seek_key,
                  /*b_has_ts=*/false) <= 0 ||
              (ro.iterate_lower_bound != nullptr &&
               options_.comparator->CompareWithoutTimestamp(
                   *ro.iterate_lower_bound, /*a_has_ts=*/false,
                   *ro.iterate_upper_bound, /*b_has_ts=*/false) >= 0))) {
    // Upper bound behavior is not well-defined if it is smaller than
    // seek key or lower bound. Disable the check for now.
    *diverged = true;
    return;
  }

  const SliceTransform* pe = (ro.total_order_seek || ro.auto_prefix_mode)
                                 ? nullptr
                                 : options_.prefix_extractor.get();
  const Comparator* cmp = options_.comparator;
  std::ostringstream read_opt_oss;
  read_opt_oss << "pin_data: " << ro.pin_data
               << ", background_purge_on_iterator_cleanup: "
               << ro.background_purge_on_iterator_cleanup
               << ", total_order_seek: " << ro.total_order_seek
               << ", auto_prefix_mode: " << ro.auto_prefix_mode
               << ", iterate_upper_bound: "
               << (ro.iterate_upper_bound
                       ? ro.iterate_upper_bound->ToString(true).c_str()
                       : "")
               << ", iterate_lower_bound: "
               << (ro.iterate_lower_bound
                       ? ro.iterate_lower_bound->ToString(true).c_str()
                       : "");
  if (iter->Valid() && !cmp_iter->Valid()) {
    if (pe != nullptr) {
      if (!pe->InDomain(seek_key)) {
        // Prefix seek a non-in-domain key is undefined. Skip checking for
        // this scenario.
        *diverged = true;
        return;
      } else if (!pe->InDomain(iter->key())) {
        // out of range is iterator key is not in domain anymore.
        *diverged = true;
        return;
      } else if (pe->Transform(iter->key()) != pe->Transform(seek_key)) {
        *diverged = true;
        return;
      }
    }
    fprintf(stderr,
            "Control iterator is invalid but iterator has key %s "
            "%s under specified iterator ReadOptions: %s (Empty string or "
            "missing field indicates default option or value is used)\n",
            iter->key().ToString(true).c_str(), op_logs.c_str(),
            read_opt_oss.str().c_str());

    *diverged = true;
  } else if (cmp_iter->Valid()) {
    // Iterator is not valid. It can be legitimate if it has already been
    // out of upper or lower bound, or filtered out by prefix iterator.
    const Slice& total_order_key = cmp_iter->key();

    if (pe != nullptr) {
      if (!pe->InDomain(seek_key)) {
        // Prefix seek a non-in-domain key is undefined. Skip checking for
        // this scenario.
        *diverged = true;
        return;
      }

      if (!pe->InDomain(total_order_key) ||
          pe->Transform(total_order_key) != pe->Transform(seek_key)) {
        // If the prefix is exhausted, the only thing needs to check
        // is the iterator isn't return a position in prefix.
        // Either way, checking can stop from here.
        *diverged = true;
        if (!iter->Valid() || !pe->InDomain(iter->key()) ||
            pe->Transform(iter->key()) != pe->Transform(seek_key)) {
          return;
        }
        fprintf(stderr,
                "Iterator stays in prefix but control doesn't"
                " iterator key %s control iterator key %s %s under specified "
                "iterator ReadOptions: %s (Empty string or "
                "missing field indicates default option or value is used)\n",
                iter->key().ToString(true).c_str(),
                cmp_iter->key().ToString(true).c_str(), op_logs.c_str(),
                read_opt_oss.str().c_str());
      }
    }
    // Check upper or lower bounds.
    if (!*diverged) {
      if ((iter->Valid() && iter->key() != cmp_iter->key()) ||
          (!iter->Valid() &&
           (ro.iterate_upper_bound == nullptr ||
            cmp->CompareWithoutTimestamp(total_order_key, /*a_has_ts=*/false,
                                         *ro.iterate_upper_bound,
                                         /*b_has_ts=*/false) < 0) &&
           (ro.iterate_lower_bound == nullptr ||
            cmp->CompareWithoutTimestamp(total_order_key, /*a_has_ts=*/false,
                                         *ro.iterate_lower_bound,
                                         /*b_has_ts=*/false) > 0))) {
        fprintf(stderr,
                "Iterator diverged from control iterator which"
                " has value %s %s  under specified iterator ReadOptions: %s "
                "(Empty string or "
                "missing field indicates default option or value is used)\n",
                total_order_key.ToString(true).c_str(), op_logs.c_str(),
                read_opt_oss.str().c_str());
        if (iter->Valid()) {
          fprintf(stderr, "iterator has value %s\n",
                  iter->key().ToString(true).c_str());
        } else {
          fprintf(stderr, "iterator is not valid with status: %s\n",
                  iter->status().ToString().c_str());
        }
        *diverged = true;
      }
    }
  }

  if (!*diverged && iter->Valid()) {
    if (!verify_func(iter)) {
      *diverged = true;
    }
  }
  if (*diverged) {
    fprintf(stderr, "VerifyIterator failed. Control CF %s\n",
            cmp_cfh->GetName().c_str());
    thread->stats.AddErrors(1);
    // Fail fast to preserve the DB state.
    thread->shared->SetVerificationFailure();
  }
}

Status StressTest::TestBackupRestore(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  std::vector<std::unique_ptr<MutexLock>> locks;
  if (ShouldAcquireMutexOnKey()) {
    for (int rand_column_family : rand_column_families) {
      // `rand_keys[0]` on each chosen CF will be verified.
      locks.emplace_back(new MutexLock(
          thread->shared->GetMutexForKey(rand_column_family, rand_keys[0])));
    }
  }

  const std::string backup_dir =
      FLAGS_db + "/.backup" + std::to_string(thread->tid);
  const std::string restore_dir =
      FLAGS_db + "/.restore" + std::to_string(thread->tid);
  BackupEngineOptions backup_opts(backup_dir);
  // For debugging, get info_log from live options
  backup_opts.info_log = db_->GetDBOptions().info_log.get();
  if (thread->rand.OneIn(10)) {
    backup_opts.share_table_files = false;
  } else {
    backup_opts.share_table_files = true;
    if (thread->rand.OneIn(5)) {
      backup_opts.share_files_with_checksum = false;
    } else {
      backup_opts.share_files_with_checksum = true;
      if (thread->rand.OneIn(2)) {
        // old
        backup_opts.share_files_with_checksum_naming =
            BackupEngineOptions::kLegacyCrc32cAndFileSize;
      } else {
        // new
        backup_opts.share_files_with_checksum_naming =
            BackupEngineOptions::kUseDbSessionId;
      }
      if (thread->rand.OneIn(2)) {
        backup_opts.share_files_with_checksum_naming =
            backup_opts.share_files_with_checksum_naming |
            BackupEngineOptions::kFlagIncludeFileSize;
      }
    }
  }
  if (thread->rand.OneIn(2)) {
    backup_opts.schema_version = 1;
  } else {
    backup_opts.schema_version = 2;
  }
  if (thread->rand.OneIn(3)) {
    backup_opts.max_background_operations = 16;
  } else {
    backup_opts.max_background_operations = 1;
  }
  if (thread->rand.OneIn(2)) {
    backup_opts.backup_rate_limiter.reset(NewGenericRateLimiter(
        FLAGS_backup_max_size * 1000000 /* rate_bytes_per_sec */,
        1 /* refill_period_us */));
  }
  if (thread->rand.OneIn(2)) {
    backup_opts.restore_rate_limiter.reset(NewGenericRateLimiter(
        FLAGS_backup_max_size * 1000000 /* rate_bytes_per_sec */,
        1 /* refill_period_us */));
  }
  backup_opts.current_temperatures_override_manifest = thread->rand.OneIn(2);
  std::ostringstream backup_opt_oss;
  backup_opt_oss << "share_table_files: " << backup_opts.share_table_files
                 << ", share_files_with_checksum: "
                 << backup_opts.share_files_with_checksum
                 << ", share_files_with_checksum_naming: "
                 << backup_opts.share_files_with_checksum_naming
                 << ", schema_version: " << backup_opts.schema_version
                 << ", max_background_operations: "
                 << backup_opts.max_background_operations
                 << ", backup_rate_limiter: "
                 << backup_opts.backup_rate_limiter.get()
                 << ", restore_rate_limiter: "
                 << backup_opts.restore_rate_limiter.get()
                 << ", current_temperatures_override_manifest: "
                 << backup_opts.current_temperatures_override_manifest;

  std::ostringstream create_backup_opt_oss;
  std::ostringstream restore_opts_oss;
  BackupEngine* backup_engine = nullptr;
  std::string from = "a backup/restore operation";
  Status s = BackupEngine::Open(db_stress_env, backup_opts, &backup_engine);
  if (!s.ok()) {
    from = "BackupEngine::Open";
  }

  if (s.ok() && FLAGS_manual_wal_flush_one_in > 0) {
    // To avoid missing buffered WAL data during backup and cause false-positive
    // inconsistent values between original DB and restored DB
    s = db_->FlushWAL(/*sync=*/false);
    if (!s.ok()) {
      from = "FlushWAL";
    }
  }

  if (s.ok()) {
    if (backup_opts.schema_version >= 2 && thread->rand.OneIn(2)) {
      TEST_BackupMetaSchemaOptions test_opts;
      test_opts.crc32c_checksums = thread->rand.OneIn(2) == 0;
      test_opts.file_sizes = thread->rand.OneIn(2) == 0;
      TEST_SetBackupMetaSchemaOptions(backup_engine, test_opts);
    }
    CreateBackupOptions create_opts;
    if (FLAGS_disable_wal) {
      // The verification can only work when latest value of `key` is backed up,
      // which requires flushing in case of WAL disabled.
      //
      // Note this triggers a flush with a key lock held. Meanwhile, operations
      // like flush/compaction may attempt to grab key locks like in
      // `DbStressCompactionFilter`. The philosophy around preventing deadlock
      // is the background operation key lock acquisition only tries but does
      // not wait for the lock. So here in the foreground it is OK to hold the
      // lock and wait on a background operation (flush).
      create_opts.flush_before_backup = true;
    }
    create_opts.decrease_background_thread_cpu_priority = thread->rand.OneIn(2);
    create_opts.background_thread_cpu_priority = static_cast<CpuPriority>(
        thread->rand.Next() % (static_cast<int>(CpuPriority::kHigh) + 1));
    create_backup_opt_oss << "flush_before_backup: "
                          << create_opts.flush_before_backup
                          << ", decrease_background_thread_cpu_priority: "
                          << create_opts.decrease_background_thread_cpu_priority
                          << ", background_thread_cpu_priority: "
                          << static_cast<int>(
                                 create_opts.background_thread_cpu_priority);
    s = backup_engine->CreateNewBackup(create_opts, db_);
    if (!s.ok()) {
      from = "BackupEngine::CreateNewBackup";
    }
  }
  if (s.ok()) {
    delete backup_engine;
    backup_engine = nullptr;
    s = BackupEngine::Open(db_stress_env, backup_opts, &backup_engine);
    if (!s.ok()) {
      from = "BackupEngine::Open (again)";
    }
  }
  std::vector<BackupInfo> backup_info;
  // If inplace_not_restore, we verify the backup by opening it as a
  // read-only DB. If !inplace_not_restore, we restore it to a temporary
  // directory for verification.
  bool inplace_not_restore = thread->rand.OneIn(3);
  if (s.ok()) {
    backup_engine->GetBackupInfo(&backup_info,
                                 /*include_file_details*/ inplace_not_restore);
    if (backup_info.empty()) {
      s = Status::NotFound("no backups found");
      from = "BackupEngine::GetBackupInfo";
    }
  }
  if (s.ok() && thread->rand.OneIn(2)) {
    s = backup_engine->VerifyBackup(
        backup_info.front().backup_id,
        thread->rand.OneIn(2) /* verify_with_checksum */);
    if (!s.ok()) {
      from = "BackupEngine::VerifyBackup";
    }
  }
  const bool allow_persistent = thread->tid == 0;  // not too many
  bool from_latest = false;
  int count = static_cast<int>(backup_info.size());
  RestoreOptions restore_options;
  restore_options.keep_log_files = thread->rand.OneIn(2);
  restore_opts_oss << "keep_log_files: " << restore_options.keep_log_files;
  if (s.ok() && !inplace_not_restore) {
    if (count > 1) {
      s = backup_engine->RestoreDBFromBackup(
          restore_options, backup_info[thread->rand.Uniform(count)].backup_id,
          restore_dir /* db_dir */, restore_dir /* wal_dir */);
      if (!s.ok()) {
        from = "BackupEngine::RestoreDBFromBackup";
      }
    } else {
      from_latest = true;
      s = backup_engine->RestoreDBFromLatestBackup(
          restore_options, restore_dir /* db_dir */, restore_dir /* wal_dir */);
      if (!s.ok()) {
        from = "BackupEngine::RestoreDBFromLatestBackup";
      }
    }
  }
  if (s.ok() && !inplace_not_restore) {
    // Purge early if restoring, to ensure the restored directory doesn't
    // have some secret dependency on the backup directory.
    uint32_t to_keep = 0;
    if (allow_persistent) {
      // allow one thread to keep up to 2 backups
      to_keep = thread->rand.Uniform(3);
    }
    s = backup_engine->PurgeOldBackups(to_keep);
    if (!s.ok()) {
      from = "BackupEngine::PurgeOldBackups";
    }
  }
  DB* restored_db = nullptr;
  std::vector<ColumnFamilyHandle*> restored_cf_handles;

  // Not yet implemented: opening restored BlobDB or TransactionDB
  Options db_opt;
  if (s.ok() && !FLAGS_use_txn && !FLAGS_use_blob_db) {
    s = PrepareOptionsForRestoredDB(&db_opt);
    if (!s.ok()) {
      from = "PrepareRestoredDBOptions in backup/restore";
    }
  }
  if (s.ok() && !FLAGS_use_txn && !FLAGS_use_blob_db) {
    std::vector<ColumnFamilyDescriptor> cf_descriptors;
    // TODO(ajkr): `column_family_names_` is not safe to access here when
    // `clear_column_family_one_in != 0`. But we can't easily switch to
    // `ListColumnFamilies` to get names because it won't necessarily give
    // the same order as `column_family_names_`.
    assert(FLAGS_clear_column_family_one_in == 0);
    for (const auto& name : column_family_names_) {
      cf_descriptors.emplace_back(name, ColumnFamilyOptions(db_opt));
    }
    if (inplace_not_restore) {
      BackupInfo& info = backup_info[thread->rand.Uniform(count)];
      db_opt.env = info.env_for_open.get();
      s = DB::OpenForReadOnly(DBOptions(db_opt), info.name_for_open,
                              cf_descriptors, &restored_cf_handles,
                              &restored_db);
      if (!s.ok()) {
        from = "DB::OpenForReadOnly in backup/restore";
      }
    } else {
      s = DB::Open(DBOptions(db_opt), restore_dir, cf_descriptors,
                   &restored_cf_handles, &restored_db);
      if (!s.ok()) {
        from = "DB::Open in backup/restore";
      }
    }
  }
  // Note the column families chosen by `rand_column_families` cannot be
  // dropped while the locks for `rand_keys` are held. So we should not have
  // to worry about accessing those column families throughout this function.
  //
  // For simplicity, currently only verifies existence/non-existence of a
  // single key
  for (size_t i = 0; restored_db && s.ok() && i < rand_column_families.size();
       ++i) {
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    std::string restored_value;
    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions read_opts;
    std::string ts_str;
    Slice ts;
    if (FLAGS_user_timestamp_size > 0) {
      ts_str = GetNowNanos();
      ts = ts_str;
      read_opts.timestamp = &ts;
    }
    Status get_status = restored_db->Get(
        read_opts, restored_cf_handles[rand_column_families[i]], key,
        &restored_value);
    bool exists = thread->shared->Exists(rand_column_families[i], rand_keys[0]);
    if (get_status.ok()) {
      if (!exists && from_latest && ShouldAcquireMutexOnKey()) {
        std::ostringstream oss;
        oss << "0x" << key.ToString(true)
            << " exists in restore but not in original db";
        s = Status::Corruption(oss.str());
      }
    } else if (get_status.IsNotFound()) {
      if (exists && from_latest && ShouldAcquireMutexOnKey()) {
        std::ostringstream oss;
        oss << "0x" << key.ToString(true)
            << " exists in original db but not in restore";
        s = Status::Corruption(oss.str());
      }
    } else {
      s = get_status;
      if (!s.ok()) {
        from = "DB::Get in backup/restore";
      }
    }
  }
  if (restored_db != nullptr) {
    for (auto* cf_handle : restored_cf_handles) {
      restored_db->DestroyColumnFamilyHandle(cf_handle);
    }
    delete restored_db;
    restored_db = nullptr;
  }
  if (s.ok() && inplace_not_restore) {
    // Purge late if inplace open read-only
    uint32_t to_keep = 0;
    if (allow_persistent) {
      // allow one thread to keep up to 2 backups
      to_keep = thread->rand.Uniform(3);
    }
    s = backup_engine->PurgeOldBackups(to_keep);
    if (!s.ok()) {
      from = "BackupEngine::PurgeOldBackups";
    }
  }
  if (backup_engine != nullptr) {
    delete backup_engine;
    backup_engine = nullptr;
  }

  // Temporarily disable error injection for clean up
  if (fault_fs_guard) {
    fault_fs_guard->DisableAllThreadLocalErrorInjection();
  }

  if (s.ok() || IsErrorInjectedAndRetryable(s)) {
    // Preserve directories on failure, or allowed persistent backup
    if (!allow_persistent) {
      s = DestroyDir(db_stress_env, backup_dir);
      if (!s.ok()) {
        from = "Destroy backup dir";
      }
    }
  }

  if (s.ok() || IsErrorInjectedAndRetryable(s)) {
    s = DestroyDir(db_stress_env, restore_dir);
    if (!s.ok()) {
      from = "Destroy restore dir";
    }
  }

  // Enable back error injection disabled for clean up
  if (fault_fs_guard) {
    fault_fs_guard->EnableAllThreadLocalErrorInjection();
  }

  if (!s.ok() && !IsErrorInjectedAndRetryable(s)) {
    fprintf(stderr,
            "Failure in %s with: %s under specified BackupEngineOptions: %s, "
            "CreateBackupOptions: %s, RestoreOptions: %s (Empty string or "
            "missing field indicates default option or value is used)\n",
            from.c_str(), s.ToString().c_str(), backup_opt_oss.str().c_str(),
            create_backup_opt_oss.str().c_str(),
            restore_opts_oss.str().c_str());
  }
  return s;
}

void InitializeMergeOperator(Options& options) {
  if (FLAGS_use_full_merge_v1) {
    options.merge_operator = MergeOperators::CreateDeprecatedPutOperator();
  } else {
    if (FLAGS_use_put_entity_one_in > 0) {
      options.merge_operator = std::make_shared<DBStressWideMergeOperator>();
    } else {
      options.merge_operator = MergeOperators::CreatePutOperator();
    }
  }
}

Status StressTest::PrepareOptionsForRestoredDB(Options* options) {
  assert(options);
  // To avoid race with other threads' operations (e.g, SetOptions())
  // on the same pointer sub-option (e.g, `std::shared_ptr<const FilterPolicy>
  // filter_policy`) while having the same settings as `options_`, we create a
  // new Options object from `options_`'s string to deep copy these pointer
  // sub-options
  Status s;
  ConfigOptions config_opts;

  std::string db_options_str;
  s = GetStringFromDBOptions(config_opts, options_, &db_options_str);
  if (!s.ok()) {
    return s;
  }
  DBOptions db_options;
  s = GetDBOptionsFromString(config_opts, Options(), db_options_str,
                             &db_options);
  if (!s.ok()) {
    return s;
  }

  std::string cf_options_str;
  s = GetStringFromColumnFamilyOptions(config_opts, options_, &cf_options_str);
  if (!s.ok()) {
    return s;
  }
  ColumnFamilyOptions cf_options;
  s = GetColumnFamilyOptionsFromString(config_opts, Options(), cf_options_str,
                                       &cf_options);
  if (!s.ok()) {
    return s;
  }

  *options = Options(db_options, cf_options);
  options->best_efforts_recovery = false;
  options->listeners.clear();
  // Avoid dangling/shared file descriptors, for reliable destroy
  options->sst_file_manager = nullptr;
  // GetColumnFamilyOptionsFromString does not create customized merge operator.
  InitializeMergeOperator(*options);
  if (FLAGS_user_timestamp_size > 0) {
    // Check OPTIONS string loading can bootstrap the correct user comparator
    // from object registry.
    assert(options->comparator);
    assert(options->comparator == test::BytewiseComparatorWithU64TsWrapper());
  }

  return Status::OK();
}
Status StressTest::TestApproximateSize(
    ThreadState* thread, uint64_t iteration,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  // rand_keys likely only has one key. Just use the first one.
  assert(!rand_keys.empty());
  assert(!rand_column_families.empty());
  int64_t key1 = rand_keys[0];
  int64_t key2;
  if (thread->rand.OneIn(2)) {
    // Two totally random keys. This tends to cover large ranges.
    key2 = GenerateOneKey(thread, iteration);
    if (key2 < key1) {
      std::swap(key1, key2);
    }
  } else {
    // Unless users pass a very large FLAGS_max_key, it we should not worry
    // about overflow. It is for testing, so we skip the overflow checking
    // for simplicity.
    key2 = key1 + static_cast<int64_t>(thread->rand.Uniform(1000));
  }
  std::string key1_str = Key(key1);
  std::string key2_str = Key(key2);
  Range range{Slice(key1_str), Slice(key2_str)};
  SizeApproximationOptions sao;
  sao.include_memtables = thread->rand.OneIn(2);
  if (sao.include_memtables) {
    sao.include_files = thread->rand.OneIn(2);
  }
  if (thread->rand.OneIn(2)) {
    if (thread->rand.OneIn(2)) {
      sao.files_size_error_margin = 0.0;
    } else {
      sao.files_size_error_margin =
          static_cast<double>(thread->rand.Uniform(3));
    }
  }
  uint64_t result;
  return db_->GetApproximateSizes(
      sao, column_families_[rand_column_families[0]], &range, 1, &result);
}

Status StressTest::TestCheckpoint(ThreadState* thread,
                                  const std::vector<int>& rand_column_families,
                                  const std::vector<int64_t>& rand_keys) {
  std::vector<std::unique_ptr<MutexLock>> locks;
  if (ShouldAcquireMutexOnKey()) {
    for (int rand_column_family : rand_column_families) {
      // `rand_keys[0]` on each chosen CF will be verified.
      locks.emplace_back(new MutexLock(
          thread->shared->GetMutexForKey(rand_column_family, rand_keys[0])));
    }
  }

  std::string checkpoint_dir =
      FLAGS_db + "/.checkpoint" + std::to_string(thread->tid);
  Options tmp_opts(options_);
  tmp_opts.listeners.clear();
  tmp_opts.env = db_stress_env;
  // Avoid delayed deletion so whole directory can be deleted
  tmp_opts.sst_file_manager.reset();
  //  Temporarily disable error injection for clean-up
  if (fault_fs_guard) {
    fault_fs_guard->DisableAllThreadLocalErrorInjection();
  }
  DestroyDB(checkpoint_dir, tmp_opts);
  // Enable back error injection disabled for clean-up
  if (fault_fs_guard) {
    fault_fs_guard->EnableAllThreadLocalErrorInjection();
  }
  Checkpoint* checkpoint = nullptr;
  Status s = Checkpoint::Create(db_, &checkpoint);
  if (s.ok()) {
    s = checkpoint->CreateCheckpoint(checkpoint_dir);
    if (!s.ok() && !IsErrorInjectedAndRetryable(s)) {
      fprintf(stderr, "Fail to create checkpoint to %s\n",
              checkpoint_dir.c_str());
      std::vector<std::string> files;

      // Temporarily disable error injection to print debugging information
      if (fault_fs_guard) {
        fault_fs_guard->DisableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }

      Status my_s = db_stress_env->GetChildren(checkpoint_dir, &files);

      // Enable back disable error injection disabled for printing debugging
      // information
      if (fault_fs_guard) {
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      }
      if (!my_s.ok()) {
        fprintf(stderr, "Fail to GetChildren under %s due to %s\n",
                checkpoint_dir.c_str(), my_s.ToString().c_str());
      } else {
        for (const auto& f : files) {
          fprintf(stderr, " %s\n", f.c_str());
        }
      }
    }
  }
  delete checkpoint;
  checkpoint = nullptr;
  std::vector<ColumnFamilyHandle*> cf_handles;
  DB* checkpoint_db = nullptr;
  if (s.ok()) {
    Options options(options_);
    options.best_efforts_recovery = false;
    options.listeners.clear();
    // Avoid race condition in trash handling after delete checkpoint_db
    options.sst_file_manager.reset();
    std::vector<ColumnFamilyDescriptor> cf_descs;
    // TODO(ajkr): `column_family_names_` is not safe to access here when
    // `clear_column_family_one_in != 0`. But we can't easily switch to
    // `ListColumnFamilies` to get names because it won't necessarily give
    // the same order as `column_family_names_`.
    assert(FLAGS_clear_column_family_one_in == 0);
    if (FLAGS_clear_column_family_one_in == 0) {
      for (const auto& name : column_family_names_) {
        cf_descs.emplace_back(name, ColumnFamilyOptions(options));
      }
      s = DB::OpenForReadOnly(DBOptions(options), checkpoint_dir, cf_descs,
                              &cf_handles, &checkpoint_db);
    }
  }
  if (checkpoint_db != nullptr) {
    // Note the column families chosen by `rand_column_families` cannot be
    // dropped while the locks for `rand_keys` are held. So we should not have
    // to worry about accessing those column families throughout this function.
    for (size_t i = 0; s.ok() && i < rand_column_families.size(); ++i) {
      std::string key_str = Key(rand_keys[0]);
      Slice key = key_str;
      std::string ts_str;
      Slice ts;
      ReadOptions read_opts;
      if (FLAGS_user_timestamp_size > 0) {
        ts_str = GetNowNanos();
        ts = ts_str;
        read_opts.timestamp = &ts;
      }
      std::string value;
      Status get_status = checkpoint_db->Get(
          read_opts, cf_handles[rand_column_families[i]], key, &value);
      bool exists =
          thread->shared->Exists(rand_column_families[i], rand_keys[0]);
      if (get_status.ok()) {
        if (!exists && ShouldAcquireMutexOnKey()) {
          std::ostringstream oss;
          oss << "0x" << key.ToString(true) << " exists in checkpoint "
              << checkpoint_dir << " but not in original db";
          s = Status::Corruption(oss.str());
        }
      } else if (get_status.IsNotFound()) {
        if (exists && ShouldAcquireMutexOnKey()) {
          std::ostringstream oss;
          oss << "0x" << key.ToString(true)
              << " exists in original db but not in checkpoint "
              << checkpoint_dir;
          s = Status::Corruption(oss.str());
        }
      } else {
        s = get_status;
      }
    }
    for (auto cfh : cf_handles) {
      delete cfh;
    }
    cf_handles.clear();
    delete checkpoint_db;
    checkpoint_db = nullptr;
  }

  //  Temporarily disable error injection for clean-up
  if (fault_fs_guard) {
    fault_fs_guard->DisableAllThreadLocalErrorInjection();
  }

  if (!s.ok() && !IsErrorInjectedAndRetryable(s)) {
    fprintf(stderr, "A checkpoint operation failed with: %s\n",
            s.ToString().c_str());
  } else {
    DestroyDB(checkpoint_dir, tmp_opts);
  }

  // Enable back error injection disabled for clean-up
  if (fault_fs_guard) {
    fault_fs_guard->EnableAllThreadLocalErrorInjection();
  }
  return s;
}

void StressTest::TestGetProperty(ThreadState* thread) const {
  std::unordered_set<std::string> levelPropertyNames = {
      DB::Properties::kAggregatedTablePropertiesAtLevel,
      DB::Properties::kCompressionRatioAtLevelPrefix,
      DB::Properties::kNumFilesAtLevelPrefix,
  };
  std::unordered_set<std::string> unknownPropertyNames = {
      DB::Properties::kEstimateOldestKeyTime,
      DB::Properties::kOptionsStatistics,
      DB::Properties::
          kLiveSstFilesSizeAtTemperature,  // similar to levelPropertyNames, it
                                           // requires a number suffix
  };
  unknownPropertyNames.insert(levelPropertyNames.begin(),
                              levelPropertyNames.end());

  std::unordered_set<std::string> blobCachePropertyNames = {
      DB::Properties::kBlobCacheCapacity,
      DB::Properties::kBlobCacheUsage,
      DB::Properties::kBlobCachePinnedUsage,
  };
  if (db_->GetOptions().blob_cache == nullptr) {
    unknownPropertyNames.insert(blobCachePropertyNames.begin(),
                                blobCachePropertyNames.end());
  }

  std::string prop;
  for (const auto& ppt_name_and_info : InternalStats::ppt_name_to_info) {
    bool res = db_->GetProperty(ppt_name_and_info.first, &prop);
    if (unknownPropertyNames.find(ppt_name_and_info.first) ==
        unknownPropertyNames.end()) {
      if (!res) {
        fprintf(stderr, "Failed to get DB property: %s\n",
                ppt_name_and_info.first.c_str());
        thread->shared->SetVerificationFailure();
      }
      if (ppt_name_and_info.second.handle_int != nullptr) {
        uint64_t prop_int;
        if (!db_->GetIntProperty(ppt_name_and_info.first, &prop_int)) {
          fprintf(stderr, "Failed to get Int property: %s\n",
                  ppt_name_and_info.first.c_str());
          thread->shared->SetVerificationFailure();
        }
      }
      if (ppt_name_and_info.second.handle_map != nullptr) {
        std::map<std::string, std::string> prop_map;
        if (!db_->GetMapProperty(ppt_name_and_info.first, &prop_map)) {
          fprintf(stderr, "Failed to get Map property: %s\n",
                  ppt_name_and_info.first.c_str());
          thread->shared->SetVerificationFailure();
        }
      }
    }
  }

  ROCKSDB_NAMESPACE::ColumnFamilyMetaData cf_meta_data;
  db_->GetColumnFamilyMetaData(&cf_meta_data);
  int level_size = static_cast<int>(cf_meta_data.levels.size());
  for (int level = 0; level < level_size; level++) {
    for (const auto& ppt_name : levelPropertyNames) {
      bool res = db_->GetProperty(ppt_name + std::to_string(level), &prop);
      if (!res) {
        fprintf(stderr, "Failed to get DB property: %s\n",
                (ppt_name + std::to_string(level)).c_str());
        thread->shared->SetVerificationFailure();
      }
    }
  }

  // Test for an invalid property name
  if (thread->rand.OneIn(100)) {
    if (db_->GetProperty("rocksdb.invalid_property_name", &prop)) {
      fprintf(stderr, "Failed to return false for invalid property name\n");
      thread->shared->SetVerificationFailure();
    }
  }
}

Status StressTest::TestGetPropertiesOfAllTables() const {
  TablePropertiesCollection props;
  return db_->GetPropertiesOfAllTables(&props);
}

void StressTest::TestCompactFiles(ThreadState* thread,
                                  ColumnFamilyHandle* column_family) {
  ROCKSDB_NAMESPACE::ColumnFamilyMetaData cf_meta_data;
  db_->GetColumnFamilyMetaData(column_family, &cf_meta_data);

  if (cf_meta_data.levels.empty()) {
    return;
  }

  // Randomly compact up to three consecutive files from a level
  const int kMaxRetry = 3;
  for (int attempt = 0; attempt < kMaxRetry; ++attempt) {
    size_t random_level =
        thread->rand.Uniform(static_cast<int>(cf_meta_data.levels.size()));

    const auto& files = cf_meta_data.levels[random_level].files;
    if (files.size() > 0) {
      size_t random_file_index =
          thread->rand.Uniform(static_cast<int>(files.size()));
      if (files[random_file_index].being_compacted) {
        // Retry as the selected file is currently being compacted
        continue;
      }

      std::vector<std::string> input_files;
      input_files.push_back(files[random_file_index].name);
      if (random_file_index > 0 &&
          !files[random_file_index - 1].being_compacted) {
        input_files.push_back(files[random_file_index - 1].name);
      }
      if (random_file_index + 1 < files.size() &&
          !files[random_file_index + 1].being_compacted) {
        input_files.push_back(files[random_file_index + 1].name);
      }

      size_t output_level =
          std::min(random_level + 1, cf_meta_data.levels.size() - 1);
      CompactionOptions compact_options;
      if (thread->rand.OneIn(2)) {
        compact_options.output_file_size_limit = FLAGS_target_file_size_base;
      }
      std::ostringstream compact_opt_oss;
      compact_opt_oss << "output_file_size_limit: "
                      << compact_options.output_file_size_limit;
      auto s = db_->CompactFiles(compact_options, column_family, input_files,
                                 static_cast<int>(output_level));
      if (!s.ok()) {
        thread->stats.AddNumCompactFilesFailed(1);
        // TOOD (hx235): allow an exact list of tolerable failures under stress
        // test
        bool non_ok_status_allowed =
            s.IsManualCompactionPaused() || IsErrorInjectedAndRetryable(s) ||
            s.IsAborted() || s.IsInvalidArgument() || s.IsNotSupported();
        if (!non_ok_status_allowed) {
          fprintf(stderr,
                  "Unable to perform CompactFiles(): %s under specified "
                  "CompactionOptions: %s (Empty string or "
                  "missing field indicates default option or value is used)\n",
                  s.ToString().c_str(), compact_opt_oss.str().c_str());
          thread->shared->SafeTerminate();
        }
      } else {
        thread->stats.AddNumCompactFilesSucceed(1);
      }
      break;
    }
  }
}

void StressTest::TestPromoteL0(ThreadState* thread,
                               ColumnFamilyHandle* column_family) {
  int target_level = thread->rand.Next() % options_.num_levels;
  Status s = db_->PromoteL0(column_family, target_level);
  if (!s.ok()) {
    // The second error occurs when another concurrent PromoteL0() moving the
    // same files finishes first which is an allowed behavior
    bool non_ok_status_allowed =
        s.IsInvalidArgument() ||
        (s.IsCorruption() &&
         s.ToString().find("VersionBuilder: Cannot delete table file") !=
             std::string::npos &&
         s.ToString().find("since it is on level") != std::string::npos);

    if (!non_ok_status_allowed) {
      fprintf(stderr,
              "Unable to perform PromoteL0(): %s under specified "
              "target_level: %d.\n",
              s.ToString().c_str(), target_level);
      thread->shared->SafeTerminate();
    }
  }
}

Status StressTest::TestFlush(const std::vector<int>& rand_column_families) {
  FlushOptions flush_opts;
  assert(flush_opts.wait);
  if (FLAGS_atomic_flush) {
    return db_->Flush(flush_opts, column_families_);
  }
  std::vector<ColumnFamilyHandle*> cfhs;
  std::for_each(rand_column_families.begin(), rand_column_families.end(),
                [this, &cfhs](int k) { cfhs.push_back(column_families_[k]); });
  return db_->Flush(flush_opts, cfhs);
}

Status StressTest::TestResetStats() { return db_->ResetStats(); }

Status StressTest::TestPauseBackground(ThreadState* thread) {
  Status status = db_->PauseBackgroundWork();
  if (!status.ok()) {
    return status;
  }
  // To avoid stalling/deadlocking ourself in this thread, just
  // sleep here during pause and let other threads do db operations.
  // Sleep up to ~16 seconds (2**24 microseconds), but very skewed
  // toward short pause. (1 chance in 25 of pausing >= 1s;
  // 1 chance in 625 of pausing full 16s.)
  int pwr2_micros =
      std::min(thread->rand.Uniform(25), thread->rand.Uniform(25));
  clock_->SleepForMicroseconds(1 << pwr2_micros);
  return db_->ContinueBackgroundWork();
}

Status StressTest::TestDisableFileDeletions(ThreadState* thread) {
  Status status = db_->DisableFileDeletions();
  if (!status.ok()) {
    return status;
  }
  // Similar to TestPauseBackground()
  int pwr2_micros =
      std::min(thread->rand.Uniform(25), thread->rand.Uniform(25));
  clock_->SleepForMicroseconds(1 << pwr2_micros);
  return db_->EnableFileDeletions();
}

Status StressTest::TestDisableManualCompaction(ThreadState* thread) {
  db_->DisableManualCompaction();
  // Similar to TestPauseBackground()
  int pwr2_micros =
      std::min(thread->rand.Uniform(25), thread->rand.Uniform(25));
  clock_->SleepForMicroseconds(1 << pwr2_micros);
  db_->EnableManualCompaction();
  return Status::OK();
}

void StressTest::TestAcquireSnapshot(ThreadState* thread,
                                     int rand_column_family,
                                     const std::string& keystr, uint64_t i) {
  Slice key = keystr;
  ColumnFamilyHandle* column_family = column_families_[rand_column_family];
  // This `ReadOptions` is for validation purposes. Ignore
  // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
  ReadOptions ropt;
  auto db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
  const bool ww_snapshot = thread->rand.OneIn(10);
  const Snapshot* snapshot =
      ww_snapshot ? db_impl->GetSnapshotForWriteConflictBoundary()
                  : db_->GetSnapshot();
  ropt.snapshot = snapshot;

  // Ideally, we want snapshot taking and timestamp generation to be atomic
  // here, so that the snapshot corresponds to the timestamp. However, it is
  // not possible with current GetSnapshot() API.
  std::string ts_str;
  Slice ts;
  if (FLAGS_user_timestamp_size > 0) {
    ts_str = GetNowNanos();
    ts = ts_str;
    ropt.timestamp = &ts;
  }

  std::string value_at;
  // When taking a snapshot, we also read a key from that snapshot. We
  // will later read the same key before releasing the snapshot and
  // verify that the results are the same.
  Status status_at = db_->Get(ropt, column_family, key, &value_at);
  if (!status_at.ok() && IsErrorInjectedAndRetryable(status_at)) {
    db_->ReleaseSnapshot(snapshot);
    return;
  }
  std::vector<bool>* key_vec = nullptr;

  if (FLAGS_compare_full_db_state_snapshot && (thread->tid == 0)) {
    key_vec = new std::vector<bool>(FLAGS_max_key);
    // When `prefix_extractor` is set, seeking to beginning and scanning
    // across prefixes are only supported with `total_order_seek` set.
    ropt.total_order_seek = true;
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ropt));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      uint64_t key_val;
      if (GetIntVal(iterator->key().ToString(), &key_val)) {
        (*key_vec)[key_val] = true;
      }
    }
  }

  ThreadState::SnapshotState snap_state = {snapshot,
                                           rand_column_family,
                                           column_family->GetName(),
                                           keystr,
                                           status_at,
                                           value_at,
                                           key_vec,
                                           ts_str};
  uint64_t hold_for = FLAGS_snapshot_hold_ops;
  if (FLAGS_long_running_snapshots) {
    // Hold 10% of snapshots for 10x more
    if (thread->rand.OneIn(10)) {
      assert(hold_for < std::numeric_limits<uint64_t>::max() / 10);
      hold_for *= 10;
      // Hold 1% of snapshots for 100x more
      if (thread->rand.OneIn(10)) {
        assert(hold_for < std::numeric_limits<uint64_t>::max() / 10);
        hold_for *= 10;
      }
    }
  }
  uint64_t release_at = std::min(FLAGS_ops_per_thread - 1, i + hold_for);
  thread->snapshot_queue.emplace(release_at, snap_state);
}

Status StressTest::MaybeReleaseSnapshots(ThreadState* thread, uint64_t i) {
  while (!thread->snapshot_queue.empty() &&
         i >= thread->snapshot_queue.front().first) {
    auto snap_state = thread->snapshot_queue.front().second;
    assert(snap_state.snapshot);
    // Note: this is unsafe as the cf might be dropped concurrently. But
    // it is ok since unclean cf drop is cunnrently not supported by write
    // prepared transactions.
    Status s = AssertSame(db_, column_families_[snap_state.cf_at], snap_state);
    db_->ReleaseSnapshot(snap_state.snapshot);
    delete snap_state.key_vec;
    thread->snapshot_queue.pop();
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

void StressTest::TestCompactRange(ThreadState* thread, int64_t rand_key,
                                  const Slice& start_key,
                                  ColumnFamilyHandle* column_family) {
  int64_t end_key_num;
  if (std::numeric_limits<int64_t>::max() - rand_key <
      FLAGS_compact_range_width) {
    end_key_num = std::numeric_limits<int64_t>::max();
  } else {
    end_key_num = FLAGS_compact_range_width + rand_key;
  }
  std::string end_key_buf = Key(end_key_num);
  Slice end_key(end_key_buf);

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = static_cast<bool>(thread->rand.Next() % 2);
  if (static_cast<ROCKSDB_NAMESPACE::CompactionStyle>(FLAGS_compaction_style) !=
      ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleFIFO) {
    cro.change_level = static_cast<bool>(thread->rand.Next() % 2);
  }
  if (thread->rand.OneIn(2)) {
    cro.target_level = thread->rand.Next() % options_.num_levels;
  }
  std::vector<BottommostLevelCompaction> bottom_level_styles = {
      BottommostLevelCompaction::kSkip,
      BottommostLevelCompaction::kIfHaveCompactionFilter,
      BottommostLevelCompaction::kForce,
      BottommostLevelCompaction::kForceOptimized};
  cro.bottommost_level_compaction =
      bottom_level_styles[thread->rand.Next() %
                          static_cast<uint32_t>(bottom_level_styles.size())];
  cro.allow_write_stall = static_cast<bool>(thread->rand.Next() % 2);
  cro.max_subcompactions = static_cast<uint32_t>(thread->rand.Next() % 4);
  std::vector<BlobGarbageCollectionPolicy> blob_gc_policies = {
      BlobGarbageCollectionPolicy::kForce,
      BlobGarbageCollectionPolicy::kDisable,
      BlobGarbageCollectionPolicy::kUseDefault};
  cro.blob_garbage_collection_policy =
      blob_gc_policies[thread->rand.Next() %
                       static_cast<uint32_t>(blob_gc_policies.size())];
  cro.blob_garbage_collection_age_cutoff =
      static_cast<double>(thread->rand.Next() % 100) / 100.0;

  const Snapshot* pre_snapshot = nullptr;
  uint32_t pre_hash = 0;
  if (thread->rand.OneIn(2)) {
    // Temporarily disable error injection to for validation
    if (fault_fs_guard) {
      fault_fs_guard->DisableAllThreadLocalErrorInjection();
    }

    // Declare a snapshot and compare the data before and after the compaction
    pre_snapshot = db_->GetSnapshot();
    pre_hash =
        GetRangeHash(thread, pre_snapshot, column_family, start_key, end_key);

    // Enable back error injection disabled for validation
    if (fault_fs_guard) {
      fault_fs_guard->EnableAllThreadLocalErrorInjection();
    }
  }
  std::ostringstream compact_range_opt_oss;
  compact_range_opt_oss << "exclusive_manual_compaction: "
                        << cro.exclusive_manual_compaction
                        << ", change_level: " << cro.change_level
                        << ", target_level: " << cro.target_level
                        << ", bottommost_level_compaction: "
                        << static_cast<int>(cro.bottommost_level_compaction)
                        << ", allow_write_stall: " << cro.allow_write_stall
                        << ", max_subcompactions: " << cro.max_subcompactions
                        << ", blob_garbage_collection_policy: "
                        << static_cast<int>(cro.blob_garbage_collection_policy)
                        << ", blob_garbage_collection_age_cutoff: "
                        << cro.blob_garbage_collection_age_cutoff;
  Status status = db_->CompactRange(cro, column_family, &start_key, &end_key);

  if (!status.ok()) {
    // TOOD (hx235): allow an exact list of tolerable failures under stress test
    bool non_ok_status_allowed =
        status.IsManualCompactionPaused() ||
        IsErrorInjectedAndRetryable(status) || status.IsAborted() ||
        status.IsInvalidArgument() || status.IsNotSupported();
    if (!non_ok_status_allowed) {
      fprintf(stderr,
              "Unable to perform CompactRange(): %s under specified "
              "CompactRangeOptions: %s (Empty string or "
              "missing field indicates default option or value is used)\n",
              status.ToString().c_str(), compact_range_opt_oss.str().c_str());
      // Fail fast to preserve the DB state.
      thread->shared->SetVerificationFailure();
    }
  }

  if (pre_snapshot != nullptr) {
    // Temporarily disable error injection for validation
    if (fault_fs_guard) {
      fault_fs_guard->DisableAllThreadLocalErrorInjection();
    }
    uint32_t post_hash =
        GetRangeHash(thread, pre_snapshot, column_family, start_key, end_key);
    if (pre_hash != post_hash) {
      fprintf(stderr,
              "Data hash different before and after compact range "
              "start_key %s end_key %s under specified CompactRangeOptions: %s "
              "(Empty string or "
              "missing field indicates default option or value is used)\n",
              start_key.ToString(true).c_str(), end_key.ToString(true).c_str(),
              compact_range_opt_oss.str().c_str());
      thread->stats.AddErrors(1);
      // Fail fast to preserve the DB state.
      thread->shared->SetVerificationFailure();
    }
    db_->ReleaseSnapshot(pre_snapshot);
    if (fault_fs_guard) {
      // Enable back error injection disabled for validation
      fault_fs_guard->EnableAllThreadLocalErrorInjection();
    }
  }
}

uint32_t StressTest::GetRangeHash(ThreadState* thread, const Snapshot* snapshot,
                                  ColumnFamilyHandle* column_family,
                                  const Slice& start_key,
                                  const Slice& end_key) {
  // This `ReadOptions` is for validation purposes. Ignore
  // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
  ReadOptions ro;
  ro.snapshot = snapshot;
  ro.total_order_seek = true;
  std::string ts_str;
  Slice ts;
  if (FLAGS_user_timestamp_size > 0) {
    ts_str = GetNowNanos();
    ts = ts_str;
    ro.timestamp = &ts;
  }

  std::unique_ptr<Iterator> it(db_->NewIterator(ro, column_family));

  constexpr char kCrcCalculatorSepearator = ';';

  uint32_t crc = 0;

  for (it->Seek(start_key);
       it->Valid() && options_.comparator->Compare(it->key(), end_key) <= 0;
       it->Next()) {
    crc = crc32c::Extend(crc, it->key().data(), it->key().size());
    crc = crc32c::Extend(crc, &kCrcCalculatorSepearator, sizeof(char));
    crc = crc32c::Extend(crc, it->value().data(), it->value().size());
    crc = crc32c::Extend(crc, &kCrcCalculatorSepearator, sizeof(char));

    for (const auto& column : it->columns()) {
      crc = crc32c::Extend(crc, column.name().data(), column.name().size());
      crc = crc32c::Extend(crc, &kCrcCalculatorSepearator, sizeof(char));
      crc = crc32c::Extend(crc, column.value().data(), column.value().size());
      crc = crc32c::Extend(crc, &kCrcCalculatorSepearator, sizeof(char));
    }
  }

  if (!it->status().ok()) {
    fprintf(stderr, "Iterator non-OK when calculating range CRC: %s\n",
            it->status().ToString().c_str());
    thread->stats.AddErrors(1);
    // Fail fast to preserve the DB state.
    thread->shared->SetVerificationFailure();
  }

  return crc;
}

void StressTest::PrintEnv() const {
  fprintf(stdout, "RocksDB version           : %d.%d\n", kMajorVersion,
          kMinorVersion);
  fprintf(stdout, "Format version            : %d\n", FLAGS_format_version);
  fprintf(stdout, "TransactionDB             : %s\n",
          FLAGS_use_txn ? "true" : "false");
  if (FLAGS_use_txn) {
    fprintf(stdout, "TransactionDB Type        : %s\n",
            FLAGS_use_optimistic_txn ? "Optimistic" : "Pessimistic");
    if (FLAGS_use_optimistic_txn) {
      fprintf(stdout, "OCC Validation Type       : %d\n",
              static_cast<int>(FLAGS_occ_validation_policy));
      if (static_cast<uint64_t>(OccValidationPolicy::kValidateParallel) ==
          FLAGS_occ_validation_policy) {
        fprintf(stdout, "Share Lock Buckets        : %s\n",
                FLAGS_share_occ_lock_buckets ? "true" : "false");
        if (FLAGS_share_occ_lock_buckets) {
          fprintf(stdout, "Lock Bucket Count         : %d\n",
                  static_cast<int>(FLAGS_occ_lock_bucket_count));
        }
      }
    } else {
      fprintf(stdout, "Two write queues:         : %s\n",
              FLAGS_two_write_queues ? "true" : "false");
      fprintf(stdout, "Write policy              : %d\n",
              static_cast<int>(FLAGS_txn_write_policy));
      if (static_cast<uint64_t>(TxnDBWritePolicy::WRITE_PREPARED) ==
              FLAGS_txn_write_policy ||
          static_cast<uint64_t>(TxnDBWritePolicy::WRITE_UNPREPARED) ==
              FLAGS_txn_write_policy) {
        fprintf(stdout, "Snapshot cache bits       : %d\n",
                static_cast<int>(FLAGS_wp_snapshot_cache_bits));
        fprintf(stdout, "Commit cache bits         : %d\n",
                static_cast<int>(FLAGS_wp_commit_cache_bits));
      }
      fprintf(stdout, "last cwb for recovery    : %s\n",
              FLAGS_use_only_the_last_commit_time_batch_for_recovery ? "true"
                                                                     : "false");
    }
  }

  fprintf(stdout, "Stacked BlobDB            : %s\n",
          FLAGS_use_blob_db ? "true" : "false");
  fprintf(stdout, "Read only mode            : %s\n",
          FLAGS_read_only ? "true" : "false");
  fprintf(stdout, "Atomic flush              : %s\n",
          FLAGS_atomic_flush ? "true" : "false");
  fprintf(stdout, "Manual WAL flush          : %s\n",
          FLAGS_manual_wal_flush_one_in > 0 ? "true" : "false");
  fprintf(stdout, "Column families           : %d\n", FLAGS_column_families);
  if (!FLAGS_test_batches_snapshots) {
    fprintf(stdout, "Clear CFs one in          : %d\n",
            FLAGS_clear_column_family_one_in);
  }
  fprintf(stdout, "Number of threads         : %d\n", FLAGS_threads);
  fprintf(stdout, "Ops per thread            : %lu\n",
          (unsigned long)FLAGS_ops_per_thread);
  std::string ttl_state("unused");
  if (FLAGS_ttl > 0) {
    ttl_state = std::to_string(FLAGS_ttl);
  }
  fprintf(stdout, "Time to live(sec)         : %s\n", ttl_state.c_str());
  fprintf(stdout, "Read percentage           : %d%%\n", FLAGS_readpercent);
  fprintf(stdout, "Prefix percentage         : %d%%\n", FLAGS_prefixpercent);
  fprintf(stdout, "Write percentage          : %d%%\n", FLAGS_writepercent);
  fprintf(stdout, "Delete percentage         : %d%%\n", FLAGS_delpercent);
  fprintf(stdout, "Delete range percentage   : %d%%\n", FLAGS_delrangepercent);
  fprintf(stdout, "No overwrite percentage   : %d%%\n",
          FLAGS_nooverwritepercent);
  fprintf(stdout, "Iterate percentage        : %d%%\n", FLAGS_iterpercent);
  fprintf(stdout, "Custom ops percentage     : %d%%\n", FLAGS_customopspercent);
  fprintf(stdout, "DB-write-buffer-size      : %" PRIu64 "\n",
          FLAGS_db_write_buffer_size);
  fprintf(stdout, "Write-buffer-size         : %d\n", FLAGS_write_buffer_size);
  fprintf(stdout, "Iterations                : %lu\n",
          (unsigned long)FLAGS_num_iterations);
  fprintf(stdout, "Max key                   : %lu\n",
          (unsigned long)FLAGS_max_key);
  fprintf(stdout, "Ratio #ops/#keys          : %f\n",
          (1.0 * FLAGS_ops_per_thread * FLAGS_threads) / FLAGS_max_key);
  fprintf(stdout, "Num times DB reopens      : %d\n", FLAGS_reopen);
  fprintf(stdout, "Batches/snapshots         : %d\n",
          FLAGS_test_batches_snapshots);
  fprintf(stdout, "Do update in place        : %d\n",
          FLAGS_inplace_update_support);
  fprintf(stdout, "Num keys per lock         : %d\n",
          1 << FLAGS_log2_keys_per_lock);
  std::string compression = CompressionTypeToString(compression_type_e);
  fprintf(stdout, "Compression               : %s\n", compression.c_str());
  std::string bottommost_compression =
      CompressionTypeToString(bottommost_compression_type_e);
  fprintf(stdout, "Bottommost Compression    : %s\n",
          bottommost_compression.c_str());
  std::string checksum = ChecksumTypeToString(checksum_type_e);
  fprintf(stdout, "Checksum type             : %s\n", checksum.c_str());
  fprintf(stdout, "File checksum impl        : %s\n",
          FLAGS_file_checksum_impl.c_str());
  fprintf(stdout, "Bloom bits / key          : %s\n",
          FormatDoubleParam(FLAGS_bloom_bits).c_str());
  fprintf(stdout, "Max subcompactions        : %" PRIu64 "\n",
          FLAGS_subcompactions);
  fprintf(stdout, "Use MultiGet              : %s\n",
          FLAGS_use_multiget ? "true" : "false");
  fprintf(stdout, "Use GetEntity             : %s\n",
          FLAGS_use_get_entity ? "true" : "false");
  fprintf(stdout, "Use MultiGetEntity        : %s\n",
          FLAGS_use_multi_get_entity ? "true" : "false");
  fprintf(stdout, "Verification only         : %s\n",
          FLAGS_verification_only ? "true" : "false");

  const char* memtablerep = "";
  switch (FLAGS_rep_factory) {
    case kSkipList:
      memtablerep = "skip_list";
      break;
    case kHashSkipList:
      memtablerep = "prefix_hash";
      break;
    case kVectorRep:
      memtablerep = "vector";
      break;
  }

  fprintf(stdout, "Memtablerep               : %s\n", memtablerep);

#ifndef NDEBUG
  KillPoint* kp = KillPoint::GetInstance();
  fprintf(stdout, "Test kill odd             : %d\n", kp->rocksdb_kill_odds);
  if (!kp->rocksdb_kill_exclude_prefixes.empty()) {
    fprintf(stdout, "Skipping kill points prefixes:\n");
    for (auto& p : kp->rocksdb_kill_exclude_prefixes) {
      fprintf(stdout, "  %s\n", p.c_str());
    }
  }
#endif
  fprintf(stdout, "Periodic Compaction Secs  : %" PRIu64 "\n",
          FLAGS_periodic_compaction_seconds);
  fprintf(stdout, "Daily Offpeak UTC         : %s\n",
          FLAGS_daily_offpeak_time_utc.c_str());
  fprintf(stdout, "Compaction TTL            : %" PRIu64 "\n",
          FLAGS_compaction_ttl);
  const char* compaction_pri = "";
  switch (FLAGS_compaction_pri) {
    case kByCompensatedSize:
      compaction_pri = "kByCompensatedSize";
      break;
    case kOldestLargestSeqFirst:
      compaction_pri = "kOldestLargestSeqFirst";
      break;
    case kOldestSmallestSeqFirst:
      compaction_pri = "kOldestSmallestSeqFirst";
      break;
    case kMinOverlappingRatio:
      compaction_pri = "kMinOverlappingRatio";
      break;
    case kRoundRobin:
      compaction_pri = "kRoundRobin";
      break;
  }
  fprintf(stdout, "Compaction Pri            : %s\n", compaction_pri);
  fprintf(stdout, "Background Purge          : %d\n",
          static_cast<int>(FLAGS_avoid_unnecessary_blocking_io));
  fprintf(stdout, "Write DB ID to manifest   : %d\n",
          static_cast<int>(FLAGS_write_dbid_to_manifest));
  fprintf(stdout, "Max Write Batch Group Size: %" PRIu64 "\n",
          FLAGS_max_write_batch_group_size_bytes);
  fprintf(stdout, "Use dynamic level         : %d\n",
          static_cast<int>(FLAGS_level_compaction_dynamic_level_bytes));
  fprintf(stdout, "Metadata read fault one in         : %d\n",
          FLAGS_metadata_read_fault_one_in);
  fprintf(stdout, "Metadata write fault one in        : %d\n",
          FLAGS_metadata_write_fault_one_in);
  fprintf(stdout, "Read fault one in         : %d\n", FLAGS_read_fault_one_in);
  fprintf(stdout, "Write fault one in        : %d\n", FLAGS_write_fault_one_in);
  fprintf(stdout, "Open metadata read fault one in:\n");
  fprintf(stdout, "                            %d\n",
          FLAGS_open_metadata_read_fault_one_in);
  fprintf(stdout, "Open metadata write fault one in:\n");
  fprintf(stdout, "                            %d\n",
          FLAGS_open_metadata_write_fault_one_in);
  fprintf(stdout, "Open read fault one in          :\n");
  fprintf(stdout, "                            %d\n",
          FLAGS_open_read_fault_one_in);
  fprintf(stdout, "Open write fault one in         :\n");
  fprintf(stdout, "                            %d\n",
          FLAGS_open_write_fault_one_in);
  fprintf(stdout, "Sync fault injection      : %d\n",
          FLAGS_sync_fault_injection);
  fprintf(stdout, "Best efforts recovery     : %d\n",
          static_cast<int>(FLAGS_best_efforts_recovery));
  fprintf(stdout, "Fail if OPTIONS file error: %d\n",
          static_cast<int>(FLAGS_fail_if_options_file_error));
  fprintf(stdout, "User timestamp size bytes : %d\n",
          static_cast<int>(FLAGS_user_timestamp_size));
  fprintf(stdout, "Persist user defined timestamps : %d\n",
          FLAGS_persist_user_defined_timestamps);
  fprintf(stdout, "WAL compression           : %s\n",
          FLAGS_wal_compression.c_str());
  fprintf(stdout, "Try verify sst unique id  : %d\n",
          static_cast<int>(FLAGS_verify_sst_unique_id_in_manifest));

  fprintf(stdout, "------------------------------------------------\n");
}

void StressTest::Open(SharedState* shared, bool reopen) {
  assert(db_ == nullptr);
  assert(txn_db_ == nullptr);
  assert(optimistic_txn_db_ == nullptr);
  if (!InitializeOptionsFromFile(options_)) {
    InitializeOptionsFromFlags(cache_, filter_policy_, options_);
  }
  InitializeOptionsGeneral(cache_, filter_policy_, sqfc_factory_, options_);

  if (FLAGS_prefix_size == 0 && FLAGS_rep_factory == kHashSkipList) {
    fprintf(stderr,
            "prefeix_size cannot be zero if memtablerep == prefix_hash\n");
    exit(1);
  }
  if (FLAGS_prefix_size != 0 && FLAGS_rep_factory != kHashSkipList) {
    fprintf(stdout,
            "WARNING: prefix_size is non-zero but "
            "memtablerep != prefix_hash\n");
  }

  if ((options_.enable_blob_files || options_.enable_blob_garbage_collection ||
       FLAGS_allow_setting_blob_options_dynamically) &&
      FLAGS_best_efforts_recovery) {
    fprintf(stderr,
            "Integrated BlobDB is currently incompatible with best-effort "
            "recovery\n");
    exit(1);
  }

  fprintf(stdout,
          "Integrated BlobDB: blob files enabled %d, min blob size %" PRIu64
          ", blob file size %" PRIu64
          ", blob compression type %s, blob GC enabled %d, cutoff %f, force "
          "threshold %f, blob compaction readahead size %" PRIu64
          ", blob file starting level %d\n",
          options_.enable_blob_files, options_.min_blob_size,
          options_.blob_file_size,
          CompressionTypeToString(options_.blob_compression_type).c_str(),
          options_.enable_blob_garbage_collection,
          options_.blob_garbage_collection_age_cutoff,
          options_.blob_garbage_collection_force_threshold,
          options_.blob_compaction_readahead_size,
          options_.blob_file_starting_level);

  if (FLAGS_use_blob_cache) {
    fprintf(stdout,
            "Integrated BlobDB: blob cache enabled"
            ", block and blob caches shared: %d",
            FLAGS_use_shared_block_and_blob_cache);
    if (!FLAGS_use_shared_block_and_blob_cache) {
      fprintf(stdout,
              ", blob cache size %" PRIu64 ", blob cache num shard bits: %d",
              FLAGS_blob_cache_size, FLAGS_blob_cache_numshardbits);
    }
    fprintf(stdout, ", blob cache prepopulated: %d\n",
            FLAGS_prepopulate_blob_cache);
  } else {
    fprintf(stdout, "Integrated BlobDB: blob cache disabled\n");
  }

  fprintf(stdout, "DB path: [%s]\n", FLAGS_db.c_str());

  Status s;

  if (FLAGS_ttl == -1) {
    std::vector<std::string> existing_column_families;
    s = DB::ListColumnFamilies(DBOptions(options_), FLAGS_db,
                               &existing_column_families);  // ignore errors
    if (!s.ok()) {
      // DB doesn't exist
      assert(existing_column_families.empty());
      assert(column_family_names_.empty());
      column_family_names_.push_back(kDefaultColumnFamilyName);
    } else if (column_family_names_.empty()) {
      // this is the first call to the function Open()
      column_family_names_ = existing_column_families;
    } else {
      // this is a reopen. just assert that existing column_family_names are
      // equivalent to what we remember
      auto sorted_cfn = column_family_names_;
      std::sort(sorted_cfn.begin(), sorted_cfn.end());
      std::sort(existing_column_families.begin(),
                existing_column_families.end());
      if (sorted_cfn != existing_column_families) {
        fprintf(stderr, "Expected column families differ from the existing:\n");
        fprintf(stderr, "Expected: {");
        for (const auto& cf : sorted_cfn) {
          fprintf(stderr, "%s ", cf.c_str());
        }
        fprintf(stderr, "}\n");
        fprintf(stderr, "Existing: {");
        for (const auto& cf : existing_column_families) {
          fprintf(stderr, "%s ", cf.c_str());
        }
        fprintf(stderr, "}\n");
      }
      assert(sorted_cfn == existing_column_families);
    }
    std::vector<ColumnFamilyDescriptor> cf_descriptors;
    for (const auto& name : column_family_names_) {
      if (name != kDefaultColumnFamilyName) {
        new_column_family_name_ =
            std::max(new_column_family_name_.load(), std::stoi(name) + 1);
      }
      cf_descriptors.emplace_back(name, ColumnFamilyOptions(options_));
    }
    while (cf_descriptors.size() < (size_t)FLAGS_column_families) {
      std::string name = std::to_string(new_column_family_name_.load());
      new_column_family_name_++;
      cf_descriptors.emplace_back(name, ColumnFamilyOptions(options_));
      column_family_names_.push_back(name);
    }

    options_.listeners.clear();
    options_.listeners.emplace_back(new DbStressListener(
        FLAGS_db, options_.db_paths, cf_descriptors, db_stress_listener_env));
    RegisterAdditionalListeners();

    // If this is for DB reopen,  error injection may have been enabled.
    // Disable it here in case there is no open fault injection.
    if (fault_fs_guard) {
      fault_fs_guard->DisableAllThreadLocalErrorInjection();
    }
    // TODO; test transaction DB Open with fault injection
    if (!FLAGS_use_txn) {
      bool inject_sync_fault = FLAGS_sync_fault_injection;
      bool inject_open_meta_read_error =
          FLAGS_open_metadata_read_fault_one_in > 0;
      bool inject_open_meta_write_error =
          FLAGS_open_metadata_write_fault_one_in > 0;
      bool inject_open_read_error = FLAGS_open_read_fault_one_in > 0;
      bool inject_open_write_error = FLAGS_open_write_fault_one_in > 0;
      if ((inject_sync_fault || inject_open_meta_read_error ||
           inject_open_meta_write_error || inject_open_read_error ||
           inject_open_write_error) &&
          fault_fs_guard
              ->FileExists(FLAGS_db + "/CURRENT", IOOptions(), nullptr)
              .ok()) {
        if (inject_sync_fault || inject_open_write_error) {
          fault_fs_guard->SetFilesystemDirectWritable(false);
          fault_fs_guard->SetInjectUnsyncedDataLoss(inject_sync_fault);
        }
        fault_fs_guard->SetThreadLocalErrorContext(
            FaultInjectionIOType::kMetadataRead,
            static_cast<uint32_t>(FLAGS_seed),
            FLAGS_open_metadata_read_fault_one_in, false /* retryable */,
            false /* has_data_loss */);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);

        fault_fs_guard->SetThreadLocalErrorContext(
            FaultInjectionIOType::kMetadataWrite,
            static_cast<uint32_t>(FLAGS_seed),
            FLAGS_open_metadata_write_fault_one_in, false /* retryable */,
            false /* has_data_loss */);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataWrite);

        fault_fs_guard->SetThreadLocalErrorContext(
            FaultInjectionIOType::kRead, static_cast<uint32_t>(FLAGS_seed),
            FLAGS_open_read_fault_one_in, false /* retryable */,
            false /* has_data_loss */);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kRead);

        fault_fs_guard->SetThreadLocalErrorContext(
            FaultInjectionIOType::kWrite, static_cast<uint32_t>(FLAGS_seed),
            FLAGS_open_write_fault_one_in, false /* retryable */,
            false /* has_data_loss */);
        fault_fs_guard->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kWrite);
      }
      while (true) {
        // StackableDB-based BlobDB
        if (FLAGS_use_blob_db) {
          blob_db::BlobDBOptions blob_db_options;
          blob_db_options.min_blob_size = FLAGS_blob_db_min_blob_size;
          blob_db_options.bytes_per_sync = FLAGS_blob_db_bytes_per_sync;
          blob_db_options.blob_file_size = FLAGS_blob_db_file_size;
          blob_db_options.enable_garbage_collection = FLAGS_blob_db_enable_gc;
          blob_db_options.garbage_collection_cutoff = FLAGS_blob_db_gc_cutoff;

          blob_db::BlobDB* blob_db = nullptr;
          s = blob_db::BlobDB::Open(options_, blob_db_options, FLAGS_db,
                                    cf_descriptors, &column_families_,
                                    &blob_db);
          if (s.ok()) {
            db_ = blob_db;
          }
        } else {
          if (db_preload_finished_.load() && FLAGS_read_only) {
            s = DB::OpenForReadOnly(DBOptions(options_), FLAGS_db,
                                    cf_descriptors, &column_families_, &db_);
          } else {
            s = DB::Open(DBOptions(options_), FLAGS_db, cf_descriptors,
                         &column_families_, &db_);
          }
        }

        if (inject_sync_fault || inject_open_meta_read_error ||
            inject_open_meta_write_error || inject_open_read_error ||
            inject_open_write_error) {
          fault_fs_guard->DisableAllThreadLocalErrorInjection();

          if (s.ok()) {
            // Injected errors might happen in background compactions. We
            // wait for all compactions to finish to make sure DB is in
            // clean state before executing queries.
            s = db_->GetRootDB()->WaitForCompact(WaitForCompactOptions());
            if (!s.ok()) {
              for (auto cf : column_families_) {
                delete cf;
              }
              column_families_.clear();
              delete db_;
              db_ = nullptr;
            }
          }
          if (!s.ok()) {
            // After failure to opening a DB due to IO error or unsynced data
            // loss, retry should successfully open the DB with correct data if
            // no IO error shows up.
            inject_sync_fault = false;
            inject_open_meta_read_error = false;
            inject_open_meta_write_error = false;
            inject_open_read_error = false;
            inject_open_write_error = false;

            // TODO: Unsynced data loss during DB reopen is not supported yet in
            //  stress test. Will need to recreate expected state if we decide
            //  to support unsynced data loss during DB reopen.
            if (!reopen) {
              Random rand(static_cast<uint32_t>(FLAGS_seed));
              if (rand.OneIn(2)) {
                fault_fs_guard->DeleteFilesCreatedAfterLastDirSync(IOOptions(),
                                                                   nullptr);
              }
              if (rand.OneIn(3)) {
                fault_fs_guard->DropUnsyncedFileData();
              } else if (rand.OneIn(2)) {
                fault_fs_guard->DropRandomUnsyncedFileData(&rand);
              }
            }
            continue;
          }
        }
        break;
      }
    } else {
      if (FLAGS_use_optimistic_txn) {
        OptimisticTransactionDBOptions optimistic_txn_db_options;
        optimistic_txn_db_options.validate_policy =
            static_cast<OccValidationPolicy>(FLAGS_occ_validation_policy);

        if (FLAGS_share_occ_lock_buckets) {
          optimistic_txn_db_options.shared_lock_buckets =
              MakeSharedOccLockBuckets(FLAGS_occ_lock_bucket_count);
        } else {
          optimistic_txn_db_options.occ_lock_buckets =
              FLAGS_occ_lock_bucket_count;
          optimistic_txn_db_options.shared_lock_buckets = nullptr;
        }
        s = OptimisticTransactionDB::Open(
            options_, optimistic_txn_db_options, FLAGS_db, cf_descriptors,
            &column_families_, &optimistic_txn_db_);
        if (!s.ok()) {
          fprintf(stderr, "Error in opening the OptimisticTransactionDB [%s]\n",
                  s.ToString().c_str());
          fflush(stderr);
        }
        assert(s.ok());
        {
          db_ = optimistic_txn_db_;
          db_aptr_.store(optimistic_txn_db_, std::memory_order_release);
        }
      } else {
        TransactionDBOptions txn_db_options;
        assert(FLAGS_txn_write_policy <= TxnDBWritePolicy::WRITE_UNPREPARED);
        txn_db_options.write_policy =
            static_cast<TxnDBWritePolicy>(FLAGS_txn_write_policy);
        if (FLAGS_unordered_write) {
          assert(txn_db_options.write_policy ==
                 TxnDBWritePolicy::WRITE_PREPARED);
          options_.unordered_write = true;
          options_.two_write_queues = true;
          txn_db_options.skip_concurrency_control = true;
        } else {
          options_.two_write_queues = FLAGS_two_write_queues;
        }
        txn_db_options.wp_snapshot_cache_bits =
            static_cast<size_t>(FLAGS_wp_snapshot_cache_bits);
        txn_db_options.wp_commit_cache_bits =
            static_cast<size_t>(FLAGS_wp_commit_cache_bits);
        PrepareTxnDbOptions(shared, txn_db_options);
        s = TransactionDB::Open(options_, txn_db_options, FLAGS_db,
                                cf_descriptors, &column_families_, &txn_db_);
        if (!s.ok()) {
          fprintf(stderr, "Error in opening the TransactionDB [%s]\n",
                  s.ToString().c_str());
          fflush(stderr);
        }
        assert(s.ok());

        // Do not swap the order of the following.
        {
          db_ = txn_db_;
          db_aptr_.store(txn_db_, std::memory_order_release);
        }
      }
    }
    if (!s.ok()) {
      fprintf(stderr, "Error in opening the DB [%s]\n", s.ToString().c_str());
      fflush(stderr);
    }
    assert(s.ok());
    assert(column_families_.size() ==
           static_cast<size_t>(FLAGS_column_families));

    // Secondary instance does not support write-prepared/write-unprepared
    // transactions, thus just disable secondary instance if we use
    // transaction.
    if (s.ok() && FLAGS_test_secondary && !FLAGS_use_txn) {
      Options tmp_opts;
      // TODO(yanqin) support max_open_files != -1 for secondary instance.
      tmp_opts.max_open_files = -1;
      tmp_opts.env = db_stress_env;
      const std::string& secondary_path = FLAGS_secondaries_base;
      s = DB::OpenAsSecondary(tmp_opts, FLAGS_db, secondary_path,
                              cf_descriptors, &cmp_cfhs_, &cmp_db_);
      assert(s.ok());
      assert(cmp_cfhs_.size() == static_cast<size_t>(FLAGS_column_families));
    }
  } else {
    DBWithTTL* db_with_ttl;
    s = DBWithTTL::Open(options_, FLAGS_db, &db_with_ttl, FLAGS_ttl);
    db_ = db_with_ttl;
  }

  if (FLAGS_preserve_unverified_changes) {
    // Up until now, no live file should have become obsolete due to these
    // options. After `DisableFileDeletions()` we can reenable auto compactions
    // since, even if live files become obsolete, they won't be deleted.
    assert(options_.avoid_flush_during_recovery);
    assert(options_.disable_auto_compactions);
    if (s.ok()) {
      s = db_->DisableFileDeletions();
    }
    if (s.ok()) {
      s = db_->EnableAutoCompaction(column_families_);
    }
  }

  if (!s.ok()) {
    fprintf(stderr, "open error: %s\n", s.ToString().c_str());
    exit(1);
  }
}

void StressTest::Reopen(ThreadState* thread) {
  // BG jobs in WritePrepared must be canceled first because i) they can access
  // the db via a callbac ii) they hold on to a snapshot and the upcoming
  // ::Close would complain about it.
  const bool write_prepared = FLAGS_use_txn && FLAGS_txn_write_policy != 0;
  bool bg_canceled __attribute__((unused)) = false;
  if (write_prepared || thread->rand.OneIn(2)) {
    const bool wait =
        write_prepared || static_cast<bool>(thread->rand.OneIn(2));
    CancelAllBackgroundWork(db_, wait);
    bg_canceled = wait;
  }
  assert(!write_prepared || bg_canceled);

  for (auto cf : column_families_) {
    delete cf;
  }
  column_families_.clear();

  // Currently reopen does not restore expected state
  // with potential data loss in mind like the first open before
  // crash-recovery verification does. Therefore it always expects no data loss
  // and we should ensure no data loss in testing.
  // TODO(hx235): eliminate the FlushWAL(true /* sync */)/SyncWAL() below
  if (!FLAGS_disable_wal) {
    Status s;
    if (FLAGS_manual_wal_flush_one_in > 0) {
      s = db_->FlushWAL(/*sync=*/true);
    } else {
      s = db_->SyncWAL();
    }
    if (!s.ok()) {
      fprintf(stderr,
              "Error persisting WAL data which is needed before reopening the "
              "DB: %s\n",
              s.ToString().c_str());
      exit(1);
    }
  }

  if (thread->rand.OneIn(2)) {
    Status s = db_->Close();
    if (!s.ok()) {
      fprintf(stderr, "Non-ok close status: %s\n", s.ToString().c_str());
      fflush(stderr);
    }
    assert(s.ok());
  }
  assert((txn_db_ == nullptr && optimistic_txn_db_ == nullptr) ||
         (db_ == txn_db_ || db_ == optimistic_txn_db_));
  delete db_;
  db_ = nullptr;
  txn_db_ = nullptr;
  optimistic_txn_db_ = nullptr;

  num_times_reopened_++;
  auto now = clock_->NowMicros();
  fprintf(stdout, "%s Reopening database for the %dth time\n",
          clock_->TimeToString(now / 1000000).c_str(), num_times_reopened_);
  Open(thread->shared, /*reopen=*/true);

  if (thread->shared->GetStressTest()->MightHaveUnsyncedDataLoss() &&
      IsStateTracked()) {
    Status s = thread->shared->SaveAtAndAfter(db_);
    if (!s.ok()) {
      fprintf(stderr, "Error enabling history tracing: %s\n",
              s.ToString().c_str());
      exit(1);
    }
  }
}

bool StressTest::MaybeUseOlderTimestampForPointLookup(ThreadState* thread,
                                                      std::string& ts_str,
                                                      Slice& ts_slice,
                                                      ReadOptions& read_opts) {
  if (FLAGS_user_timestamp_size == 0) {
    return false;
  }

  if (!FLAGS_persist_user_defined_timestamps) {
    // Not read with older timestamps to avoid get InvalidArgument.
    return false;
  }

  assert(thread);
  if (!thread->rand.OneInOpt(3)) {
    return false;
  }

  const SharedState* const shared = thread->shared;
  assert(shared);
  const uint64_t start_ts = shared->GetStartTimestamp();

  uint64_t now = db_stress_env->NowNanos();

  assert(now > start_ts);
  uint64_t time_diff = now - start_ts;
  uint64_t ts = start_ts + (thread->rand.Next64() % time_diff);
  ts_str.clear();
  PutFixed64(&ts_str, ts);
  ts_slice = ts_str;
  read_opts.timestamp = &ts_slice;
  return true;
}

void StressTest::MaybeUseOlderTimestampForRangeScan(ThreadState* thread,
                                                    std::string& ts_str,
                                                    Slice& ts_slice,
                                                    ReadOptions& read_opts) {
  if (FLAGS_user_timestamp_size == 0) {
    return;
  }

  if (!FLAGS_persist_user_defined_timestamps) {
    // Not read with older timestamps to avoid get InvalidArgument.
    return;
  }

  assert(thread);
  if (!thread->rand.OneInOpt(3)) {
    return;
  }

  const Slice* const saved_ts = read_opts.timestamp;
  assert(saved_ts != nullptr);

  const SharedState* const shared = thread->shared;
  assert(shared);
  const uint64_t start_ts = shared->GetStartTimestamp();

  uint64_t now = db_stress_env->NowNanos();

  assert(now > start_ts);
  uint64_t time_diff = now - start_ts;
  uint64_t ts = start_ts + (thread->rand.Next64() % time_diff);
  ts_str.clear();
  PutFixed64(&ts_str, ts);
  ts_slice = ts_str;
  read_opts.timestamp = &ts_slice;

  // TODO (yanqin): support Merge with iter_start_ts
  if (!thread->rand.OneInOpt(3) || FLAGS_use_merge || FLAGS_use_full_merge_v1) {
    return;
  }

  ts_str.clear();
  PutFixed64(&ts_str, start_ts);
  ts_slice = ts_str;
  read_opts.iter_start_ts = &ts_slice;
  read_opts.timestamp = saved_ts;
}

void CheckAndSetOptionsForUserTimestamp(Options& options) {
  assert(FLAGS_user_timestamp_size > 0);
  const Comparator* const cmp = test::BytewiseComparatorWithU64TsWrapper();
  assert(cmp);
  if (FLAGS_user_timestamp_size != cmp->timestamp_size()) {
    fprintf(stderr,
            "Only -user_timestamp_size=%d is supported in stress test.\n",
            static_cast<int>(cmp->timestamp_size()));
    exit(1);
  }
  if (FLAGS_use_txn) {
    fprintf(stderr, "TransactionDB does not support timestamp yet.\n");
    exit(1);
  }
  if (FLAGS_test_cf_consistency || FLAGS_test_batches_snapshots) {
    fprintf(stderr,
            "Due to per-key ts-seq ordering constraint, only the (default) "
            "non-batched test is supported with timestamp.\n");
    exit(1);
  }
  if (FLAGS_ingest_external_file_one_in > 0) {
    fprintf(stderr, "Bulk loading may not support timestamp yet.\n");
    exit(1);
  }
  options.comparator = cmp;
  options.persist_user_defined_timestamps =
      FLAGS_persist_user_defined_timestamps;
}

bool ShouldDisableAutoCompactionsBeforeVerifyDb() {
  return !FLAGS_disable_auto_compactions && FLAGS_enable_compaction_filter;
}

bool InitializeOptionsFromFile(Options& options) {
  DBOptions db_options;
  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.input_strings_escaped = true;
  config_options.env = db_stress_env;
  std::vector<ColumnFamilyDescriptor> cf_descriptors;
  if (!FLAGS_options_file.empty()) {
    Status s = LoadOptionsFromFile(config_options, FLAGS_options_file,
                                   &db_options, &cf_descriptors);
    if (!s.ok()) {
      fprintf(stderr, "Unable to load options file %s --- %s\n",
              FLAGS_options_file.c_str(), s.ToString().c_str());
      exit(1);
    }
    db_options.env = new CompositeEnvWrapper(db_stress_env);
    options = Options(db_options, cf_descriptors[0].options);
    return true;
  }
  return false;
}

void InitializeOptionsFromFlags(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<const FilterPolicy>& filter_policy,
    Options& options) {
  BlockBasedTableOptions block_based_options;
  block_based_options.decouple_partitioned_filters =
      FLAGS_decouple_partitioned_filters;
  block_based_options.block_cache = cache;
  block_based_options.cache_index_and_filter_blocks =
      FLAGS_cache_index_and_filter_blocks;
  block_based_options.metadata_cache_options.top_level_index_pinning =
      static_cast<PinningTier>(FLAGS_top_level_index_pinning);
  block_based_options.metadata_cache_options.partition_pinning =
      static_cast<PinningTier>(FLAGS_partition_pinning);
  block_based_options.metadata_cache_options.unpartitioned_pinning =
      static_cast<PinningTier>(FLAGS_unpartitioned_pinning);
  block_based_options.checksum = checksum_type_e;
  block_based_options.block_size = FLAGS_block_size;
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kCompressionDictionaryBuildingBuffer,
       {/*.charged = */ FLAGS_charge_compression_dictionary_building_buffer
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kFilterConstruction,
       {/*.charged = */ FLAGS_charge_filter_construction
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kBlockBasedTableReader,
       {/*.charged = */ FLAGS_charge_table_reader
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kFileMetadata,
       {/*.charged = */ FLAGS_charge_file_metadata
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kBlobCache,
       {/*.charged = */ FLAGS_charge_blob_cache
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.format_version =
      static_cast<uint32_t>(FLAGS_format_version);
  block_based_options.index_block_restart_interval =
      static_cast<int32_t>(FLAGS_index_block_restart_interval);
  block_based_options.filter_policy = filter_policy;
  block_based_options.partition_filters = FLAGS_partition_filters;
  block_based_options.optimize_filters_for_memory =
      FLAGS_optimize_filters_for_memory;
  block_based_options.detect_filter_construct_corruption =
      FLAGS_detect_filter_construct_corruption;
  block_based_options.index_type =
      static_cast<BlockBasedTableOptions::IndexType>(FLAGS_index_type);
  block_based_options.data_block_index_type =
      static_cast<BlockBasedTableOptions::DataBlockIndexType>(
          FLAGS_data_block_index_type);
  block_based_options.prepopulate_block_cache =
      static_cast<BlockBasedTableOptions::PrepopulateBlockCache>(
          FLAGS_prepopulate_block_cache);
  block_based_options.initial_auto_readahead_size =
      FLAGS_initial_auto_readahead_size;
  block_based_options.max_auto_readahead_size = FLAGS_max_auto_readahead_size;
  block_based_options.num_file_reads_for_auto_readahead =
      FLAGS_num_file_reads_for_auto_readahead;
  block_based_options.cache_index_and_filter_blocks_with_high_priority =
      FLAGS_cache_index_and_filter_blocks_with_high_priority;
  block_based_options.use_delta_encoding = FLAGS_use_delta_encoding;
  block_based_options.verify_compression = FLAGS_verify_compression;
  block_based_options.read_amp_bytes_per_bit = FLAGS_read_amp_bytes_per_bit;
  block_based_options.enable_index_compression = FLAGS_enable_index_compression;
  block_based_options.index_shortening =
      static_cast<BlockBasedTableOptions::IndexShorteningMode>(
          FLAGS_index_shortening);
  block_based_options.block_align = FLAGS_block_align;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
  options.db_write_buffer_size = FLAGS_db_write_buffer_size;
  options.write_buffer_size = FLAGS_write_buffer_size;
  options.max_write_buffer_number = FLAGS_max_write_buffer_number;
  options.min_write_buffer_number_to_merge =
      FLAGS_min_write_buffer_number_to_merge;
  options.max_write_buffer_number_to_maintain =
      FLAGS_max_write_buffer_number_to_maintain;
  options.max_write_buffer_size_to_maintain =
      FLAGS_max_write_buffer_size_to_maintain;
  options.memtable_prefix_bloom_size_ratio =
      FLAGS_memtable_prefix_bloom_size_ratio;
  if (FLAGS_use_write_buffer_manager) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(FLAGS_db_write_buffer_size, block_cache));
  }
  options.memtable_whole_key_filtering = FLAGS_memtable_whole_key_filtering;
  if (ShouldDisableAutoCompactionsBeforeVerifyDb()) {
    options.disable_auto_compactions = true;
  } else {
    options.disable_auto_compactions = FLAGS_disable_auto_compactions;
  }
  options.max_background_compactions = FLAGS_max_background_compactions;
  options.max_background_flushes = FLAGS_max_background_flushes;
  options.compaction_style =
      static_cast<ROCKSDB_NAMESPACE::CompactionStyle>(FLAGS_compaction_style);
  if (options.compaction_style ==
      ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleFIFO) {
    options.compaction_options_fifo.allow_compaction =
        FLAGS_fifo_allow_compaction;
  }
  options.compaction_pri =
      static_cast<ROCKSDB_NAMESPACE::CompactionPri>(FLAGS_compaction_pri);
  options.num_levels = FLAGS_num_levels;
  if (FLAGS_prefix_size >= 0) {
    options.prefix_extractor.reset(NewFixedPrefixTransform(FLAGS_prefix_size));
    if (FLAGS_enable_memtable_insert_with_hint_prefix_extractor) {
      options.memtable_insert_with_hint_prefix_extractor =
          options.prefix_extractor;
    }
  }
  options.max_open_files = FLAGS_open_files;
  options.statistics = dbstats;
  options.env = db_stress_env;
  options.use_fsync = FLAGS_use_fsync;
  options.compaction_readahead_size = FLAGS_compaction_readahead_size;
  options.allow_mmap_reads = FLAGS_mmap_read;
  options.allow_mmap_writes = FLAGS_mmap_write;
  options.use_direct_reads = FLAGS_use_direct_reads;
  options.use_direct_io_for_flush_and_compaction =
      FLAGS_use_direct_io_for_flush_and_compaction;
  options.recycle_log_file_num =
      static_cast<size_t>(FLAGS_recycle_log_file_num);
  options.target_file_size_base = FLAGS_target_file_size_base;
  options.target_file_size_multiplier = FLAGS_target_file_size_multiplier;
  options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
  options.max_bytes_for_level_multiplier = FLAGS_max_bytes_for_level_multiplier;
  options.level0_stop_writes_trigger = FLAGS_level0_stop_writes_trigger;
  options.level0_slowdown_writes_trigger = FLAGS_level0_slowdown_writes_trigger;
  options.level0_file_num_compaction_trigger =
      FLAGS_level0_file_num_compaction_trigger;
  options.compression = compression_type_e;
  options.bottommost_compression = bottommost_compression_type_e;
  options.compression_opts.max_dict_bytes = FLAGS_compression_max_dict_bytes;
  options.compression_opts.zstd_max_train_bytes =
      FLAGS_compression_zstd_max_train_bytes;
  options.compression_opts.parallel_threads =
      FLAGS_compression_parallel_threads;
  options.compression_opts.max_dict_buffer_bytes =
      FLAGS_compression_max_dict_buffer_bytes;
  if (ZSTD_FinalizeDictionarySupported()) {
    options.compression_opts.use_zstd_dict_trainer =
        FLAGS_compression_use_zstd_dict_trainer;
  } else if (!FLAGS_compression_use_zstd_dict_trainer) {
    fprintf(
        stdout,
        "WARNING: use_zstd_dict_trainer is false but zstd finalizeDictionary "
        "cannot be used because ZSTD 1.4.5+ is not linked with the binary."
        " zstd dictionary trainer will be used.\n");
  }
  if (FLAGS_compression_checksum) {
    options.compression_opts.checksum = true;
  }
  options.max_manifest_file_size = FLAGS_max_manifest_file_size;
  options.max_subcompactions = static_cast<uint32_t>(FLAGS_subcompactions);
  options.allow_concurrent_memtable_write =
      FLAGS_allow_concurrent_memtable_write;
  options.experimental_mempurge_threshold =
      FLAGS_experimental_mempurge_threshold;
  options.periodic_compaction_seconds = FLAGS_periodic_compaction_seconds;
  options.daily_offpeak_time_utc = FLAGS_daily_offpeak_time_utc;
  options.stats_dump_period_sec =
      static_cast<unsigned int>(FLAGS_stats_dump_period_sec);
  options.ttl = FLAGS_compaction_ttl;
  options.enable_pipelined_write = FLAGS_enable_pipelined_write;
  options.enable_write_thread_adaptive_yield =
      FLAGS_enable_write_thread_adaptive_yield;
  options.compaction_options_universal.size_ratio = FLAGS_universal_size_ratio;
  options.compaction_options_universal.min_merge_width =
      FLAGS_universal_min_merge_width;
  options.compaction_options_universal.max_merge_width =
      FLAGS_universal_max_merge_width;
  options.compaction_options_universal.max_size_amplification_percent =
      FLAGS_universal_max_size_amplification_percent;
  options.compaction_options_universal.max_read_amp =
      FLAGS_universal_max_read_amp;
  options.atomic_flush = FLAGS_atomic_flush;
  options.manual_wal_flush = FLAGS_manual_wal_flush_one_in > 0 ? true : false;
  options.avoid_unnecessary_blocking_io = FLAGS_avoid_unnecessary_blocking_io;
  options.write_dbid_to_manifest = FLAGS_write_dbid_to_manifest;
  options.write_identity_file = FLAGS_write_identity_file;
  options.avoid_flush_during_recovery = FLAGS_avoid_flush_during_recovery;
  options.max_write_batch_group_size_bytes =
      FLAGS_max_write_batch_group_size_bytes;
  options.level_compaction_dynamic_level_bytes =
      FLAGS_level_compaction_dynamic_level_bytes;
  options.track_and_verify_wals_in_manifest = true;
  options.verify_sst_unique_id_in_manifest =
      FLAGS_verify_sst_unique_id_in_manifest;
  options.memtable_protection_bytes_per_key =
      FLAGS_memtable_protection_bytes_per_key;
  options.block_protection_bytes_per_key = FLAGS_block_protection_bytes_per_key;
  options.paranoid_memory_checks = FLAGS_paranoid_memory_checks;

  // Integrated BlobDB
  options.enable_blob_files = FLAGS_enable_blob_files;
  options.min_blob_size = FLAGS_min_blob_size;
  options.blob_file_size = FLAGS_blob_file_size;
  options.blob_compression_type =
      StringToCompressionType(FLAGS_blob_compression_type.c_str());
  options.enable_blob_garbage_collection = FLAGS_enable_blob_garbage_collection;
  options.blob_garbage_collection_age_cutoff =
      FLAGS_blob_garbage_collection_age_cutoff;
  options.blob_garbage_collection_force_threshold =
      FLAGS_blob_garbage_collection_force_threshold;
  options.blob_compaction_readahead_size = FLAGS_blob_compaction_readahead_size;
  options.blob_file_starting_level = FLAGS_blob_file_starting_level;

  if (FLAGS_use_blob_cache) {
    if (FLAGS_use_shared_block_and_blob_cache) {
      options.blob_cache = cache;
    } else {
      if (FLAGS_blob_cache_size > 0) {
        LRUCacheOptions co;
        co.capacity = FLAGS_blob_cache_size;
        co.num_shard_bits = FLAGS_blob_cache_numshardbits;
        options.blob_cache = NewLRUCache(co);
      } else {
        fprintf(stderr,
                "Unable to create a standalone blob cache if blob_cache_size "
                "<= 0.\n");
        exit(1);
      }
    }
    switch (FLAGS_prepopulate_blob_cache) {
      case 0:
        options.prepopulate_blob_cache = PrepopulateBlobCache::kDisable;
        break;
      case 1:
        options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
        break;
      default:
        fprintf(stderr, "Unknown prepopulate blob cache mode\n");
        exit(1);
    }
  }

  options.wal_compression =
      StringToCompressionType(FLAGS_wal_compression.c_str());

  options.last_level_temperature =
      StringToTemperature(FLAGS_last_level_temperature.c_str());
  options.default_write_temperature =
      StringToTemperature(FLAGS_default_write_temperature.c_str());
  options.default_temperature =
      StringToTemperature(FLAGS_default_temperature.c_str());

  options.preclude_last_level_data_seconds =
      FLAGS_preclude_last_level_data_seconds;
  options.preserve_internal_time_seconds = FLAGS_preserve_internal_time_seconds;

  switch (FLAGS_rep_factory) {
    case kSkipList:
      // no need to do anything
      break;
    case kHashSkipList:
      options.memtable_factory.reset(NewHashSkipListRepFactory(10000));
      break;
    case kVectorRep:
      options.memtable_factory.reset(new VectorRepFactory());
      break;
  }

  InitializeMergeOperator(options);

  if (FLAGS_enable_compaction_filter) {
    options.compaction_filter_factory =
        std::make_shared<DbStressCompactionFilterFactory>();
  }

  options.best_efforts_recovery = FLAGS_best_efforts_recovery;
  options.paranoid_file_checks = FLAGS_paranoid_file_checks;
  options.fail_if_options_file_error = FLAGS_fail_if_options_file_error;

  if (FLAGS_user_timestamp_size > 0) {
    CheckAndSetOptionsForUserTimestamp(options);
  }

  options.allow_data_in_errors = FLAGS_allow_data_in_errors;

  options.enable_thread_tracking = FLAGS_enable_thread_tracking;

  options.memtable_max_range_deletions = FLAGS_memtable_max_range_deletions;

  options.bottommost_file_compaction_delay =
      FLAGS_bottommost_file_compaction_delay;

  options.allow_fallocate = FLAGS_allow_fallocate;
  options.table_cache_numshardbits = FLAGS_table_cache_numshardbits;
  options.log_readahead_size = FLAGS_log_readahead_size;
  options.bgerror_resume_retry_interval = FLAGS_bgerror_resume_retry_interval;
  options.delete_obsolete_files_period_micros =
      FLAGS_delete_obsolete_files_period_micros;
  options.max_log_file_size = FLAGS_max_log_file_size;
  options.log_file_time_to_roll = FLAGS_log_file_time_to_roll;
  options.use_adaptive_mutex = FLAGS_use_adaptive_mutex;
  options.advise_random_on_open = FLAGS_advise_random_on_open;
  // TODO (hx235): test the functionality of `WAL_ttl_seconds`,
  // `WAL_size_limit_MB` i.e, `GetUpdatesSince()`
  options.WAL_ttl_seconds = FLAGS_WAL_ttl_seconds;
  options.WAL_size_limit_MB = FLAGS_WAL_size_limit_MB;
  options.wal_bytes_per_sync = FLAGS_wal_bytes_per_sync;
  options.strict_bytes_per_sync = FLAGS_strict_bytes_per_sync;
  options.avoid_flush_during_shutdown = FLAGS_avoid_flush_during_shutdown;
  options.dump_malloc_stats = FLAGS_dump_malloc_stats;
  options.stats_history_buffer_size = FLAGS_stats_history_buffer_size;
  options.skip_stats_update_on_db_open = FLAGS_skip_stats_update_on_db_open;
  options.optimize_filters_for_hits = FLAGS_optimize_filters_for_hits;
  options.sample_for_compression = FLAGS_sample_for_compression;
  options.report_bg_io_stats = FLAGS_report_bg_io_stats;
  options.manifest_preallocation_size = FLAGS_manifest_preallocation_size;
  if (FLAGS_enable_checksum_handoff) {
    options.checksum_handoff_file_types = {FileTypeSet::All()};
  } else {
    options.checksum_handoff_file_types = {};
  }
  options.max_total_wal_size = FLAGS_max_total_wal_size;
  options.soft_pending_compaction_bytes_limit =
      FLAGS_soft_pending_compaction_bytes_limit;
  options.hard_pending_compaction_bytes_limit =
      FLAGS_hard_pending_compaction_bytes_limit;
  options.max_sequential_skip_in_iterations =
      FLAGS_max_sequential_skip_in_iterations;
  if (FLAGS_enable_sst_partitioner_factory) {
    options.sst_partitioner_factory = std::shared_ptr<SstPartitionerFactory>(
        NewSstPartitionerFixedPrefixFactory(1));
  }
  options.lowest_used_cache_tier =
      static_cast<CacheTier>(FLAGS_lowest_used_cache_tier);
  options.inplace_update_support = FLAGS_inplace_update_support;
  options.uncache_aggressiveness = FLAGS_uncache_aggressiveness;
}

void InitializeOptionsGeneral(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<const FilterPolicy>& filter_policy,
    const std::shared_ptr<SstQueryFilterConfigsManager::Factory>& sqfc_factory,
    Options& options) {
  options.create_missing_column_families = true;
  options.create_if_missing = true;

  if (!options.statistics) {
    options.statistics = dbstats;
  }

  if (options.env == Options().env) {
    options.env = db_stress_env;
  }

  assert(options.table_factory);
  auto table_options =
      options.table_factory->GetOptions<BlockBasedTableOptions>();
  if (table_options) {
    if (FLAGS_cache_size > 0) {
      table_options->block_cache = cache;
    }
    if (!table_options->filter_policy) {
      table_options->filter_policy = filter_policy;
    }
  }

  // TODO: row_cache, thread-pool IO priority, CPU priority.

  if (!options.rate_limiter) {
    if (FLAGS_rate_limiter_bytes_per_sec > 0) {
      options.rate_limiter.reset(NewGenericRateLimiter(
          FLAGS_rate_limiter_bytes_per_sec, 1000 /* refill_period_us */,
          10 /* fairness */,
          FLAGS_rate_limit_bg_reads ? RateLimiter::Mode::kReadsOnly
                                    : RateLimiter::Mode::kWritesOnly));
    }
  }

  if (!options.file_checksum_gen_factory) {
    options.file_checksum_gen_factory =
        GetFileChecksumImpl(FLAGS_file_checksum_impl);
  }

  if (FLAGS_sst_file_manager_bytes_per_sec > 0 ||
      FLAGS_sst_file_manager_bytes_per_truncate > 0) {
    Status status;
    options.sst_file_manager.reset(NewSstFileManager(
        db_stress_env, options.info_log, "" /* trash_dir */,
        static_cast<int64_t>(FLAGS_sst_file_manager_bytes_per_sec),
        true /* delete_existing_trash */, &status,
        0.25 /* max_trash_db_ratio */,
        FLAGS_sst_file_manager_bytes_per_truncate));
    if (!status.ok()) {
      fprintf(stderr, "SstFileManager creation failed: %s\n",
              status.ToString().c_str());
      exit(1);
    }
  }

  if (FLAGS_preserve_unverified_changes) {
    if (!options.avoid_flush_during_recovery) {
      fprintf(stderr,
              "WARNING: flipping `avoid_flush_during_recovery` to true for "
              "`preserve_unverified_changes` to keep all files\n");
      options.avoid_flush_during_recovery = true;
    }
    // Together with `avoid_flush_during_recovery == true`, this will prevent
    // live files from becoming obsolete and deleted between `DB::Open()` and
    // `DisableFileDeletions()` due to flush or compaction. We do not need to
    // warn the user since we will reenable compaction soon.
    options.disable_auto_compactions = true;
  }

  options.table_properties_collector_factories.clear();
  options.table_properties_collector_factories.emplace_back(
      std::make_shared<DbStressTablePropertiesCollectorFactory>());

  if (sqfc_factory && !sqfc_factory->GetConfigs().IsEmptyNotFound()) {
    options.table_properties_collector_factories.emplace_back(sqfc_factory);
  }
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
