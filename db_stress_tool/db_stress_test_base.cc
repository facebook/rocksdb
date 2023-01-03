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

#include "util/compression.h"
#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_compaction_filter.h"
#include "db_stress_tool/db_stress_driver.h"
#include "db_stress_tool/db_stress_table_properties_collector.h"
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
  if (FLAGS_ribbon_starting_level >= 999) {
    // Use Bloom API
    new_policy = NewBloomFilterPolicy(FLAGS_bloom_bits, false);
  } else {
    new_policy = NewRibbonFilterPolicy(
        FLAGS_bloom_bits, /* bloom_before_level */ FLAGS_ribbon_starting_level);
  }
  return std::shared_ptr<const FilterPolicy>(new_policy);
}

}  // namespace

StressTest::StressTest()
    : cache_(NewCache(FLAGS_cache_size, FLAGS_cache_numshardbits)),
      compressed_cache_(NewLRUCache(FLAGS_compressed_cache_size,
                                    FLAGS_compressed_cache_numshardbits)),
      filter_policy_(CreateFilterPolicy()),
      db_(nullptr),
#ifndef ROCKSDB_LITE
      txn_db_(nullptr),
#endif
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
#ifndef ROCKSDB_LITE
    const Status s = !FLAGS_use_blob_db
                         ? DestroyDB(FLAGS_db, options)
                         : blob_db::DestroyBlobDB(FLAGS_db, options,
                                                  blob_db::BlobDBOptions());
#else
    const Status s = DestroyDB(FLAGS_db, options);
#endif  // !ROCKSDB_LITE

    if (!s.ok()) {
      fprintf(stderr, "Cannot destroy original db: %s\n", s.ToString().c_str());
      exit(1);
    }
  }
}

StressTest::~StressTest() {
  for (auto cf : column_families_) {
    delete cf;
  }
  column_families_.clear();
  delete db_;

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

  if (FLAGS_cache_type == "clock_cache") {
    fprintf(stderr, "Old clock cache implementation has been removed.\n");
    exit(1);
  } else if (FLAGS_cache_type == "hyper_clock_cache") {
    return HyperClockCacheOptions(static_cast<size_t>(capacity),
                                  FLAGS_block_size /*estimated_entry_charge*/,
                                  num_shard_bits)
        .MakeSharedCache();
  } else if (FLAGS_cache_type == "lru_cache") {
    LRUCacheOptions opts;
    opts.capacity = capacity;
    opts.num_shard_bits = num_shard_bits;
#ifndef ROCKSDB_LITE
    std::shared_ptr<SecondaryCache> secondary_cache;
    if (!FLAGS_secondary_cache_uri.empty()) {
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
      opts.secondary_cache = secondary_cache;
    }
#endif
    return NewLRUCache(opts);
  } else {
    fprintf(stderr, "Cache type not supported.");
    exit(1);
  }
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
      {"max_successive_merges", {"0", "2", "4"}},
      {"inplace_update_num_locks", {"100", "200", "300"}},
      // TODO: re-enable once internal task T124324915 is fixed.
      // {"experimental_mempurge_threshold", {"0.0", "1.0"}},
      // TODO(ljin): enable test for this option
      // {"disable_auto_compactions", {"100", "200", "300"}},
      {"level0_file_num_compaction_trigger",
       {
           std::to_string(options_.level0_file_num_compaction_trigger),
           std::to_string(options_.level0_file_num_compaction_trigger + 2),
           std::to_string(options_.level0_file_num_compaction_trigger + 4),
       }},
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
#ifndef ROCKSDB_LITE
  if (FLAGS_use_txn) {
    // It's OK here without sync because unsynced data cannot be lost at this
    // point
    // - even with sync_fault_injection=1 as the
    // file is still directly writable until after FinishInitDb()
    ProcessRecoveredPreparedTxns(shared);
  }
#endif
  if (FLAGS_enable_compaction_filter) {
    auto* compaction_filter_factory =
        reinterpret_cast<DbStressCompactionFilterFactory*>(
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
  // For `FLAGS_manual_wal_flush_one_inWAL`
  // data can be lost when `manual_wal_flush_one_in > 0` and `FlushWAL()` is not
  // explictly called by users of RocksDB (in our case, db stress).
  // Therefore recovery from such potential WAL data loss is a prefix recovery
  // that requires tracing
  if ((FLAGS_sync_fault_injection || FLAGS_disable_wal ||
       FLAGS_manual_wal_flush_one_in > 0) &&
      IsStateTracked()) {
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

void StressTest::VerificationAbort(SharedState* shared, std::string msg,
                                   Status s) const {
  fprintf(stderr, "Verification failed: %s. Status is %s\n", msg.c_str(),
          s.ToString().c_str());
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
                                   const WideColumns& columns,
                                   const WideColumns& expected_columns) const {
  assert(shared);

  auto key_str = Key(key);

  fprintf(stderr,
          "Verification failed for column family %d key %s (%" PRIi64
          "): Value and columns inconsistent: %s\n",
          cf, Slice(key_str).ToString(/* hex */ true).c_str(), key,
          DebugString(value, columns, expected_columns).c_str());

  shared->SetVerificationFailure();
}

std::string StressTest::DebugString(const Slice& value,
                                    const WideColumns& columns,
                                    const WideColumns& expected_columns) {
  std::ostringstream oss;

  oss << "value: " << value.ToString(/* hex */ true);

  auto dump = [](const WideColumns& cols, std::ostream& os) {
    if (cols.empty()) {
      return;
    }

    os << std::hex;

    auto it = cols.begin();
    os << *it;
    for (++it; it != cols.end(); ++it) {
      os << ' ' << *it;
    }
  };

  oss << ", columns: ";
  dump(columns, oss);

  oss << ", expected_columns: ";
  dump(expected_columns, oss);

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

      constexpr uint32_t value_base = 0;
      const size_t sz = GenerateValue(value_base, value, sizeof(value));

      const Slice v(value, sz);

      shared->Put(cf_idx, k, value_base, true /* pending */);

      std::string ts;
      if (FLAGS_user_timestamp_size > 0) {
        ts = GetNowNanos();
      }

      if (FLAGS_use_merge) {
        if (!FLAGS_use_txn) {
          if (FLAGS_user_timestamp_size > 0) {
            s = db_->Merge(write_opts, cfh, key, ts, v);
          } else {
            s = db_->Merge(write_opts, cfh, key, v);
          }
        } else {
#ifndef ROCKSDB_LITE
          Transaction* txn;
          s = NewTxn(write_opts, &txn);
          if (s.ok()) {
            s = txn->Merge(cfh, key, v);
            if (s.ok()) {
              s = CommitTxn(txn);
            }
          }
#endif
        }
      } else if (FLAGS_use_put_entity_one_in > 0) {
        s = db_->PutEntity(write_opts, cfh, key,
                           GenerateWideColumns(value_base, v));
      } else {
        if (!FLAGS_use_txn) {
          if (FLAGS_user_timestamp_size > 0) {
            s = db_->Put(write_opts, cfh, key, ts, v);
          } else {
            s = db_->Put(write_opts, cfh, key, v);
          }
        } else {
#ifndef ROCKSDB_LITE
          Transaction* txn;
          s = NewTxn(write_opts, &txn);
          if (s.ok()) {
            s = txn->Put(cfh, key, v);
            if (s.ok()) {
              s = CommitTxn(txn);
            }
          }
#endif
        }
      }

      shared->Put(cf_idx, k, value_base, false /* pending */);
      if (!s.ok()) {
        break;
      }
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
#ifndef ROCKSDB_LITE
    txn_db_ = nullptr;
#endif

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

#ifndef ROCKSDB_LITE
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
        shared->Put(static_cast<int>(i) /* cf_idx */, key_val,
                    0 /* value_base */, true /* pending */);
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

Status StressTest::NewTxn(WriteOptions& write_opts, Transaction** txn) {
  if (!FLAGS_use_txn) {
    return Status::InvalidArgument("NewTxn when FLAGS_use_txn is not set");
  }
  write_opts.disableWAL = FLAGS_disable_wal;
  static std::atomic<uint64_t> txn_id = {0};
  TransactionOptions txn_options;
  txn_options.use_only_the_last_commit_time_batch_for_recovery =
      FLAGS_use_only_the_last_commit_time_batch_for_recovery;
  txn_options.lock_timeout = 600000;  // 10 min
  txn_options.deadlock_detect = true;
  *txn = txn_db_->BeginTransaction(write_opts, txn_options);
  auto istr = std::to_string(txn_id.fetch_add(1));
  Status s = (*txn)->SetName("xid" + istr);
  return s;
}

Status StressTest::CommitTxn(Transaction* txn, ThreadState* thread) {
  if (!FLAGS_use_txn) {
    return Status::InvalidArgument("CommitTxn when FLAGS_use_txn is not set");
  }
  assert(txn_db_);
  Status s = txn->Prepare();
  std::shared_ptr<const Snapshot> timestamped_snapshot;
  if (s.ok()) {
    if (thread && FLAGS_create_timestamped_snapshot_one_in &&
        thread->rand.OneIn(FLAGS_create_timestamped_snapshot_one_in)) {
      uint64_t ts = db_stress_env->NowNanos();
      s = txn->CommitAndTryCreateSnapshot(/*notifier=*/nullptr, ts,
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
      s = txn->Commit();
    }
  }
  if (thread && FLAGS_create_timestamped_snapshot_one_in > 0 &&
      thread->rand.OneInOpt(50000)) {
    uint64_t now = db_stress_env->NowNanos();
    constexpr uint64_t time_diff = static_cast<uint64_t>(1000) * 1000 * 1000;
    txn_db_->ReleaseTimestampedSnapshotsOlderThan(now - time_diff);
  }
  delete txn;
  return s;
}

Status StressTest::RollbackTxn(Transaction* txn) {
  if (!FLAGS_use_txn) {
    return Status::InvalidArgument(
        "RollbackTxn when FLAGS_use_txn is not"
        " set");
  }
  Status s = txn->Rollback();
  delete txn;
  return s;
}
#endif

void StressTest::OperateDb(ThreadState* thread) {
  ReadOptions read_opts(FLAGS_verify_checksum, true);
  read_opts.rate_limiter_priority =
      FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
  read_opts.async_io = FLAGS_async_io;
  read_opts.adaptive_readahead = FLAGS_adaptive_readahead;
  read_opts.readahead_size = FLAGS_readahead_size;
  WriteOptions write_opts;
  if (FLAGS_rate_limit_auto_wal_flush) {
    write_opts.rate_limiter_priority = Env::IO_USER;
  }
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

#ifndef NDEBUG
  if (FLAGS_read_fault_one_in) {
    fault_fs_guard->SetThreadLocalReadErrorContext(thread->shared->GetSeed(),
                                                   FLAGS_read_fault_one_in);
  }
#endif  // NDEBUG
  if (FLAGS_write_fault_one_in) {
    IOStatus error_msg;
    if (FLAGS_injest_error_severity <= 1 || FLAGS_injest_error_severity > 2) {
      error_msg = IOStatus::IOError("Retryable IO Error");
      error_msg.SetRetryable(true);
    } else if (FLAGS_injest_error_severity == 2) {
      // Ingest the fatal error
      error_msg = IOStatus::IOError("Fatal IO Error");
      error_msg.SetDataLoss(true);
    }
    std::vector<FileType> types = {FileType::kTableFile,
                                   FileType::kDescriptorFile,
                                   FileType::kCurrentFile};
    fault_fs_guard->SetRandomWriteError(
        thread->shared->GetSeed(), FLAGS_write_fault_one_in, error_msg,
        /*inject_for_all_file_types=*/false, types);
  }
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

    for (uint64_t i = 0; i < ops_per_open; i++) {
      if (thread->shared->HasVerificationFailedYet()) {
        break;
      }

      // Change Options
      if (thread->rand.OneInOpt(FLAGS_set_options_one_in)) {
        SetOptions(thread);
      }

      if (thread->rand.OneInOpt(FLAGS_set_in_place_one_in)) {
        options_.inplace_update_support ^= options_.inplace_update_support;
      }

      if (thread->tid == 0 && FLAGS_verify_db_one_in > 0 &&
          thread->rand.OneIn(FLAGS_verify_db_one_in)) {
        ContinuouslyVerifyDb(thread);
        if (thread->shared->ShouldStopTest()) {
          break;
        }
      }

      MaybeClearOneColumnFamily(thread);

      if (thread->rand.OneInOpt(FLAGS_manual_wal_flush_one_in)) {
        bool sync = thread->rand.OneIn(2) ? true : false;
        Status s = db_->FlushWAL(sync);
        if (!s.ok() && !(sync && s.IsNotSupported())) {
          fprintf(stderr, "FlushWAL(sync=%s) failed: %s\n",
                  (sync ? "true" : "false"), s.ToString().c_str());
        }
      }

      if (thread->rand.OneInOpt(FLAGS_sync_wal_one_in)) {
        Status s = db_->SyncWAL();
        if (!s.ok() && !s.IsNotSupported()) {
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

      std::vector<int> rand_column_families =
          GenerateColumnFamilies(FLAGS_column_families, rand_column_family);

      if (thread->rand.OneInOpt(FLAGS_flush_one_in)) {
        Status status = TestFlush(rand_column_families);
        if (!status.ok()) {
          fprintf(stdout, "Unable to perform Flush(): %s\n",
                  status.ToString().c_str());
        }
      }

#ifndef ROCKSDB_LITE
      // Verify GetLiveFiles with a 1 in N chance.
      if (thread->rand.OneInOpt(FLAGS_get_live_files_one_in) &&
          !FLAGS_write_fault_one_in) {
        Status status = VerifyGetLiveFiles();
        if (!status.ok()) {
          VerificationAbort(shared, "VerifyGetLiveFiles status not OK", status);
        }
      }

      // Verify GetSortedWalFiles with a 1 in N chance.
      if (thread->rand.OneInOpt(FLAGS_get_sorted_wal_files_one_in)) {
        Status status = VerifyGetSortedWalFiles();
        if (!status.ok()) {
          VerificationAbort(shared, "VerifyGetSortedWalFiles status not OK",
                            status);
        }
      }

      // Verify GetCurrentWalFile with a 1 in N chance.
      if (thread->rand.OneInOpt(FLAGS_get_current_wal_file_one_in)) {
        Status status = VerifyGetCurrentWalFile();
        if (!status.ok()) {
          VerificationAbort(shared, "VerifyGetCurrentWalFile status not OK",
                            status);
        }
      }
#endif  // !ROCKSDB_LITE

      if (thread->rand.OneInOpt(FLAGS_pause_background_one_in)) {
        Status status = TestPauseBackground(thread);
        if (!status.ok()) {
          VerificationAbort(
              shared, "Pause/ContinueBackgroundWork status not OK", status);
        }
      }

#ifndef ROCKSDB_LITE
      if (thread->rand.OneInOpt(FLAGS_verify_checksum_one_in)) {
        Status status = db_->VerifyChecksum();
        if (!status.ok()) {
          VerificationAbort(shared, "VerifyChecksum status not OK", status);
        }
      }

      if (thread->rand.OneInOpt(FLAGS_get_property_one_in)) {
        TestGetProperty(thread);
      }
#endif

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
          Status s = TestBackupRestore(thread, rand_column_families, rand_keys);
          if (!s.ok()) {
            VerificationAbort(shared, "Backup/restore gave inconsistent state",
                              s);
          }
        }
      }

      if (thread->rand.OneInOpt(FLAGS_checkpoint_one_in)) {
        Status s = TestCheckpoint(thread, rand_column_families, rand_keys);
        if (!s.ok()) {
          VerificationAbort(shared, "Checkpoint gave inconsistent state", s);
        }
      }

#ifndef ROCKSDB_LITE
      if (thread->rand.OneInOpt(FLAGS_approximate_size_one_in)) {
        Status s =
            TestApproximateSize(thread, i, rand_column_families, rand_keys);
        if (!s.ok()) {
          VerificationAbort(shared, "ApproximateSize Failed", s);
        }
      }
#endif  // !ROCKSDB_LITE
      if (thread->rand.OneInOpt(FLAGS_acquire_snapshot_one_in)) {
        TestAcquireSnapshot(thread, rand_column_family, keystr, i);
      }

      /*always*/ {
        Status s = MaybeReleaseSnapshots(thread, i);
        if (!s.ok()) {
          VerificationAbort(shared, "Snapshot gave inconsistent state", s);
        }
      }

      // Assign timestamps if necessary.
      std::string read_ts_str;
      Slice read_ts;
      if (FLAGS_user_timestamp_size > 0) {
        read_ts_str = GetNowNanos();
        read_ts = read_ts_str;
        read_opts.timestamp = &read_ts;
      }

      int prob_op = thread->rand.Uniform(100);
      // Reset this in case we pick something other than a read op. We don't
      // want to use a stale value when deciding at the beginning of the loop
      // whether to vote to reopen
      if (prob_op >= 0 && prob_op < static_cast<int>(FLAGS_readpercent)) {
        assert(0 <= prob_op);
        // OPERATION read
        if (FLAGS_use_multiget) {
          // Leave room for one more iteration of the loop with a single key
          // batch. This is to ensure that each thread does exactly the same
          // number of ops
          int multiget_batch_size = static_cast<int>(
              std::min(static_cast<uint64_t>(thread->rand.Uniform(64)),
                       FLAGS_ops_per_thread - i - 1));
          // If its the last iteration, ensure that multiget_batch_size is 1
          multiget_batch_size = std::max(multiget_batch_size, 1);
          rand_keys = GenerateNKeys(thread, multiget_batch_size, i);
          TestMultiGet(thread, read_opts, rand_column_families, rand_keys);
          i += multiget_batch_size - 1;
        } else {
          TestGet(thread, read_opts, rand_column_families, rand_keys);
        }
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
        TestPut(thread, write_opts, read_opts, rand_column_families, rand_keys,
                value);
      } else if (prob_op < del_bound) {
        assert(write_bound <= prob_op);
        // OPERATION delete
        TestDelete(thread, write_opts, rand_column_families, rand_keys);
      } else if (prob_op < delrange_bound) {
        assert(del_bound <= prob_op);
        // OPERATION delete range
        TestDeleteRange(thread, write_opts, rand_column_families, rand_keys);
      } else if (prob_op < iterate_bound) {
        assert(delrange_bound <= prob_op);
        // OPERATION iterate
        if (!FLAGS_skip_verifydb &&
            thread->rand.OneInOpt(
                FLAGS_verify_iterator_with_expected_state_one_in)) {
          TestIterateAgainstExpected(thread, read_opts, rand_column_families,
                                     rand_keys);
        } else {
          int num_seeks = static_cast<int>(std::min(
              std::max(static_cast<uint64_t>(thread->rand.Uniform(4)),
                       static_cast<uint64_t>(1)),
              std::max(static_cast<uint64_t>(FLAGS_ops_per_thread - i - 1),
                       static_cast<uint64_t>(1))));
          rand_keys = GenerateNKeys(thread, num_seeks, i);
          i += num_seeks - 1;
          TestIterate(thread, read_opts, rand_column_families, rand_keys);
        }
      } else {
        assert(iterate_bound <= prob_op);
        TestCustomOperations(thread, rand_column_families);
      }
      thread->stats.FinishedSingleOp();
    }
  }
  while (!thread->snapshot_queue.empty()) {
    db_->ReleaseSnapshot(thread->snapshot_queue.front().second.snapshot);
    delete thread->snapshot_queue.front().second.key_vec;
    thread->snapshot_queue.pop();
  }

  thread->stats.Stop();
}

#ifndef ROCKSDB_LITE
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
#endif  // !ROCKSDB_LITE

// Given a key K, this creates an iterator which scans to K and then
// does a random sequence of Next/Prev operations.
Status StressTest::TestIterate(ThreadState* thread,
                               const ReadOptions& read_opts,
                               const std::vector<int>& rand_column_families,
                               const std::vector<int64_t>& rand_keys) {
  assert(!rand_column_families.empty());
  assert(!rand_keys.empty());

  ManagedSnapshot snapshot_guard(db_);

  ReadOptions ro = read_opts;
  ro.snapshot = snapshot_guard.snapshot();

  std::string read_ts_str;
  Slice read_ts_slice;
  MaybeUseOlderTimestampForRangeScan(thread, read_ts_str, read_ts_slice, ro);

  bool expect_total_order = false;
  if (thread->rand.OneIn(16)) {
    // When prefix extractor is used, it's useful to cover total order seek.
    ro.total_order_seek = true;
    expect_total_order = true;
  } else if (thread->rand.OneIn(4)) {
    ro.total_order_seek = false;
    ro.auto_prefix_mode = true;
    expect_total_order = true;
  } else if (options_.prefix_extractor.get() == nullptr) {
    expect_total_order = true;
  }

  std::string upper_bound_str;
  Slice upper_bound;
  if (thread->rand.OneIn(16)) {
    // With a 1/16 chance, set an iterator upper bound.
    // Note: upper_bound can be smaller than the seek key.
    const int64_t rand_upper_key = GenerateOneKey(thread, FLAGS_ops_per_thread);
    upper_bound_str = Key(rand_upper_key);
    upper_bound = Slice(upper_bound_str);
    ro.iterate_upper_bound = &upper_bound;
  }
  std::string lower_bound_str;
  Slice lower_bound;
  if (thread->rand.OneIn(16)) {
    // With a 1/16 chance, enable iterator lower bound.
    // Note: lower_bound can be greater than the seek key.
    const int64_t rand_lower_key = GenerateOneKey(thread, FLAGS_ops_per_thread);
    lower_bound_str = Key(rand_lower_key);
    lower_bound = Slice(lower_bound_str);
    ro.iterate_lower_bound = &lower_bound;
  }

  ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
  assert(cfh);

  std::unique_ptr<Iterator> iter(db_->NewIterator(ro, cfh));

  std::vector<std::string> key_strs;
  if (thread->rand.OneIn(16)) {
    // Generate keys close to lower or upper bound of SST files.
    key_strs = GetWhiteBoxKeys(thread, db_, cfh, rand_keys.size());
  }
  if (key_strs.empty()) {
    // Use the random keys passed in.
    for (int64_t rkey : rand_keys) {
      key_strs.push_back(Key(rkey));
    }
  }

  std::string op_logs;
  constexpr size_t kOpLogsLimit = 10000;

  for (const std::string& key_str : key_strs) {
    if (op_logs.size() > kOpLogsLimit) {
      // Shouldn't take too much memory for the history log. Clear it.
      op_logs = "(cleared...)\n";
    }

    if (ro.iterate_upper_bound != nullptr && thread->rand.OneIn(2)) {
      // With a 1/2 chance, change the upper bound.
      // It is possible that it is changed before first use, but there is no
      // problem with that.
      const int64_t rand_upper_key =
          GenerateOneKey(thread, FLAGS_ops_per_thread);
      upper_bound_str = Key(rand_upper_key);
      upper_bound = Slice(upper_bound_str);
    }
    if (ro.iterate_lower_bound != nullptr && thread->rand.OneIn(4)) {
      // With a 1/4 chance, change the lower bound.
      // It is possible that it is changed before first use, but there is no
      // problem with that.
      const int64_t rand_lower_key =
          GenerateOneKey(thread, FLAGS_ops_per_thread);
      lower_bound_str = Key(rand_lower_key);
      lower_bound = Slice(lower_bound_str);
    }

    // Record some options to op_logs
    op_logs += "total_order_seek: ";
    op_logs += (ro.total_order_seek ? "1 " : "0 ");
    op_logs += "auto_prefix_mode: ";
    op_logs += (ro.auto_prefix_mode ? "1 " : "0 ");
    if (ro.iterate_upper_bound != nullptr) {
      op_logs += "ub: " + upper_bound.ToString(true) + " ";
    }
    if (ro.iterate_lower_bound != nullptr) {
      op_logs += "lb: " + lower_bound.ToString(true) + " ";
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

    VerifyIterator(thread, cmp_cfh, ro, iter.get(), cmp_iter.get(), last_op,
                   key, op_logs, &diverged);

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

      VerifyIterator(thread, cmp_cfh, ro, iter.get(), cmp_iter.get(), last_op,
                     key, op_logs, &diverged);
    }

    thread->stats.AddIterations(1);

    op_logs += "; ";
  }

  return Status::OK();
}

#ifndef ROCKSDB_LITE
// Test the return status of GetLiveFiles.
Status StressTest::VerifyGetLiveFiles() const {
  std::vector<std::string> live_file;
  uint64_t manifest_size = 0;
  return db_->GetLiveFiles(live_file, &manifest_size);
}

// Test the return status of GetSortedWalFiles.
Status StressTest::VerifyGetSortedWalFiles() const {
  VectorLogPtr log_ptr;
  return db_->GetSortedWalFiles(log_ptr);
}

// Test the return status of GetCurrentWalFile.
Status StressTest::VerifyGetCurrentWalFile() const {
  std::unique_ptr<LogFile> cur_wal_file;
  return db_->GetCurrentWalFile(&cur_wal_file);
}
#endif  // !ROCKSDB_LITE

// Compare the two iterator, iter and cmp_iter are in the same position,
// unless iter might be made invalidate or undefined because of
// upper or lower bounds, or prefix extractor.
// Will flag failure if the verification fails.
// diverged = true if the two iterator is already diverged.
// True if verification passed, false if not.
void StressTest::VerifyIterator(ThreadState* thread,
                                ColumnFamilyHandle* cmp_cfh,
                                const ReadOptions& ro, Iterator* iter,
                                Iterator* cmp_iter, LastIterateOp op,
                                const Slice& seek_key,
                                const std::string& op_logs, bool* diverged) {
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
    // SeekToFirst() with lower bound is not well defined.
    *diverged = true;
    return;
  } else if (op == kLastOpSeekToLast && ro.iterate_upper_bound != nullptr) {
    // SeekToLast() with higher bound is not well defined.
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
    // Lower bound behavior is not well defined if it is larger than
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
    // Uppder bound behavior is not well defined if it is smaller than
    // seek key or lower bound. Disable the check for now.
    *diverged = true;
    return;
  }

  const SliceTransform* pe = (ro.total_order_seek || ro.auto_prefix_mode)
                                 ? nullptr
                                 : options_.prefix_extractor.get();
  const Comparator* cmp = options_.comparator;

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
            "Control interator is invalid but iterator has key %s "
            "%s\n",
            iter->key().ToString(true).c_str(), op_logs.c_str());

    *diverged = true;
  } else if (cmp_iter->Valid()) {
    // Iterator is not valid. It can be legimate if it has already been
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
                "Iterator stays in prefix but contol doesn't"
                " iterator key %s control iterator key %s %s\n",
                iter->key().ToString(true).c_str(),
                cmp_iter->key().ToString(true).c_str(), op_logs.c_str());
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
                " has value %s %s\n",
                total_order_key.ToString(true).c_str(), op_logs.c_str());
        if (iter->Valid()) {
          fprintf(stderr, "iterator has value %s\n",
                  iter->key().ToString(true).c_str());
        } else {
          fprintf(stderr, "iterator is not valid\n");
        }
        *diverged = true;
      }
    }
  }

  if (!*diverged && iter->Valid()) {
    const WideColumns expected_columns =
        GenerateExpectedWideColumns(GetValueBase(iter->value()), iter->value());
    if (iter->columns() != expected_columns) {
      fprintf(stderr, "Value and columns inconsistent for iterator: %s\n",
              DebugString(iter->value(), iter->columns(), expected_columns)
                  .c_str());

      *diverged = true;
    }
  }

  if (*diverged) {
    fprintf(stderr, "Control CF %s\n", cmp_cfh->GetName().c_str());
    thread->stats.AddErrors(1);
    // Fail fast to preserve the DB state.
    thread->shared->SetVerificationFailure();
  }
}

#ifdef ROCKSDB_LITE
Status StressTest::TestBackupRestore(
    ThreadState* /* thread */,
    const std::vector<int>& /* rand_column_families */,
    const std::vector<int64_t>& /* rand_keys */) {
  assert(false);
  fprintf(stderr,
          "RocksDB lite does not support "
          "TestBackupRestore\n");
  std::terminate();
}

Status StressTest::TestCheckpoint(
    ThreadState* /* thread */,
    const std::vector<int>& /* rand_column_families */,
    const std::vector<int64_t>& /* rand_keys */) {
  assert(false);
  fprintf(stderr,
          "RocksDB lite does not support "
          "TestCheckpoint\n");
  std::terminate();
}

void StressTest::TestCompactFiles(ThreadState* /* thread */,
                                  ColumnFamilyHandle* /* column_family */) {
  assert(false);
  fprintf(stderr,
          "RocksDB lite does not support "
          "CompactFiles\n");
  std::terminate();
}
#else   // ROCKSDB_LITE
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
  BackupEngine* backup_engine = nullptr;
  std::string from = "a backup/restore operation";
  Status s = BackupEngine::Open(db_stress_env, backup_opts, &backup_engine);
  if (!s.ok()) {
    from = "BackupEngine::Open";
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
  if (s.ok() && !inplace_not_restore) {
    if (count > 1) {
      s = backup_engine->RestoreDBFromBackup(
          RestoreOptions(), backup_info[thread->rand.Uniform(count)].backup_id,
          restore_dir /* db_dir */, restore_dir /* wal_dir */);
      if (!s.ok()) {
        from = "BackupEngine::RestoreDBFromBackup";
      }
    } else {
      from_latest = true;
      s = backup_engine->RestoreDBFromLatestBackup(RestoreOptions(),
                                                   restore_dir /* db_dir */,
                                                   restore_dir /* wal_dir */);
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
  if (s.ok() && !FLAGS_use_txn && !FLAGS_use_blob_db) {
    Options restore_options(options_);
    restore_options.best_efforts_recovery = false;
    restore_options.listeners.clear();
    // Avoid dangling/shared file descriptors, for reliable destroy
    restore_options.sst_file_manager = nullptr;
    std::vector<ColumnFamilyDescriptor> cf_descriptors;
    // TODO(ajkr): `column_family_names_` is not safe to access here when
    // `clear_column_family_one_in != 0`. But we can't easily switch to
    // `ListColumnFamilies` to get names because it won't necessarily give
    // the same order as `column_family_names_`.
    assert(FLAGS_clear_column_family_one_in == 0);
    for (auto name : column_family_names_) {
      cf_descriptors.emplace_back(name, ColumnFamilyOptions(restore_options));
    }
    if (inplace_not_restore) {
      BackupInfo& info = backup_info[thread->rand.Uniform(count)];
      restore_options.env = info.env_for_open.get();
      s = DB::OpenForReadOnly(DBOptions(restore_options), info.name_for_open,
                              cf_descriptors, &restored_cf_handles,
                              &restored_db);
      if (!s.ok()) {
        from = "DB::OpenForReadOnly in backup/restore";
      }
    } else {
      s = DB::Open(DBOptions(restore_options), restore_dir, cf_descriptors,
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
  if (s.ok()) {
    // Preserve directories on failure, or allowed persistent backup
    if (!allow_persistent) {
      s = DestroyDir(db_stress_env, backup_dir);
      if (!s.ok()) {
        from = "Destroy backup dir";
      }
    }
  }
  if (s.ok()) {
    s = DestroyDir(db_stress_env, restore_dir);
    if (!s.ok()) {
      from = "Destroy restore dir";
    }
  }
  if (!s.ok()) {
    fprintf(stderr, "Failure in %s with: %s\n", from.c_str(),
            s.ToString().c_str());
  }
  return s;
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

  DestroyDB(checkpoint_dir, tmp_opts);

  if (db_stress_env->FileExists(checkpoint_dir).ok()) {
    // If the directory might still exist, try to delete the files one by one.
    // Likely a trash file is still there.
    Status my_s = DestroyDir(db_stress_env, checkpoint_dir);
    if (!my_s.ok()) {
      fprintf(stderr, "Fail to destory directory before checkpoint: %s",
              my_s.ToString().c_str());
    }
  }

  Checkpoint* checkpoint = nullptr;
  Status s = Checkpoint::Create(db_, &checkpoint);
  if (s.ok()) {
    s = checkpoint->CreateCheckpoint(checkpoint_dir);
    if (!s.ok()) {
      fprintf(stderr, "Fail to create checkpoint to %s\n",
              checkpoint_dir.c_str());
      std::vector<std::string> files;
      Status my_s = db_stress_env->GetChildren(checkpoint_dir, &files);
      if (my_s.ok()) {
        for (const auto& f : files) {
          fprintf(stderr, " %s\n", f.c_str());
        }
      } else {
        fprintf(stderr, "Fail to get files under the directory to %s\n",
                my_s.ToString().c_str());
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

  if (!s.ok()) {
    fprintf(stderr, "A checkpoint operation failed with: %s\n",
            s.ToString().c_str());
  } else {
    DestroyDB(checkpoint_dir, tmp_opts);
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
      auto s = db_->CompactFiles(CompactionOptions(), column_family,
                                 input_files, static_cast<int>(output_level));
      if (!s.ok()) {
        fprintf(stdout, "Unable to perform CompactFiles(): %s\n",
                s.ToString().c_str());
        thread->stats.AddNumCompactFilesFailed(1);
      } else {
        thread->stats.AddNumCompactFilesSucceed(1);
      }
      break;
    }
  }
}
#endif  // ROCKSDB_LITE

Status StressTest::TestFlush(const std::vector<int>& rand_column_families) {
  FlushOptions flush_opts;
  if (FLAGS_atomic_flush) {
    return db_->Flush(flush_opts, column_families_);
  }
  std::vector<ColumnFamilyHandle*> cfhs;
  std::for_each(rand_column_families.begin(), rand_column_families.end(),
                [this, &cfhs](int k) { cfhs.push_back(column_families_[k]); });
  return db_->Flush(flush_opts, cfhs);
}

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

void StressTest::TestAcquireSnapshot(ThreadState* thread,
                                     int rand_column_family,
                                     const std::string& keystr, uint64_t i) {
  Slice key = keystr;
  ColumnFamilyHandle* column_family = column_families_[rand_column_family];
  // This `ReadOptions` is for validation purposes. Ignore
  // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
  ReadOptions ropt;
#ifndef ROCKSDB_LITE
  auto db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
  const bool ww_snapshot = thread->rand.OneIn(10);
  const Snapshot* snapshot =
      ww_snapshot ? db_impl->GetSnapshotForWriteConflictBoundary()
                  : db_->GetSnapshot();
#else
  const Snapshot* snapshot = db_->GetSnapshot();
#endif  // !ROCKSDB_LITE
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
  auto status_at = db_->Get(ropt, column_family, key, &value_at);
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
  cro.change_level = static_cast<bool>(thread->rand.Next() % 2);
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
    // Do some validation by declaring a snapshot and compare the data before
    // and after the compaction
    pre_snapshot = db_->GetSnapshot();
    pre_hash =
        GetRangeHash(thread, pre_snapshot, column_family, start_key, end_key);
  }

  Status status = db_->CompactRange(cro, column_family, &start_key, &end_key);

  if (!status.ok()) {
    fprintf(stdout, "Unable to perform CompactRange(): %s\n",
            status.ToString().c_str());
  }

  if (pre_snapshot != nullptr) {
    uint32_t post_hash =
        GetRangeHash(thread, pre_snapshot, column_family, start_key, end_key);
    if (pre_hash != post_hash) {
      fprintf(stderr,
              "Data hash different before and after compact range "
              "start_key %s end_key %s\n",
              start_key.ToString(true).c_str(), end_key.ToString(true).c_str());
      thread->stats.AddErrors(1);
      // Fail fast to preserve the DB state.
      thread->shared->SetVerificationFailure();
    }
    db_->ReleaseSnapshot(pre_snapshot);
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
#ifndef ROCKSDB_LITE
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
#endif  // !ROCKSDB_LITE
  }

#ifndef ROCKSDB_LITE
  fprintf(stdout, "Stacked BlobDB            : %s\n",
          FLAGS_use_blob_db ? "true" : "false");
#endif  // !ROCKSDB_LITE
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
  fprintf(stdout, "Do update in place        : %d\n", FLAGS_in_place_update);
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
  fprintf(stdout, "Read fault one in         : %d\n", FLAGS_read_fault_one_in);
  fprintf(stdout, "Write fault one in        : %d\n", FLAGS_write_fault_one_in);
  fprintf(stdout, "Open metadata write fault one in:\n");
  fprintf(stdout, "                            %d\n",
          FLAGS_open_metadata_write_fault_one_in);
  fprintf(stdout, "Sync fault injection      : %d\n",
          FLAGS_sync_fault_injection);
  fprintf(stdout, "Best efforts recovery     : %d\n",
          static_cast<int>(FLAGS_best_efforts_recovery));
  fprintf(stdout, "Fail if OPTIONS file error: %d\n",
          static_cast<int>(FLAGS_fail_if_options_file_error));
  fprintf(stdout, "User timestamp size bytes : %d\n",
          static_cast<int>(FLAGS_user_timestamp_size));
  fprintf(stdout, "WAL compression           : %s\n",
          FLAGS_wal_compression.c_str());
  fprintf(stdout, "Try verify sst unique id  : %d\n",
          static_cast<int>(FLAGS_verify_sst_unique_id_in_manifest));

  fprintf(stdout, "------------------------------------------------\n");
}

void StressTest::Open(SharedState* shared) {
  assert(db_ == nullptr);
#ifndef ROCKSDB_LITE
  assert(txn_db_ == nullptr);
#else
  (void)shared;
#endif
  if (!InitializeOptionsFromFile(options_)) {
    InitializeOptionsFromFlags(cache_, compressed_cache_, filter_policy_,
                               options_);
  }
  InitializeOptionsGeneral(cache_, compressed_cache_, filter_policy_, options_);

  if (FLAGS_prefix_size == 0 && FLAGS_rep_factory == kHashSkipList) {
    fprintf(stderr,
            "prefeix_size cannot be zero if memtablerep == prefix_hash\n");
    exit(1);
  }
  if (FLAGS_prefix_size != 0 && FLAGS_rep_factory != kHashSkipList) {
    fprintf(stderr,
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
        for (auto cf : sorted_cfn) {
          fprintf(stderr, "%s ", cf.c_str());
        }
        fprintf(stderr, "}\n");
        fprintf(stderr, "Existing: {");
        for (auto cf : existing_column_families) {
          fprintf(stderr, "%s ", cf.c_str());
        }
        fprintf(stderr, "}\n");
      }
      assert(sorted_cfn == existing_column_families);
    }
    std::vector<ColumnFamilyDescriptor> cf_descriptors;
    for (auto name : column_family_names_) {
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
#ifndef ROCKSDB_LITE
    options_.listeners.emplace_back(new DbStressListener(
        FLAGS_db, options_.db_paths, cf_descriptors, db_stress_listener_env));
#endif  // !ROCKSDB_LITE
    RegisterAdditionalListeners();

    if (!FLAGS_use_txn) {
      // Determine whether we need to ingest file metadata write failures
      // during DB reopen. If it does, enable it.
      // Only ingest metadata error if it is reopening, as initial open
      // failure doesn't need to be handled.
      // TODO cover transaction DB is not covered in this fault test too.
      bool ingest_meta_error = false;
      bool ingest_write_error = false;
      bool ingest_read_error = false;
      if ((FLAGS_open_metadata_write_fault_one_in ||
           FLAGS_open_write_fault_one_in || FLAGS_open_read_fault_one_in) &&
          fault_fs_guard
              ->FileExists(FLAGS_db + "/CURRENT", IOOptions(), nullptr)
              .ok()) {
        if (!FLAGS_sync) {
          // When DB Stress is not sync mode, we expect all WAL writes to
          // WAL is durable. Buffering unsynced writes will cause false
          // positive in crash tests. Before we figure out a way to
          // solve it, skip WAL from failure injection.
          fault_fs_guard->SetSkipDirectWritableTypes({kWalFile});
        }
        ingest_meta_error = FLAGS_open_metadata_write_fault_one_in;
        ingest_write_error = FLAGS_open_write_fault_one_in;
        ingest_read_error = FLAGS_open_read_fault_one_in;
        if (ingest_meta_error) {
          fault_fs_guard->EnableMetadataWriteErrorInjection();
          fault_fs_guard->SetRandomMetadataWriteError(
              FLAGS_open_metadata_write_fault_one_in);
        }
        if (ingest_write_error) {
          fault_fs_guard->SetFilesystemDirectWritable(false);
          fault_fs_guard->EnableWriteErrorInjection();
          fault_fs_guard->SetRandomWriteError(
              static_cast<uint32_t>(FLAGS_seed), FLAGS_open_write_fault_one_in,
              IOStatus::IOError("Injected Open Error"),
              /*inject_for_all_file_types=*/true, /*types=*/{});
        }
        if (ingest_read_error) {
          fault_fs_guard->SetRandomReadError(FLAGS_open_read_fault_one_in);
        }
      }
      while (true) {
#ifndef ROCKSDB_LITE
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
        } else
#endif  // !ROCKSDB_LITE
        {
          if (db_preload_finished_.load() && FLAGS_read_only) {
            s = DB::OpenForReadOnly(DBOptions(options_), FLAGS_db,
                                    cf_descriptors, &column_families_, &db_);
          } else {
            s = DB::Open(DBOptions(options_), FLAGS_db, cf_descriptors,
                         &column_families_, &db_);
          }
        }

        if (ingest_meta_error || ingest_write_error || ingest_read_error) {
          fault_fs_guard->SetFilesystemDirectWritable(true);
          fault_fs_guard->DisableMetadataWriteErrorInjection();
          fault_fs_guard->DisableWriteErrorInjection();
          fault_fs_guard->SetSkipDirectWritableTypes({});
          fault_fs_guard->SetRandomReadError(0);
          if (s.ok()) {
            // Ingested errors might happen in background compactions. We
            // wait for all compactions to finish to make sure DB is in
            // clean state before executing queries.
            s = static_cast_with_check<DBImpl>(db_->GetRootDB())
                    ->WaitForCompact(true /* wait_unscheduled */);
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
            // After failure to opening a DB due to IO error, retry should
            // successfully open the DB with correct data if no IO error shows
            // up.
            ingest_meta_error = false;
            ingest_write_error = false;
            ingest_read_error = false;

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
            continue;
          }
        }
        break;
      }
    } else {
#ifndef ROCKSDB_LITE
      TransactionDBOptions txn_db_options;
      assert(FLAGS_txn_write_policy <= TxnDBWritePolicy::WRITE_UNPREPARED);
      txn_db_options.write_policy =
          static_cast<TxnDBWritePolicy>(FLAGS_txn_write_policy);
      if (FLAGS_unordered_write) {
        assert(txn_db_options.write_policy == TxnDBWritePolicy::WRITE_PREPARED);
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
#endif
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
#ifndef ROCKSDB_LITE
      Options tmp_opts;
      // TODO(yanqin) support max_open_files != -1 for secondary instance.
      tmp_opts.max_open_files = -1;
      tmp_opts.env = db_stress_env;
      const std::string& secondary_path = FLAGS_secondaries_base;
      s = DB::OpenAsSecondary(tmp_opts, FLAGS_db, secondary_path,
                              cf_descriptors, &cmp_cfhs_, &cmp_db_);
      assert(s.ok());
      assert(cmp_cfhs_.size() == static_cast<size_t>(FLAGS_column_families));
#else
      fprintf(stderr, "Secondary is not supported in RocksDBLite\n");
      exit(1);
#endif  // !ROCKSDB_LITE
    }
  } else {
#ifndef ROCKSDB_LITE
    DBWithTTL* db_with_ttl;
    s = DBWithTTL::Open(options_, FLAGS_db, &db_with_ttl, FLAGS_ttl);
    db_ = db_with_ttl;
#else
    fprintf(stderr, "TTL is not supported in RocksDBLite\n");
    exit(1);
#endif
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
#ifndef ROCKSDB_LITE
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
#else
  (void)thread;
#endif

  for (auto cf : column_families_) {
    delete cf;
  }
  column_families_.clear();

#ifndef ROCKSDB_LITE
  if (thread->rand.OneIn(2)) {
    Status s = db_->Close();
    if (!s.ok()) {
      fprintf(stderr, "Non-ok close status: %s\n", s.ToString().c_str());
      fflush(stderr);
    }
    assert(s.ok());
  }
#endif
  delete db_;
  db_ = nullptr;
#ifndef ROCKSDB_LITE
  txn_db_ = nullptr;
#endif

  num_times_reopened_++;
  auto now = clock_->NowMicros();
  fprintf(stdout, "%s Reopening database for the %dth time\n",
          clock_->TimeToString(now / 1000000).c_str(), num_times_reopened_);
  Open(thread->shared);

  if ((FLAGS_sync_fault_injection || FLAGS_disable_wal ||
       FLAGS_manual_wal_flush_one_in > 0) &&
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
#ifndef ROCKSDB_LITE
  if (FLAGS_enable_blob_files || FLAGS_use_blob_db) {
    fprintf(stderr, "BlobDB not supported with timestamp.\n");
    exit(1);
  }
#endif  // !ROCKSDB_LITE
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
}

bool InitializeOptionsFromFile(Options& options) {
#ifndef ROCKSDB_LITE
  DBOptions db_options;
  std::vector<ColumnFamilyDescriptor> cf_descriptors;
  if (!FLAGS_options_file.empty()) {
    Status s = LoadOptionsFromFile(FLAGS_options_file, db_stress_env,
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
#else
  (void)options;
  fprintf(stderr, "--options_file not supported in lite mode\n");
  exit(1);
#endif  //! ROCKSDB_LITE
  return false;
}

void InitializeOptionsFromFlags(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<Cache>& block_cache_compressed,
    const std::shared_ptr<const FilterPolicy>& filter_policy,
    Options& options) {
  BlockBasedTableOptions block_based_options;
  block_based_options.block_cache = cache;
  block_based_options.cache_index_and_filter_blocks =
      FLAGS_cache_index_and_filter_blocks;
  block_based_options.metadata_cache_options.top_level_index_pinning =
      static_cast<PinningTier>(FLAGS_top_level_index_pinning);
  block_based_options.metadata_cache_options.partition_pinning =
      static_cast<PinningTier>(FLAGS_partition_pinning);
  block_based_options.metadata_cache_options.unpartitioned_pinning =
      static_cast<PinningTier>(FLAGS_unpartitioned_pinning);
  block_based_options.block_cache_compressed = block_cache_compressed;
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
  options.memtable_whole_key_filtering = FLAGS_memtable_whole_key_filtering;
  options.disable_auto_compactions = FLAGS_disable_auto_compactions;
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
        stderr,
        "WARNING: use_zstd_dict_trainer is false but zstd finalizeDictionary "
        "cannot be used because ZSTD 1.4.5+ is not linked with the binary."
        " zstd dictionary trainer will be used.\n");
  }
  options.max_manifest_file_size = FLAGS_max_manifest_file_size;
  options.inplace_update_support = FLAGS_in_place_update;
  options.max_subcompactions = static_cast<uint32_t>(FLAGS_subcompactions);
  options.allow_concurrent_memtable_write =
      FLAGS_allow_concurrent_memtable_write;
  options.experimental_mempurge_threshold =
      FLAGS_experimental_mempurge_threshold;
  options.periodic_compaction_seconds = FLAGS_periodic_compaction_seconds;
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
  options.atomic_flush = FLAGS_atomic_flush;
  options.manual_wal_flush = FLAGS_manual_wal_flush_one_in > 0 ? true : false;
  options.avoid_unnecessary_blocking_io = FLAGS_avoid_unnecessary_blocking_io;
  options.write_dbid_to_manifest = FLAGS_write_dbid_to_manifest;
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

  if (FLAGS_enable_tiered_storage) {
    options.bottommost_temperature = Temperature::kCold;
  }
  options.preclude_last_level_data_seconds =
      FLAGS_preclude_last_level_data_seconds;
  options.preserve_internal_time_seconds = FLAGS_preserve_internal_time_seconds;

  switch (FLAGS_rep_factory) {
    case kSkipList:
      // no need to do anything
      break;
#ifndef ROCKSDB_LITE
    case kHashSkipList:
      options.memtable_factory.reset(NewHashSkipListRepFactory(10000));
      break;
    case kVectorRep:
      options.memtable_factory.reset(new VectorRepFactory());
      break;
#else
    default:
      fprintf(stderr,
              "RocksdbLite only supports skip list mem table. Skip "
              "--rep_factory\n");
#endif  // ROCKSDB_LITE
  }

  if (FLAGS_use_full_merge_v1) {
    options.merge_operator = MergeOperators::CreateDeprecatedPutOperator();
  } else {
    options.merge_operator = MergeOperators::CreatePutOperator();
  }

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
}

void InitializeOptionsGeneral(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<Cache>& block_cache_compressed,
    const std::shared_ptr<const FilterPolicy>& filter_policy,
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
    if (!table_options->block_cache_compressed &&
        FLAGS_compressed_cache_size > 0) {
      table_options->block_cache_compressed = block_cache_compressed;
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

  options.table_properties_collector_factories.emplace_back(
      std::make_shared<DbStressTablePropertiesCollectorFactory>());
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
