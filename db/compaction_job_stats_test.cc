//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <iostream>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <unordered_set>
#include <utility>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/experimental.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/thread_status.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/convenience.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "table/block_based_table_factory.h"
#include "table/mock_table.h"
#include "table/plain_table_factory.h"
#include "util/compression.h"
#include "util/hash.h"
#include "util/hash_linklist_rep.h"
#include "util/logging.h"
#include "util/mock_env.h"
#include "util/mutexlock.h"
#include "util/rate_limiter.h"
#include "util/scoped_arena_iterator.h"
#include "util/statistics.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/thread_status_util.h"
#include "util/xfunc.h"
#include "utilities/merge_operators.h"

#if !defined(IOS_CROSS_COMPILE)
#ifndef ROCKSDB_LITE
namespace rocksdb {

static std::string RandomString(Random* rnd, int len, double ratio) {
  std::string r;
  test::CompressibleString(rnd, ratio, len, &r);
  return r;
}

std::string Key(uint64_t key, int length) {
  const int kBufSize = 1000;
  char buf[kBufSize];
  if (length > kBufSize) {
    length = kBufSize;
  }
  snprintf(buf, kBufSize, "%0*" PRIu64, length, key);
  return std::string(buf);
}

class CompactionJobStatsTest : public testing::Test {
 public:
  std::string dbname_;
  std::string alternative_wal_dir_;
  Env* env_;
  DB* db_;
  std::vector<ColumnFamilyHandle*> handles_;

  Options last_options_;

  CompactionJobStatsTest() : env_(Env::Default()) {
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->SetBackgroundThreads(1, Env::HIGH);
    dbname_ = test::TmpDir(env_) + "/compaction_job_stats_test";
    alternative_wal_dir_ = dbname_ + "/wal";
    Options options;
    options.create_if_missing = true;
    auto delete_options = options;
    delete_options.wal_dir = alternative_wal_dir_;
    EXPECT_OK(DestroyDB(dbname_, delete_options));
    // Destroy it for not alternative WAL dir is used.
    EXPECT_OK(DestroyDB(dbname_, options));
    db_ = nullptr;
    Reopen(options);
  }

  ~CompactionJobStatsTest() {
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->LoadDependency({});
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
    Close();
    Options options;
    options.db_paths.emplace_back(dbname_, 0);
    options.db_paths.emplace_back(dbname_ + "_2", 0);
    options.db_paths.emplace_back(dbname_ + "_3", 0);
    options.db_paths.emplace_back(dbname_ + "_4", 0);
    EXPECT_OK(DestroyDB(dbname_, options));
  }

  DBImpl* dbfull() {
    return reinterpret_cast<DBImpl*>(db_);
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const Options& options) {
    ColumnFamilyOptions cf_opts(options);
    size_t cfi = handles_.size();
    handles_.resize(cfi + cfs.size());
    for (auto cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
    }
  }

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const Options& options) {
    CreateColumnFamilies(cfs, options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    ReopenWithColumnFamilies(cfs_plus_default, options);
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const std::vector<Options>& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const Options& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  Status TryReopenWithColumnFamilies(
      const std::vector<std::string>& cfs,
      const std::vector<Options>& options) {
    Close();
    EXPECT_EQ(cfs.size(), options.size());
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(cfs[i], options[i]));
    }
    DBOptions db_opts = DBOptions(options[0]);
    return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const Options& options) {
    Close();
    std::vector<Options> v_opts(cfs.size(), options);
    return TryReopenWithColumnFamilies(cfs, v_opts);
  }

  void Reopen(const Options& options) {
    ASSERT_OK(TryReopen(options));
  }

  void Close() {
    for (auto h : handles_) {
      delete h;
    }
    handles_.clear();
    delete db_;
    db_ = nullptr;
  }

  void DestroyAndReopen(const Options& options) {
    // Destroy using last options
    Destroy(last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(const Options& options) {
    Close();
    ASSERT_OK(DestroyDB(dbname_, options));
  }

  Status ReadOnlyReopen(const Options& options) {
    return DB::OpenForReadOnly(options, dbname_, &db_);
  }

  Status TryReopen(const Options& options) {
    Close();
    last_options_ = options;
    return DB::Open(options, dbname_, &db_);
  }

  Status Flush(int cf = 0) {
    if (cf == 0) {
      return db_->Flush(FlushOptions());
    } else {
      return db_->Flush(FlushOptions(), handles_[cf]);
    }
  }

  Status Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, k, v);
  }

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, handles_[cf], k, v);
  }

  Status Delete(const std::string& k) {
    return db_->Delete(WriteOptions(), k);
  }

  Status Delete(int cf, const std::string& k) {
    return db_->Delete(WriteOptions(), handles_[cf], k);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  std::string Get(int cf, const std::string& k,
                  const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, handles_[cf], k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  int NumTableFilesAtLevel(int level, int cf = 0) {
    std::string property;
    if (cf == 0) {
      // default cfd
      EXPECT_TRUE(db_->GetProperty(
          "rocksdb.num-files-at-level" + NumberToString(level), &property));
    } else {
      EXPECT_TRUE(db_->GetProperty(
          handles_[cf], "rocksdb.num-files-at-level" + NumberToString(level),
          &property));
    }
    return atoi(property.c_str());
  }

  // Return spread of files per level
  std::string FilesPerLevel(int cf = 0) {
    int num_levels =
        (cf == 0) ? db_->NumberLevels() : db_->NumberLevels(handles_[1]);
    std::string result;
    size_t last_non_zero_offset = 0;
    for (int level = 0; level < num_levels; level++) {
      int f = NumTableFilesAtLevel(level, cf);
      char buf[100];
      snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
      result += buf;
      if (f > 0) {
        last_non_zero_offset = result.size();
      }
    }
    result.resize(last_non_zero_offset);
    return result;
  }

  uint64_t Size(const Slice& start, const Slice& limit, int cf = 0) {
    Range r(start, limit);
    uint64_t size;
    if (cf == 0) {
      db_->GetApproximateSizes(&r, 1, &size);
    } else {
      db_->GetApproximateSizes(handles_[1], &r, 1, &size);
    }
    return size;
  }

  void Compact(int cf, const Slice& start, const Slice& limit,
               uint32_t target_path_id) {
    ASSERT_OK(db_->CompactRange(handles_[cf], &start, &limit, false, -1,
                                target_path_id));
  }

  void Compact(int cf, const Slice& start, const Slice& limit) {
    ASSERT_OK(db_->CompactRange(handles_[cf], &start, &limit));
  }

  void Compact(const Slice& start, const Slice& limit) {
    ASSERT_OK(db_->CompactRange(&start, &limit));
  }

  void TEST_Compact(int level, int cf, const Slice& start, const Slice& limit) {
    ASSERT_OK(dbfull()->TEST_CompactRange(level, &start, &limit, handles_[cf],
                                          true /* disallow trivial move */));
  }

  // Do n memtable compactions, each of which produces an sstable
  // covering the range [small,large].
  void MakeTables(int n, const std::string& small, const std::string& large,
                  int cf = 0) {
    for (int i = 0; i < n; i++) {
      ASSERT_OK(Put(cf, small, "begin"));
      ASSERT_OK(Put(cf, large, "end"));
      ASSERT_OK(Flush(cf));
    }
  }

  void MakeTableWithKeyValues(
    Random* rnd, uint64_t smallest, uint64_t largest,
    int key_size, int value_size, uint64_t interval,
    double ratio, int cf = 0) {
    for (auto key = smallest; key < largest; key += interval) {
      ASSERT_OK(Put(cf, Slice(Key(key, key_size)),
                        Slice(RandomString(rnd, value_size, ratio))));
    }
    ASSERT_OK(Flush(cf));
  }
};

// An EventListener which helps verify the compaction results in
// test CompactionJobStatsTest.
class CompactionJobStatsChecker : public EventListener {
 public:
  CompactionJobStatsChecker() : compression_enabled_(false) {}

  size_t NumberOfUnverifiedStats() { return expected_stats_.size(); }

  // Once a compaction completed, this functionw will verify the returned
  // CompactionJobInfo with the oldest CompactionJobInfo added earlier
  // in "expected_stats_" which has not yet being used for verification.
  virtual void OnCompactionCompleted(DB *db, const CompactionJobInfo& ci) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (expected_stats_.size()) {
      Verify(ci.stats, expected_stats_.front());
      expected_stats_.pop();
    }
  }

  // A helper function which verifies whether two CompactionJobStats
  // match.  The verification of all compaction stats are done by
  // ASSERT_EQ except for the total input / output bytes, which we
  // use ASSERT_GE and ASSERT_LE with a reasonable bias ---
  // 10% in uncompressed case and 20% when compression is used.
  void Verify(const CompactionJobStats& current_stats,
              const CompactionJobStats& stats) {
    // time
    ASSERT_GT(current_stats.elapsed_micros, 0U);

    ASSERT_EQ(current_stats.num_input_records,
        stats.num_input_records);
    ASSERT_EQ(current_stats.num_input_files,
        stats.num_input_files);
    ASSERT_EQ(current_stats.num_input_files_at_output_level,
        stats.num_input_files_at_output_level);

    ASSERT_EQ(current_stats.num_output_records,
        stats.num_output_records);
    ASSERT_EQ(current_stats.num_output_files,
        stats.num_output_files);

    ASSERT_EQ(current_stats.is_manual_compaction,
        stats.is_manual_compaction);

    // file size
    double kFileSizeBias = compression_enabled_ ? 0.20 : 0.10;
    ASSERT_GE(current_stats.total_input_bytes * (1.00 + kFileSizeBias),
              stats.total_input_bytes);
    ASSERT_LE(current_stats.total_input_bytes,
              stats.total_input_bytes * (1.00 + kFileSizeBias));
    ASSERT_GE(current_stats.total_output_bytes * (1.00 + kFileSizeBias),
              stats.total_output_bytes);
    ASSERT_LE(current_stats.total_output_bytes,
              stats.total_output_bytes * (1.00 + kFileSizeBias));
    ASSERT_EQ(current_stats.total_input_raw_key_bytes,
              stats.total_input_raw_key_bytes);
    ASSERT_EQ(current_stats.total_input_raw_value_bytes,
              stats.total_input_raw_value_bytes);

    ASSERT_EQ(current_stats.num_records_replaced,
        stats.num_records_replaced);

    ASSERT_EQ(
        std::string(current_stats.smallest_output_key_prefix),
        std::string(stats.smallest_output_key_prefix));
    ASSERT_EQ(
        std::string(current_stats.largest_output_key_prefix),
        std::string(stats.largest_output_key_prefix));
  }

  // Add an expected compaction stats, which will be used to
  // verify the CompactionJobStats returned by the OnCompactionCompleted()
  // callback.
  void AddExpectedStats(const CompactionJobStats& stats) {
    std::lock_guard<std::mutex> lock(mutex_);
    expected_stats_.push(stats);
  }

  void EnableCompression(bool flag) {
    compression_enabled_ = flag;
  }

 private:
  std::mutex mutex_;
  std::queue<CompactionJobStats> expected_stats_;
  bool compression_enabled_;
};

namespace {

uint64_t EstimatedFileSize(
    uint64_t num_records, size_t key_size, size_t value_size,
    double compression_ratio = 1.0,
    size_t block_size = 4096,
    int bloom_bits_per_key = 10) {
  const size_t kPerKeyOverhead = 8;
  const size_t kFooterSize = 512;

  uint64_t data_size =
      num_records * (key_size + value_size * compression_ratio +
                     kPerKeyOverhead);

  return data_size + kFooterSize
         + num_records * bloom_bits_per_key / 8      // filter block
         + data_size * (key_size + 8) / block_size;  // index block
}

namespace {

void CopyPrefix(
    const Slice& src, size_t prefix_length, std::string* dst) {
  assert(prefix_length > 0);
  size_t length = src.size() > prefix_length ? prefix_length : src.size();
  dst->assign(src.data(), length);
}

}  // namespace

CompactionJobStats NewManualCompactionJobStats(
    const std::string& smallest_key, const std::string& largest_key,
    size_t num_input_files, size_t num_input_files_at_output_level,
    uint64_t num_input_records, size_t key_size, size_t value_size,
    size_t num_output_files, uint64_t num_output_records,
    double compression_ratio, uint64_t num_records_replaced) {
  CompactionJobStats stats;
  stats.Reset();

  stats.num_input_records = num_input_records;
  stats.num_input_files = num_input_files;
  stats.num_input_files_at_output_level = num_input_files_at_output_level;

  stats.num_output_records = num_output_records;
  stats.num_output_files = num_output_files;

  stats.total_input_bytes =
      EstimatedFileSize(
          num_input_records / num_input_files,
          key_size, value_size, compression_ratio) * num_input_files;
  stats.total_output_bytes =
      EstimatedFileSize(
          num_output_records / num_output_files,
          key_size, value_size, compression_ratio) * num_output_files;
  stats.total_input_raw_key_bytes =
      num_input_records * (key_size + 8);
  stats.total_input_raw_value_bytes =
      num_input_records * value_size;

  stats.is_manual_compaction = true;

  stats.num_records_replaced = num_records_replaced;

  CopyPrefix(smallest_key,
             CompactionJobStats::kMaxPrefixLength,
             &stats.smallest_output_key_prefix);
  CopyPrefix(largest_key,
             CompactionJobStats::kMaxPrefixLength,
             &stats.largest_output_key_prefix);

  return stats;
}

CompressionType GetAnyCompression() {
  if (Snappy_Supported()) {
    return kSnappyCompression;
  } else if (Zlib_Supported()) {
    return kZlibCompression;
  } else if (BZip2_Supported()) {
    return kBZip2Compression;
  } else if (LZ4_Supported()) {
    return kLZ4Compression;
  }
  return kNoCompression;
}

}  // namespace

TEST_F(CompactionJobStatsTest, CompactionJobStatsTest) {
  Random rnd(301);
  const int kBufSize = 100;
  char buf[kBufSize];
  uint64_t key_base = 100000000l;
  // Note: key_base must be multiple of num_keys_per_L0_file
  int num_keys_per_L0_file = 100;
  const int kTestScale = 8;
  const int kKeySize = 10;
  const int kValueSize = 1000;
  const double kCompressionRatio = 0.5;
  double compression_ratio = 1.0;
  uint64_t key_interval = key_base / num_keys_per_L0_file;

  // Whenever a compaction completes, this listener will try to
  // verify whether the returned CompactionJobStats matches
  // what we expect.  The expected CompactionJobStats is added
  // via AddExpectedStats().
  auto* stats_checker = new CompactionJobStatsChecker();
  Options options;
  options.listeners.emplace_back(stats_checker);
  options.create_if_missing = true;
  options.max_background_flushes = 0;
  options.max_mem_compaction_level = 0;
  // just enough setting to hold off auto-compaction.
  options.level0_file_num_compaction_trigger = kTestScale + 1;
  options.num_levels = 3;
  options.compression = kNoCompression;

  for (int test = 0; test < 2; ++test) {
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // 1st Phase: generate "num_L0_files" L0 files.
    int num_L0_files = 0;
    for (uint64_t start_key = key_base;
                  start_key <= key_base * kTestScale;
                  start_key += key_base) {
      MakeTableWithKeyValues(
          &rnd, start_key, start_key + key_base - 1,
          kKeySize, kValueSize, key_interval,
          compression_ratio, 1);
      snprintf(buf, kBufSize, "%d", ++num_L0_files);
      ASSERT_EQ(std::string(buf), FilesPerLevel(1));
    }
    ASSERT_EQ(ToString(num_L0_files), FilesPerLevel(1));

    // 2nd Phase: perform L0 -> L1 compaction.
    int L0_compaction_count = 6;
    int count = 1;
    std::string smallest_key;
    std::string largest_key;
    for (uint64_t start_key = key_base;
         start_key <= key_base * L0_compaction_count;
         start_key += key_base, count++) {
      smallest_key = Key(start_key, 10);
      largest_key = Key(start_key + key_base - key_interval, 10);
      stats_checker->AddExpectedStats(
          NewManualCompactionJobStats(
              smallest_key, largest_key,
              1, 0, num_keys_per_L0_file,
              kKeySize, kValueSize,
              1, num_keys_per_L0_file,
              compression_ratio, 0));
      ASSERT_EQ(stats_checker->NumberOfUnverifiedStats(), 1U);
      TEST_Compact(0, 1, smallest_key, largest_key);
      snprintf(buf, kBufSize, "%d,%d", num_L0_files - count, count);
      ASSERT_EQ(std::string(buf), FilesPerLevel(1));
    }

    // compact two files into one in the last L0 -> L1 compaction
    int num_remaining_L0 = num_L0_files - L0_compaction_count;
    smallest_key = Key(key_base * (L0_compaction_count + 1), 10);
    largest_key = Key(key_base * (kTestScale + 1) - key_interval, 10);
    stats_checker->AddExpectedStats(
        NewManualCompactionJobStats(
            smallest_key, largest_key,
            num_remaining_L0,
            0, num_keys_per_L0_file * num_remaining_L0,
            kKeySize, kValueSize,
            1, num_keys_per_L0_file * num_remaining_L0,
            compression_ratio, 0));
    ASSERT_EQ(stats_checker->NumberOfUnverifiedStats(), 1U);
    TEST_Compact(0, 1, smallest_key, largest_key);

    int num_L1_files = num_L0_files - num_remaining_L0 + 1;
    num_L0_files = 0;
    snprintf(buf, kBufSize, "%d,%d", num_L0_files, num_L1_files);
    ASSERT_EQ(std::string(buf), FilesPerLevel(1));

    // 3rd Phase: generate sparse L0 files (wider key-range, same num of keys)
    int sparseness = 2;
    for (uint64_t start_key = key_base;
                  start_key <= key_base * kTestScale;
                  start_key += key_base * sparseness) {
      MakeTableWithKeyValues(
          &rnd, start_key, start_key + key_base * sparseness - 1,
          kKeySize, kValueSize,
          key_base * sparseness / num_keys_per_L0_file,
          compression_ratio, 1);
      snprintf(buf, kBufSize, "%d,%d", ++num_L0_files, num_L1_files);
      ASSERT_EQ(std::string(buf), FilesPerLevel(1));
    }

    // 4th Phase: perform L0 -> L1 compaction again, expect higher write amp
    for (uint64_t start_key = key_base;
         num_L0_files > 1;
         start_key += key_base * sparseness) {
      smallest_key = Key(start_key, 10);
      largest_key =
          Key(start_key + key_base * sparseness - key_interval, 10);
      stats_checker->AddExpectedStats(
          NewManualCompactionJobStats(
              smallest_key, largest_key,
              3, 2, num_keys_per_L0_file * 3,
              kKeySize, kValueSize,
              1, num_keys_per_L0_file * 2,  // 1/3 of the data will be updated.
              compression_ratio,
              num_keys_per_L0_file));
      ASSERT_EQ(stats_checker->NumberOfUnverifiedStats(), 1U);
      Compact(1, smallest_key, largest_key);
      snprintf(buf, kBufSize, "%d,%d",
          --num_L0_files, --num_L1_files);
      ASSERT_EQ(std::string(buf), FilesPerLevel(1));
    }

    // 5th Phase: Do a full compaction, which involves in two sub-compactions.
    // Here we expect to have 1 L0 files and 4 L1 files
    // In the first sub-compaction, we expect L0 compaction.
    smallest_key = Key(key_base, 10);
    largest_key = Key(key_base * (kTestScale + 1) - key_interval, 10);
    stats_checker->AddExpectedStats(
        NewManualCompactionJobStats(
            Key(key_base * (kTestScale + 1 - sparseness), 10), largest_key,
            2, 1, num_keys_per_L0_file * 3,
            kKeySize, kValueSize,
            1, num_keys_per_L0_file * 2,
            compression_ratio,
            num_keys_per_L0_file));
    // In the second sub-compaction, we expect L1 compaction.
    stats_checker->AddExpectedStats(
        NewManualCompactionJobStats(
            smallest_key, largest_key,
            4, 0, num_keys_per_L0_file * 8,
            kKeySize, kValueSize,
            1, num_keys_per_L0_file * 8,
            compression_ratio, 0));
    ASSERT_EQ(stats_checker->NumberOfUnverifiedStats(), 2U);
    Compact(1, smallest_key, largest_key);
    ASSERT_EQ("0,1", FilesPerLevel(1));
    options.compression = GetAnyCompression();
    if (options.compression == kNoCompression) {
      break;
    }
    stats_checker->EnableCompression(true);
    compression_ratio = kCompressionRatio;
  }
  ASSERT_EQ(stats_checker->NumberOfUnverifiedStats(), 0U);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#endif  // !ROCKSDB_LITE
#endif  // !defined(IOS_CROSS_COMPILE)
