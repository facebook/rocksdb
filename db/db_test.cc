//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <iostream>
#include <set>
#include <unistd.h>
#include <thread>
#include <unordered_set>
#include <utility>

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/thread_status.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/convenience.h"
#include "table/block_based_table_factory.h"
#include "table/mock_table.h"
#include "table/plain_table_factory.h"
#include "util/hash.h"
#include "util/hash_linklist_rep.h"
#include "utilities/merge_operators.h"
#include "util/logging.h"
#include "util/compression.h"
#include "util/mutexlock.h"
#include "util/rate_limiter.h"
#include "util/statistics.h"
#include "util/testharness.h"
#include "util/scoped_arena_iterator.h"
#include "util/sync_point.h"
#include "util/testutil.h"
#include "util/mock_env.h"
#include "util/string_util.h"
#include "util/thread_status_util.h"
#include "util/xfunc.h"

namespace rocksdb {

static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}

namespace anon {
class AtomicCounter {
 private:
  port::Mutex mu_;
  int count_;
 public:
  AtomicCounter() : count_(0) { }
  void Increment() {
    MutexLock l(&mu_);
    count_++;
  }
  int Read() {
    MutexLock l(&mu_);
    return count_;
  }
  void Reset() {
    MutexLock l(&mu_);
    count_ = 0;
  }
};

struct OptionsOverride {
  std::shared_ptr<const FilterPolicy> filter_policy = nullptr;

  // Used as a bit mask of individual enums in which to skip an XF test point
  int skip_policy = 0;
};

}  // namespace anon

static std::string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key%06d", i);
  return std::string(buf);
}

// Special Env used to delay background operations
class SpecialEnv : public EnvWrapper {
 public:
  Random rnd_;
  port::Mutex rnd_mutex_;  // Lock to pretect rnd_

  // sstable Sync() calls are blocked while this pointer is non-nullptr.
  std::atomic<bool> delay_sstable_sync_;

  // Drop writes on the floor while this pointer is non-nullptr.
  std::atomic<bool> drop_writes_;

  // Simulate no-space errors while this pointer is non-nullptr.
  std::atomic<bool> no_space_;

  // Simulate non-writable file system while this pointer is non-nullptr
  std::atomic<bool> non_writable_;

  // Force sync of manifest files to fail while this pointer is non-nullptr
  std::atomic<bool> manifest_sync_error_;

  // Force write to manifest files to fail while this pointer is non-nullptr
  std::atomic<bool> manifest_write_error_;

  // Force write to log files to fail while this pointer is non-nullptr
  std::atomic<bool> log_write_error_;

  // Slow down every log write, in micro-seconds.
  std::atomic<int> log_write_slowdown_;

  bool count_random_reads_;
  anon::AtomicCounter random_read_counter_;

  bool count_sequential_reads_;
  anon::AtomicCounter sequential_read_counter_;

  anon::AtomicCounter sleep_counter_;

  std::atomic<int64_t> bytes_written_;

  std::atomic<int> sync_counter_;

  std::atomic<uint32_t> non_writeable_rate_;

  std::atomic<uint32_t> new_writable_count_;

  std::atomic<uint32_t> non_writable_count_;

  std::function<void()>* table_write_callback_;

  int64_t addon_time_;

  explicit SpecialEnv(Env* base) : EnvWrapper(base), rnd_(301), addon_time_(0) {
    delay_sstable_sync_.store(false, std::memory_order_release);
    drop_writes_.store(false, std::memory_order_release);
    no_space_.store(false, std::memory_order_release);
    non_writable_.store(false, std::memory_order_release);
    count_random_reads_ = false;
    count_sequential_reads_ = false;
    manifest_sync_error_.store(false, std::memory_order_release);
    manifest_write_error_.store(false, std::memory_order_release);
    log_write_error_.store(false, std::memory_order_release);
    log_write_slowdown_ = 0;
    bytes_written_ = 0;
    sync_counter_ = 0;
    non_writeable_rate_ = 0;
    new_writable_count_ = 0;
    non_writable_count_ = 0;
    table_write_callback_ = nullptr;
  }

  Status NewWritableFile(const std::string& f, unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    class SSTableFile : public WritableFile {
     private:
      SpecialEnv* env_;
      unique_ptr<WritableFile> base_;

     public:
      SSTableFile(SpecialEnv* env, unique_ptr<WritableFile>&& base)
          : env_(env),
            base_(std::move(base)) {
      }
      Status Append(const Slice& data) override {
        if (env_->table_write_callback_) {
          (*env_->table_write_callback_)();
        }
        if (env_->drop_writes_.load(std::memory_order_acquire)) {
          // Drop writes on the floor
          return Status::OK();
        } else if (env_->no_space_.load(std::memory_order_acquire)) {
          return Status::IOError("No space left on device");
        } else {
          env_->bytes_written_ += data.size();
          return base_->Append(data);
        }
      }
      Status Close() override { return base_->Close(); }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        ++env_->sync_counter_;
        while (env_->delay_sstable_sync_.load(std::memory_order_acquire)) {
          env_->SleepForMicroseconds(100000);
        }
        return base_->Sync();
      }
      void SetIOPriority(Env::IOPriority pri) override {
        base_->SetIOPriority(pri);
      }
    };
    class ManifestFile : public WritableFile {
     private:
      SpecialEnv* env_;
      unique_ptr<WritableFile> base_;
     public:
      ManifestFile(SpecialEnv* env, unique_ptr<WritableFile>&& b)
          : env_(env), base_(std::move(b)) { }
      Status Append(const Slice& data) override {
        if (env_->manifest_write_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated writer error");
        } else {
          return base_->Append(data);
        }
      }
      Status Close() override { return base_->Close(); }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        ++env_->sync_counter_;
        if (env_->manifest_sync_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated sync error");
        } else {
          return base_->Sync();
        }
      }
      uint64_t GetFileSize() override { return base_->GetFileSize(); }
    };
    class WalFile : public WritableFile {
     private:
      SpecialEnv* env_;
      unique_ptr<WritableFile> base_;
     public:
      WalFile(SpecialEnv* env, unique_ptr<WritableFile>&& b)
          : env_(env), base_(std::move(b)) {}
      Status Append(const Slice& data) override {
        if (env_->log_write_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated writer error");
        } else {
          int slowdown =
              env_->log_write_slowdown_.load(std::memory_order_acquire);
          if (slowdown > 0) {
            env_->SleepForMicroseconds(slowdown);
          }
          return base_->Append(data);
        }
      }
      Status Close() override { return base_->Close(); }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        ++env_->sync_counter_;
        return base_->Sync();
      }
    };

    if (non_writeable_rate_.load(std::memory_order_acquire) > 0) {
      uint32_t random_number;
      {
        MutexLock l(&rnd_mutex_);
        random_number = rnd_.Uniform(100);
      }
      if (random_number < non_writeable_rate_.load()) {
        return Status::IOError("simulated random write error");
      }
    }

    new_writable_count_++;

    if (non_writable_count_.load() > 0) {
      non_writable_count_--;
      return Status::IOError("simulated write error");
    }

    Status s = target()->NewWritableFile(f, r, soptions);
    if (s.ok()) {
      if (strstr(f.c_str(), ".sst") != nullptr) {
        r->reset(new SSTableFile(this, std::move(*r)));
      } else if (strstr(f.c_str(), "MANIFEST") != nullptr) {
        r->reset(new ManifestFile(this, std::move(*r)));
      } else if (strstr(f.c_str(), "log") != nullptr) {
        r->reset(new WalFile(this, std::move(*r)));
      }
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& soptions) override {
    class CountingFile : public RandomAccessFile {
     private:
      unique_ptr<RandomAccessFile> target_;
      anon::AtomicCounter* counter_;
     public:
      CountingFile(unique_ptr<RandomAccessFile>&& target,
                   anon::AtomicCounter* counter)
          : target_(std::move(target)), counter_(counter) {
      }
      virtual Status Read(uint64_t offset, size_t n, Slice* result,
                          char* scratch) const override {
        counter_->Increment();
        return target_->Read(offset, n, result, scratch);
      }
    };

    Status s = target()->NewRandomAccessFile(f, r, soptions);
    if (s.ok() && count_random_reads_) {
      r->reset(new CountingFile(std::move(*r), &random_read_counter_));
    }
    return s;
  }

  Status NewSequentialFile(const std::string& f, unique_ptr<SequentialFile>* r,
                           const EnvOptions& soptions) override {
    class CountingFile : public SequentialFile {
     private:
      unique_ptr<SequentialFile> target_;
      anon::AtomicCounter* counter_;

     public:
      CountingFile(unique_ptr<SequentialFile>&& target,
                   anon::AtomicCounter* counter)
          : target_(std::move(target)), counter_(counter) {}
      virtual Status Read(size_t n, Slice* result, char* scratch) override {
        counter_->Increment();
        return target_->Read(n, result, scratch);
      }
      virtual Status Skip(uint64_t n) override { return target_->Skip(n); }
    };

    Status s = target()->NewSequentialFile(f, r, soptions);
    if (s.ok() && count_sequential_reads_) {
      r->reset(new CountingFile(std::move(*r), &sequential_read_counter_));
    }
    return s;
  }

  virtual void SleepForMicroseconds(int micros) override {
    sleep_counter_.Increment();
    target()->SleepForMicroseconds(micros);
  }

  virtual Status GetCurrentTime(int64_t* unix_time) override {
    Status s = target()->GetCurrentTime(unix_time);
    if (s.ok()) {
      *unix_time += addon_time_;
    }
    return s;
  }

  virtual uint64_t NowNanos() override {
    return target()->NowNanos() + addon_time_ * 1000;
  }
};

class DBTest : public testing::Test {
 protected:
  // Sequence of option configurations to try
  enum OptionConfig {
    kDefault = 0,
    kBlockBasedTableWithPrefixHashIndex = 1,
    kBlockBasedTableWithWholeKeyHashIndex = 2,
    kPlainTableFirstBytePrefix = 3,
    kPlainTableCappedPrefix = 4,
    kPlainTableAllBytesPrefix = 5,
    kVectorRep = 6,
    kHashLinkList = 7,
    kHashCuckoo = 8,
    kMergePut = 9,
    kFilter = 10,
    kFullFilter = 11,
    kUncompressed = 12,
    kNumLevel_3 = 13,
    kDBLogDir = 14,
    kWalDirAndMmapReads = 15,
    kManifestFileSize = 16,
    kCompactOnFlush = 17,
    kPerfOptions = 18,
    kDeletesFilterFirst = 19,
    kHashSkipList = 20,
    kUniversalCompaction = 21,
    kUniversalCompactionMultiLevel = 22,
    kCompressedBlockCache = 23,
    kInfiniteMaxOpenFiles = 24,
    kxxHashChecksum = 25,
    kFIFOCompaction = 26,
    kOptimizeFiltersForHits = 27,
    kEnd = 28
  };
  int option_config_;

 public:
  std::string dbname_;
  std::string alternative_wal_dir_;
  MockEnv* mem_env_;
  SpecialEnv* env_;
  DB* db_;
  std::vector<ColumnFamilyHandle*> handles_;

  Options last_options_;

  // Skip some options, as they may not be applicable to a specific test.
  // To add more skip constants, use values 4, 8, 16, etc.
  enum OptionSkip {
    kNoSkip = 0,
    kSkipDeletesFilterFirst = 1,
    kSkipUniversalCompaction = 2,
    kSkipMergePut = 4,
    kSkipPlainTable = 8,
    kSkipHashIndex = 16,
    kSkipNoSeekToLast = 32,
    kSkipHashCuckoo = 64,
    kSkipFIFOCompaction = 128,
    kSkipMmapReads = 256,
  };


  DBTest() : option_config_(kDefault),
             mem_env_(!getenv("MEM_ENV") ? nullptr :
                                           new MockEnv(Env::Default())),
             env_(new SpecialEnv(mem_env_ ? mem_env_ : Env::Default())) {
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->SetBackgroundThreads(1, Env::HIGH);
    dbname_ = test::TmpDir(env_) + "/db_test";
    alternative_wal_dir_ = dbname_ + "/wal";
    auto options = CurrentOptions();
    auto delete_options = options;
    delete_options.wal_dir = alternative_wal_dir_;
    EXPECT_OK(DestroyDB(dbname_, delete_options));
    // Destroy it for not alternative WAL dir is used.
    EXPECT_OK(DestroyDB(dbname_, options));
    db_ = nullptr;
    Reopen(options);
  }

  ~DBTest() {
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
    delete env_;
  }

  // Switch to a fresh database with the next option configuration to
  // test.  Return false if there are no more configurations to test.
  bool ChangeOptions(int skip_mask = kNoSkip) {
    for(option_config_++; option_config_ < kEnd; option_config_++) {
      if ((skip_mask & kSkipDeletesFilterFirst) &&
          option_config_ == kDeletesFilterFirst) {
        continue;
      }
      if ((skip_mask & kSkipUniversalCompaction) &&
          (option_config_ == kUniversalCompaction ||
           option_config_ == kUniversalCompactionMultiLevel)) {
        continue;
      }
      if ((skip_mask & kSkipMergePut) && option_config_ == kMergePut) {
        continue;
      }
      if ((skip_mask & kSkipNoSeekToLast) &&
          (option_config_ == kHashLinkList ||
           option_config_ == kHashSkipList)) {;
        continue;
      }
      if ((skip_mask & kSkipPlainTable) &&
          (option_config_ == kPlainTableAllBytesPrefix ||
           option_config_ == kPlainTableFirstBytePrefix ||
           option_config_ == kPlainTableCappedPrefix)) {
        continue;
      }
      if ((skip_mask & kSkipHashIndex) &&
          (option_config_ == kBlockBasedTableWithPrefixHashIndex ||
           option_config_ == kBlockBasedTableWithWholeKeyHashIndex)) {
        continue;
      }
      if ((skip_mask & kSkipHashCuckoo) && (option_config_ == kHashCuckoo)) {
        continue;
      }
      if ((skip_mask & kSkipFIFOCompaction) &&
          option_config_ == kFIFOCompaction) {
        continue;
      }
      if ((skip_mask & kSkipMmapReads) &&
          option_config_ == kWalDirAndMmapReads) {
        continue;
      }
      break;
    }

    if (option_config_ >= kEnd) {
      Destroy(last_options_);
      return false;
    } else {
      auto options = CurrentOptions();
      options.create_if_missing = true;
      DestroyAndReopen(options);
      return true;
    }
  }

  // Switch between different compaction styles (we have only 2 now).
  bool ChangeCompactOptions() {
    if (option_config_ == kDefault) {
      option_config_ = kUniversalCompaction;
      Destroy(last_options_);
      auto options = CurrentOptions();
      options.create_if_missing = true;
      TryReopen(options);
      return true;
    } else if (option_config_ == kUniversalCompaction) {
      option_config_ = kUniversalCompactionMultiLevel;
      Destroy(last_options_);
      auto options = CurrentOptions();
      options.create_if_missing = true;
      TryReopen(options);
      return true;
    } else {
      return false;
    }
  }

  // Switch between different filter policy
  // Jump from kDefault to kFilter to kFullFilter
  bool ChangeFilterOptions() {
    if (option_config_ == kDefault) {
      option_config_ = kFilter;
    } else if (option_config_ == kFilter) {
      option_config_ = kFullFilter;
    } else {
      return false;
    }
    Destroy(last_options_);

    auto options = CurrentOptions();
    options.create_if_missing = true;
    TryReopen(options);
    return true;
  }

  // Return the current option configuration.
  Options CurrentOptions(
      const anon::OptionsOverride& options_override = anon::OptionsOverride()) {
    Options options;
    return CurrentOptions(options, options_override);
  }

  Options CurrentOptions(
      const Options& defaultOptions,
      const anon::OptionsOverride& options_override = anon::OptionsOverride()) {
    // this redudant copy is to minimize code change w/o having lint error.
    Options options = defaultOptions;
    XFUNC_TEST("", "dbtest_options", inplace_options1, GetXFTestOptions,
               reinterpret_cast<Options*>(&options),
               options_override.skip_policy);
    BlockBasedTableOptions table_options;
    bool set_block_based_table_factory = true;
    switch (option_config_) {
      case kHashSkipList:
        options.prefix_extractor.reset(NewFixedPrefixTransform(1));
        options.memtable_factory.reset(
            NewHashSkipListRepFactory(16));
        break;
      case kPlainTableFirstBytePrefix:
        options.table_factory.reset(new PlainTableFactory());
        options.prefix_extractor.reset(NewFixedPrefixTransform(1));
        options.allow_mmap_reads = true;
        options.max_sequential_skip_in_iterations = 999999;
        set_block_based_table_factory = false;
        break;
      case kPlainTableCappedPrefix:
        options.table_factory.reset(new PlainTableFactory());
        options.prefix_extractor.reset(NewCappedPrefixTransform(8));
        options.allow_mmap_reads = true;
        options.max_sequential_skip_in_iterations = 999999;
        set_block_based_table_factory = false;
        break;
      case kPlainTableAllBytesPrefix:
        options.table_factory.reset(new PlainTableFactory());
        options.prefix_extractor.reset(NewNoopTransform());
        options.allow_mmap_reads = true;
        options.max_sequential_skip_in_iterations = 999999;
        set_block_based_table_factory = false;
        break;
      case kMergePut:
        options.merge_operator = MergeOperators::CreatePutOperator();
        break;
      case kFilter:
        table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
        break;
      case kFullFilter:
        table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
        break;
      case kUncompressed:
        options.compression = kNoCompression;
        break;
      case kNumLevel_3:
        options.num_levels = 3;
        break;
      case kDBLogDir:
        options.db_log_dir = test::TmpDir(env_);
        break;
      case kWalDirAndMmapReads:
        options.wal_dir = alternative_wal_dir_;
        // mmap reads should be orthogonal to WalDir setting, so we piggyback to
        // this option config to test mmap reads as well
        options.allow_mmap_reads = true;
        break;
      case kManifestFileSize:
        options.max_manifest_file_size = 50; // 50 bytes
      case kCompactOnFlush:
        options.purge_redundant_kvs_while_flush =
          !options.purge_redundant_kvs_while_flush;
        break;
      case kPerfOptions:
        options.hard_rate_limit = 2.0;
        options.rate_limit_delay_max_milliseconds = 2;
        // TODO -- test more options
        break;
      case kDeletesFilterFirst:
        options.filter_deletes = true;
        break;
      case kVectorRep:
        options.memtable_factory.reset(new VectorRepFactory(100));
        break;
      case kHashLinkList:
        options.prefix_extractor.reset(NewFixedPrefixTransform(1));
        options.memtable_factory.reset(
            NewHashLinkListRepFactory(4, 0, 3, true, 4));
        break;
      case kHashCuckoo:
        options.memtable_factory.reset(
            NewHashCuckooRepFactory(options.write_buffer_size));
        break;
      case kUniversalCompaction:
        options.compaction_style = kCompactionStyleUniversal;
        options.num_levels = 1;
        break;
      case kUniversalCompactionMultiLevel:
        options.compaction_style = kCompactionStyleUniversal;
        options.num_levels = 8;
        break;
      case kCompressedBlockCache:
        options.allow_mmap_writes = true;
        table_options.block_cache_compressed = NewLRUCache(8*1024*1024);
        break;
      case kInfiniteMaxOpenFiles:
        options.max_open_files = -1;
        break;
      case kxxHashChecksum: {
        table_options.checksum = kxxHash;
        break;
      }
      case kFIFOCompaction: {
        options.compaction_style = kCompactionStyleFIFO;
        break;
      }
      case kBlockBasedTableWithPrefixHashIndex: {
        table_options.index_type = BlockBasedTableOptions::kHashSearch;
        options.prefix_extractor.reset(NewFixedPrefixTransform(1));
        break;
      }
      case kBlockBasedTableWithWholeKeyHashIndex: {
        table_options.index_type = BlockBasedTableOptions::kHashSearch;
        options.prefix_extractor.reset(NewNoopTransform());
        break;
      }
      case kOptimizeFiltersForHits: {
        options.optimize_filters_for_hits = true;
        set_block_based_table_factory = true;
        break;
      }

      default:
        break;
    }

    if (options_override.filter_policy) {
      table_options.filter_policy = options_override.filter_policy;
    }
    if (set_block_based_table_factory) {
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    }
    options.env = env_;
    options.create_if_missing = true;
    return options;
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
    //Destroy using last options
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
    if (kMergePut == option_config_ ) {
      return db_->Merge(wo, k, v);
    } else {
      return db_->Put(wo, k, v);
    }
  }

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions()) {
    if (kMergePut == option_config_) {
      return db_->Merge(wo, handles_[cf], k, v);
    } else {
      return db_->Put(wo, handles_[cf], k, v);
    }
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

  uint64_t GetNumSnapshots() {
    uint64_t int_num;
    EXPECT_TRUE(dbfull()->GetIntProperty("rocksdb.num-snapshots", &int_num));
    return int_num;
  }

  uint64_t GetTimeOldestSnapshots() {
    uint64_t int_num;
    EXPECT_TRUE(
        dbfull()->GetIntProperty("rocksdb.oldest-snapshot-time", &int_num));
    return int_num;
  }

  // Return a string that contains all key,value pairs in order,
  // formatted like "(k1->v1)(k2->v2)".
  std::string Contents(int cf = 0) {
    std::vector<std::string> forward;
    std::string result;
    Iterator* iter = (cf == 0) ? db_->NewIterator(ReadOptions())
                               : db_->NewIterator(ReadOptions(), handles_[cf]);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      std::string s = IterStatus(iter);
      result.push_back('(');
      result.append(s);
      result.push_back(')');
      forward.push_back(s);
    }

    // Check reverse iteration results are the reverse of forward results
    unsigned int matched = 0;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      EXPECT_LT(matched, forward.size());
      EXPECT_EQ(IterStatus(iter), forward[forward.size() - matched - 1]);
      matched++;
    }
    EXPECT_EQ(matched, forward.size());

    delete iter;
    return result;
  }

  std::string AllEntriesFor(const Slice& user_key, int cf = 0) {
    Arena arena;
    ScopedArenaIterator iter;
    if (cf == 0) {
      iter.set(dbfull()->TEST_NewInternalIterator(&arena));
    } else {
      iter.set(dbfull()->TEST_NewInternalIterator(&arena, handles_[cf]));
    }
    InternalKey target(user_key, kMaxSequenceNumber, kTypeValue);
    iter->Seek(target.Encode());
    std::string result;
    if (!iter->status().ok()) {
      result = iter->status().ToString();
    } else {
      result = "[ ";
      bool first = true;
      while (iter->Valid()) {
        ParsedInternalKey ikey(Slice(), 0, kTypeValue);
        if (!ParseInternalKey(iter->key(), &ikey)) {
          result += "CORRUPTED";
        } else {
          if (last_options_.comparator->Compare(ikey.user_key, user_key) != 0) {
            break;
          }
          if (!first) {
            result += ", ";
          }
          first = false;
          switch (ikey.type) {
            case kTypeValue:
              result += iter->value().ToString();
              break;
            case kTypeMerge:
              // keep it the same as kTypeValue for testing kMergePut
              result += iter->value().ToString();
              break;
            case kTypeDeletion:
              result += "DEL";
              break;
            default:
              assert(false);
              break;
          }
        }
        iter->Next();
      }
      if (!first) {
        result += " ";
      }
      result += "]";
    }
    return result;
  }

  int NumSortedRuns(int cf = 0) {
    ColumnFamilyMetaData cf_meta;
    if (cf == 0) {
      db_->GetColumnFamilyMetaData(&cf_meta);
    } else {
      db_->GetColumnFamilyMetaData(handles_[cf], &cf_meta);
    }
    int num_sr = static_cast<int>(cf_meta.levels[0].files.size());
    for (size_t i = 1U; i < cf_meta.levels.size(); i++) {
      if (cf_meta.levels[i].files.size() > 0) {
        num_sr++;
      }
    }
    return num_sr;
  }

  uint64_t TotalSize(int cf = 0) {
    ColumnFamilyMetaData cf_meta;
    if (cf == 0) {
      db_->GetColumnFamilyMetaData(&cf_meta);
    } else {
      db_->GetColumnFamilyMetaData(handles_[cf], &cf_meta);
    }
    return cf_meta.size;
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

  uint64_t SizeAtLevel(int level) {
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    uint64_t sum = 0;
    for (const auto& m : metadata) {
      if (m.level == level) {
        sum += m.size;
      }
    }
    return sum;
  }

  int TotalLiveFiles(int cf = 0) {
    ColumnFamilyMetaData cf_meta;
    if (cf == 0) {
      db_->GetColumnFamilyMetaData(&cf_meta);
    } else {
      db_->GetColumnFamilyMetaData(handles_[cf], &cf_meta);
    }
    int num_files = 0;
    for (auto& level : cf_meta.levels) {
      num_files += level.files.size();
    }
    return num_files;
  }

  int TotalTableFiles(int cf = 0, int levels = -1) {
    if (levels == -1) {
      levels = CurrentOptions().num_levels;
    }
    int result = 0;
    for (int level = 0; level < levels; level++) {
      result += NumTableFilesAtLevel(level, cf);
    }
    return result;
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

  size_t CountFiles() {
    std::vector<std::string> files;
    env_->GetChildren(dbname_, &files);

    std::vector<std::string> logfiles;
    if (dbname_ != last_options_.wal_dir) {
      env_->GetChildren(last_options_.wal_dir, &logfiles);
    }

    return files.size() + logfiles.size();
  }

  size_t CountLiveFiles() {
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    return metadata.size();
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

  // Prevent pushing of new sstables into deeper levels by adding
  // tables that cover a specified range to all levels.
  void FillLevels(const std::string& smallest, const std::string& largest,
                  int cf) {
    MakeTables(db_->NumberLevels(handles_[cf]), smallest, largest, cf);
  }

  void DumpFileCounts(const char* label) {
    fprintf(stderr, "---\n%s:\n", label);
    fprintf(stderr, "maxoverlap: %lld\n",
            static_cast<long long>(
                dbfull()->TEST_MaxNextLevelOverlappingBytes()));
    for (int level = 0; level < db_->NumberLevels(); level++) {
      int num = NumTableFilesAtLevel(level);
      if (num > 0) {
        fprintf(stderr, "  level %3d : %d files\n", level, num);
      }
    }
  }

  std::string DumpSSTableList() {
    std::string property;
    db_->GetProperty("rocksdb.sstables", &property);
    return property;
  }

  int GetSstFileCount(std::string path) {
    std::vector<std::string> files;
    env_->GetChildren(path, &files);

    int sst_count = 0;
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < files.size(); i++) {
      if (ParseFileName(files[i], &number, &type) && type == kTableFile) {
        sst_count++;
      }
    }
    return sst_count;
  }

  void GenerateNewFile(Random* rnd, int* key_idx, bool nowait = false) {
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(*key_idx), RandomString(rnd, (i == 10) ? 1 : 10000)));
      (*key_idx)++;
    }
    if (!nowait) {
      dbfull()->TEST_WaitForFlushMemTable();
      dbfull()->TEST_WaitForCompact();
    }
  }

  std::string IterStatus(Iterator* iter) {
    std::string result;
    if (iter->Valid()) {
      result = iter->key().ToString() + "->" + iter->value().ToString();
    } else {
      result = "(invalid)";
    }
    return result;
  }

  Options OptionsForLogIterTest() {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.WAL_ttl_seconds = 1000;
    return options;
  }

  std::unique_ptr<TransactionLogIterator> OpenTransactionLogIter(
      const SequenceNumber seq) {
    unique_ptr<TransactionLogIterator> iter;
    Status status = dbfull()->GetUpdatesSince(seq, &iter);
    EXPECT_OK(status);
    EXPECT_TRUE(iter->Valid());
    return std::move(iter);
  }

  std::string DummyString(size_t len, char c = 'a') {
    return std::string(len, c);
  }

  void VerifyIterLast(std::string expected_key, int cf = 0) {
    Iterator* iter;
    ReadOptions ro;
    if (cf == 0) {
      iter = db_->NewIterator(ro);
    } else {
      iter = db_->NewIterator(ro, handles_[cf]);
    }
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), expected_key);
    delete iter;
  }

  // Used to test InplaceUpdate

  // If previous value is nullptr or delta is > than previous value,
  //   sets newValue with delta
  // If previous value is not empty,
  //   updates previous value with 'b' string of previous value size - 1.
  static UpdateStatus
      updateInPlaceSmallerSize(char* prevValue, uint32_t* prevSize,
                               Slice delta, std::string* newValue) {
    if (prevValue == nullptr) {
      *newValue = std::string(delta.size(), 'c');
      return UpdateStatus::UPDATED;
    } else {
      *prevSize = *prevSize - 1;
      std::string str_b = std::string(*prevSize, 'b');
      memcpy(prevValue, str_b.c_str(), str_b.size());
      return UpdateStatus::UPDATED_INPLACE;
    }
  }

  static UpdateStatus
      updateInPlaceSmallerVarintSize(char* prevValue, uint32_t* prevSize,
                                     Slice delta, std::string* newValue) {
    if (prevValue == nullptr) {
      *newValue = std::string(delta.size(), 'c');
      return UpdateStatus::UPDATED;
    } else {
      *prevSize = 1;
      std::string str_b = std::string(*prevSize, 'b');
      memcpy(prevValue, str_b.c_str(), str_b.size());
      return UpdateStatus::UPDATED_INPLACE;
    }
  }

  static UpdateStatus
      updateInPlaceLargerSize(char* prevValue, uint32_t* prevSize,
                              Slice delta, std::string* newValue) {
    *newValue = std::string(delta.size(), 'c');
    return UpdateStatus::UPDATED;
  }

  static UpdateStatus
      updateInPlaceNoAction(char* prevValue, uint32_t* prevSize,
                            Slice delta, std::string* newValue) {
    return UpdateStatus::UPDATE_FAILED;
  }

  // Utility method to test InplaceUpdate
  void validateNumberOfEntries(int numValues, int cf = 0) {
    ScopedArenaIterator iter;
    Arena arena;
    if (cf != 0) {
      iter.set(dbfull()->TEST_NewInternalIterator(&arena, handles_[cf]));
    } else {
      iter.set(dbfull()->TEST_NewInternalIterator(&arena));
    }
    iter->SeekToFirst();
    ASSERT_EQ(iter->status().ok(), true);
    int seq = numValues;
    while (iter->Valid()) {
      ParsedInternalKey ikey;
      ikey.sequence = -1;
      ASSERT_EQ(ParseInternalKey(iter->key(), &ikey), true);

      // checks sequence number for updates
      ASSERT_EQ(ikey.sequence, (unsigned)seq--);
      iter->Next();
    }
    ASSERT_EQ(0, seq);
  }

  void CopyFile(const std::string& source, const std::string& destination,
                uint64_t size = 0) {
    const EnvOptions soptions;
    unique_ptr<SequentialFile> srcfile;
    ASSERT_OK(env_->NewSequentialFile(source, &srcfile, soptions));
    unique_ptr<WritableFile> destfile;
    ASSERT_OK(env_->NewWritableFile(destination, &destfile, soptions));

    if (size == 0) {
      // default argument means copy everything
      ASSERT_OK(env_->GetFileSize(source, &size));
    }

    char buffer[4096];
    Slice slice;
    while (size > 0) {
      uint64_t one = std::min(uint64_t(sizeof(buffer)), size);
      ASSERT_OK(srcfile->Read(one, &slice, buffer));
      ASSERT_OK(destfile->Append(slice));
      size -= slice.size();
    }
    ASSERT_OK(destfile->Close());
  }

};

static long TestGetTickerCount(const Options& options, Tickers ticker_type) {
  return options.statistics->getTickerCount(ticker_type);
}

// A helper function that ensures the table properties returned in
// `GetPropertiesOfAllTablesTest` is correct.
// This test assumes entries size is differnt for each of the tables.
namespace {
void VerifyTableProperties(DB* db, uint64_t expected_entries_size) {
  TablePropertiesCollection props;
  ASSERT_OK(db->GetPropertiesOfAllTables(&props));

  ASSERT_EQ(4U, props.size());
  std::unordered_set<uint64_t> unique_entries;

  // Indirect test
  uint64_t sum = 0;
  for (const auto& item : props) {
    unique_entries.insert(item.second->num_entries);
    sum += item.second->num_entries;
  }

  ASSERT_EQ(props.size(), unique_entries.size());
  ASSERT_EQ(expected_entries_size, sum);
}

uint64_t GetNumberOfSstFilesForColumnFamily(DB* db,
                                            std::string column_family_name) {
  std::vector<LiveFileMetaData> metadata;
  db->GetLiveFilesMetaData(&metadata);
  uint64_t result = 0;
  for (auto& fileMetadata : metadata) {
    result += (fileMetadata.column_family_name == column_family_name);
  }
  return result;
}
}  // namespace

TEST_F(DBTest, Empty) {
  do {
    Options options;
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    std::string num;
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("0", num);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("1", num);

    // Block sync calls
    env_->delay_sstable_sync_.store(true, std::memory_order_release);
    Put(1, "k1", std::string(100000, 'x'));         // Fill memtable
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("2", num);

    Put(1, "k2", std::string(100000, 'y'));         // Trigger compaction
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("1", num);

    ASSERT_EQ("v1", Get(1, "foo"));
    // Release sync calls
    env_->delay_sstable_sync_.store(false, std::memory_order_release);

    ASSERT_OK(db_->DisableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("1", num);

    ASSERT_OK(db_->DisableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("2", num);

    ASSERT_OK(db_->DisableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("3", num);

    ASSERT_OK(db_->EnableFileDeletions(false));
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("2", num);

    ASSERT_OK(db_->EnableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("0", num);
  } while (ChangeOptions());
}

TEST_F(DBTest, WriteEmptyBatch) {
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  env_->sync_counter_.store(0);
  WriteOptions wo;
  wo.sync = true;
  wo.disableWAL = false;
  WriteBatch empty_batch;
  ASSERT_OK(dbfull()->Write(wo, &empty_batch));
  ASSERT_GE(env_->sync_counter_.load(), 1);

  // make sure we can re-open it.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
  ASSERT_EQ("bar", Get(1, "foo"));
}

TEST_F(DBTest, ReadOnlyDB) {
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  Close();

  auto options = CurrentOptions();
  assert(options.env = env_);
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  Iterator* iter = db_->NewIterator(ReadOptions());
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ++count;
  }
  ASSERT_EQ(count, 2);
  delete iter;
  Close();

  // Reopen and flush memtable.
  Reopen(options);
  Flush();
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
}

TEST_F(DBTest, CompactedDB) {
  const uint64_t kFileSize = 1 << 20;
  Options options;
  options.disable_auto_compactions = true;
  options.max_mem_compaction_level = 0;
  options.write_buffer_size = kFileSize;
  options.target_file_size_base = kFileSize;
  options.max_bytes_for_level_base = 1 << 30;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  Reopen(options);
  // 1 L0 file, use CompactedDB if max_open_files = -1
  ASSERT_OK(Put("aaa", DummyString(kFileSize / 2, '1')));
  Flush();
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  Status s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
  ASSERT_EQ(DummyString(kFileSize / 2, '1'), Get("aaa"));
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported in compacted db mode.");
  ASSERT_EQ(DummyString(kFileSize / 2, '1'), Get("aaa"));
  Close();
  Reopen(options);
  // Add more L0 files
  ASSERT_OK(Put("bbb", DummyString(kFileSize / 2, '2')));
  Flush();
  ASSERT_OK(Put("aaa", DummyString(kFileSize / 2, 'a')));
  Flush();
  ASSERT_OK(Put("bbb", DummyString(kFileSize / 2, 'b')));
  Flush();
  Close();

  ASSERT_OK(ReadOnlyReopen(options));
  // Fallback to read-only DB
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
  Close();

  // Full compaction
  Reopen(options);
  // Add more keys
  ASSERT_OK(Put("eee", DummyString(kFileSize / 2, 'e')));
  ASSERT_OK(Put("fff", DummyString(kFileSize / 2, 'f')));
  ASSERT_OK(Put("hhh", DummyString(kFileSize / 2, 'h')));
  ASSERT_OK(Put("iii", DummyString(kFileSize / 2, 'i')));
  ASSERT_OK(Put("jjj", DummyString(kFileSize / 2, 'j')));
  db_->CompactRange(nullptr, nullptr);
  ASSERT_EQ(3, NumTableFilesAtLevel(1));
  Close();

  // CompactedDB
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported in compacted db mode.");
  ASSERT_EQ("NOT_FOUND", Get("abc"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'a'), Get("aaa"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'b'), Get("bbb"));
  ASSERT_EQ("NOT_FOUND", Get("ccc"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'e'), Get("eee"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'f'), Get("fff"));
  ASSERT_EQ("NOT_FOUND", Get("ggg"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'h'), Get("hhh"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'i'), Get("iii"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'j'), Get("jjj"));
  ASSERT_EQ("NOT_FOUND", Get("kkk"));

  // MultiGet
  std::vector<std::string> values;
  std::vector<Status> status_list = dbfull()->MultiGet(ReadOptions(),
      std::vector<Slice>({Slice("aaa"), Slice("ccc"), Slice("eee"),
                          Slice("ggg"), Slice("iii"), Slice("kkk")}),
      &values);
  ASSERT_EQ(status_list.size(), static_cast<uint64_t>(6));
  ASSERT_EQ(values.size(), static_cast<uint64_t>(6));
  ASSERT_OK(status_list[0]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'a'), values[0]);
  ASSERT_TRUE(status_list[1].IsNotFound());
  ASSERT_OK(status_list[2]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'e'), values[2]);
  ASSERT_TRUE(status_list[3].IsNotFound());
  ASSERT_OK(status_list[4]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'i'), values[4]);
  ASSERT_TRUE(status_list[5].IsNotFound());
}

// Make sure that when options.block_cache is set, after a new table is
// created its index/filter blocks are added to block cache.
TEST_F(DBTest, IndexAndFilterBlocksOfNewTableAddedToCache) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "val"));
  // Create a new table.
  ASSERT_OK(Flush(1));

  // index/filter blocks added to block cache right after table creation.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(2, /* only index/filter were added */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));
  uint64_t int_num;
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_EQ(int_num, 0U);

  // Make sure filter block is in cache.
  std::string value;
  ReadOptions ropt;
  db_->KeyMayExist(ReadOptions(), handles_[1], "key", &value);

  // Miss count should remain the same.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  db_->KeyMayExist(ReadOptions(), handles_[1], "key", &value);
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Make sure index block is in cache.
  auto index_block_hit = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  value = Get(1, "key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(index_block_hit + 1,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  value = Get(1, "key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(index_block_hit + 2,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
}

TEST_F(DBTest, GetPropertiesOfAllTablesTest) {
  Options options = CurrentOptions();
  options.max_background_flushes = 0;
  Reopen(options);
  // Create 4 tables
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      db_->Put(WriteOptions(), ToString(table * 100 + i), "val");
    }
    db_->Flush(FlushOptions());
  }

  // 1. Read table properties directly from file
  Reopen(options);
  VerifyTableProperties(db_, 10 + 11 + 12 + 13);

  // 2. Put two tables to table cache and
  Reopen(options);
  // fetch key from 1st and 2nd table, which will internally place that table to
  // the table cache.
  for (int i = 0; i < 2; ++i) {
    Get(ToString(i * 100 + 0));
  }

  VerifyTableProperties(db_, 10 + 11 + 12 + 13);

  // 3. Put all tables to table cache
  Reopen(options);
  // fetch key from 1st and 2nd table, which will internally place that table to
  // the table cache.
  for (int i = 0; i < 4; ++i) {
    Get(ToString(i * 100 + 0));
  }
  VerifyTableProperties(db_, 10 + 11 + 12 + 13);
}

class CoutingUserTblPropCollector : public TablePropertiesCollector {
 public:
  const char* Name() const override { return "CoutingUserTblPropCollector"; }

  Status Finish(UserCollectedProperties* properties) override {
    std::string encoded;
    PutVarint32(&encoded, count_);
    *properties = UserCollectedProperties{
        {"CoutingUserTblPropCollector", message_}, {"Count", encoded},
    };
    return Status::OK();
  }

  Status AddUserKey(const Slice& user_key, const Slice& value, EntryType type,
                    SequenceNumber seq, uint64_t file_size) override {
    ++count_;
    return Status::OK();
  }

  virtual UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties{};
  }

 private:
  std::string message_ = "Rocksdb";
  uint32_t count_ = 0;
};

class CoutingUserTblPropCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  virtual TablePropertiesCollector* CreateTablePropertiesCollector() override {
    return new CoutingUserTblPropCollector();
  }
  const char* Name() const override {
    return "CoutingUserTblPropCollectorFactory";
  }
};

TEST_F(DBTest, GetUserDefinedTablaProperties) {
  Options options = CurrentOptions();
  options.max_background_flushes = 0;
  options.table_properties_collector_factories.resize(1);
  options.table_properties_collector_factories[0] =
      std::make_shared<CoutingUserTblPropCollectorFactory>();
  Reopen(options);
  // Create 4 tables
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      db_->Put(WriteOptions(), ToString(table * 100 + i), "val");
    }
    db_->Flush(FlushOptions());
  }

  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
  ASSERT_EQ(4U, props.size());
  uint32_t sum = 0;
  for (const auto& item : props) {
    auto& user_collected = item.second->user_collected_properties;
    ASSERT_TRUE(user_collected.find("CoutingUserTblPropCollector") !=
                user_collected.end());
    ASSERT_EQ(user_collected.at("CoutingUserTblPropCollector"), "Rocksdb");
    ASSERT_TRUE(user_collected.find("Count") != user_collected.end());
    Slice key(user_collected.at("Count"));
    uint32_t count;
    ASSERT_TRUE(GetVarint32(&key, &count));
    sum += count;
  }
  ASSERT_EQ(10u + 11u + 12u + 13u, sum);
}

TEST_F(DBTest, LevelLimitReopen) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  const std::string value(1024 * 1024, ' ');
  int i = 0;
  while (NumTableFilesAtLevel(2, 1) == 0) {
    ASSERT_OK(Put(1, Key(i++), value));
  }

  options.num_levels = 1;
  options.max_bytes_for_level_multiplier_additional.resize(1, 1);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ(s.IsInvalidArgument(), true);
  ASSERT_EQ(s.ToString(),
            "Invalid argument: db has more levels than options.num_levels");

  options.num_levels = 10;
  options.max_bytes_for_level_multiplier_additional.resize(10, 1);
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
}

TEST_F(DBTest, PutDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetFromImmutableLayer) {
  do {
    Options options;
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));

    // Block sync calls
    env_->delay_sstable_sync_.store(true, std::memory_order_release);
    Put(1, "k1", std::string(100000, 'x'));          // Fill memtable
    Put(1, "k2", std::string(100000, 'y'));          // Trigger flush
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
    // Release sync calls
    env_->delay_sstable_sync_.store(false, std::memory_order_release);
  } while (ChangeOptions());
}

TEST_F(DBTest, GetFromVersions) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetSnapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    // Try with both a short key and a long key
    for (int i = 0; i < 2; i++) {
      std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
      ASSERT_OK(Put(1, key, "v1"));
      const Snapshot* s1 = db_->GetSnapshot();
      if (option_config_ == kHashCuckoo) {
        // NOt supported case.
        ASSERT_TRUE(s1 == nullptr);
        break;
      }
      ASSERT_OK(Put(1, key, "v2"));
      ASSERT_EQ("v2", Get(1, key));
      ASSERT_EQ("v1", Get(1, key, s1));
      ASSERT_OK(Flush(1));
      ASSERT_EQ("v2", Get(1, key));
      ASSERT_EQ("v1", Get(1, key, s1));
      db_->ReleaseSnapshot(s1);
    }
  } while (ChangeOptions());
}

TEST_F(DBTest, GetSnapshotLink) {
  do {
    Options options;
    const std::string snapshot_name = test::TmpDir(env_) + "/snapshot";
    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    options = CurrentOptions(options);
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_OK(DestroyDB(snapshot_name, options));
    env_->DeleteDir(snapshot_name);

    // Create a database
    Status s;
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name));
    ASSERT_OK(Put(key, "v2"));
    ASSERT_EQ("v2", Get(key));
    ASSERT_OK(Flush());
    ASSERT_EQ("v2", Get(key));
    // Open snapshot and verify contents while DB is running
    options.create_if_missing = false;
    ASSERT_OK(DB::Open(options, snapshot_name, &snapshotDB));
    ASSERT_OK(snapshotDB->Get(roptions, key, &result));
    ASSERT_EQ("v1", result);
    delete snapshotDB;
    snapshotDB = nullptr;
    delete db_;
    db_ = nullptr;

    // Destroy original DB
    ASSERT_OK(DestroyDB(dbname_, options));

    // Open snapshot and verify contents
    options.create_if_missing = false;
    dbname_ = snapshot_name;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_EQ("v1", Get(key));
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    delete checkpoint;

    // Restore DB name
    dbname_ = test::TmpDir(env_) + "/db_test";
  } while (ChangeOptions());
}

TEST_F(DBTest, GetLevel0Ordering) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    // Check that we process level-0 files in correct order.  The code
    // below generates two level-0 files where the earlier one comes
    // before the later one in the level-0 file list since the earlier
    // one has a smaller "smallest" key.
    ASSERT_OK(Put(1, "bar", "b"));
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v2", Get(1, "foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, WrongLevel0Config) {
  Options options = CurrentOptions();
  Close();
  ASSERT_OK(DestroyDB(dbname_, options));
  options.level0_stop_writes_trigger = 1;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_file_num_compaction_trigger = 3;
  ASSERT_OK(DB::Open(options, dbname_, &db_));
}

TEST_F(DBTest, GetOrderedByLevels) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    Compact(1, "a", "z");
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v2", Get(1, "foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetPicksCorrectFile) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    // Arrange to have multiple files in a non-level-0 level.
    ASSERT_OK(Put(1, "a", "va"));
    Compact(1, "a", "b");
    ASSERT_OK(Put(1, "x", "vx"));
    Compact(1, "x", "y");
    ASSERT_OK(Put(1, "f", "vf"));
    Compact(1, "f", "g");
    ASSERT_EQ("va", Get(1, "a"));
    ASSERT_EQ("vf", Get(1, "f"));
    ASSERT_EQ("vx", Get(1, "x"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetEncountersEmptyLevel) {
  do {
    Options options = CurrentOptions();
    options.max_background_flushes = 0;
    options.disableDataSync = true;
    CreateAndReopenWithCF({"pikachu"}, options);
    // Arrange for the following to happen:
    //   * sstable A in level 0
    //   * nothing in level 1
    //   * sstable B in level 2
    // Then do enough Get() calls to arrange for an automatic compaction
    // of sstable A.  A bug would cause the compaction to be marked as
    // occuring at level 1 (instead of the correct level 0).

    // Step 1: First place sstables in levels 0 and 2
    int compaction_count = 0;
    while (NumTableFilesAtLevel(0, 1) == 0 || NumTableFilesAtLevel(2, 1) == 0) {
      ASSERT_LE(compaction_count, 100) << "could not fill levels 0 and 2";
      compaction_count++;
      Put(1, "a", "begin");
      Put(1, "z", "end");
      ASSERT_OK(Flush(1));
    }

    // Step 2: clear level 1 if necessary.
    dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]);
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);
    ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);
    ASSERT_EQ(NumTableFilesAtLevel(2, 1), 1);

    // Step 3: read a bunch of times
    for (int i = 0; i < 1000; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, "missing"));
    }

    // Step 4: Wait for compaction to finish
    env_->SleepForMicroseconds(1000000);

    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);  // XXX
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipFIFOCompaction));
}

// KeyMayExist can lead to a few false positives, but not false negatives.
// To make test deterministic, use a much larger number of bits per key-20 than
// bits in the key, so that false positives are eliminated
TEST_F(DBTest, KeyMayExist) {
  do {
    ReadOptions ropts;
    std::string value;
    anon::OptionsOverride options_override;
    options_override.filter_policy.reset(NewBloomFilterPolicy(20));
    Options options = CurrentOptions(options_override);
    options.statistics = rocksdb::CreateDBStatistics();
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "a", &value));

    ASSERT_OK(Put(1, "a", "b"));
    bool value_found = false;
    ASSERT_TRUE(
        db_->KeyMayExist(ropts, handles_[1], "a", &value, &value_found));
    ASSERT_TRUE(value_found);
    ASSERT_EQ("b", value);

    ASSERT_OK(Flush(1));
    value.clear();

    long numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    long cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(
        db_->KeyMayExist(ropts, handles_[1], "a", &value, &value_found));
    ASSERT_TRUE(!value_found);
    // assert that no new files were opened and no new blocks were
    // read into block cache.
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Delete(1, "a"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Flush(1));
    db_->CompactRange(handles_[1], nullptr, nullptr);

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Delete(1, "c"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "c", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    // KeyMayExist function only checks data in block caches, which is not used
    // by plain table format.
  } while (
      ChangeOptions(kSkipPlainTable | kSkipHashIndex | kSkipFIFOCompaction));
}

TEST_F(DBTest, NonBlockingIteration) {
  do {
    ReadOptions non_blocking_opts, regular_opts;
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    non_blocking_opts.read_tier = kBlockCacheTier;
    CreateAndReopenWithCF({"pikachu"}, options);
    // write one kv to the database.
    ASSERT_OK(Put(1, "a", "b"));

    // scan using non-blocking iterator. We should find it because
    // it is in memtable.
    Iterator* iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_EQ(count, 1);
    delete iter;

    // flush memtable to storage. Now, the key should not be in the
    // memtable neither in the block cache.
    ASSERT_OK(Flush(1));

    // verify that a non-blocking iterator does not find any
    // kvs. Neither does it do any IOs to storage.
    long numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    long cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      count++;
    }
    ASSERT_EQ(count, 0);
    ASSERT_TRUE(iter->status().IsIncomplete());
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // read in the specified block via a regular get
    ASSERT_EQ(Get(1, "a"), "b");

    // verify that we can find it via a non-blocking scan
    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_EQ(count, 1);
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // This test verifies block cache behaviors, which is not used by plain
    // table format.
    // Exclude kHashCuckoo as it does not support iteration currently
  } while (ChangeOptions(kSkipPlainTable | kSkipNoSeekToLast | kSkipHashCuckoo |
                         kSkipMmapReads));
}

TEST_F(DBTest, ManagedNonBlockingIteration) {
  do {
    ReadOptions non_blocking_opts, regular_opts;
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    non_blocking_opts.read_tier = kBlockCacheTier;
    non_blocking_opts.managed = true;
    CreateAndReopenWithCF({"pikachu"}, options);
    // write one kv to the database.
    ASSERT_OK(Put(1, "a", "b"));

    // scan using non-blocking iterator. We should find it because
    // it is in memtable.
    Iterator* iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_EQ(count, 1);
    delete iter;

    // flush memtable to storage. Now, the key should not be in the
    // memtable neither in the block cache.
    ASSERT_OK(Flush(1));

    // verify that a non-blocking iterator does not find any
    // kvs. Neither does it do any IOs to storage.
    int64_t numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    int64_t cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      count++;
    }
    ASSERT_EQ(count, 0);
    ASSERT_TRUE(iter->status().IsIncomplete());
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // read in the specified block via a regular get
    ASSERT_EQ(Get(1, "a"), "b");

    // verify that we can find it via a non-blocking scan
    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_EQ(count, 1);
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // This test verifies block cache behaviors, which is not used by plain
    // table format.
    // Exclude kHashCuckoo as it does not support iteration currently
  } while (ChangeOptions(kSkipPlainTable | kSkipNoSeekToLast | kSkipHashCuckoo |
                         kSkipMmapReads));
}

// A delete is skipped for key if KeyMayExist(key) returns False
// Tests Writebatch consistency and proper delete behaviour
TEST_F(DBTest, FilterDeletes) {
  do {
    anon::OptionsOverride options_override;
    options_override.filter_policy.reset(NewBloomFilterPolicy(20));
    Options options = CurrentOptions(options_override);
    options.filter_deletes = true;
    CreateAndReopenWithCF({"pikachu"}, options);
    WriteBatch batch;

    batch.Delete(handles_[1], "a");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(AllEntriesFor("a", 1), "[ ]");  // Delete skipped
    batch.Clear();

    batch.Put(handles_[1], "a", "b");
    batch.Delete(handles_[1], "a");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(Get(1, "a"), "NOT_FOUND");
    ASSERT_EQ(AllEntriesFor("a", 1), "[ DEL, b ]");  // Delete issued
    batch.Clear();

    batch.Delete(handles_[1], "c");
    batch.Put(handles_[1], "c", "d");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(Get(1, "c"), "d");
    ASSERT_EQ(AllEntriesFor("c", 1), "[ d ]");  // Delete skipped
    batch.Clear();

    ASSERT_OK(Flush(1));  // A stray Flush

    batch.Delete(handles_[1], "c");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(AllEntriesFor("c", 1), "[ DEL, d ]");  // Delete issued
    batch.Clear();
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, GetFilterByPrefixBloom) {
  Options options = last_options_;
  options.prefix_extractor.reset(NewFixedPrefixTransform(8));
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  WriteOptions wo;
  ReadOptions ro;
  FlushOptions fo;
  fo.wait = true;
  std::string value;

  ASSERT_OK(dbfull()->Put(wo, "barbarbar", "foo"));
  ASSERT_OK(dbfull()->Put(wo, "barbarbar2", "foo2"));
  ASSERT_OK(dbfull()->Put(wo, "foofoofoo", "bar"));

  dbfull()->Flush(fo);

  ASSERT_EQ("foo", Get("barbarbar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("foo2", Get("barbarbar2"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("barbarbar3"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);

  ASSERT_EQ("NOT_FOUND", Get("barfoofoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);

  ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);
}

TEST_F(DBTest, WholeKeyFilterProp) {
  Options options = last_options_;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.statistics = rocksdb::CreateDBStatistics();

  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  WriteOptions wo;
  ReadOptions ro;
  FlushOptions fo;
  fo.wait = true;
  std::string value;

  ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
  // Needs insert some keys to make sure files are not filtered out by key
  // ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  dbfull()->Flush(fo);

  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);

  // Reopen with whole key filtering enabled and prefix extractor
  // NULL. Bloom filter should be off for both of whole key and
  // prefix bloom.
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.prefix_extractor.reset();
  Reopen(options);

  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  // Write DB with only full key filtering.
  ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
  // Needs insert some keys to make sure files are not filtered out by key
  // ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  db_->CompactRange(nullptr, nullptr);

  // Reopen with both of whole key off and prefix extractor enabled.
  // Still no bloom filter should be used.
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);

  // Try to create a DB with mixed files:
  ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
  // Needs insert some keys to make sure files are not filtered out by key
  // ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  db_->CompactRange(nullptr, nullptr);

  options.prefix_extractor.reset();
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  // Try to create a DB with mixed files.
  ASSERT_OK(dbfull()->Put(wo, "barfoo", "bar"));
  // In this case needs insert some keys to make sure files are
  // not filtered out by key ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  Flush();

  // Now we have two files:
  // File 1: An older file with prefix bloom.
  // File 2: A newer file with whole bloom filter.
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 3);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 4);
  ASSERT_EQ("bar", Get("barfoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 4);

  // Reopen with the same setting: only whole key is used
  Reopen(options);
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 4);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 5);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 6);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 7);
  ASSERT_EQ("bar", Get("barfoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 7);

  // Restart with both filters are allowed
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 7);
  // File 1 will has it filtered out.
  // File 2 will not, as prefix `foo` exists in the file.
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 8);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 10);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);
  ASSERT_EQ("bar", Get("barfoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);

  // Restart with only prefix bloom is allowed.
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 12);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 12);
  ASSERT_EQ("bar", Get("barfoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 12);
}

TEST_F(DBTest, IterSeekBeforePrev) {
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("0", "f"));
  ASSERT_OK(Put("1", "h"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("2", "j"));
  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  iter->Prev();
  iter->Seek(Slice("a"));
  iter->Prev();
  delete iter;
}

namespace {
std::string MakeLongKey(size_t length, char c) {
  return std::string(length, c);
}
}  // namespace

TEST_F(DBTest, IterLongKeys) {
  ASSERT_OK(Put(MakeLongKey(20, 0), "0"));
  ASSERT_OK(Put(MakeLongKey(32, 2), "2"));
  ASSERT_OK(Put("a", "b"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put(MakeLongKey(50, 1), "1"));
  ASSERT_OK(Put(MakeLongKey(127, 3), "3"));
  ASSERT_OK(Put(MakeLongKey(64, 4), "4"));
  auto iter = db_->NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  iter->Seek(MakeLongKey(20, 0));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(20, 0) + "->0");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(64, 4) + "->4");
  delete iter;

  iter = db_->NewIterator(ReadOptions());
  iter->Seek(MakeLongKey(50, 1));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  delete iter;
}

TEST_F(DBTest, IterNextWithNewerSeq) {
  ASSERT_OK(Put("0", "0"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = db_->NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("a"));
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->d");
  delete iter;
}

TEST_F(DBTest, IterPrevWithNewerSeq) {
  ASSERT_OK(Put("0", "0"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = db_->NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "d->e");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");

  iter->Prev();
  delete iter;
}

TEST_F(DBTest, IterPrevWithNewerSeq2) {
  ASSERT_OK(Put("0", "0"));
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  ASSERT_EQ(IterStatus(iter), "c->d");

  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
      i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");

  iter->Prev();
  delete iter;
}

TEST_F(DBTest, IterEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("foo");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, IterSingle) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, IterMulti) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", "vb"));
    ASSERT_OK(Put(1, "c", "vc"));
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("ax");
    ASSERT_EQ(IterStatus(iter), "b->vb");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Seek("z");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    // Switch from reverse to forward
    iter->SeekToLast();
    iter->Prev();
    iter->Prev();
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Switch from forward to reverse
    iter->SeekToFirst();
    iter->Next();
    iter->Next();
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Make sure iter stays at snapshot
    ASSERT_OK(Put(1, "a", "va2"));
    ASSERT_OK(Put(1, "a2", "va3"));
    ASSERT_OK(Put(1, "b", "vb2"));
    ASSERT_OK(Put(1, "c", "vc2"));
    ASSERT_OK(Delete(1, "b"));
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

// Check that we can skip over a run of user keys
// by using reseek rather than sequential scan
TEST_F(DBTest, IterReseek) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  options.max_sequential_skip_in_iterations = 3;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // insert two keys with same userkey and verify that
  // reseek is not invoked. For each of these test cases,
  // verify that we can find the next key "b".
  ASSERT_OK(Put(1, "a", "one"));
  ASSERT_OK(Put(1, "a", "two"));
  ASSERT_OK(Put(1, "b", "bone"));
  Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "a->two");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of three keys with same userkey and verify
  // that reseek is still not invoked.
  ASSERT_OK(Put(1, "a", "three"));
  iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->three");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of four keys with same userkey and verify
  // that reseek is invoked.
  ASSERT_OK(Put(1, "a", "four"));
  iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->four");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 1);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // Testing reverse iterator
  // At this point, we have three versions of "a" and one version of "b".
  // The reseek statistics is already at 1.
  int num_reseeks =
      (int)TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION);

  // Insert another version of b and assert that reseek is not invoked
  ASSERT_OK(Put(1, "b", "btwo"));
  iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->btwo");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks);
  iter->Prev();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 1);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;

  // insert two more versions of b. This makes a total of 4 versions
  // of b and 4 versions of a.
  ASSERT_OK(Put(1, "b", "bthree"));
  ASSERT_OK(Put(1, "b", "bfour"));
  iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->bfour");
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 2);
  iter->Prev();

  // the previous Prev call should have invoked reseek
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION),
            num_reseeks + 3);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;
}

TEST_F(DBTest, IterSmallAndLargeMix) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", std::string(100000, 'b')));
    ASSERT_OK(Put(1, "c", "vc"));
    ASSERT_OK(Put(1, "d", std::string(100000, 'd')));
    ASSERT_OK(Put(1, "e", std::string(100000, 'e')));

    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, IterMultiWithDelete) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "ka", "va"));
    ASSERT_OK(Put(1, "kb", "vb"));
    ASSERT_OK(Put(1, "kc", "vc"));
    ASSERT_OK(Delete(1, "kb"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kb"));

    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
    iter->Seek("kc");
    ASSERT_EQ(IterStatus(iter), "kc->vc");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_&&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_) {
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "ka->va");
      }
    }
    delete iter;
  } while (ChangeOptions());
}

TEST_F(DBTest, IterPrevMaxSkip) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    for (int i = 0; i < 2; i++) {
      ASSERT_OK(Put(1, "key1", "v1"));
      ASSERT_OK(Put(1, "key2", "v2"));
      ASSERT_OK(Put(1, "key3", "v3"));
      ASSERT_OK(Put(1, "key4", "v4"));
      ASSERT_OK(Put(1, "key5", "v5"));
    }

    VerifyIterLast("key5->v5", 1);

    ASSERT_OK(Delete(1, "key5"));
    VerifyIterLast("key4->v4", 1);

    ASSERT_OK(Delete(1, "key4"));
    VerifyIterLast("key3->v3", 1);

    ASSERT_OK(Delete(1, "key3"));
    VerifyIterLast("key2->v2", 1);

    ASSERT_OK(Delete(1, "key2"));
    VerifyIterLast("key1->v1", 1);

    ASSERT_OK(Delete(1, "key1"));
    VerifyIterLast("(invalid)", 1);
  } while (ChangeOptions(kSkipMergePut | kSkipNoSeekToLast));
}

TEST_F(DBTest, IterWithSnapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    ASSERT_OK(Put(1, "key1", "val1"));
    ASSERT_OK(Put(1, "key2", "val2"));
    ASSERT_OK(Put(1, "key3", "val3"));
    ASSERT_OK(Put(1, "key4", "val4"));
    ASSERT_OK(Put(1, "key5", "val5"));

    const Snapshot *snapshot = db_->GetSnapshot();
    ReadOptions options;
    options.snapshot = snapshot;
    Iterator* iter = db_->NewIterator(options, handles_[1]);

    // Put more values after the snapshot
    ASSERT_OK(Put(1, "key100", "val100"));
    ASSERT_OK(Put(1, "key101", "val101"));

    iter->Seek("key5");
    ASSERT_EQ(IterStatus(iter), "key5->val5");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_&&
        kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
        kHashLinkList != option_config_) {
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key3->val3");

        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key5->val5");
      }
      iter->Next();
      ASSERT_TRUE(!iter->Valid());
    }
    db_->ReleaseSnapshot(snapshot);
    delete iter;
    // skip as HashCuckooRep does not support snapshot
  } while (ChangeOptions(kSkipHashCuckoo));
}

TEST_F(DBTest, Recover) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v5", Get(1, "baz"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Put(1, "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v4"));
    ASSERT_EQ("v4", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v5", Get(1, "baz"));
  } while (ChangeOptions());
}

TEST_F(DBTest, RecoverWithTableHandle) {
  do {
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 100;
    options.disable_auto_compactions = true;
    options = CurrentOptions(options);
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "foo", "v3"));
    ASSERT_OK(Put(1, "bar", "v4"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "big", std::string(100, 'a')));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());

    std::vector<std::vector<FileMetaData>> files;
    dbfull()->TEST_GetFilesMetaData(handles_[1], &files);
    int total_files = 0;
    for (const auto& level : files) {
      total_files += level.size();
    }
    ASSERT_EQ(total_files, 3);
    for (const auto& level : files) {
      for (const auto& file : level) {
        if (kInfiniteMaxOpenFiles == option_config_) {
          ASSERT_TRUE(file.table_reader_handle != nullptr);
        } else {
          ASSERT_TRUE(file.table_reader_handle == nullptr);
        }
      }
    }
  } while (ChangeOptions());
}

TEST_F(DBTest, IgnoreRecoveredLog) {
  std::string backup_logs = dbname_ + "/backup_logs";

  // delete old files in backup_logs directory
  env_->CreateDirIfMissing(backup_logs);
  std::vector<std::string> old_files;
  env_->GetChildren(backup_logs, &old_files);
  for (auto& file : old_files) {
    if (file != "." && file != "..") {
      env_->DeleteFile(backup_logs + "/" + file);
    }
  }

  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.merge_operator = MergeOperators::CreateUInt64AddOperator();
    options.wal_dir = dbname_ + "/logs";
    DestroyAndReopen(options);

    // fill up the DB
    std::string one, two;
    PutFixed64(&one, 1);
    PutFixed64(&two, 2);
    ASSERT_OK(db_->Merge(WriteOptions(), Slice("foo"), Slice(one)));
    ASSERT_OK(db_->Merge(WriteOptions(), Slice("foo"), Slice(one)));
    ASSERT_OK(db_->Merge(WriteOptions(), Slice("bar"), Slice(one)));

    // copy the logs to backup
    std::vector<std::string> logs;
    env_->GetChildren(options.wal_dir, &logs);
    for (auto& log : logs) {
      if (log != ".." && log != ".") {
        CopyFile(options.wal_dir + "/" + log, backup_logs + "/" + log);
      }
    }

    // recover the DB
    Reopen(options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));
    Close();

    // copy the logs from backup back to wal dir
    for (auto& log : logs) {
      if (log != ".." && log != ".") {
        CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
      }
    }
    // this should ignore the log files, recovery should not happen again
    // if the recovery happens, the same merge operator would be called twice,
    // leading to incorrect results
    Reopen(options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));
    Close();
    Destroy(options);
    Reopen(options);
    Close();

    // copy the logs from backup back to wal dir
    env_->CreateDirIfMissing(options.wal_dir);
    for (auto& log : logs) {
      if (log != ".." && log != ".") {
        CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
      }
    }
    // assert that we successfully recovered only from logs, even though we
    // destroyed the DB
    Reopen(options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));

    // Recovery will fail if DB directory doesn't exist.
    Destroy(options);
    // copy the logs from backup back to wal dir
    env_->CreateDirIfMissing(options.wal_dir);
    for (auto& log : logs) {
      if (log != ".." && log != ".") {
        CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
        // we won't be needing this file no more
        env_->DeleteFile(backup_logs + "/" + log);
      }
    }
    Status s = TryReopen(options);
    ASSERT_TRUE(!s.ok());
  } while (ChangeOptions(kSkipHashCuckoo));
}

TEST_F(DBTest, RollLog) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    }
    ASSERT_OK(Put(1, "foo", "v4"));
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    }
  } while (ChangeOptions());
}

TEST_F(DBTest, WAL) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // Both value's should be present.
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v2", Get(1, "foo"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // again both values should be present.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, CheckLock) {
  do {
    DB* localdb;
    Options options = CurrentOptions();
    ASSERT_OK(TryReopen(options));

    // second open should fail
    ASSERT_TRUE(!(DB::Open(options, dbname_, &localdb)).ok());
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, FlushMultipleMemtable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));
    ASSERT_OK(Flush(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, NumImmutableMemTable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    options.write_buffer_size = 1000000;
    CreateAndReopenWithCF({"pikachu"}, options);

    std::string big_value(1000000 * 2, 'x');
    std::string num;
    SetPerfLevel(kEnableTime);;
    ASSERT_TRUE(GetPerfLevel() == kEnableTime);

    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k1", big_value));
    ASSERT_TRUE(dbfull()->GetProperty(handles_[1],
                                      "rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "0");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ(num, "1");
    perf_context.Reset();
    Get(1, "k1");
    ASSERT_EQ(1, (int) perf_context.get_from_memtable_count);

    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k2", big_value));
    ASSERT_TRUE(dbfull()->GetProperty(handles_[1],
                                      "rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "1");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ(num, "1");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-imm-mem-tables", &num));
    ASSERT_EQ(num, "1");

    perf_context.Reset();
    Get(1, "k1");
    ASSERT_EQ(2, (int) perf_context.get_from_memtable_count);
    perf_context.Reset();
    Get(1, "k2");
    ASSERT_EQ(1, (int) perf_context.get_from_memtable_count);

    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k3", big_value));
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.cur-size-active-mem-table", &num));
    ASSERT_TRUE(dbfull()->GetProperty(handles_[1],
                                      "rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "2");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ(num, "1");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-imm-mem-tables", &num));
    ASSERT_EQ(num, "2");
    perf_context.Reset();
    Get(1, "k2");
    ASSERT_EQ(2, (int) perf_context.get_from_memtable_count);
    perf_context.Reset();
    Get(1, "k3");
    ASSERT_EQ(1, (int) perf_context.get_from_memtable_count);
    perf_context.Reset();
    Get(1, "k1");
    ASSERT_EQ(3, (int) perf_context.get_from_memtable_count);

    ASSERT_OK(Flush(1));
    ASSERT_TRUE(dbfull()->GetProperty(handles_[1],
                                      "rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "0");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.cur-size-active-mem-table", &num));
    // "200" is the size of the metadata of an empty skiplist, this would
    // break if we change the default skiplist implementation
    ASSERT_EQ(num, "200");

    uint64_t int_num;
    uint64_t base_total_size;
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.estimate-num-keys", &base_total_size));

    ASSERT_OK(dbfull()->Delete(writeOpt, handles_[1], "k2"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k3", ""));
    ASSERT_OK(dbfull()->Delete(writeOpt, handles_[1], "k3"));
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.num-deletes-active-mem-table", &int_num));
    ASSERT_EQ(int_num, 2U);
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &int_num));
    ASSERT_EQ(int_num, 3U);

    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k2", big_value));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k2", big_value));
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.num-entries-imm-mem-tables", &int_num));
    ASSERT_EQ(int_num, 4U);
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.num-deletes-imm-mem-tables", &int_num));
    ASSERT_EQ(int_num, 2U);

    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.estimate-num-keys", &int_num));
    ASSERT_EQ(int_num, base_total_size + 1);

    SetPerfLevel(kDisable);
    ASSERT_TRUE(GetPerfLevel() == kDisable);
  } while (ChangeCompactOptions());
}

class SleepingBackgroundTask {
 public:
  SleepingBackgroundTask()
      : bg_cv_(&mutex_), should_sleep_(true), done_with_sleep_(false) {}
  void DoSleep() {
    MutexLock l(&mutex_);
    while (should_sleep_) {
      bg_cv_.Wait();
    }
    done_with_sleep_ = true;
    bg_cv_.SignalAll();
  }
  void WakeUp() {
    MutexLock l(&mutex_);
    should_sleep_ = false;
    bg_cv_.SignalAll();
  }
  void WaitUntilDone() {
    MutexLock l(&mutex_);
    while (!done_with_sleep_) {
      bg_cv_.Wait();
    }
  }

  static void DoSleepTask(void* arg) {
    reinterpret_cast<SleepingBackgroundTask*>(arg)->DoSleep();
  }

 private:
  port::Mutex mutex_;
  port::CondVar bg_cv_;  // Signalled when background work finishes
  bool should_sleep_;
  bool done_with_sleep_;
};

TEST_F(DBTest, FlushEmptyColumnFamily) {
  // Block flush thread and disable compaction thread
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_high,
                 Env::Priority::HIGH);

  Options options = CurrentOptions();
  // disable compaction
  options.disable_auto_compactions = true;
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.max_write_buffer_number = 2;
  options.min_write_buffer_number_to_merge = 1;
  CreateAndReopenWithCF({"pikachu"}, options);

  // Compaction can still go through even if no thread can flush the
  // mem table.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  // Insert can go through
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[0], "foo", "v1"));
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

  ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(1, "bar"));

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();

  // Flush can still go through.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBTest, GetProperty) {
  // Set sizes to both background thread pool to be 1 and block them.
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_high,
                 Env::Priority::HIGH);

  Options options = CurrentOptions();
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = 1;
  options.compaction_options_universal.size_ratio = 50;
  options.max_background_compactions = 1;
  options.max_background_flushes = 1;
  options.max_write_buffer_number = 10;
  options.min_write_buffer_number_to_merge = 1;
  options.write_buffer_size = 1000000;
  Reopen(options);

  std::string big_value(1000000 * 2, 'x');
  std::string num;
  uint64_t int_num;
  SetPerfLevel(kEnableTime);

  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_EQ(int_num, 0U);

  ASSERT_OK(dbfull()->Put(writeOpt, "k1", big_value));
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.mem-table-flush-pending", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.compaction-pending", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.estimate-num-keys", &num));
  ASSERT_EQ(num, "1");
  perf_context.Reset();

  ASSERT_OK(dbfull()->Put(writeOpt, "k2", big_value));
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
  ASSERT_EQ(num, "1");
  ASSERT_OK(dbfull()->Delete(writeOpt, "k-non-existing"));
  ASSERT_OK(dbfull()->Put(writeOpt, "k3", big_value));
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
  ASSERT_EQ(num, "2");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.mem-table-flush-pending", &num));
  ASSERT_EQ(num, "1");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.compaction-pending", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.estimate-num-keys", &num));
  ASSERT_EQ(num, "2");
  // Verify the same set of properties through GetIntProperty
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.num-immutable-mem-table", &int_num));
  ASSERT_EQ(int_num, 2U);
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.mem-table-flush-pending", &int_num));
  ASSERT_EQ(int_num, 1U);
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.compaction-pending", &int_num));
  ASSERT_EQ(int_num, 0U);
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.estimate-num-keys", &int_num));
  ASSERT_EQ(int_num, 2U);

  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_EQ(int_num, 0U);

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
  dbfull()->TEST_WaitForFlushMemTable();

  ASSERT_OK(dbfull()->Put(writeOpt, "k4", big_value));
  ASSERT_OK(dbfull()->Put(writeOpt, "k5", big_value));
  dbfull()->TEST_WaitForFlushMemTable();
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.mem-table-flush-pending", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.compaction-pending", &num));
  ASSERT_EQ(num, "1");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.estimate-num-keys", &num));
  ASSERT_EQ(num, "4");

  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_GT(int_num, 0U);

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  dbfull()->TEST_WaitForFlushMemTable();
  options.max_open_files = 10;
  Reopen(options);
  // After reopening, no table reader is loaded, so no memory for table readers
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_EQ(int_num, 0U);
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.estimate-num-keys", &int_num));
  ASSERT_GT(int_num, 0U);

  // After reading a key, at least one table reader is loaded.
  Get("k5");
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_GT(int_num, 0U);

  // Test rocksdb.num-live-versions
  {
    options.level0_file_num_compaction_trigger = 20;
    Reopen(options);
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 1U);

    // Use an iterator to hold current version
    std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));

    ASSERT_OK(dbfull()->Put(writeOpt, "k6", big_value));
    Flush();
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 2U);

    // Use an iterator to hold current version
    std::unique_ptr<Iterator> iter2(dbfull()->NewIterator(ReadOptions()));

    ASSERT_OK(dbfull()->Put(writeOpt, "k7", big_value));
    Flush();
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 3U);

    iter2.reset();
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 2U);

    iter1.reset();
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 1U);
  }
}

TEST_F(DBTest, FLUSH) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    SetPerfLevel(kEnableTime);;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    // this will now also flush the last 2 writes
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    perf_context.Reset();
    Get(1, "foo");
    ASSERT_TRUE((int) perf_context.get_from_output_files_time > 0);

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v2", Get(1, "bar"));
    perf_context.Reset();
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_TRUE((int) perf_context.get_from_output_files_time > 0);

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // 'foo' should be there because its put
    // has WAL enabled.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, RecoveryWithEmptyLog) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v3"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v3", Get(1, "foo"));
  } while (ChangeOptions());
}

// Check that writes done during a memtable compaction are recovered
// if the database is shutdown during the memtable compaction.
TEST_F(DBTest, RecoverDuringMemtableCompaction) {
  do {
    Options options;
    options.env = env_;
    options.write_buffer_size = 1000000;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Trigger a long memtable compaction and reopen the database during it
    ASSERT_OK(Put(1, "foo", "v1"));  // Goes to 1st log file
    ASSERT_OK(Put(1, "big1", std::string(10000000, 'x')));  // Fills memtable
    ASSERT_OK(Put(1, "big2", std::string(1000, 'y')));  // Triggers compaction
    ASSERT_OK(Put(1, "bar", "v2"));                     // Goes to new log file

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ(std::string(10000000, 'x'), Get(1, "big1"));
    ASSERT_EQ(std::string(1000, 'y'), Get(1, "big2"));
  } while (ChangeOptions());
}

// false positive TSAN report on shared_ptr --
// https://groups.google.com/forum/#!topic/thread-sanitizer/vz_s-t226Vg
#ifndef ROCKSDB_TSAN_RUN
TEST_F(DBTest, FlushSchedule) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_stop_writes_trigger = 1 << 10;
  options.level0_slowdown_writes_trigger = 1 << 10;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number = 2;
  options.write_buffer_size = 100 * 1000;
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<std::thread> threads;

  std::atomic<int> thread_num(0);
  // each column family will have 5 thread, each thread generating 2 memtables.
  // each column family should end up with 10 table files
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&]() {
      int a = thread_num.fetch_add(1);
      Random rnd(a);
      WriteOptions wo;
      // this should fill up 2 memtables
      for (int k = 0; k < 5000; ++k) {
        ASSERT_OK(db_->Put(wo, handles_[a & 1], RandomString(&rnd, 13), ""));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto default_tables = GetNumberOfSstFilesForColumnFamily(db_, "default");
  auto pikachu_tables = GetNumberOfSstFilesForColumnFamily(db_, "pikachu");
  ASSERT_LE(default_tables, static_cast<uint64_t>(10));
  ASSERT_GT(default_tables, static_cast<uint64_t>(0));
  ASSERT_LE(pikachu_tables, static_cast<uint64_t>(10));
  ASSERT_GT(pikachu_tables, static_cast<uint64_t>(0));
}
#endif  // enabled only if not TSAN run

TEST_F(DBTest, MinorCompactionsHappen) {
  do {
    Options options;
    options.write_buffer_size = 10000;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    const int N = 500;

    int starting_num_tables = TotalTableFiles(1);
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i) + std::string(1000, 'v')));
    }
    int ending_num_tables = TotalTableFiles(1);
    ASSERT_GT(ending_num_tables, starting_num_tables);

    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(1, Key(i)));
    }

    ReopenWithColumnFamilies({"default", "pikachu"}, options);

    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(1, Key(i)));
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, ManifestRollOver) {
  do {
    Options options;
    options.max_manifest_file_size = 10 ;  // 10 bytes
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    {
      ASSERT_OK(Put(1, "manifest_key1", std::string(1000, '1')));
      ASSERT_OK(Put(1, "manifest_key2", std::string(1000, '2')));
      ASSERT_OK(Put(1, "manifest_key3", std::string(1000, '3')));
      uint64_t manifest_before_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_OK(Flush(1));  // This should trigger LogAndApply.
      uint64_t manifest_after_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_GT(manifest_after_flush, manifest_before_flush);
      ReopenWithColumnFamilies({"default", "pikachu"}, options);
      ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(), manifest_after_flush);
      // check if a new manifest file got inserted or not.
      ASSERT_EQ(std::string(1000, '1'), Get(1, "manifest_key1"));
      ASSERT_EQ(std::string(1000, '2'), Get(1, "manifest_key2"));
      ASSERT_EQ(std::string(1000, '3'), Get(1, "manifest_key3"));
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, IdentityAcrossRestarts) {
  do {
    std::string id1;
    ASSERT_OK(db_->GetDbIdentity(id1));

    Options options = CurrentOptions();
    Reopen(options);
    std::string id2;
    ASSERT_OK(db_->GetDbIdentity(id2));
    // id1 should match id2 because identity was not regenerated
    ASSERT_EQ(id1.compare(id2), 0);

    std::string idfilename = IdentityFileName(dbname_);
    ASSERT_OK(env_->DeleteFile(idfilename));
    Reopen(options);
    std::string id3;
    ASSERT_OK(db_->GetDbIdentity(id3));
    // id1 should NOT match id3 because identity was regenerated
    ASSERT_NE(id1.compare(id3), 0);
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, RecoverWithLargeLog) {
  do {
    {
      Options options = CurrentOptions();
      CreateAndReopenWithCF({"pikachu"}, options);
      ASSERT_OK(Put(1, "big1", std::string(200000, '1')));
      ASSERT_OK(Put(1, "big2", std::string(200000, '2')));
      ASSERT_OK(Put(1, "small3", std::string(10, '3')));
      ASSERT_OK(Put(1, "small4", std::string(10, '4')));
      ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    }

    // Make sure that if we re-open with a small write buffer size that
    // we flush table files in the middle of a large log file.
    Options options;
    options.write_buffer_size = 100000;
    options = CurrentOptions(options);
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 3);
    ASSERT_EQ(std::string(200000, '1'), Get(1, "big1"));
    ASSERT_EQ(std::string(200000, '2'), Get(1, "big2"));
    ASSERT_EQ(std::string(10, '3'), Get(1, "small3"));
    ASSERT_EQ(std::string(10, '4'), Get(1, "small4"));
    ASSERT_GT(NumTableFilesAtLevel(0, 1), 1);
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, CompactionsGenerateMultipleFiles) {
  Options options;
  options.write_buffer_size = 100000000;        // Large write buffer
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);

  // Write 8MB (80 values, each 100K)
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  std::vector<std::string> values;
  for (int i = 0; i < 80; i++) {
    values.push_back(RandomString(&rnd, 100000));
    ASSERT_OK(Put(1, Key(i), values[i]));
  }

  // Reopening moves updates to level-0
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);

  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_GT(NumTableFilesAtLevel(1, 1), 1);
  for (int i = 0; i < 80; i++) {
    ASSERT_EQ(Get(1, Key(i)), values[i]);
  }
}

TEST_F(DBTest, CompactionTrigger) {
  Options options;
  options.write_buffer_size = 100<<10; //100KB
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.level0_file_num_compaction_trigger = 3;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);

  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    std::vector<std::string> values;
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      values.push_back(RandomString(&rnd, 10000));
      ASSERT_OK(Put(1, Key(i), values[i]));
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), num + 1);
  }

  //generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < 12; i++) {
    values.push_back(RandomString(&rnd, 10000));
    ASSERT_OK(Put(1, Key(i), values[i]));
  }
  dbfull()->TEST_WaitForCompact();

  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 1);
}

namespace {
static const int kCDTValueSize = 1000;
static const int kCDTKeysPerBuffer = 4;
static const int kCDTNumLevels = 8;
Options DeletionTriggerOptions() {
  Options options;
  options.compression = kNoCompression;
  options.write_buffer_size = kCDTKeysPerBuffer * (kCDTValueSize + 24);
  options.min_write_buffer_number_to_merge = 1;
  options.num_levels = kCDTNumLevels;
  options.max_mem_compaction_level = 0;
  options.level0_file_num_compaction_trigger = 1;
  options.target_file_size_base = options.write_buffer_size * 2;
  options.target_file_size_multiplier = 2;
  options.max_bytes_for_level_base =
      options.target_file_size_base * options.target_file_size_multiplier;
  options.max_bytes_for_level_multiplier = 2;
  options.disable_auto_compactions = false;
  return options;
}
}  // anonymous namespace

TEST_F(DBTest, CompactionDeletionTrigger) {
  for (int tid = 0; tid < 2; ++tid) {
    uint64_t db_size[2];
    Options options = CurrentOptions(DeletionTriggerOptions());

    if (tid == 1) {
      // second pass with universal compaction
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 1;
    }

    DestroyAndReopen(options);
    Random rnd(301);

    const int kTestSize = kCDTKeysPerBuffer * 512;
    std::vector<std::string> values;
    for (int k = 0; k < kTestSize; ++k) {
      values.push_back(RandomString(&rnd, kCDTValueSize));
      ASSERT_OK(Put(Key(k), values[k]));
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
    db_size[0] = Size(Key(0), Key(kTestSize - 1));

    for (int k = 0; k < kTestSize; ++k) {
      ASSERT_OK(Delete(Key(k)));
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
    db_size[1] = Size(Key(0), Key(kTestSize - 1));

    // must have much smaller db size.
    ASSERT_GT(db_size[0] / 3, db_size[1]);
  }
}

TEST_F(DBTest, CompactionDeletionTriggerReopen) {
  for (int tid = 0; tid < 2; ++tid) {
    uint64_t db_size[3];
    Options options = CurrentOptions(DeletionTriggerOptions());

    if (tid == 1) {
      // second pass with universal compaction
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 1;
    }

    DestroyAndReopen(options);
    Random rnd(301);

    // round 1 --- insert key/value pairs.
    const int kTestSize = kCDTKeysPerBuffer * 512;
    std::vector<std::string> values;
    for (int k = 0; k < kTestSize; ++k) {
      values.push_back(RandomString(&rnd, kCDTValueSize));
      ASSERT_OK(Put(Key(k), values[k]));
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
    db_size[0] = Size(Key(0), Key(kTestSize - 1));
    Close();

    // round 2 --- disable auto-compactions and issue deletions.
    options.create_if_missing = false;
    options.disable_auto_compactions = true;
    Reopen(options);

    for (int k = 0; k < kTestSize; ++k) {
      ASSERT_OK(Delete(Key(k)));
    }
    db_size[1] = Size(Key(0), Key(kTestSize - 1));
    Close();
    // as auto_compaction is off, we shouldn't see too much reduce
    // in db size.
    ASSERT_LT(db_size[0] / 3, db_size[1]);

    // round 3 --- reopen db with auto_compaction on and see if
    // deletion compensation still work.
    options.disable_auto_compactions = false;
    Reopen(options);
    // insert relatively small amount of data to trigger auto compaction.
    for (int k = 0; k < kTestSize / 10; ++k) {
      ASSERT_OK(Put(Key(k), values[k]));
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
    db_size[2] = Size(Key(0), Key(kTestSize - 1));
    // this time we're expecting significant drop in size.
    ASSERT_GT(db_size[0] / 3, db_size[2]);
  }
}

// This is a static filter used for filtering
// kvs during the compaction process.
static int cfilter_count;
static std::string NEW_VALUE = "NewValue";

class KeepFilter : public CompactionFilter {
 public:
  virtual bool Filter(int level, const Slice& key, const Slice& value,
                      std::string* new_value, bool* value_changed) const
      override {
    cfilter_count++;
    return false;
  }

  virtual const char* Name() const override { return "KeepFilter"; }
};

class DeleteFilter : public CompactionFilter {
 public:
  virtual bool Filter(int level, const Slice& key, const Slice& value,
                      std::string* new_value, bool* value_changed) const
      override {
    cfilter_count++;
    return true;
  }

  virtual const char* Name() const override { return "DeleteFilter"; }
};

class DelayFilter : public CompactionFilter {
 public:
  explicit DelayFilter(DBTest* d) : db_test(d) {}
  virtual bool Filter(int level, const Slice& key, const Slice& value,
                      std::string* new_value,
                      bool* value_changed) const override {
    db_test->env_->addon_time_ += 1000;
    return true;
  }

  virtual const char* Name() const override { return "DelayFilter"; }

 private:
  DBTest* db_test;
};

class ConditionalFilter : public CompactionFilter {
 public:
  explicit ConditionalFilter(const std::string* filtered_value)
      : filtered_value_(filtered_value) {}
  virtual bool Filter(int level, const Slice& key, const Slice& value,
                      std::string* new_value,
                      bool* value_changed) const override {
    return value.ToString() == *filtered_value_;
  }

  virtual const char* Name() const override { return "ConditionalFilter"; }

 private:
  const std::string* filtered_value_;
};

class ChangeFilter : public CompactionFilter {
 public:
  explicit ChangeFilter() {}

  virtual bool Filter(int level, const Slice& key, const Slice& value,
                      std::string* new_value, bool* value_changed) const
      override {
    assert(new_value != nullptr);
    *new_value = NEW_VALUE;
    *value_changed = true;
    return false;
  }

  virtual const char* Name() const override { return "ChangeFilter"; }
};

class KeepFilterFactory : public CompactionFilterFactory {
 public:
  explicit KeepFilterFactory(bool check_context = false)
      : check_context_(check_context) {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    if (check_context_) {
      EXPECT_EQ(expect_full_compaction_.load(), context.is_full_compaction);
      EXPECT_EQ(expect_manual_compaction_.load(), context.is_manual_compaction);
    }
    return std::unique_ptr<CompactionFilter>(new KeepFilter());
  }

  virtual const char* Name() const override { return "KeepFilterFactory"; }
  bool check_context_;
  std::atomic_bool expect_full_compaction_;
  std::atomic_bool expect_manual_compaction_;
};

class DeleteFilterFactory : public CompactionFilterFactory {
 public:
  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    if (context.is_manual_compaction) {
      return std::unique_ptr<CompactionFilter>(new DeleteFilter());
    } else {
      return std::unique_ptr<CompactionFilter>(nullptr);
    }
  }

  virtual const char* Name() const override { return "DeleteFilterFactory"; }
};

class DelayFilterFactory : public CompactionFilterFactory {
 public:
  explicit DelayFilterFactory(DBTest* d) : db_test(d) {}
  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    return std::unique_ptr<CompactionFilter>(new DelayFilter(db_test));
  }

  virtual const char* Name() const override { return "DelayFilterFactory"; }

 private:
  DBTest* db_test;
};

class ConditionalFilterFactory : public CompactionFilterFactory {
 public:
  explicit ConditionalFilterFactory(const Slice& filtered_value)
      : filtered_value_(filtered_value.ToString()) {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    return std::unique_ptr<CompactionFilter>(
        new ConditionalFilter(&filtered_value_));
  }

  virtual const char* Name() const override {
    return "ConditionalFilterFactory";
  }

 private:
  std::string filtered_value_;
};

class ChangeFilterFactory : public CompactionFilterFactory {
 public:
  explicit ChangeFilterFactory() {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    return std::unique_ptr<CompactionFilter>(new ChangeFilter());
  }

  virtual const char* Name() const override { return "ChangeFilterFactory"; }
};

class DBTestUniversalCompactionBase
    : public DBTest,
      public ::testing::WithParamInterface<int> {
 public:
  virtual void SetUp() override { num_levels_ = GetParam(); }
  int num_levels_;
};

class DBTestUniversalCompaction : public DBTestUniversalCompactionBase {};

// TODO(kailiu) The tests on UniversalCompaction has some issues:
//  1. A lot of magic numbers ("11" or "12").
//  2. Made assumption on the memtable flush conditions, which may change from
//     time to time.
TEST_P(DBTestUniversalCompaction, UniversalCompactionTrigger) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
  KeepFilterFactory* filter = new KeepFilterFactory(true);
  filter->expect_manual_compaction_.store(false);
  options.compaction_filter_factory.reset(filter);

  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  int key_idx = 0;

  filter->expect_full_compaction_.store(true);
  // Stage 1:
  //   Generate a set of files at level 0, but don't trigger level-0
  //   compaction.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 12; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 1);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Suppose each file flushed from mem table has size 1. Now we compact
  // (level0_file_num_compaction_trigger+1)=4 files and should have a big
  // file of size 4.
  ASSERT_EQ(NumSortedRuns(1), 1);

  // Stage 2:
  //   Now we have one file at level 0, with size 4. We also have some data in
  //   mem table. Let's continue generating new files at level 0, but don't
  //   trigger level-0 compaction.
  //   First, clean up memtable before inserting new data. This will generate
  //   a level-0 file, with size around 0.4 (according to previously written
  //   data amount).
  filter->expect_full_compaction_.store(false);
  ASSERT_OK(Flush(1));
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 3;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 0.4, 1, 1.
  // After compaction, we should have 2 files, with size 4, 2.4.
  ASSERT_EQ(NumSortedRuns(1), 2);

  // Stage 3:
  //   Now we have 2 files at level 0, with size 4 and 2.4. Continue
  //   generating new files at level 0.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 3;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 12; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 2.4, 1, 1.
  // After compaction, we should have 3 files, with size 4, 2.4, 2.
  ASSERT_EQ(NumSortedRuns(1), 3);

  // Stage 4:
  //   Now we have 3 files at level 0, with size 4, 2.4, 2. Let's generate a
  //   new file of size 1.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Level-0 compaction is triggered, but no file will be picked up.
  ASSERT_EQ(NumSortedRuns(1), 4);

  // Stage 5:
  //   Now we have 4 files at level 0, with size 4, 2.4, 2, 1. Let's generate
  //   a new file of size 1.
  filter->expect_full_compaction_.store(true);
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // All files at level 0 will be compacted into a single one.
  ASSERT_EQ(NumSortedRuns(1), 1);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionSizeAmplification) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 3;
  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int key_idx = 0;

  //   Generate two files in Level 0. Both files are approx the same size.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 1);
  }
  ASSERT_EQ(NumSortedRuns(1), 2);

  // Flush whatever is remaining in memtable. This is typically
  // small, which should not trigger size ratio based compaction
  // but will instead trigger size amplification.
  ASSERT_OK(Flush(1));

  dbfull()->TEST_WaitForCompact();

  // Verify that size amplification did occur
  ASSERT_EQ(NumSortedRuns(1), 1);
}

class DBTestUniversalCompactionMultiLevels
    : public DBTestUniversalCompactionBase {};

TEST_P(DBTestUniversalCompactionMultiLevels, UniversalCompactionMultiLevels) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 8;
  options.max_background_compactions = 3;
  options.target_file_size_base = 32 * 1024;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 100000;
  for (int i = 0; i < num_keys * 2; i++) {
    ASSERT_OK(Put(1, Key(i % num_keys), Key(i)));
  }

  dbfull()->TEST_WaitForCompact();

  for (int i = num_keys; i < num_keys * 2; i++) {
    ASSERT_EQ(Get(1, Key(i % num_keys)), Key(i));
  }
}

INSTANTIATE_TEST_CASE_P(DBTestUniversalCompactionMultiLevels,
                        DBTestUniversalCompactionMultiLevels,
                        ::testing::Values(3, 20));

class DBTestUniversalCompactionParallel : public DBTestUniversalCompactionBase {
};

TEST_P(DBTestUniversalCompactionParallel, UniversalCompactionParallel) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 1 << 10;  // 1KB
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 3;
  options.max_background_flushes = 3;
  options.target_file_size_base = 1 * 1024;
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Delay every compaction so multiple compactions will happen.
  std::atomic<int> num_compactions_running(0);
  std::atomic<bool> has_parallel(false);
  rocksdb::SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():Start",
                                                 [&](void* arg) {
    if (num_compactions_running.fetch_add(1) > 0) {
      has_parallel.store(true);
      return;
    }
    for (int nwait = 0; nwait < 20000; nwait++) {
      if (has_parallel.load() || num_compactions_running.load() > 1) {
        has_parallel.store(true);
        break;
      }
      env_->SleepForMicroseconds(1000);
    }
  });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():End",
      [&](void* arg) { num_compactions_running.fetch_add(-1); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 30000;
  for (int i = 0; i < num_keys * 2; i++) {
    ASSERT_OK(Put(1, Key(i % num_keys), Key(i)));
  }
  dbfull()->TEST_WaitForCompact();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(num_compactions_running.load(), 0);
  ASSERT_TRUE(has_parallel.load());

  for (int i = num_keys; i < num_keys * 2; i++) {
    ASSERT_EQ(Get(1, Key(i % num_keys)), Key(i));
  }

  // Reopen and check.
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  for (int i = num_keys; i < num_keys * 2; i++) {
    ASSERT_EQ(Get(1, Key(i % num_keys)), Key(i));
  }
}

INSTANTIATE_TEST_CASE_P(DBTestUniversalCompactionParallel,
                        DBTestUniversalCompactionParallel,
                        ::testing::Values(1, 10));

TEST_P(DBTestUniversalCompaction, UniversalCompactionOptions) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = num_levels_;
  options.compaction_options_universal.compression_size_percent = -1;
  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  int key_idx = 0;

  for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);

    if (num < options.level0_file_num_compaction_trigger - 1) {
      ASSERT_EQ(NumSortedRuns(1), num + 1);
    }
  }

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(NumSortedRuns(1), 1);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionStopStyleSimilarSize) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
  options.compaction_options_universal.size_ratio = 10;
  options.compaction_options_universal.stop_style =
      kCompactionStopStyleSimilarSize;
  options.num_levels = num_levels_;
  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // Stage 1:
  //   Generate a set of files at level 0, but don't trigger level-0
  //   compaction.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumSortedRuns(), num + 1);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Suppose each file flushed from mem table has size 1. Now we compact
  // (level0_file_num_compaction_trigger+1)=4 files and should have a big
  // file of size 4.
  ASSERT_EQ(NumSortedRuns(), 1);

  // Stage 2:
  //   Now we have one file at level 0, with size 4. We also have some data in
  //   mem table. Let's continue generating new files at level 0, but don't
  //   trigger level-0 compaction.
  //   First, clean up memtable before inserting new data. This will generate
  //   a level-0 file, with size around 0.4 (according to previously written
  //   data amount).
  dbfull()->Flush(FlushOptions());
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 3;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumSortedRuns(), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 0.4, 1, 1.
  // After compaction, we should have 3 files, with size 4, 0.4, 2.
  ASSERT_EQ(NumSortedRuns(), 3);
  // Stage 3:
  //   Now we have 3 files at level 0, with size 4, 0.4, 2. Generate one
  //   more file at level-0, which should trigger level-0 compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Level-0 compaction is triggered, but no file will be picked up.
  ASSERT_EQ(NumSortedRuns(), 4);
}

TEST_F(DBTest, CompressedCache) {
  if (!Snappy_Supported()) {
    return;
  }
  int num_iter = 80;

  // Run this test three iterations.
  // Iteration 1: only a uncompressed block cache
  // Iteration 2: only a compressed block cache
  // Iteration 3: both block cache and compressed cache
  // Iteration 4: both block cache and compressed cache, but DB is not
  // compressed
  for (int iter = 0; iter < 4; iter++) {
    Options options;
    options.write_buffer_size = 64*1024;        // small write buffer
    options.statistics = rocksdb::CreateDBStatistics();
    options = CurrentOptions(options);

    BlockBasedTableOptions table_options;
    switch (iter) {
      case 0:
        // only uncompressed block cache
        table_options.block_cache = NewLRUCache(8*1024);
        table_options.block_cache_compressed = nullptr;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 1:
        // no block cache, only compressed cache
        table_options.no_block_cache = true;
        table_options.block_cache = nullptr;
        table_options.block_cache_compressed = NewLRUCache(8*1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 2:
        // both compressed and uncompressed block cache
        table_options.block_cache = NewLRUCache(1024);
        table_options.block_cache_compressed = NewLRUCache(8*1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 3:
        // both block cache and compressed cache, but DB is not compressed
        // also, make block cache sizes bigger, to trigger block cache hits
        table_options.block_cache = NewLRUCache(1024 * 1024);
        table_options.block_cache_compressed = NewLRUCache(8 * 1024 * 1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        options.compression = kNoCompression;
        break;
      default:
        ASSERT_TRUE(false);
    }
    CreateAndReopenWithCF({"pikachu"}, options);
    // default column family doesn't have block cache
    Options no_block_cache_opts;
    no_block_cache_opts.statistics = options.statistics;
    no_block_cache_opts = CurrentOptions(no_block_cache_opts);
    BlockBasedTableOptions table_options_no_bc;
    table_options_no_bc.no_block_cache = true;
    no_block_cache_opts.table_factory.reset(
        NewBlockBasedTableFactory(table_options_no_bc));
    ReopenWithColumnFamilies({"default", "pikachu"},
        std::vector<Options>({no_block_cache_opts, options}));

    Random rnd(301);

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    std::vector<std::string> values;
    std::string str;
    for (int i = 0; i < num_iter; i++) {
      if (i % 4 == 0) {        // high compression ratio
        str = RandomString(&rnd, 1000);
      }
      values.push_back(str);
      ASSERT_OK(Put(1, Key(i), values[i]));
    }

    // flush all data from memtable so that reads are from block cache
    ASSERT_OK(Flush(1));

    for (int i = 0; i < num_iter; i++) {
      ASSERT_EQ(Get(1, Key(i)), values[i]);
    }

    // check that we triggered the appropriate code paths in the cache
    switch (iter) {
      case 0:
        // only uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 1:
        // no block cache, only compressed cache
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 2:
        // both compressed and uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 3:
        // both compressed and uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_HIT), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        // compressed doesn't have any hits since blocks are not compressed on
        // storage
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_HIT), 0);
        break;
      default:
        ASSERT_TRUE(false);
    }

    options.create_if_missing = true;
    DestroyAndReopen(options);
  }
}

static std::string CompressibleString(Random* rnd, int len) {
  std::string r;
  test::CompressibleString(rnd, 0.8, len, &r);
  return r;
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionCompressRatio1) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = num_levels_;
  options.compaction_options_universal.compression_size_percent = 70;
  options = CurrentOptions(options);
  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // The first compaction (2) is compressed.
  for (int num = 0; num < 2; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 110000U * 2 * 0.9);

  // The second compaction (4) is compressed
  for (int num = 0; num < 2; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 110000 * 4 * 0.9);

  // The third compaction (2 4) is compressed since this time it is
  // (1 1 3.2) and 3.2/5.2 doesn't reach ratio.
  for (int num = 0; num < 2; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 110000 * 6 * 0.9);

  // When we start for the compaction up to (2 4 8), the latest
  // compressed is not compressed.
  for (int num = 0; num < 8; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_GT(TotalSize(), 110000 * 11 * 0.8 + 110000 * 2);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionCompressRatio2) {
  if (!Snappy_Supported()) {
    return;
  }
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = num_levels_;
  options.compaction_options_universal.compression_size_percent = 95;
  options = CurrentOptions(options);
  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // When we start for the compaction up to (2 4 8), the latest
  // compressed is compressed given the size ratio to compress.
  for (int num = 0; num < 14; num++) {
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 120000U * 12 * 0.8 + 120000 * 2);
}

INSTANTIATE_TEST_CASE_P(UniversalCompactionNumLevels, DBTestUniversalCompaction,
                        ::testing::Values(1, 3, 5));

TEST_F(DBTest, FailMoreDbPaths) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 10000000);
  options.db_paths.emplace_back(dbname_ + "_2", 1000000);
  options.db_paths.emplace_back(dbname_ + "_3", 1000000);
  options.db_paths.emplace_back(dbname_ + "_4", 1000000);
  options.db_paths.emplace_back(dbname_ + "_5", 1000000);
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

TEST_F(DBTest, UniversalCompactionSecondPathRatio) {
  if (!Snappy_Supported()) {
    return;
  }
  Options options;
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 1024 * 1024 * 1024);
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 1;
  options = CurrentOptions(options);

  std::vector<std::string> filenames;
  env_->GetChildren(options.db_paths[1].path, &filenames);
  // Delete archival files.
  for (size_t i = 0; i < filenames.size(); ++i) {
    env_->DeleteFile(options.db_paths[1].path + "/" + filenames[i]);
  }
  env_->DeleteDir(options.db_paths[1].path);
  Reopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are not going to second path.
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to second path
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1,1,4) -> (2, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 2, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(2, GetSstFileCount(dbname_));

  // (1, 1, 2, 4) -> (8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 1, 8) -> (2, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 2, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(2, GetSstFileCount(dbname_));

  // (1, 1, 2, 8) -> (4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Destroy(options);
}

TEST_F(DBTest, LevelCompactionThirdPath) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 1024 * 1024 * 1024);
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  //  options = CurrentOptions(options);

  std::vector<std::string> filenames;
  env_->GetChildren(options.db_paths[1].path, &filenames);
  // Delete archival files.
  for (size_t i = 0; i < filenames.size(); ++i) {
    env_->DeleteFile(options.db_paths[1].path + "/" + filenames[i]);
  }
  env_->DeleteDir(options.db_paths[1].path);
  Reopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are not going to second path.
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to fill up first path
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(3, GetSstFileCount(options.db_paths[1].path));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4", FilesPerLevel(0));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 1)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,1", FilesPerLevel(0));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 2)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,2", FilesPerLevel(0));
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 3)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,3", FilesPerLevel(0));
  ASSERT_EQ(3, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,4", FilesPerLevel(0));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 5)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,5", FilesPerLevel(0));
  ASSERT_EQ(5, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 6)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,6", FilesPerLevel(0));
  ASSERT_EQ(6, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 7)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,7", FilesPerLevel(0));
  ASSERT_EQ(7, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,8", FilesPerLevel(0));
  ASSERT_EQ(8, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Destroy(options);
}

TEST_F(DBTest, LevelCompactionPathUse) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 1024 * 1024 * 1024);
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  //  options = CurrentOptions(options);

  std::vector<std::string> filenames;
  env_->GetChildren(options.db_paths[1].path, &filenames);
  // Delete archival files.
  for (size_t i = 0; i < filenames.size(); ++i) {
    env_->DeleteFile(options.db_paths[1].path + "/" + filenames[i]);
  }
  env_->DeleteDir(options.db_paths[1].path);
  Reopen(options);

  Random rnd(301);
  int key_idx = 0;

  // Always gets compacted into 1 Level1 file,
  // 0/1 Level 0 file
  for (int num = 0; num < 3; num++) {
    key_idx = 0;
    GenerateNewFile(&rnd, &key_idx);
  }

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Destroy(options);
}

TEST_F(DBTest, UniversalCompactionFourPaths) {
  Options options;
  options.db_paths.emplace_back(dbname_, 300 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 300 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_4", 1024 * 1024 * 1024);
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 1;
  options = CurrentOptions(options);

  std::vector<std::string> filenames;
  env_->GetChildren(options.db_paths[1].path, &filenames);
  // Delete archival files.
  for (size_t i = 0; i < filenames.size(); ++i) {
    env_->DeleteFile(options.db_paths[1].path + "/" + filenames[i]);
  }
  env_->DeleteDir(options.db_paths[1].path);
  Reopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are not going to second path.
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to second path
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1,1,4) -> (2, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // (1, 2, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 1, 2, 4) -> (8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));

  // (1, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 1, 8) -> (2, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  // (1, 2, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 1, 2, 8) -> (4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));

  // (1, 4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[3].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Destroy(options);
}

void CheckColumnFamilyMeta(const ColumnFamilyMetaData& cf_meta) {
  uint64_t cf_size = 0;
  uint64_t cf_csize = 0;
  size_t file_count = 0;
  for (auto level_meta : cf_meta.levels) {
    uint64_t level_size = 0;
    uint64_t level_csize = 0;
    file_count += level_meta.files.size();
    for (auto file_meta : level_meta.files) {
      level_size += file_meta.size;
    }
    ASSERT_EQ(level_meta.size, level_size);
    cf_size += level_size;
    cf_csize += level_csize;
  }
  ASSERT_EQ(cf_meta.file_count, file_count);
  ASSERT_EQ(cf_meta.size, cf_size);
}

TEST_F(DBTest, ColumnFamilyMetaDataTest) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);

  Random rnd(301);
  int key_index = 0;
  ColumnFamilyMetaData cf_meta;
  for (int i = 0; i < 100; ++i) {
    GenerateNewFile(&rnd, &key_index);
    db_->GetColumnFamilyMetaData(&cf_meta);
    CheckColumnFamilyMeta(cf_meta);
  }
}

TEST_F(DBTest, ConvertCompactionStyle) {
  Random rnd(301);
  int max_key_level_insert = 200;
  int max_key_universal_insert = 600;

  // Stage 1: generate a db with level compaction
  Options options;
  options.write_buffer_size = 100<<10; //100KB
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.max_bytes_for_level_base = 500<<10; // 500KB
  options.max_bytes_for_level_multiplier = 1;
  options.target_file_size_base = 200<<10; // 200KB
  options.target_file_size_multiplier = 1;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  for (int i = 0; i <= max_key_level_insert; i++) {
    // each value is 10K
    ASSERT_OK(Put(1, Key(i), RandomString(&rnd, 10000)));
  }
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  ASSERT_GT(TotalTableFiles(1, 4), 1);
  int non_level0_num_files = 0;
  for (int i = 1; i < options.num_levels; i++) {
    non_level0_num_files += NumTableFilesAtLevel(i, 1);
  }
  ASSERT_GT(non_level0_num_files, 0);

  // Stage 2: reopen with universal compaction - should fail
  options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options = CurrentOptions(options);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Stage 3: compact into a single file and move the file to level 0
  options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = INT_MAX;
  options.target_file_size_multiplier = 1;
  options.max_bytes_for_level_base = INT_MAX;
  options.max_bytes_for_level_multiplier = 1;
  options.num_levels = 4;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  dbfull()->CompactRange(handles_[1], nullptr, nullptr, true /* reduce level */,
                         0 /* reduce to level 0 */);

  for (int i = 0; i < options.num_levels; i++) {
    int num = NumTableFilesAtLevel(i, 1);
    if (i == 0) {
      ASSERT_EQ(num, 1);
    } else {
      ASSERT_EQ(num, 0);
    }
  }

  // Stage 4: re-open in universal compaction style and do some db operations
  options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 4;
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 3;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  options.num_levels = 1;
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  for (int i = max_key_level_insert / 2; i <= max_key_universal_insert; i++) {
    ASSERT_OK(Put(1, Key(i), RandomString(&rnd, 10000)));
  }
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  for (int i = 1; i < options.num_levels; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i, 1), 0);
  }

  // verify keys inserted in both level compaction style and universal
  // compaction style
  std::string keys_in_db;
  Iterator* iter = dbfull()->NewIterator(ReadOptions(), handles_[1]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    keys_in_db.append(iter->key().ToString());
    keys_in_db.push_back(',');
  }
  delete iter;

  std::string expected_keys;
  for (int i = 0; i <= max_key_universal_insert; i++) {
    expected_keys.append(Key(i));
    expected_keys.push_back(',');
  }

  ASSERT_EQ(keys_in_db, expected_keys);
}

TEST_F(DBTest, IncreaseUniversalCompactionNumLevels) {
  std::function<void(int)> verify_func = [&](int num_keys_in_db) {
    std::string keys_in_db;
    Iterator* iter = dbfull()->NewIterator(ReadOptions(), handles_[1]);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys_in_db.append(iter->key().ToString());
      keys_in_db.push_back(',');
    }
    delete iter;

    std::string expected_keys;
    for (int i = 0; i <= num_keys_in_db; i++) {
      expected_keys.append(Key(i));
      expected_keys.push_back(',');
    }

    ASSERT_EQ(keys_in_db, expected_keys);
  };

  Random rnd(301);
  int max_key1 = 200;
  int max_key2 = 600;
  int max_key3 = 800;

  // Stage 1: open a DB with universal compaction, num_levels=1
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 3;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  for (int i = 0; i <= max_key1; i++) {
    // each value is 10K
    ASSERT_OK(Put(1, Key(i), RandomString(&rnd, 10000)));
  }
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  int non_level0_num_files = 0;
  for (int i = 1; i < options.num_levels; i++) {
    non_level0_num_files += NumTableFilesAtLevel(i, 1);
  }
  ASSERT_EQ(non_level0_num_files, 0);

  // Stage 2: reopen with universal compaction, num_levels=4
  options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 4;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  verify_func(max_key1);

  // Insert more keys
  for (int i = max_key1 + 1; i <= max_key2; i++) {
    // each value is 10K
    ASSERT_OK(Put(1, Key(i), RandomString(&rnd, 10000)));
  }
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  verify_func(max_key2);
  // Compaction to non-L0 has happened.
  ASSERT_GT(NumTableFilesAtLevel(options.num_levels - 1, 1), 0);

  // Stage 3: Revert it back to one level and revert to num_levels=1.
  options.num_levels = 4;
  options.target_file_size_base = INT_MAX;
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  // Compact all to level 0
  dbfull()->CompactRange(handles_[1], nullptr, nullptr, true /* reduce level */,
                         0 /* reduce to level 0 */);
  // Need to restart it once to remove higher level records in manifest.
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  // Final reopen
  options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  // Insert more keys
  for (int i = max_key2 + 1; i <= max_key3; i++) {
    // each value is 10K
    ASSERT_OK(Put(1, Key(i), RandomString(&rnd, 10000)));
  }
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();
  verify_func(max_key3);
}

namespace {
void MinLevelHelper(DBTest* self, Options& options) {
  Random rnd(301);

  for (int num = 0;
    num < options.level0_file_num_compaction_trigger - 1;
    num++)
  {
    std::vector<std::string> values;
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      values.push_back(RandomString(&rnd, 10000));
      ASSERT_OK(self->Put(Key(i), values[i]));
    }
    self->dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(self->NumTableFilesAtLevel(0), num + 1);
  }

  //generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < 12; i++) {
    values.push_back(RandomString(&rnd, 10000));
    ASSERT_OK(self->Put(Key(i), values[i]));
  }
  self->dbfull()->TEST_WaitForCompact();

  ASSERT_EQ(self->NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(self->NumTableFilesAtLevel(1), 1);
}

// returns false if the calling-Test should be skipped
bool MinLevelToCompress(CompressionType& type, Options& options, int wbits,
                        int lev, int strategy) {
  fprintf(stderr, "Test with compression options : window_bits = %d, level =  %d, strategy = %d}\n", wbits, lev, strategy);
  options.write_buffer_size = 100<<10; //100KB
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.level0_file_num_compaction_trigger = 3;
  options.create_if_missing = true;

  if (Snappy_Supported()) {
    type = kSnappyCompression;
    fprintf(stderr, "using snappy\n");
  } else if (Zlib_Supported()) {
    type = kZlibCompression;
    fprintf(stderr, "using zlib\n");
  } else if (BZip2_Supported()) {
    type = kBZip2Compression;
    fprintf(stderr, "using bzip2\n");
  } else if (LZ4_Supported()) {
    type = kLZ4Compression;
    fprintf(stderr, "using lz4\n");
  } else {
    fprintf(stderr, "skipping test, compression disabled\n");
    return false;
  }
  options.compression_per_level.resize(options.num_levels);

  // do not compress L0
  for (int i = 0; i < 1; i++) {
    options.compression_per_level[i] = kNoCompression;
  }
  for (int i = 1; i < options.num_levels; i++) {
    options.compression_per_level[i] = type;
  }
  return true;
}
}  // namespace

TEST_F(DBTest, MinLevelToCompress1) {
  Options options = CurrentOptions();
  CompressionType type = kSnappyCompression;
  if (!MinLevelToCompress(type, options, -14, -1, 0)) {
    return;
  }
  Reopen(options);
  MinLevelHelper(this, options);

  // do not compress L0 and L1
  for (int i = 0; i < 2; i++) {
    options.compression_per_level[i] = kNoCompression;
  }
  for (int i = 2; i < options.num_levels; i++) {
    options.compression_per_level[i] = type;
  }
  DestroyAndReopen(options);
  MinLevelHelper(this, options);
}

TEST_F(DBTest, MinLevelToCompress2) {
  Options options = CurrentOptions();
  CompressionType type = kSnappyCompression;
  if (!MinLevelToCompress(type, options, 15, -1, 0)) {
    return;
  }
  Reopen(options);
  MinLevelHelper(this, options);

  // do not compress L0 and L1
  for (int i = 0; i < 2; i++) {
    options.compression_per_level[i] = kNoCompression;
  }
  for (int i = 2; i < options.num_levels; i++) {
    options.compression_per_level[i] = type;
  }
  DestroyAndReopen(options);
  MinLevelHelper(this, options);
}

TEST_F(DBTest, RepeatedWritesToSameKey) {
  do {
    Options options;
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // We must have at most one file per level except for level-0,
    // which may have up to kL0_StopWritesTrigger files.
    const int kMaxFiles =
        options.num_levels + options.level0_stop_writes_trigger;

    Random rnd(301);
    std::string value =
        RandomString(&rnd, static_cast<int>(2 * options.write_buffer_size));
    for (int i = 0; i < 5 * kMaxFiles; i++) {
      ASSERT_OK(Put(1, "key", value));
      ASSERT_LE(TotalTableFiles(1), kMaxFiles);
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, InPlaceUpdate) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller size
    int numValues = 10;
    for (int i = numValues; i > 0; i--) {
      std::string value = DummyString(i, 'a');
      ASSERT_OK(Put(1, "key", value));
      ASSERT_EQ(value, Get(1, "key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);

  } while (ChangeCompactOptions());
}

TEST_F(DBTest, InPlaceUpdateLargeNewValue) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of larger size
    int numValues = 10;
    for (int i = 0; i < numValues; i++) {
      std::string value = DummyString(i, 'a');
      ASSERT_OK(Put(1, "key", value));
      ASSERT_EQ(value, Get(1, "key"));
    }

    // All 10 updates exist in the internal iterator
    validateNumberOfEntries(numValues, 1);

  } while (ChangeCompactOptions());
}

TEST_F(DBTest, InPlaceUpdateCallbackSmallerSize) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceSmallerSize;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller size
    int numValues = 10;
    ASSERT_OK(Put(1, "key", DummyString(numValues, 'a')));
    ASSERT_EQ(DummyString(numValues, 'c'), Get(1, "key"));

    for (int i = numValues; i > 0; i--) {
      ASSERT_OK(Put(1, "key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(i - 1, 'b'), Get(1, "key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);

  } while (ChangeCompactOptions());
}

TEST_F(DBTest, InPlaceUpdateCallbackSmallerVarintSize) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceSmallerVarintSize;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of smaller varint size
    int numValues = 265;
    ASSERT_OK(Put(1, "key", DummyString(numValues, 'a')));
    ASSERT_EQ(DummyString(numValues, 'c'), Get(1, "key"));

    for (int i = numValues; i > 0; i--) {
      ASSERT_OK(Put(1, "key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(1, 'b'), Get(1, "key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1, 1);

  } while (ChangeCompactOptions());
}

TEST_F(DBTest, InPlaceUpdateCallbackLargeNewValue) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceLargerSize;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Update key with values of larger size
    int numValues = 10;
    for (int i = 0; i < numValues; i++) {
      ASSERT_OK(Put(1, "key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(i, 'c'), Get(1, "key"));
    }

    // No inplace updates. All updates are puts with new seq number
    // All 10 updates exist in the internal iterator
    validateNumberOfEntries(numValues, 1);

  } while (ChangeCompactOptions());
}

TEST_F(DBTest, InPlaceUpdateCallbackNoAction) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceNoAction;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Callback function requests no actions from db
    ASSERT_OK(Put(1, "key", DummyString(1, 'a')));
    ASSERT_EQ(Get(1, "key"), "NOT_FOUND");

  } while (ChangeCompactOptions());
}

TEST_F(DBTest, CompactionFilter) {
  Options options = CurrentOptions();
  options.max_open_files = -1;
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.compaction_filter_factory = std::make_shared<KeepFilterFactory>();
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Write 100K keys, these are written to a few files in L0.
  const std::string value(10, 'x');
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    Put(1, key, value);
  }
  ASSERT_OK(Flush(1));

  // Push all files to the highest level L2. Verify that
  // the compaction is each level invokes the filter for
  // all the keys in that level.
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);
  ASSERT_EQ(cfilter_count, 100000);
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]);
  ASSERT_EQ(cfilter_count, 100000);

  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);
  ASSERT_NE(NumTableFilesAtLevel(2, 1), 0);
  cfilter_count = 0;

  // All the files are in the lowest level.
  // Verify that all but the 100001st record
  // has sequence number zero. The 100001st record
  // is at the tip of this snapshot and cannot
  // be zeroed out.
  // TODO: figure out sequence number squashtoo
  int count = 0;
  int total = 0;
  Arena arena;
  {
    ScopedArenaIterator iter(
        dbfull()->TEST_NewInternalIterator(&arena, handles_[1]));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    while (iter->Valid()) {
      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      ikey.sequence = -1;
      ASSERT_EQ(ParseInternalKey(iter->key(), &ikey), true);
      total++;
      if (ikey.sequence != 0) {
        count++;
      }
      iter->Next();
    }
  }
  ASSERT_EQ(total, 100000);
  ASSERT_EQ(count, 1);

  // overwrite all the 100K keys once again.
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    ASSERT_OK(Put(1, key, value));
  }
  ASSERT_OK(Flush(1));

  // push all files to the highest level L2. This
  // means that all keys should pass at least once
  // via the compaction filter
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);
  ASSERT_EQ(cfilter_count, 100000);
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]);
  ASSERT_EQ(cfilter_count, 100000);
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);
  ASSERT_NE(NumTableFilesAtLevel(2, 1), 0);

  // create a new database with the compaction
  // filter in such a way that it deletes all keys
  options.compaction_filter_factory = std::make_shared<DeleteFilterFactory>();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // write all the keys once again.
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    ASSERT_OK(Put(1, key, value));
  }
  ASSERT_OK(Flush(1));
  ASSERT_NE(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2, 1), 0);

  // Push all files to the highest level L2. This
  // triggers the compaction filter to delete all keys,
  // verify that at the end of the compaction process,
  // nothing is left.
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);
  ASSERT_EQ(cfilter_count, 100000);
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]);
  ASSERT_EQ(cfilter_count, 0);
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);

  {
    // Scan the entire database to ensure that nothing is left
    std::unique_ptr<Iterator> iter(
        db_->NewIterator(ReadOptions(), handles_[1]));
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
      count++;
      iter->Next();
    }
    ASSERT_EQ(count, 0);
  }

  // The sequence number of the remaining record
  // is not zeroed out even though it is at the
  // level Lmax because this record is at the tip
  // TODO: remove the following or design a different
  // test
  count = 0;
  {
    ScopedArenaIterator iter(
        dbfull()->TEST_NewInternalIterator(&arena, handles_[1]));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    while (iter->Valid()) {
      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      ASSERT_EQ(ParseInternalKey(iter->key(), &ikey), true);
      ASSERT_NE(ikey.sequence, (unsigned)0);
      count++;
      iter->Next();
    }
    ASSERT_EQ(count, 0);
  }
}

// Tests the edge case where compaction does not produce any output -- all
// entries are deleted. The compaction should create bunch of 'DeleteFile'
// entries in VersionEdit, but none of the 'AddFile's.
TEST_F(DBTest, CompactionFilterDeletesAll) {
  Options options;
  options.compaction_filter_factory = std::make_shared<DeleteFilterFactory>();
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  options = CurrentOptions(options);
  DestroyAndReopen(options);

  // put some data
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      Put(ToString(table * 100 + i), "val");
    }
    Flush();
  }

  // this will produce empty file (delete compaction filter)
  ASSERT_OK(db_->CompactRange(nullptr, nullptr));
  ASSERT_EQ(0U, CountLiveFiles());

  Reopen(options);

  Iterator* itr = db_->NewIterator(ReadOptions());
  itr->SeekToFirst();
  // empty db
  ASSERT_TRUE(!itr->Valid());

  delete itr;
}

TEST_F(DBTest, CompactionFilterWithValueChange) {
  do {
    Options options;
    options.num_levels = 3;
    options.max_mem_compaction_level = 0;
    options.compaction_filter_factory =
      std::make_shared<ChangeFilterFactory>();
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Write 100K+1 keys, these are written to a few files
    // in L0. We do this so that the current snapshot points
    // to the 100001 key.The compaction filter is  not invoked
    // on keys that are visible via a snapshot because we
    // anyways cannot delete it.
    const std::string value(10, 'x');
    for (int i = 0; i < 100001; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%010d", i);
      Put(1, key, value);
    }

    // push all files to  lower levels
    ASSERT_OK(Flush(1));
    dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);
    dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]);

    // re-write all data again
    for (int i = 0; i < 100001; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%010d", i);
      Put(1, key, value);
    }

    // push all files to  lower levels. This should
    // invoke the compaction filter for all 100000 keys.
    ASSERT_OK(Flush(1));
    dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);
    dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]);

    // verify that all keys now have the new value that
    // was set by the compaction process.
    for (int i = 0; i < 100001; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%010d", i);
      std::string newvalue = Get(1, key);
      ASSERT_EQ(newvalue.compare(NEW_VALUE), 0);
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, CompactionFilterWithMergeOperator) {
  std::string one, two, three, four;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  PutFixed64(&four, 4);

  Options options;
  options = CurrentOptions(options);
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  // Filter out keys with value is 2.
  options.compaction_filter_factory =
      std::make_shared<ConditionalFilterFactory>(two);
  DestroyAndReopen(options);

  // In the same compaction, a value type needs to be deleted based on
  // compaction filter, and there is a merge type for the key. compaction
  // filter result is ignored.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", two));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "foo", one));
  ASSERT_OK(Flush());
  std::string newvalue = Get("foo");
  ASSERT_EQ(newvalue, three);
  dbfull()->CompactRange(nullptr, nullptr);
  newvalue = Get("foo");
  ASSERT_EQ(newvalue, three);

  // value key can be deleted based on compaction filter, leaving only
  // merge keys.
  ASSERT_OK(db_->Put(WriteOptions(), "bar", two));
  ASSERT_OK(Flush());
  dbfull()->CompactRange(nullptr, nullptr);
  newvalue = Get("bar");
  ASSERT_EQ("NOT_FOUND", newvalue);
  ASSERT_OK(db_->Merge(WriteOptions(), "bar", two));
  ASSERT_OK(Flush());
  dbfull()->CompactRange(nullptr, nullptr);
  newvalue = Get("bar");
  ASSERT_EQ(two, two);

  // Compaction filter never applies to merge keys.
  ASSERT_OK(db_->Put(WriteOptions(), "foobar", one));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "foobar", two));
  ASSERT_OK(Flush());
  newvalue = Get("foobar");
  ASSERT_EQ(newvalue, three);
  dbfull()->CompactRange(nullptr, nullptr);
  newvalue = Get("foobar");
  ASSERT_EQ(newvalue, three);

  // In the same compaction, both of value type and merge type keys need to be
  // deleted based on compaction filter, and there is a merge type for the key.
  // For both keys, compaction filter results are ignored.
  ASSERT_OK(db_->Put(WriteOptions(), "barfoo", two));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "barfoo", two));
  ASSERT_OK(Flush());
  newvalue = Get("barfoo");
  ASSERT_EQ(newvalue, four);
  dbfull()->CompactRange(nullptr, nullptr);
  newvalue = Get("barfoo");
  ASSERT_EQ(newvalue, four);
}

TEST_F(DBTest, CompactionFilterContextManual) {
  KeepFilterFactory* filter = new KeepFilterFactory();

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_filter_factory.reset(filter);
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 8;
  Reopen(options);
  int num_keys_per_file = 400;
  for (int j = 0; j < 3; j++) {
    // Write several keys.
    const std::string value(10, 'x');
    for (int i = 0; i < num_keys_per_file; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%08d%02d", i, j);
      Put(key, value);
    }
    dbfull()->TEST_FlushMemTable();
    // Make sure next file is much smaller so automatic compaction will not
    // be triggered.
    num_keys_per_file /= 2;
  }

  // Force a manual compaction
  cfilter_count = 0;
  filter->expect_manual_compaction_.store(true);
  filter->expect_full_compaction_.store(false);  // Manual compaction always
                                                 // set this flag.
  dbfull()->CompactRange(nullptr, nullptr);
  ASSERT_EQ(cfilter_count, 700);
  ASSERT_EQ(NumSortedRuns(0), 1);

  // Verify total number of keys is correct after manual compaction.
  {
    int count = 0;
    int total = 0;
    Arena arena;
    ScopedArenaIterator iter(dbfull()->TEST_NewInternalIterator(&arena));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    while (iter->Valid()) {
      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      ikey.sequence = -1;
      ASSERT_EQ(ParseInternalKey(iter->key(), &ikey), true);
      total++;
      if (ikey.sequence != 0) {
        count++;
      }
      iter->Next();
    }
    ASSERT_EQ(total, 700);
    ASSERT_EQ(count, 1);
  }
}

class KeepFilterV2 : public CompactionFilterV2 {
 public:
  virtual std::vector<bool> Filter(int level,
                                   const SliceVector& keys,
                                   const SliceVector& existing_values,
                                   std::vector<std::string>* new_values,
                                   std::vector<bool>* values_changed)
    const override {
    cfilter_count++;
    std::vector<bool> ret;
    new_values->clear();
    values_changed->clear();
    for (unsigned int i = 0; i < keys.size(); ++i) {
      values_changed->push_back(false);
      ret.push_back(false);
    }
    return ret;
  }

  virtual const char* Name() const override {
    return "KeepFilterV2";
  }
};

class DeleteFilterV2 : public CompactionFilterV2 {
 public:
  virtual std::vector<bool> Filter(int level,
                                   const SliceVector& keys,
                                   const SliceVector& existing_values,
                                   std::vector<std::string>* new_values,
                                   std::vector<bool>* values_changed)
    const override {
    cfilter_count++;
    new_values->clear();
    values_changed->clear();
    std::vector<bool> ret;
    for (unsigned int i = 0; i < keys.size(); ++i) {
      values_changed->push_back(false);
      ret.push_back(true);
    }
    return ret;
  }

  virtual const char* Name() const override {
    return "DeleteFilterV2";
  }
};

class ChangeFilterV2 : public CompactionFilterV2 {
 public:
  virtual std::vector<bool> Filter(int level,
                                   const SliceVector& keys,
                                   const SliceVector& existing_values,
                                   std::vector<std::string>* new_values,
                                   std::vector<bool>* values_changed)
    const override {
    std::vector<bool> ret;
    new_values->clear();
    values_changed->clear();
    for (unsigned int i = 0; i < keys.size(); ++i) {
      values_changed->push_back(true);
      new_values->push_back(NEW_VALUE);
      ret.push_back(false);
    }
    return ret;
  }

  virtual const char* Name() const override {
    return "ChangeFilterV2";
  }
};

class KeepFilterFactoryV2 : public CompactionFilterFactoryV2 {
 public:
  explicit KeepFilterFactoryV2(const SliceTransform* prefix_extractor)
    : CompactionFilterFactoryV2(prefix_extractor) { }

  virtual std::unique_ptr<CompactionFilterV2>
  CreateCompactionFilterV2(
      const CompactionFilterContext& context) override {
    return std::unique_ptr<CompactionFilterV2>(new KeepFilterV2());
  }

  virtual const char* Name() const override {
    return "KeepFilterFactoryV2";
  }
};

class DeleteFilterFactoryV2 : public CompactionFilterFactoryV2 {
 public:
  explicit DeleteFilterFactoryV2(const SliceTransform* prefix_extractor)
    : CompactionFilterFactoryV2(prefix_extractor) { }

  virtual std::unique_ptr<CompactionFilterV2>
  CreateCompactionFilterV2(
      const CompactionFilterContext& context) override {
    return std::unique_ptr<CompactionFilterV2>(new DeleteFilterV2());
  }

  virtual const char* Name() const override {
    return "DeleteFilterFactoryV2";
  }
};

class ChangeFilterFactoryV2 : public CompactionFilterFactoryV2 {
 public:
  explicit ChangeFilterFactoryV2(const SliceTransform* prefix_extractor)
    : CompactionFilterFactoryV2(prefix_extractor) { }

  virtual std::unique_ptr<CompactionFilterV2>
  CreateCompactionFilterV2(
      const CompactionFilterContext& context) override {
    return std::unique_ptr<CompactionFilterV2>(new ChangeFilterV2());
  }

  virtual const char* Name() const override {
    return "ChangeFilterFactoryV2";
  }
};

TEST_F(DBTest, CompactionFilterV2) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  // extract prefix
  std::unique_ptr<const SliceTransform> prefix_extractor;
  prefix_extractor.reset(NewFixedPrefixTransform(8));

  options.compaction_filter_factory_v2
    = std::make_shared<KeepFilterFactoryV2>(prefix_extractor.get());
  // In a testing environment, we can only flush the application
  // compaction filter buffer using universal compaction
  option_config_ = kUniversalCompaction;
  options.compaction_style = (rocksdb::CompactionStyle)1;
  Reopen(options);

  // Write 100K keys, these are written to a few files in L0.
  const std::string value(10, 'x');
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%08d%010d", i , i);
    Put(key, value);
  }

  dbfull()->TEST_FlushMemTable();

  dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  dbfull()->TEST_CompactRange(1, nullptr, nullptr);

  ASSERT_EQ(NumSortedRuns(0), 1);

  // All the files are in the lowest level.
  int count = 0;
  int total = 0;
  {
    Arena arena;
    ScopedArenaIterator iter(dbfull()->TEST_NewInternalIterator(&arena));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    while (iter->Valid()) {
      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      ikey.sequence = -1;
      ASSERT_EQ(ParseInternalKey(iter->key(), &ikey), true);
      total++;
      if (ikey.sequence != 0) {
        count++;
      }
      iter->Next();
    }
  }

  ASSERT_EQ(total, 100000);
  // 1 snapshot only. Since we are using universal compacton,
  // the sequence no is cleared for better compression
  ASSERT_EQ(count, 1);

  // create a new database with the compaction
  // filter in such a way that it deletes all keys
  options.compaction_filter_factory_v2 =
    std::make_shared<DeleteFilterFactoryV2>(prefix_extractor.get());
  options.create_if_missing = true;
  DestroyAndReopen(options);

  // write all the keys once again.
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%08d%010d", i, i);
    Put(key, value);
  }

  dbfull()->TEST_FlushMemTable();
  ASSERT_NE(NumTableFilesAtLevel(0), 0);

  dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  dbfull()->TEST_CompactRange(1, nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);

  // Scan the entire database to ensure that nothing is left
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->SeekToFirst();
  count = 0;
  while (iter->Valid()) {
    count++;
    iter->Next();
  }

  ASSERT_EQ(count, 0);
  delete iter;
}

TEST_F(DBTest, CompactionFilterV2WithValueChange) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  std::unique_ptr<const SliceTransform> prefix_extractor;
  prefix_extractor.reset(NewFixedPrefixTransform(8));
  options.compaction_filter_factory_v2 =
    std::make_shared<ChangeFilterFactoryV2>(prefix_extractor.get());
  // In a testing environment, we can only flush the application
  // compaction filter buffer using universal compaction
  option_config_ = kUniversalCompaction;
  options.compaction_style = (rocksdb::CompactionStyle)1;
  options = CurrentOptions(options);
  Reopen(options);

  // Write 100K+1 keys, these are written to a few files
  // in L0. We do this so that the current snapshot points
  // to the 100001 key.The compaction filter is  not invoked
  // on keys that are visible via a snapshot because we
  // anyways cannot delete it.
  const std::string value(10, 'x');
  for (int i = 0; i < 100001; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%08d%010d", i, i);
    Put(key, value);
  }

  // push all files to lower levels
  dbfull()->TEST_FlushMemTable();
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  dbfull()->TEST_CompactRange(1, nullptr, nullptr);

  // verify that all keys now have the new value that
  // was set by the compaction process.
  for (int i = 0; i < 100001; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%08d%010d", i, i);
    std::string newvalue = Get(key);
    ASSERT_EQ(newvalue.compare(NEW_VALUE), 0);
  }
}

TEST_F(DBTest, CompactionFilterV2NULLPrefix) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  std::unique_ptr<const SliceTransform> prefix_extractor;
  prefix_extractor.reset(NewFixedPrefixTransform(8));
  options.compaction_filter_factory_v2 =
    std::make_shared<ChangeFilterFactoryV2>(prefix_extractor.get());
  // In a testing environment, we can only flush the application
  // compaction filter buffer using universal compaction
  option_config_ = kUniversalCompaction;
  options.compaction_style = (rocksdb::CompactionStyle)1;
  Reopen(options);

  // Write 100K+1 keys, these are written to a few files
  // in L0. We do this so that the current snapshot points
  // to the 100001 key.The compaction filter is  not invoked
  // on keys that are visible via a snapshot because we
  // anyways cannot delete it.
  const std::string value(10, 'x');
  char first_key[100];
  snprintf(first_key, sizeof(first_key), "%s0000%010d", "NULL", 1);
  Put(first_key, value);
  for (int i = 1; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "%08d%010d", i, i);
    Put(key, value);
  }

  char last_key[100];
  snprintf(last_key, sizeof(last_key), "%s0000%010d", "NULL", 2);
  Put(last_key, value);

  // push all files to lower levels
  dbfull()->TEST_FlushMemTable();
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);

  // verify that all keys now have the new value that
  // was set by the compaction process.
  std::string newvalue = Get(first_key);
  ASSERT_EQ(newvalue.compare(NEW_VALUE), 0);
  newvalue = Get(last_key);
  ASSERT_EQ(newvalue.compare(NEW_VALUE), 0);
  for (int i = 1; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "%08d%010d", i, i);
    newvalue = Get(key);
    ASSERT_EQ(newvalue.compare(NEW_VALUE), 0);
  }
}

TEST_F(DBTest, SparseMerge) {
  do {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    CreateAndReopenWithCF({"pikachu"}, options);

    FillLevels("A", "Z", 1);

    // Suppose there is:
    //    small amount of data with prefix A
    //    large amount of data with prefix B
    //    small amount of data with prefix C
    // and that recent updates have made small changes to all three prefixes.
    // Check that we do not do a compaction that merges all of B in one shot.
    const std::string value(1000, 'x');
    Put(1, "A", "va");
    // Write approximately 100MB of "B" values
    for (int i = 0; i < 100000; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%010d", i);
      Put(1, key, value);
    }
    Put(1, "C", "vc");
    ASSERT_OK(Flush(1));
    dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);

    // Make sparse update
    Put(1, "A", "va2");
    Put(1, "B100", "bvalue2");
    Put(1, "C", "vc2");
    ASSERT_OK(Flush(1));

    // Compactions should not cause us to create a situation where
    // a file overlaps too much data at the next level.
    ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(handles_[1]),
              20 * 1048576);
    dbfull()->TEST_CompactRange(0, nullptr, nullptr);
    ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(handles_[1]),
              20 * 1048576);
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);
    ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(handles_[1]),
              20 * 1048576);
  } while (ChangeCompactOptions());
}

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
            (unsigned long long)(val),
            (unsigned long long)(low),
            (unsigned long long)(high));
  }
  return result;
}

TEST_F(DBTest, ApproximateSizes) {
  do {
    Options options;
    options.write_buffer_size = 100000000;        // Large write buffer
    options.compression = kNoCompression;
    options.create_if_missing = true;
    options = CurrentOptions(options);
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_TRUE(Between(Size("", "xyz", 1), 0, 0));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_TRUE(Between(Size("", "xyz", 1), 0, 0));

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    const int N = 80;
    static const int S1 = 100000;
    static const int S2 = 105000;  // Allow some expansion from metadata
    Random rnd(301);
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(1, Key(i), RandomString(&rnd, S1)));
    }

    // 0 because GetApproximateSizes() does not account for memtable space
    ASSERT_TRUE(Between(Size("", Key(50), 1), 0, 0));

    // Check sizes across recovery by reopening a few times
    for (int run = 0; run < 3; run++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, options);

      for (int compact_start = 0; compact_start < N; compact_start += 10) {
        for (int i = 0; i < N; i += 10) {
          ASSERT_TRUE(Between(Size("", Key(i), 1), S1 * i, S2 * i));
          ASSERT_TRUE(Between(Size("", Key(i) + ".suffix", 1), S1 * (i + 1),
                              S2 * (i + 1)));
          ASSERT_TRUE(Between(Size(Key(i), Key(i + 10), 1), S1 * 10, S2 * 10));
        }
        ASSERT_TRUE(Between(Size("", Key(50), 1), S1 * 50, S2 * 50));
        ASSERT_TRUE(
            Between(Size("", Key(50) + ".suffix", 1), S1 * 50, S2 * 50));

        std::string cstart_str = Key(compact_start);
        std::string cend_str = Key(compact_start + 9);
        Slice cstart = cstart_str;
        Slice cend = cend_str;
        dbfull()->TEST_CompactRange(0, &cstart, &cend, handles_[1]);
      }

      ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
      ASSERT_GT(NumTableFilesAtLevel(1, 1), 0);
    }
    // ApproximateOffsetOf() is not yet implemented in plain table format.
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipFIFOCompaction |
                         kSkipPlainTable | kSkipHashIndex));
}

TEST_F(DBTest, ApproximateSizes_MixOfSmallAndLarge) {
  do {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    CreateAndReopenWithCF({"pikachu"}, options);

    Random rnd(301);
    std::string big1 = RandomString(&rnd, 100000);
    ASSERT_OK(Put(1, Key(0), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(1, Key(1), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(1, Key(2), big1));
    ASSERT_OK(Put(1, Key(3), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(1, Key(4), big1));
    ASSERT_OK(Put(1, Key(5), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(1, Key(6), RandomString(&rnd, 300000)));
    ASSERT_OK(Put(1, Key(7), RandomString(&rnd, 10000)));

    // Check sizes across recovery by reopening a few times
    for (int run = 0; run < 3; run++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, options);

      ASSERT_TRUE(Between(Size("", Key(0), 1), 0, 0));
      ASSERT_TRUE(Between(Size("", Key(1), 1), 10000, 11000));
      ASSERT_TRUE(Between(Size("", Key(2), 1), 20000, 21000));
      ASSERT_TRUE(Between(Size("", Key(3), 1), 120000, 121000));
      ASSERT_TRUE(Between(Size("", Key(4), 1), 130000, 131000));
      ASSERT_TRUE(Between(Size("", Key(5), 1), 230000, 231000));
      ASSERT_TRUE(Between(Size("", Key(6), 1), 240000, 241000));
      ASSERT_TRUE(Between(Size("", Key(7), 1), 540000, 541000));
      ASSERT_TRUE(Between(Size("", Key(8), 1), 550000, 560000));

      ASSERT_TRUE(Between(Size(Key(3), Key(5), 1), 110000, 111000));

      dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);
    }
    // ApproximateOffsetOf() is not yet implemented in plain table format.
  } while (ChangeOptions(kSkipPlainTable));
}

TEST_F(DBTest, IteratorPinsRef) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Put(1, "foo", "hello");

    // Get iterator that will yield the current contents of the DB.
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);

    // Write to force compactions
    Put(1, "foo", "newvalue1");
    for (int i = 0; i < 100; i++) {
      // 100K values
      ASSERT_OK(Put(1, Key(i), Key(i) + std::string(100000, 'v')));
    }
    Put(1, "foo", "newvalue2");

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ("hello", iter->value().ToString());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, Snapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    Put(0, "foo", "0v1");
    Put(1, "foo", "1v1");

    const Snapshot* s1 = db_->GetSnapshot();
    ASSERT_EQ(1U, GetNumSnapshots());
    uint64_t time_snap1 = GetTimeOldestSnapshots();
    ASSERT_GT(time_snap1, 0U);
    Put(0, "foo", "0v2");
    Put(1, "foo", "1v2");

    env_->addon_time_++;

    const Snapshot* s2 = db_->GetSnapshot();
    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    Put(0, "foo", "0v3");
    Put(1, "foo", "1v3");

    const Snapshot* s3 = db_->GetSnapshot();
    ASSERT_EQ(3U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());

    Put(0, "foo", "0v4");
    Put(1, "foo", "1v4");
    ASSERT_EQ("0v1", Get(0, "foo", s1));
    ASSERT_EQ("1v1", Get(1, "foo", s1));
    ASSERT_EQ("0v2", Get(0, "foo", s2));
    ASSERT_EQ("1v2", Get(1, "foo", s2));
    ASSERT_EQ("0v3", Get(0, "foo", s3));
    ASSERT_EQ("1v3", Get(1, "foo", s3));
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));

    db_->ReleaseSnapshot(s3);
    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ("0v1", Get(0, "foo", s1));
    ASSERT_EQ("1v1", Get(1, "foo", s1));
    ASSERT_EQ("0v2", Get(0, "foo", s2));
    ASSERT_EQ("1v2", Get(1, "foo", s2));
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));

    db_->ReleaseSnapshot(s1);
    ASSERT_EQ("0v2", Get(0, "foo", s2));
    ASSERT_EQ("1v2", Get(1, "foo", s2));
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));
    ASSERT_EQ(1U, GetNumSnapshots());
    ASSERT_LT(time_snap1, GetTimeOldestSnapshots());

    db_->ReleaseSnapshot(s2);
    ASSERT_EQ(0U, GetNumSnapshots());
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));
  } while (ChangeOptions(kSkipHashCuckoo));
}

TEST_F(DBTest, HiddenValuesAreRemoved) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.max_background_flushes = 0;
    CreateAndReopenWithCF({"pikachu"}, options);
    Random rnd(301);
    FillLevels("a", "z", 1);

    std::string big = RandomString(&rnd, 50000);
    Put(1, "foo", big);
    Put(1, "pastfoo", "v");
    const Snapshot* snapshot = db_->GetSnapshot();
    Put(1, "foo", "tiny");
    Put(1, "pastfoo2", "v2");  // Advance sequence number one more

    ASSERT_OK(Flush(1));
    ASSERT_GT(NumTableFilesAtLevel(0, 1), 0);

    ASSERT_EQ(big, Get(1, "foo", snapshot));
    ASSERT_TRUE(Between(Size("", "pastfoo", 1), 50000, 60000));
    db_->ReleaseSnapshot(snapshot);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ tiny, " + big + " ]");
    Slice x("x");
    dbfull()->TEST_CompactRange(0, nullptr, &x, handles_[1]);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ tiny ]");
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    ASSERT_GE(NumTableFilesAtLevel(1, 1), 1);
    dbfull()->TEST_CompactRange(1, nullptr, &x, handles_[1]);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ tiny ]");

    ASSERT_TRUE(Between(Size("", "pastfoo", 1), 0, 1000));
    // ApproximateOffsetOf() is not yet implemented in plain table format,
    // which is used by Size().
    // skip HashCuckooRep as it does not support snapshot
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipFIFOCompaction |
                         kSkipPlainTable | kSkipHashCuckoo));
}

TEST_F(DBTest, CompactBetweenSnapshots) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);
    Random rnd(301);
    FillLevels("a", "z", 1);

    Put(1, "foo", "first");
    const Snapshot* snapshot1 = db_->GetSnapshot();
    Put(1, "foo", "second");
    Put(1, "foo", "third");
    Put(1, "foo", "fourth");
    const Snapshot* snapshot2 = db_->GetSnapshot();
    Put(1, "foo", "fifth");
    Put(1, "foo", "sixth");

    // All entries (including duplicates) exist
    // before any compaction is triggered.
    ASSERT_OK(Flush(1));
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
    ASSERT_EQ("first", Get(1, "foo", snapshot1));
    ASSERT_EQ(AllEntriesFor("foo", 1),
              "[ sixth, fifth, fourth, third, second, first ]");

    // After a compaction, "second", "third" and "fifth" should
    // be removed
    FillLevels("a", "z", 1);
    dbfull()->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
    ASSERT_EQ("first", Get(1, "foo", snapshot1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth, first ]");

    // after we release the snapshot1, only two values left
    db_->ReleaseSnapshot(snapshot1);
    FillLevels("a", "z", 1);
    dbfull()->CompactRange(handles_[1], nullptr, nullptr);

    // We have only one valid snapshot snapshot2. Since snapshot1 is
    // not valid anymore, "first" should be removed by a compaction.
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth ]");

    // after we release the snapshot2, only one value should be left
    db_->ReleaseSnapshot(snapshot2);
    FillLevels("a", "z", 1);
    dbfull()->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth ]");
    // skip HashCuckooRep as it does not support snapshot
  } while (ChangeOptions(kSkipHashCuckoo | kSkipFIFOCompaction));
}

TEST_F(DBTest, DeletionMarkers1) {
  Options options = CurrentOptions();
  options.max_background_flushes = 0;
  CreateAndReopenWithCF({"pikachu"}, options);
  Put(1, "foo", "v1");
  ASSERT_OK(Flush(1));
  const int last = CurrentOptions().max_mem_compaction_level;
  // foo => v1 is now in last level
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put(1, "a", "begin");
  Put(1, "z", "end");
  Flush(1);
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last - 1, 1), 1);

  Delete(1, "foo");
  Put(1, "foo", "v2");
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, DEL, v1 ]");
  ASSERT_OK(Flush(1));  // Moves to level last-2
  if (CurrentOptions().purge_redundant_kvs_while_flush) {
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");
  } else {
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, DEL, v1 ]");
  }
  Slice z("z");
  dbfull()->TEST_CompactRange(last - 2, nullptr, &z, handles_[1]);
  // DEL eliminated, but v1 remains because we aren't compacting that level
  // (DEL can be eliminated because v2 hides v1).
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");
  dbfull()->TEST_CompactRange(last - 1, nullptr, nullptr, handles_[1]);
  // Merging last-1 w/ last, so we are the base level for "foo", so
  // DEL is removed.  (as is v1).
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2 ]");
}

TEST_F(DBTest, DeletionMarkers2) {
  Options options = CurrentOptions();
  options.max_background_flushes = 0;
  CreateAndReopenWithCF({"pikachu"}, options);
  Put(1, "foo", "v1");
  ASSERT_OK(Flush(1));
  const int last = CurrentOptions().max_mem_compaction_level;
  // foo => v1 is now in last level
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put(1, "a", "begin");
  Put(1, "z", "end");
  Flush(1);
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last - 1, 1), 1);

  Delete(1, "foo");
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v1 ]");
  ASSERT_OK(Flush(1));  // Moves to level last-2
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v1 ]");
  dbfull()->TEST_CompactRange(last - 2, nullptr, nullptr, handles_[1]);
  // DEL kept: "last" file overlaps
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v1 ]");
  dbfull()->TEST_CompactRange(last - 1, nullptr, nullptr, handles_[1]);
  // Merging last-1 w/ last, so we are the base level for "foo", so
  // DEL is removed.  (as is v1).
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");
}

TEST_F(DBTest, OverlapInLevel0) {
  do {
    Options options = CurrentOptions();
    options.max_background_flushes = 0;
    CreateAndReopenWithCF({"pikachu"}, options);
    int tmp = CurrentOptions().max_mem_compaction_level;
    ASSERT_EQ(tmp, 2) << "Fix test to match config";

    //Fill levels 1 and 2 to disable the pushing of new memtables to levels > 0.
    ASSERT_OK(Put(1, "100", "v100"));
    ASSERT_OK(Put(1, "999", "v999"));
    Flush(1);
    ASSERT_OK(Delete(1, "100"));
    ASSERT_OK(Delete(1, "999"));
    Flush(1);
    ASSERT_EQ("0,1,1", FilesPerLevel(1));

    // Make files spanning the following ranges in level-0:
    //  files[0]  200 .. 900
    //  files[1]  300 .. 500
    // Note that files are sorted by smallest key.
    ASSERT_OK(Put(1, "300", "v300"));
    ASSERT_OK(Put(1, "500", "v500"));
    Flush(1);
    ASSERT_OK(Put(1, "200", "v200"));
    ASSERT_OK(Put(1, "600", "v600"));
    ASSERT_OK(Put(1, "900", "v900"));
    Flush(1);
    ASSERT_EQ("2,1,1", FilesPerLevel(1));

    // Compact away the placeholder files we created initially
    dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]);
    dbfull()->TEST_CompactRange(2, nullptr, nullptr, handles_[1]);
    ASSERT_EQ("2", FilesPerLevel(1));

    // Do a memtable compaction.  Before bug-fix, the compaction would
    // not detect the overlap with level-0 files and would incorrectly place
    // the deletion in a deeper level.
    ASSERT_OK(Delete(1, "600"));
    Flush(1);
    ASSERT_EQ("3", FilesPerLevel(1));
    ASSERT_EQ("NOT_FOUND", Get(1, "600"));
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipFIFOCompaction));
}

TEST_F(DBTest, L0_CompactionBug_Issue44_a) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "b", "v"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "b"));
    ASSERT_OK(Delete(1, "a"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "a"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "v"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("(a->v)", Contents(1));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(a->v)", Contents(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, L0_CompactionBug_Issue44_b) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Put(1, "", "");
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    Delete(1, "e");
    Put(1, "", "");
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    Put(1, "c", "cv");
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    Put(1, "", "");
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    Put(1, "", "");
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    Put(1, "d", "dv");
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    Put(1, "", "");
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    Delete(1, "d");
    Delete(1, "b");
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("(->)(c->cv)", Contents(1));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(->)(c->cv)", Contents(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, ComparatorCheck) {
  class NewComparator : public Comparator {
   public:
    virtual const char* Name() const override {
      return "rocksdb.NewComparator";
    }
    virtual int Compare(const Slice& a, const Slice& b) const override {
      return BytewiseComparator()->Compare(a, b);
    }
    virtual void FindShortestSeparator(std::string* s,
                                       const Slice& l) const override {
      BytewiseComparator()->FindShortestSeparator(s, l);
    }
    virtual void FindShortSuccessor(std::string* key) const override {
      BytewiseComparator()->FindShortSuccessor(key);
    }
  };
  Options new_options, options;
  NewComparator cmp;
  do {
    options = CurrentOptions();
    CreateAndReopenWithCF({"pikachu"}, options);
    new_options = CurrentOptions();
    new_options.comparator = &cmp;
    // only the non-default column family has non-matching comparator
    Status s = TryReopenWithColumnFamilies({"default", "pikachu"},
        std::vector<Options>({options, new_options}));
    ASSERT_TRUE(!s.ok());
    ASSERT_TRUE(s.ToString().find("comparator") != std::string::npos)
        << s.ToString();
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, CustomComparator) {
  class NumberComparator : public Comparator {
   public:
    virtual const char* Name() const override {
      return "test.NumberComparator";
    }
    virtual int Compare(const Slice& a, const Slice& b) const override {
      return ToNumber(a) - ToNumber(b);
    }
    virtual void FindShortestSeparator(std::string* s,
                                       const Slice& l) const override {
      ToNumber(*s);     // Check format
      ToNumber(l);      // Check format
    }
    virtual void FindShortSuccessor(std::string* key) const override {
      ToNumber(*key);   // Check format
    }
   private:
    static int ToNumber(const Slice& x) {
      // Check that there are no extra characters.
      EXPECT_TRUE(x.size() >= 2 && x[0] == '[' && x[x.size() - 1] == ']')
          << EscapeString(x);
      int val;
      char ignored;
      EXPECT_TRUE(sscanf(x.ToString().c_str(), "[%i]%c", &val, &ignored) == 1)
          << EscapeString(x);
      return val;
    }
  };
  Options new_options;
  NumberComparator cmp;
  do {
    new_options = CurrentOptions();
    new_options.create_if_missing = true;
    new_options.comparator = &cmp;
    new_options.write_buffer_size = 1000;  // Compact more often
    new_options = CurrentOptions(new_options);
    DestroyAndReopen(new_options);
    CreateAndReopenWithCF({"pikachu"}, new_options);
    ASSERT_OK(Put(1, "[10]", "ten"));
    ASSERT_OK(Put(1, "[0x14]", "twenty"));
    for (int i = 0; i < 2; i++) {
      ASSERT_EQ("ten", Get(1, "[10]"));
      ASSERT_EQ("ten", Get(1, "[0xa]"));
      ASSERT_EQ("twenty", Get(1, "[20]"));
      ASSERT_EQ("twenty", Get(1, "[0x14]"));
      ASSERT_EQ("NOT_FOUND", Get(1, "[15]"));
      ASSERT_EQ("NOT_FOUND", Get(1, "[0xf]"));
      Compact(1, "[0]", "[9999]");
    }

    for (int run = 0; run < 2; run++) {
      for (int i = 0; i < 1000; i++) {
        char buf[100];
        snprintf(buf, sizeof(buf), "[%d]", i*10);
        ASSERT_OK(Put(1, buf, buf));
      }
      Compact(1, "[0]", "[1000000]");
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, ManualCompaction) {
  Options options = CurrentOptions();
  options.max_background_flushes = 0;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(dbfull()->MaxMemCompactionLevel(), 2)
      << "Need to update this test to match kMaxMemCompactLevel";

  // iter - 0 with 7 levels
  // iter - 1 with 3 levels
  for (int iter = 0; iter < 2; ++iter) {
    MakeTables(3, "p", "q", 1);
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls before files
    Compact(1, "", "c");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls after files
    Compact(1, "r", "z");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range overlaps files
    Compact(1, "p1", "p9");
    ASSERT_EQ("0,0,1", FilesPerLevel(1));

    // Populate a different range
    MakeTables(3, "c", "e", 1);
    ASSERT_EQ("1,1,2", FilesPerLevel(1));

    // Compact just the new range
    Compact(1, "b", "f");
    ASSERT_EQ("0,0,2", FilesPerLevel(1));

    // Compact all
    MakeTables(1, "a", "z", 1);
    ASSERT_EQ("0,1,2", FilesPerLevel(1));
    db_->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ("0,0,1", FilesPerLevel(1));

    if (iter == 0) {
      options = CurrentOptions();
      options.max_background_flushes = 0;
      options.num_levels = 3;
      options.create_if_missing = true;
      DestroyAndReopen(options);
      CreateAndReopenWithCF({"pikachu"}, options);
    }
  }

}

class DBTestUniversalManualCompactionOutputPathId
    : public DBTestUniversalCompactionBase {};

TEST_P(DBTestUniversalManualCompactionOutputPathId,
       ManualCompactionOutputPathId) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.db_paths.emplace_back(dbname_, 1000000000);
  options.db_paths.emplace_back(dbname_ + "_2", 1000000000);
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.target_file_size_base = 1 << 30;  // Big size
  options.level0_file_num_compaction_trigger = 10;
  Destroy(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  MakeTables(3, "p", "q", 1);
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(3, TotalLiveFiles(1));
  ASSERT_EQ(3, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[1].path));

  // Full compaction to DB path 0
  db_->CompactRange(handles_[1], nullptr, nullptr, false, -1, 1);
  ASSERT_EQ(1, TotalLiveFiles(1));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ(1, TotalLiveFiles(1));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  MakeTables(1, "p", "q", 1);
  ASSERT_EQ(2, TotalLiveFiles(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ(2, TotalLiveFiles(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  // Full compaction to DB path 0
  db_->CompactRange(handles_[1], nullptr, nullptr, false, -1, 0);
  ASSERT_EQ(1, TotalLiveFiles(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[1].path));

  // Fail when compacting to an invalid path ID
  ASSERT_TRUE(db_->CompactRange(handles_[1], nullptr, nullptr, false, -1, 2)
                  .IsInvalidArgument());
}

INSTANTIATE_TEST_CASE_P(DBTestUniversalManualCompactionOutputPathId,
                        DBTestUniversalManualCompactionOutputPathId,
                        ::testing::Values(1, 8));

TEST_F(DBTest, ManualLevelCompactionOutputPathId) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_ + "_2", 2 * 10485760);
  options.db_paths.emplace_back(dbname_ + "_3", 100 * 10485760);
  options.db_paths.emplace_back(dbname_ + "_4", 120 * 10485760);
  options.max_background_flushes = 1;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(dbfull()->MaxMemCompactionLevel(), 2)
      << "Need to update this test to match kMaxMemCompactLevel";

  // iter - 0 with 7 levels
  // iter - 1 with 3 levels
  for (int iter = 0; iter < 2; ++iter) {
    MakeTables(3, "p", "q", 1);
    ASSERT_EQ("3", FilesPerLevel(1));
    ASSERT_EQ(3, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    // Compaction range falls before files
    Compact(1, "", "c");
    ASSERT_EQ("3", FilesPerLevel(1));

    // Compaction range falls after files
    Compact(1, "r", "z");
    ASSERT_EQ("3", FilesPerLevel(1));

    // Compaction range overlaps files
    Compact(1, "p1", "p9", 1);
    ASSERT_EQ("0,1", FilesPerLevel(1));
    ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    // Populate a different range
    MakeTables(3, "c", "e", 1);
    ASSERT_EQ("3,1", FilesPerLevel(1));

    // Compact just the new range
    Compact(1, "b", "f", 1);
    ASSERT_EQ("0,2", FilesPerLevel(1));
    ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    // Compact all
    MakeTables(1, "a", "z", 1);
    ASSERT_EQ("1,2", FilesPerLevel(1));
    ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
    db_->CompactRange(handles_[1], nullptr, nullptr, false, 1, 1);
    ASSERT_EQ("0,1", FilesPerLevel(1));
    ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    if (iter == 0) {
      DestroyAndReopen(options);
      options = CurrentOptions();
      options.db_paths.emplace_back(dbname_ + "_2", 2 * 10485760);
      options.db_paths.emplace_back(dbname_ + "_3", 100 * 10485760);
      options.db_paths.emplace_back(dbname_ + "_4", 120 * 10485760);
      options.max_background_flushes = 1;
      options.num_levels = 3;
      options.create_if_missing = true;
      CreateAndReopenWithCF({"pikachu"}, options);
    }
  }
}

TEST_F(DBTest, DBOpen_Options) {
  Options options = CurrentOptions();
  std::string dbname = test::TmpDir(env_) + "/db_options_test";
  ASSERT_OK(DestroyDB(dbname, options));

  // Does not exist, and create_if_missing == false: error
  DB* db = nullptr;
  options.create_if_missing = false;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "does not exist") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does not exist, and create_if_missing == true: OK
  options.create_if_missing = true;
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;

  // Does exist, and error_if_exists == true: error
  options.create_if_missing = false;
  options.error_if_exists = true;
  s = DB::Open(options, dbname, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "exists") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does exist, and error_if_exists == false: OK
  options.create_if_missing = true;
  options.error_if_exists = false;
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;
}

TEST_F(DBTest, DBOpen_Change_NumLevels) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.max_background_flushes = 0;
  DestroyAndReopen(options);
  ASSERT_TRUE(db_ != nullptr);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "a", "123"));
  ASSERT_OK(Put(1, "b", "234"));
  db_->CompactRange(handles_[1], nullptr, nullptr);
  Close();

  options.create_if_missing = false;
  options.num_levels = 2;
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "Invalid argument") != nullptr);
  ASSERT_TRUE(db_ == nullptr);
}

TEST_F(DBTest, DestroyDBMetaDatabase) {
  std::string dbname = test::TmpDir(env_) + "/db_meta";
  ASSERT_OK(env_->CreateDirIfMissing(dbname));
  std::string metadbname = MetaDatabaseName(dbname, 0);
  ASSERT_OK(env_->CreateDirIfMissing(metadbname));
  std::string metametadbname = MetaDatabaseName(metadbname, 0);
  ASSERT_OK(env_->CreateDirIfMissing(metametadbname));

  // Destroy previous versions if they exist. Using the long way.
  Options options = CurrentOptions();
  ASSERT_OK(DestroyDB(metametadbname, options));
  ASSERT_OK(DestroyDB(metadbname, options));
  ASSERT_OK(DestroyDB(dbname, options));

  // Setup databases
  DB* db = nullptr;
  ASSERT_OK(DB::Open(options, dbname, &db));
  delete db;
  db = nullptr;
  ASSERT_OK(DB::Open(options, metadbname, &db));
  delete db;
  db = nullptr;
  ASSERT_OK(DB::Open(options, metametadbname, &db));
  delete db;
  db = nullptr;

  // Delete databases
  ASSERT_OK(DestroyDB(dbname, options));

  // Check if deletion worked.
  options.create_if_missing = false;
  ASSERT_TRUE(!(DB::Open(options, dbname, &db)).ok());
  ASSERT_TRUE(!(DB::Open(options, metadbname, &db)).ok());
  ASSERT_TRUE(!(DB::Open(options, metametadbname, &db)).ok());
}

// Check that number of files does not grow when writes are dropped
TEST_F(DBTest, DropWrites) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.paranoid_checks = false;
    Reopen(options);

    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    Compact("a", "z");
    const size_t num_files = CountFiles();
    // Force out-of-space errors
    env_->drop_writes_.store(true, std::memory_order_release);
    env_->sleep_counter_.Reset();
    for (int i = 0; i < 5; i++) {
      for (int level = 0; level < dbfull()->NumberLevels(); level++) {
        if (level > 0 && level == dbfull()->NumberLevels() - 1) {
          break;
        }
        dbfull()->TEST_CompactRange(level, nullptr, nullptr);
      }
    }

    std::string property_value;
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("5", property_value);

    env_->drop_writes_.store(false, std::memory_order_release);
    ASSERT_LT(CountFiles(), num_files + 3);

    // Check that compaction attempts slept after errors
    ASSERT_GE(env_->sleep_counter_.Read(), 5);
  } while (ChangeCompactOptions());
}

// Check background error counter bumped on flush failures.
TEST_F(DBTest, DropWritesFlush) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.max_background_flushes = 1;
    Reopen(options);

    ASSERT_OK(Put("foo", "v1"));
    // Force out-of-space errors
    env_->drop_writes_.store(true, std::memory_order_release);

    std::string property_value;
    // Background error count is 0 now.
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("0", property_value);

    dbfull()->TEST_FlushMemTable(true);

    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("1", property_value);

    env_->drop_writes_.store(false, std::memory_order_release);
  } while (ChangeCompactOptions());
}

// Check that CompactRange() returns failure if there is not enough space left
// on device
TEST_F(DBTest, NoSpaceCompactRange) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.disable_auto_compactions = true;
    Reopen(options);

    // generate 5 tables
    for (int i = 0; i < 5; ++i) {
      ASSERT_OK(Put(Key(i), Key(i) + "v"));
      ASSERT_OK(Flush());
    }

    // Force out-of-space errors
    env_->no_space_.store(true, std::memory_order_release);

    Status s = db_->CompactRange(nullptr, nullptr);
    ASSERT_TRUE(s.IsIOError());

    env_->no_space_.store(false, std::memory_order_release);
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, NonWritableFileSystem) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 1000;
    options.env = env_;
    Reopen(options);
    ASSERT_OK(Put("foo", "v1"));
    env_->non_writeable_rate_.store(100);
    std::string big(100000, 'x');
    int errors = 0;
    for (int i = 0; i < 20; i++) {
      if (!Put("foo", big).ok()) {
        errors++;
        env_->SleepForMicroseconds(100000);
      }
    }
    ASSERT_GT(errors, 0);
    env_->non_writeable_rate_.store(0);
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, ManifestWriteError) {
  // Test for the following problem:
  // (a) Compaction produces file F
  // (b) Log record containing F is written to MANIFEST file, but Sync() fails
  // (c) GC deletes F
  // (d) After reopening DB, reads fail since deleted F is named in log record

  // We iterate twice.  In the second iteration, everything is the
  // same except the log record never makes it to the MANIFEST file.
  for (int iter = 0; iter < 2; iter++) {
    std::atomic<bool>* error_type = (iter == 0)
        ? &env_->manifest_sync_error_
        : &env_->manifest_write_error_;

    // Insert foo=>bar mapping
    Options options = CurrentOptions();
    options.env = env_;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.max_background_flushes = 0;
    DestroyAndReopen(options);
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_EQ("bar", Get("foo"));

    // Memtable compaction (will succeed)
    Flush();
    ASSERT_EQ("bar", Get("foo"));
    const int last = dbfull()->MaxMemCompactionLevel();
    ASSERT_EQ(NumTableFilesAtLevel(last), 1);   // foo=>bar is now in last level

    // Merging compaction (will fail)
    error_type->store(true, std::memory_order_release);
    dbfull()->TEST_CompactRange(last, nullptr, nullptr);  // Should fail
    ASSERT_EQ("bar", Get("foo"));

    // Recovery: should not lose data
    error_type->store(false, std::memory_order_release);
    Reopen(options);
    ASSERT_EQ("bar", Get("foo"));
  }
}

TEST_F(DBTest, PutFailsParanoid) {
  // Test the following:
  // (a) A random put fails in paranoid mode (simulate by sync fail)
  // (b) All other puts have to fail, even if writes would succeed
  // (c) All of that should happen ONLY if paranoid_checks = true

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Status s;

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.store(true, std::memory_order_release);
  s = Put(1, "foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.store(false, std::memory_order_release);
  s = Put(1, "foo3", "bar3");
  // the next put should fail, too
  ASSERT_TRUE(!s.ok());
  // but we're still able to read
  ASSERT_EQ("bar", Get(1, "foo"));

  // do the same thing with paranoid checks off
  options.paranoid_checks = false;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.store(true, std::memory_order_release);
  s = Put(1, "foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.store(false, std::memory_order_release);
  s = Put(1, "foo3", "bar3");
  // the next put should NOT fail
  ASSERT_TRUE(s.ok());
}

TEST_F(DBTest, FilesDeletedAfterCompaction) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v2"));
    Compact(1, "a", "z");
    const size_t num_files = CountLiveFiles();
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(Put(1, "foo", "v2"));
      Compact(1, "a", "z");
    }
    ASSERT_EQ(CountLiveFiles(), num_files);
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, BloomFilter) {
  do {
    Options options = CurrentOptions();
    env_->count_random_reads_ = true;
    options.env = env_;
    // ChangeCompactOptions() only changes compaction style, which does not
    // trigger reset of table_factory
    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    CreateAndReopenWithCF({"pikachu"}, options);

    // Populate multiple layers
    const int N = 10000;
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    Compact(1, "a", "z");
    for (int i = 0; i < N; i += 100) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    Flush(1);

    // Prevent auto compactions triggered by seeks
    env_->delay_sstable_sync_.store(true, std::memory_order_release);

    // Lookup present keys.  Should rarely read from small sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    int reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d present => %d reads\n", N, reads);
    ASSERT_GE(reads, N);
    ASSERT_LE(reads, N + 2*N/100);

    // Lookup present keys.  Should rarely read from either sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i) + ".missing"));
    }
    reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d missing => %d reads\n", N, reads);
    ASSERT_LE(reads, 3*N/100);

    env_->delay_sstable_sync_.store(false, std::memory_order_release);
    Close();
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, BloomFilterRate) {
  while (ChangeFilterOptions()) {
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    CreateAndReopenWithCF({"pikachu"}, options);

    const int maxKey = 10000;
    for (int i = 0; i < maxKey; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    // Add a large key to make the file contain wide range
    ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
    Flush(1);

    // Check if they can be found
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);

    // Check if filter is useful
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i+33333)));
    }
    ASSERT_GE(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), maxKey*0.98);
  }
}

TEST_F(DBTest, BloomFilterCompatibility) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Create with block based filter
  CreateAndReopenWithCF({"pikachu"}, options);

  const int maxKey = 10000;
  for (int i = 0; i < maxKey; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
  Flush(1);

  // Check db with full filter
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  // Check if they can be found
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ(Key(i), Get(1, Key(i)));
  }
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
}

TEST_F(DBTest, BloomFilterReverseCompatibility) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Create with full filter
  CreateAndReopenWithCF({"pikachu"}, options);

  const int maxKey = 10000;
  for (int i = 0; i < maxKey; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
  Flush(1);

  // Check db with block_based filter
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  // Check if they can be found
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ(Key(i), Get(1, Key(i)));
  }
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
}

namespace {
// A wrapped bloom over default FilterPolicy
class WrappedBloom : public FilterPolicy {
 public:
  explicit WrappedBloom(int bits_per_key) :
        filter_(NewBloomFilterPolicy(bits_per_key)),
        counter_(0) {}

  ~WrappedBloom() { delete filter_; }

  const char* Name() const override { return "WrappedRocksDbFilterPolicy"; }

  void CreateFilter(const rocksdb::Slice* keys, int n, std::string* dst)
      const override {
    std::unique_ptr<rocksdb::Slice[]> user_keys(new rocksdb::Slice[n]);
    for (int i = 0; i < n; ++i) {
      user_keys[i] = convertKey(keys[i]);
    }
    return filter_->CreateFilter(user_keys.get(), n, dst);
  }

  bool KeyMayMatch(const rocksdb::Slice& key, const rocksdb::Slice& filter)
      const override {
    counter_++;
    return filter_->KeyMayMatch(convertKey(key), filter);
  }

  uint32_t GetCounter() { return counter_; }

 private:
  const FilterPolicy* filter_;
  mutable uint32_t counter_;

  rocksdb::Slice convertKey(const rocksdb::Slice& key) const {
    return key;
  }
};
}  // namespace

TEST_F(DBTest, BloomFilterWrapper) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();

  BlockBasedTableOptions table_options;
  WrappedBloom* policy = new WrappedBloom(10);
  table_options.filter_policy.reset(policy);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  CreateAndReopenWithCF({"pikachu"}, options);

  const int maxKey = 10000;
  for (int i = 0; i < maxKey; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  // Add a large key to make the file contain wide range
  ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
  ASSERT_EQ(0U, policy->GetCounter());
  Flush(1);

  // Check if they can be found
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ(Key(i), Get(1, Key(i)));
  }
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ(1U * maxKey, policy->GetCounter());

  // Check if filter is useful
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ("NOT_FOUND", Get(1, Key(i+33333)));
  }
  ASSERT_GE(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), maxKey*0.98);
  ASSERT_EQ(2U * maxKey, policy->GetCounter());
}

TEST_F(DBTest, SnapshotFiles) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 100000000;        // Large write buffer
    CreateAndReopenWithCF({"pikachu"}, options);

    Random rnd(301);

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    std::vector<std::string> values;
    for (int i = 0; i < 80; i++) {
      values.push_back(RandomString(&rnd, 100000));
      ASSERT_OK(Put((i < 40), Key(i), values[i]));
    }

    // assert that nothing makes it to disk yet.
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);

    // get a file snapshot
    uint64_t manifest_number = 0;
    uint64_t manifest_size = 0;
    std::vector<std::string> files;
    dbfull()->DisableFileDeletions();
    dbfull()->GetLiveFiles(files, &manifest_size);

    // CURRENT, MANIFEST, *.sst files (one for each CF)
    ASSERT_EQ(files.size(), 4U);

    uint64_t number = 0;
    FileType type;

    // copy these files to a new snapshot directory
    std::string snapdir = dbname_ + ".snapdir/";
    ASSERT_OK(env_->CreateDirIfMissing(snapdir));

    for (unsigned int i = 0; i < files.size(); i++) {
      // our clients require that GetLiveFiles returns
      // files with "/" as first character!
      ASSERT_EQ(files[i][0], '/');
      std::string src = dbname_ + files[i];
      std::string dest = snapdir + files[i];

      uint64_t size;
      ASSERT_OK(env_->GetFileSize(src, &size));

      // record the number and the size of the
      // latest manifest file
      if (ParseFileName(files[i].substr(1), &number, &type)) {
        if (type == kDescriptorFile) {
          if (number > manifest_number) {
            manifest_number = number;
            ASSERT_GE(size, manifest_size);
            size = manifest_size; // copy only valid MANIFEST data
          }
        }
      }
      CopyFile(src, dest, size);
    }

    // release file snapshot
    dbfull()->DisableFileDeletions();
    // overwrite one key, this key should not appear in the snapshot
    std::vector<std::string> extras;
    for (unsigned int i = 0; i < 1; i++) {
      extras.push_back(RandomString(&rnd, 100000));
      ASSERT_OK(Put(0, Key(i), extras[i]));
    }

    // verify that data in the snapshot are correct
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back("default", ColumnFamilyOptions());
    column_families.emplace_back("pikachu", ColumnFamilyOptions());
    std::vector<ColumnFamilyHandle*> cf_handles;
    DB* snapdb;
    DBOptions opts;
    opts.env = env_;
    opts.create_if_missing = false;
    Status stat =
        DB::Open(opts, snapdir, column_families, &cf_handles, &snapdb);
    ASSERT_OK(stat);

    ReadOptions roptions;
    std::string val;
    for (unsigned int i = 0; i < 80; i++) {
      stat = snapdb->Get(roptions, cf_handles[i < 40], Key(i), &val);
      ASSERT_EQ(values[i].compare(val), 0);
    }
    for (auto cfh : cf_handles) {
      delete cfh;
    }
    delete snapdb;

    // look at the new live files after we added an 'extra' key
    // and after we took the first snapshot.
    uint64_t new_manifest_number = 0;
    uint64_t new_manifest_size = 0;
    std::vector<std::string> newfiles;
    dbfull()->DisableFileDeletions();
    dbfull()->GetLiveFiles(newfiles, &new_manifest_size);

    // find the new manifest file. assert that this manifest file is
    // the same one as in the previous snapshot. But its size should be
    // larger because we added an extra key after taking the
    // previous shapshot.
    for (unsigned int i = 0; i < newfiles.size(); i++) {
      std::string src = dbname_ + "/" + newfiles[i];
      // record the lognumber and the size of the
      // latest manifest file
      if (ParseFileName(newfiles[i].substr(1), &number, &type)) {
        if (type == kDescriptorFile) {
          if (number > new_manifest_number) {
            uint64_t size;
            new_manifest_number = number;
            ASSERT_OK(env_->GetFileSize(src, &size));
            ASSERT_GE(size, new_manifest_size);
          }
        }
      }
    }
    ASSERT_EQ(manifest_number, new_manifest_number);
    ASSERT_GT(new_manifest_size, manifest_size);

    // release file snapshot
    dbfull()->DisableFileDeletions();
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, CompactOnFlush) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.purge_redundant_kvs_while_flush = true;
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    Put(1, "foo", "v1");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v1 ]");

    // Write two new keys
    Put(1, "a", "begin");
    Put(1, "z", "end");
    Flush(1);

    // Case1: Delete followed by a put
    Delete(1, "foo");
    Put(1, "foo", "v2");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, DEL, v1 ]");

    // After the current memtable is flushed, the DEL should
    // have been removed
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");

    dbfull()->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2 ]");

    // Case 2: Delete followed by another delete
    Delete(1, "foo");
    Delete(1, "foo");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, DEL, v2 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v2 ]");
    dbfull()->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 3: Put followed by a delete
    Put(1, "foo", "v3");
    Delete(1, "foo");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v3 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL ]");
    dbfull()->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 4: Put followed by another Put
    Put(1, "foo", "v4");
    Put(1, "foo", "v5");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5, v4 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");
    dbfull()->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");

    // clear database
    Delete(1, "foo");
    dbfull()->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: Put followed by snapshot followed by another Put
    // Both puts should remain.
    Put(1, "foo", "v6");
    const Snapshot* snapshot = db_->GetSnapshot();
    Put(1, "foo", "v7");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v7, v6 ]");
    db_->ReleaseSnapshot(snapshot);

    // clear database
    Delete(1, "foo");
    dbfull()->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: snapshot followed by a put followed by another Put
    // Only the last put should remain.
    const Snapshot* snapshot1 = db_->GetSnapshot();
    Put(1, "foo", "v8");
    Put(1, "foo", "v9");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v9 ]");
    db_->ReleaseSnapshot(snapshot1);
  } while (ChangeCompactOptions());
}

namespace {
std::vector<std::uint64_t> ListSpecificFiles(
    Env* env, const std::string& path, const FileType expected_file_type) {
  std::vector<std::string> files;
  std::vector<uint64_t> file_numbers;
  env->GetChildren(path, &files);
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < files.size(); ++i) {
    if (ParseFileName(files[i], &number, &type)) {
      if (type == expected_file_type) {
        file_numbers.push_back(number);
      }
    }
  }
  return std::move(file_numbers);
}

std::vector<std::uint64_t> ListTableFiles(Env* env, const std::string& path) {
  return ListSpecificFiles(env, path, kTableFile);
}
}  // namespace

TEST_F(DBTest, FlushOneColumnFamily) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "pikachu", "pikachu"));
  ASSERT_OK(Put(2, "ilya", "ilya"));
  ASSERT_OK(Put(3, "muromec", "muromec"));
  ASSERT_OK(Put(4, "dobrynia", "dobrynia"));
  ASSERT_OK(Put(5, "nikitich", "nikitich"));
  ASSERT_OK(Put(6, "alyosha", "alyosha"));
  ASSERT_OK(Put(7, "popovich", "popovich"));

  for (int i = 0; i < 8; ++i) {
    Flush(i);
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), i + 1U);
  }
}

// In https://reviews.facebook.net/D20661 we change
// recovery behavior: previously for each log file each column family
// memtable was flushed, even it was empty. Now it's changed:
// we try to create the smallest number of table files by merging
// updates from multiple logs
TEST_F(DBTest, RecoverCheckFileAmountWithSmallWriteBuffer) {
  Options options = CurrentOptions();
  options.write_buffer_size = 5000000;
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  // Since we will reopen DB with smaller write_buffer_size,
  // each key will go to new SST file
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));

  ASSERT_OK(Put(3, Key(10), DummyString(1)));
  // Make 'dobrynia' to be flushed and new WAL file to be created
  ASSERT_OK(Put(2, Key(10), DummyString(7500000)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
  {
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), static_cast<size_t>(1));
    // Make sure 'dobrynia' was flushed: check sst files amount
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(1));
  }
  // New WAL file
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(3, Key(10), DummyString(1)));
  ASSERT_OK(Put(3, Key(10), DummyString(1)));
  ASSERT_OK(Put(3, Key(10), DummyString(1)));

  options.write_buffer_size = 10;
  ReopenWithColumnFamilies({"default", "pikachu", "dobrynia", "nikitich"},
                           options);
  {
    // No inserts => default is empty
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(0));
    // First 4 keys goes to separate SSTs + 1 more SST for 2 smaller keys
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(5));
    // 1 SST for big key + 1 SST for small one
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(2));
    // 1 SST for all keys
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(1));
  }
}

// In https://reviews.facebook.net/D20661 we change
// recovery behavior: previously for each log file each column family
// memtable was flushed, even it wasn't empty. Now it's changed:
// we try to create the smallest number of table files by merging
// updates from multiple logs
TEST_F(DBTest, RecoverCheckFileAmount) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000;
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));

  // Make 'nikitich' memtable to be flushed
  ASSERT_OK(Put(3, Key(10), DummyString(1002400)));
  ASSERT_OK(Put(3, Key(1), DummyString(1)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  // 4 memtable are not flushed, 1 sst file
  {
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), static_cast<size_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(1));
  }
  // Memtable for 'nikitich' has flushed, new WAL file has opened
  // 4 memtable still not flushed

  // Write to new WAL file
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));

  // Fill up 'nikitich' one more time
  ASSERT_OK(Put(3, Key(10), DummyString(1002400)));
  // make it flush
  ASSERT_OK(Put(3, Key(1), DummyString(1)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  // There are still 4 memtable not flushed, and 2 sst tables
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));

  {
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), static_cast<size_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  ReopenWithColumnFamilies({"default", "pikachu", "dobrynia", "nikitich"},
                           options);
  {
    std::vector<uint64_t> table_files = ListTableFiles(env_, dbname_);
    // Check, that records for 'default', 'dobrynia' and 'pikachu' from
    // first, second and third WALs  went to the same SST.
    // So, there is 6 SSTs: three  for 'nikitich', one for 'default', one for
    // 'dobrynia', one for 'pikachu'
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(3));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(1));
  }
}

TEST_F(DBTest, SharedWriteBuffer) {
  Options options = CurrentOptions();
  options.db_write_buffer_size = 100000;  // this is the real limit
  options.write_buffer_size    = 500000;  // this is never hit
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  // Trigger a flush on every CF
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(3, Key(1), DummyString(90000)));
  ASSERT_OK(Put(2, Key(2), DummyString(20000)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[0]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(1));
  }

  // Flush 'dobrynia' and 'nikitich'
  ASSERT_OK(Put(2, Key(2), DummyString(50000)));
  ASSERT_OK(Put(3, Key(2), DummyString(40000)));
  ASSERT_OK(Put(2, Key(3), DummyString(20000)));
  ASSERT_OK(Put(3, Key(2), DummyString(40000)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Make 'dobrynia' and 'nikitich' both take up 40% of space
  // When 'pikachu' puts us over 100%, all 3 flush.
  ASSERT_OK(Put(2, Key(2), DummyString(40000)));
  ASSERT_OK(Put(1, Key(2), DummyString(20000)));
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(3));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(3));
  }

  // Some remaining writes so 'default' and 'nikitich' flush on closure.
  ASSERT_OK(Put(3, Key(1), DummyString(1)));
  ReopenWithColumnFamilies({"default", "pikachu", "dobrynia", "nikitich"},
                           options);
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(3));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(4));
  }
}

TEST_F(DBTest, PurgeInfoLogs) {
  Options options = CurrentOptions();
  options.keep_log_file_num = 5;
  options.create_if_missing = true;
  for (int mode = 0; mode <= 1; mode++) {
    if (mode == 1) {
      options.db_log_dir = dbname_ + "_logs";
      env_->CreateDirIfMissing(options.db_log_dir);
    } else {
      options.db_log_dir = "";
    }
    for (int i = 0; i < 8; i++) {
      Reopen(options);
    }

    std::vector<std::string> files;
    env_->GetChildren(options.db_log_dir.empty() ? dbname_ : options.db_log_dir,
                      &files);
    int info_log_count = 0;
    for (std::string file : files) {
      if (file.find("LOG") != std::string::npos) {
        info_log_count++;
      }
    }
    ASSERT_EQ(5, info_log_count);

    Destroy(options);
    // For mode (1), test DestroyDB() to delete all the logs under DB dir.
    // For mode (2), no info log file should have been put under DB dir.
    std::vector<std::string> db_files;
    env_->GetChildren(dbname_, &db_files);
    for (std::string file : db_files) {
      ASSERT_TRUE(file.find("LOG") == std::string::npos);
    }

    if (mode == 1) {
      // Cleaning up
      env_->GetChildren(options.db_log_dir, &files);
      for (std::string file : files) {
        env_->DeleteFile(options.db_log_dir + "/" + file);
      }
      env_->DeleteDir(options.db_log_dir);
    }
  }
}

namespace {
SequenceNumber ReadRecords(
    std::unique_ptr<TransactionLogIterator>& iter,
    int& count) {
  count = 0;
  SequenceNumber lastSequence = 0;
  BatchResult res;
  while (iter->Valid()) {
    res = iter->GetBatch();
    EXPECT_TRUE(res.sequence > lastSequence);
    ++count;
    lastSequence = res.sequence;
    EXPECT_OK(iter->status());
    iter->Next();
  }
  return res.sequence;
}

void ExpectRecords(
    const int expected_no_records,
    std::unique_ptr<TransactionLogIterator>& iter) {
  int num_records;
  ReadRecords(iter, num_records);
  ASSERT_EQ(num_records, expected_no_records);
}
}  // namespace

TEST_F(DBTest, TransactionLogIterator) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    Put(0, "key1", DummyString(1024));
    Put(1, "key2", DummyString(1024));
    Put(1, "key2", DummyString(1024));
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3U);
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(3, iter);
    }
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    env_->SleepForMicroseconds(2 * 1000 * 1000);
    {
      Put(0, "key4", DummyString(1024));
      Put(1, "key5", DummyString(1024));
      Put(0, "key6", DummyString(1024));
    }
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(6, iter);
    }
  } while (ChangeCompactOptions());
}

#ifndef NDEBUG // sync point is not included with DNDEBUG build
TEST_F(DBTest, TransactionLogIteratorRace) {
  static const int LOG_ITERATOR_RACE_TEST_COUNT = 2;
  static const char* sync_points[LOG_ITERATOR_RACE_TEST_COUNT][4] = {
      {"WalManager::GetSortedWalFiles:1",  "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2", "WalManager::GetSortedWalFiles:2"},
      {"WalManager::GetSortedWalsOfType:1",
       "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2",
       "WalManager::GetSortedWalsOfType:2"}};
  for (int test = 0; test < LOG_ITERATOR_RACE_TEST_COUNT; ++test) {
    // Setup sync point dependency to reproduce the race condition of
    // a log file moved to archived dir, in the middle of GetSortedWalFiles
    rocksdb::SyncPoint::GetInstance()->LoadDependency(
      { { sync_points[test][0], sync_points[test][1] },
        { sync_points[test][2], sync_points[test][3] },
      });

    do {
      rocksdb::SyncPoint::GetInstance()->ClearTrace();
      rocksdb::SyncPoint::GetInstance()->DisableProcessing();
      Options options = OptionsForLogIterTest();
      DestroyAndReopen(options);
      Put("key1", DummyString(1024));
      dbfull()->Flush(FlushOptions());
      Put("key2", DummyString(1024));
      dbfull()->Flush(FlushOptions());
      Put("key3", DummyString(1024));
      dbfull()->Flush(FlushOptions());
      Put("key4", DummyString(1024));
      ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4U);

      {
        auto iter = OpenTransactionLogIter(0);
        ExpectRecords(4, iter);
      }

      rocksdb::SyncPoint::GetInstance()->EnableProcessing();
      // trigger async flush, and log move. Well, log move will
      // wait until the GetSortedWalFiles:1 to reproduce the race
      // condition
      FlushOptions flush_options;
      flush_options.wait = false;
      dbfull()->Flush(flush_options);

      // "key5" would be written in a new memtable and log
      Put("key5", DummyString(1024));
      {
        // this iter would miss "key4" if not fixed
        auto iter = OpenTransactionLogIter(0);
        ExpectRecords(5, iter);
      }
    } while (ChangeCompactOptions());
  }
}
#endif

TEST_F(DBTest, TransactionLogIteratorStallAtLastRecord) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    Put("key1", DummyString(1024));
    auto iter = OpenTransactionLogIter(0);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    Put("key2", DummyString(1024));
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, TransactionLogIteratorCheckAfterRestart) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    Put("key1", DummyString(1024));
    Put("key2", DummyString(1023));
    dbfull()->Flush(FlushOptions());
    Reopen(options);
    auto iter = OpenTransactionLogIter(0);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, TransactionLogIteratorCorruptedLog) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    for (int i = 0; i < 1024; i++) {
      Put("key"+ToString(i), DummyString(10));
    }
    dbfull()->Flush(FlushOptions());
    // Corrupt this log to create a gap
    rocksdb::VectorLogPtr wal_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
    const auto logfile_path = dbname_ + "/" + wal_files.front()->PathName();
    if (mem_env_) {
      mem_env_->Truncate(logfile_path, wal_files.front()->SizeFileBytes() / 2);
    } else {
      ASSERT_EQ(0, truncate(logfile_path.c_str(),
                   wal_files.front()->SizeFileBytes() / 2));
    }

    // Insert a new entry to a new log file
    Put("key1025", DummyString(10));
    // Try to read from the beginning. Should stop before the gap and read less
    // than 1025 entries
    auto iter = OpenTransactionLogIter(0);
    int count;
    SequenceNumber last_sequence_read = ReadRecords(iter, count);
    ASSERT_LT(last_sequence_read, 1025U);
    // Try to read past the gap, should be able to seek to key1025
    auto iter2 = OpenTransactionLogIter(last_sequence_read + 1);
    ExpectRecords(1, iter2);
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, TransactionLogIteratorBatchOperations) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    WriteBatch batch;
    batch.Put(handles_[1], "key1", DummyString(1024));
    batch.Put(handles_[0], "key2", DummyString(1024));
    batch.Put(handles_[1], "key3", DummyString(1024));
    batch.Delete(handles_[0], "key2");
    dbfull()->Write(WriteOptions(), &batch);
    Flush(1);
    Flush(0);
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    Put(1, "key4", DummyString(1024));
    auto iter = OpenTransactionLogIter(3);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, TransactionLogIteratorBlobs) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  {
    WriteBatch batch;
    batch.Put(handles_[1], "key1", DummyString(1024));
    batch.Put(handles_[0], "key2", DummyString(1024));
    batch.PutLogData(Slice("blob1"));
    batch.Put(handles_[1], "key3", DummyString(1024));
    batch.PutLogData(Slice("blob2"));
    batch.Delete(handles_[0], "key2");
    dbfull()->Write(WriteOptions(), &batch);
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
  }

  auto res = OpenTransactionLogIter(0)->GetBatch();
  struct Handler : public WriteBatch::Handler {
    std::string seen;
    virtual Status PutCF(uint32_t cf, const Slice& key,
                         const Slice& value) override {
      seen += "Put(" + ToString(cf) + ", " + key.ToString() + ", " +
              ToString(value.size()) + ")";
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t cf, const Slice& key,
                           const Slice& value) override {
      seen += "Merge(" + ToString(cf) + ", " + key.ToString() + ", " +
              ToString(value.size()) + ")";
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) override {
      seen += "LogData(" + blob.ToString() + ")";
    }
    virtual Status DeleteCF(uint32_t cf, const Slice& key) override {
      seen += "Delete(" + ToString(cf) + ", " + key.ToString() + ")";
      return Status::OK();
    }
  } handler;
  res.writeBatchPtr->Iterate(&handler);
  ASSERT_EQ(
      "Put(1, key1, 1024)"
      "Put(0, key2, 1024)"
      "LogData(blob1)"
      "Put(1, key3, 1024)"
      "LogData(blob2)"
      "Delete(0, key2)",
      handler.seen);
}

// Multi-threaded test:
namespace {

static const int kColumnFamilies = 10;
static const int kNumThreads = 10;
static const int kTestSeconds = 10;
static const int kNumKeys = 1000;

struct MTState {
  DBTest* test;
  std::atomic<bool> stop;
  std::atomic<int> counter[kNumThreads];
  std::atomic<bool> thread_done[kNumThreads];
};

struct MTThread {
  MTState* state;
  int id;
};

static void MTThreadBody(void* arg) {
  MTThread* t = reinterpret_cast<MTThread*>(arg);
  int id = t->id;
  DB* db = t->state->test->db_;
  int counter = 0;
  fprintf(stderr, "... starting thread %d\n", id);
  Random rnd(1000 + id);
  char valbuf[1500];
  while (t->state->stop.load(std::memory_order_acquire) == false) {
    t->state->counter[id].store(counter, std::memory_order_release);

    int key = rnd.Uniform(kNumKeys);
    char keybuf[20];
    snprintf(keybuf, sizeof(keybuf), "%016d", key);

    if (rnd.OneIn(2)) {
      // Write values of the form <key, my id, counter, cf, unique_id>.
      // into each of the CFs
      // We add some padding for force compactions.
      int unique_id = rnd.Uniform(1000000);

      // Half of the time directly use WriteBatch. Half of the time use
      // WriteBatchWithIndex.
      if (rnd.OneIn(2)) {
        WriteBatch batch;
        for (int cf = 0; cf < kColumnFamilies; ++cf) {
          snprintf(valbuf, sizeof(valbuf), "%d.%d.%d.%d.%-1000d", key, id,
                   static_cast<int>(counter), cf, unique_id);
          batch.Put(t->state->test->handles_[cf], Slice(keybuf), Slice(valbuf));
        }
        ASSERT_OK(db->Write(WriteOptions(), &batch));
      } else {
        WriteBatchWithIndex batch(db->GetOptions().comparator);
        for (int cf = 0; cf < kColumnFamilies; ++cf) {
          snprintf(valbuf, sizeof(valbuf), "%d.%d.%d.%d.%-1000d", key, id,
                   static_cast<int>(counter), cf, unique_id);
          batch.Put(t->state->test->handles_[cf], Slice(keybuf), Slice(valbuf));
        }
        ASSERT_OK(db->Write(WriteOptions(), batch.GetWriteBatch()));
      }
    } else {
      // Read a value and verify that it matches the pattern written above
      // and that writes to all column families were atomic (unique_id is the
      // same)
      std::vector<Slice> keys(kColumnFamilies, Slice(keybuf));
      std::vector<std::string> values;
      std::vector<Status> statuses =
          db->MultiGet(ReadOptions(), t->state->test->handles_, keys, &values);
      Status s = statuses[0];
      // all statuses have to be the same
      for (size_t i = 1; i < statuses.size(); ++i) {
        // they are either both ok or both not-found
        ASSERT_TRUE((s.ok() && statuses[i].ok()) ||
                    (s.IsNotFound() && statuses[i].IsNotFound()));
      }
      if (s.IsNotFound()) {
        // Key has not yet been written
      } else {
        // Check that the writer thread counter is >= the counter in the value
        ASSERT_OK(s);
        int unique_id = -1;
        for (int i = 0; i < kColumnFamilies; ++i) {
          int k, w, c, cf, u;
          ASSERT_EQ(5, sscanf(values[i].c_str(), "%d.%d.%d.%d.%d", &k, &w,
                              &c, &cf, &u))
              << values[i];
          ASSERT_EQ(k, key);
          ASSERT_GE(w, 0);
          ASSERT_LT(w, kNumThreads);
          ASSERT_LE(c, t->state->counter[w].load(std::memory_order_acquire));
          ASSERT_EQ(cf, i);
          if (i == 0) {
            unique_id = u;
          } else {
            // this checks that updates across column families happened
            // atomically -- all unique ids are the same
            ASSERT_EQ(u, unique_id);
          }
        }
      }
    }
    counter++;
  }
  t->state->thread_done[id].store(true, std::memory_order_release);
  fprintf(stderr, "... stopping thread %d after %d ops\n", id, int(counter));
}

}  // namespace

class MultiThreadedDBTest : public DBTest,
                            public ::testing::WithParamInterface<int> {
 public:
  virtual void SetUp() override { option_config_ = GetParam(); }

  static std::vector<int> GenerateOptionConfigs() {
    std::vector<int> optionConfigs;
    for (int optionConfig = kDefault; optionConfig < kEnd; ++optionConfig) {
      // skip as HashCuckooRep does not support snapshot
      if (optionConfig != kHashCuckoo) {
        optionConfigs.push_back(optionConfig);
      }
    }
    return optionConfigs;
  }
};

TEST_P(MultiThreadedDBTest, MultiThreaded) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  std::vector<std::string> cfs;
  for (int i = 1; i < kColumnFamilies; ++i) {
    cfs.push_back(ToString(i));
  }
  CreateAndReopenWithCF(cfs, CurrentOptions(options_override));
  // Initialize state
  MTState mt;
  mt.test = this;
  mt.stop.store(false, std::memory_order_release);
  for (int id = 0; id < kNumThreads; id++) {
    mt.counter[id].store(0, std::memory_order_release);
    mt.thread_done[id].store(false, std::memory_order_release);
  }

  // Start threads
  MTThread thread[kNumThreads];
  for (int id = 0; id < kNumThreads; id++) {
    thread[id].state = &mt;
    thread[id].id = id;
    env_->StartThread(MTThreadBody, &thread[id]);
  }

  // Let them run for a while
  env_->SleepForMicroseconds(kTestSeconds * 1000000);

  // Stop the threads and wait for them to finish
  mt.stop.store(true, std::memory_order_release);
  for (int id = 0; id < kNumThreads; id++) {
    while (mt.thread_done[id].load(std::memory_order_acquire) == false) {
      env_->SleepForMicroseconds(100000);
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    MultiThreaded, MultiThreadedDBTest,
    ::testing::ValuesIn(MultiThreadedDBTest::GenerateOptionConfigs()));

// Group commit test:
namespace {

static const int kGCNumThreads = 4;
static const int kGCNumKeys = 1000;

struct GCThread {
  DB* db;
  int id;
  std::atomic<bool> done;
};

static void GCThreadBody(void* arg) {
  GCThread* t = reinterpret_cast<GCThread*>(arg);
  int id = t->id;
  DB* db = t->db;
  WriteOptions wo;

  for (int i = 0; i < kGCNumKeys; ++i) {
    std::string kv(ToString(i + id * kGCNumKeys));
    ASSERT_OK(db->Put(wo, kv, kv));
  }
  t->done = true;
}

}  // namespace

TEST_F(DBTest, GroupCommitTest) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    env_->log_write_slowdown_.store(100);
    options.statistics = rocksdb::CreateDBStatistics();
    Reopen(options);

    // Start threads
    GCThread thread[kGCNumThreads];
    for (int id = 0; id < kGCNumThreads; id++) {
      thread[id].id = id;
      thread[id].db = db_;
      thread[id].done = false;
      env_->StartThread(GCThreadBody, &thread[id]);
    }

    for (int id = 0; id < kGCNumThreads; id++) {
      while (thread[id].done == false) {
        env_->SleepForMicroseconds(100000);
      }
    }
    env_->log_write_slowdown_.store(0);

    ASSERT_GT(TestGetTickerCount(options, WRITE_DONE_BY_OTHER), 0);

    std::vector<std::string> expected_db;
    for (int i = 0; i < kGCNumThreads * kGCNumKeys; ++i) {
      expected_db.push_back(ToString(i));
    }
    sort(expected_db.begin(), expected_db.end());

    Iterator* itr = db_->NewIterator(ReadOptions());
    itr->SeekToFirst();
    for (auto x : expected_db) {
      ASSERT_TRUE(itr->Valid());
      ASSERT_EQ(itr->key().ToString(), x);
      ASSERT_EQ(itr->value().ToString(), x);
      itr->Next();
    }
    ASSERT_TRUE(!itr->Valid());
    delete itr;

  } while (ChangeOptions(kSkipNoSeekToLast));
}

namespace {
typedef std::map<std::string, std::string> KVMap;
}

class ModelDB: public DB {
 public:
  class ModelSnapshot : public Snapshot {
   public:
    KVMap map_;

    virtual SequenceNumber GetSequenceNumber() const override {
      // no need to call this
      assert(false);
      return 0;
    }
  };

  explicit ModelDB(const Options& options) : options_(options) {}
  using DB::Put;
  virtual Status Put(const WriteOptions& o, ColumnFamilyHandle* cf,
                     const Slice& k, const Slice& v) override {
    WriteBatch batch;
    batch.Put(cf, k, v);
    return Write(o, &batch);
  }
  using DB::Merge;
  virtual Status Merge(const WriteOptions& o, ColumnFamilyHandle* cf,
                       const Slice& k, const Slice& v) override {
    WriteBatch batch;
    batch.Merge(cf, k, v);
    return Write(o, &batch);
  }
  using DB::Delete;
  virtual Status Delete(const WriteOptions& o, ColumnFamilyHandle* cf,
                        const Slice& key) override {
    WriteBatch batch;
    batch.Delete(cf, key);
    return Write(o, &batch);
  }
  using DB::Get;
  virtual Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                     const Slice& key, std::string* value) override {
    return Status::NotSupported(key);
  }

  using DB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override {
    std::vector<Status> s(keys.size(),
                          Status::NotSupported("Not implemented."));
    return s;
  }

  using DB::GetPropertiesOfAllTables;
  virtual Status GetPropertiesOfAllTables(
      ColumnFamilyHandle* column_family,
      TablePropertiesCollection* props) override {
    return Status();
  }

  using DB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) override {
    if (value_found != nullptr) {
      *value_found = false;
    }
    return true; // Not Supported directly
  }
  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override {
    if (options.snapshot == nullptr) {
      KVMap* saved = new KVMap;
      *saved = map_;
      return new ModelIter(saved, true);
    } else {
      const KVMap* snapshot_state =
          &(reinterpret_cast<const ModelSnapshot*>(options.snapshot)->map_);
      return new ModelIter(snapshot_state, false);
    }
  }
  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      std::vector<Iterator*>* iterators) override {
    return Status::NotSupported("Not supported yet");
  }
  virtual const Snapshot* GetSnapshot() override {
    ModelSnapshot* snapshot = new ModelSnapshot;
    snapshot->map_ = map_;
    return snapshot;
  }

  virtual void ReleaseSnapshot(const Snapshot* snapshot) override {
    delete reinterpret_cast<const ModelSnapshot*>(snapshot);
  }

  virtual Status Write(const WriteOptions& options,
                       WriteBatch* batch) override {
    class Handler : public WriteBatch::Handler {
     public:
      KVMap* map_;
      virtual void Put(const Slice& key, const Slice& value) override {
        (*map_)[key.ToString()] = value.ToString();
      }
      virtual void Merge(const Slice& key, const Slice& value) override {
        // ignore merge for now
        //(*map_)[key.ToString()] = value.ToString();
      }
      virtual void Delete(const Slice& key) override {
        map_->erase(key.ToString());
      }
    };
    Handler handler;
    handler.map_ = &map_;
    return batch->Iterate(&handler);
  }

  using DB::GetProperty;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const Slice& property, std::string* value) override {
    return false;
  }
  using DB::GetIntProperty;
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const Slice& property, uint64_t* value) override {
    return false;
  }
  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(ColumnFamilyHandle* column_family,
                                   const Range* range, int n,
                                   uint64_t* sizes) override {
    for (int i = 0; i < n; i++) {
      sizes[i] = 0;
    }
  }
  using DB::CompactRange;
  virtual Status CompactRange(ColumnFamilyHandle* column_family,
                              const Slice* start, const Slice* end,
                              bool reduce_level, int target_level,
                              uint32_t output_path_id) override {
    return Status::NotSupported("Not supported operation.");
  }

  using DB::CompactFiles;
  virtual Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names,
      const int output_level, const int output_path_id = -1) override {
    return Status::NotSupported("Not supported operation.");
  }

  using DB::NumberLevels;
  virtual int NumberLevels(ColumnFamilyHandle* column_family) override {
    return 1;
  }

  using DB::MaxMemCompactionLevel;
  virtual int MaxMemCompactionLevel(
      ColumnFamilyHandle* column_family) override {
    return 1;
  }

  using DB::Level0StopWriteTrigger;
  virtual int Level0StopWriteTrigger(
      ColumnFamilyHandle* column_family) override {
    return -1;
  }

  virtual const std::string& GetName() const override { return name_; }

  virtual Env* GetEnv() const override { return nullptr; }

  using DB::GetOptions;
  virtual const Options& GetOptions(
      ColumnFamilyHandle* column_family) const override {
    return options_;
  }

  using DB::Flush;
  virtual Status Flush(const rocksdb::FlushOptions& options,
                       ColumnFamilyHandle* column_family) override {
    Status ret;
    return ret;
  }

  virtual Status DisableFileDeletions() override { return Status::OK(); }
  virtual Status EnableFileDeletions(bool force) override {
    return Status::OK();
  }
  virtual Status GetLiveFiles(std::vector<std::string>&, uint64_t* size,
                              bool flush_memtable = true) override {
    return Status::OK();
  }

  virtual Status GetSortedWalFiles(VectorLogPtr& files) override {
    return Status::OK();
  }

  virtual Status DeleteFile(std::string name) override { return Status::OK(); }

  virtual Status GetDbIdentity(std::string& identity) override {
    return Status::OK();
  }

  virtual SequenceNumber GetLatestSequenceNumber() const override { return 0; }
  virtual Status GetUpdatesSince(
      rocksdb::SequenceNumber, unique_ptr<rocksdb::TransactionLogIterator>*,
      const TransactionLogIterator::ReadOptions&
          read_options = TransactionLogIterator::ReadOptions()) override {
    return Status::NotSupported("Not supported in Model DB");
  }

  virtual ColumnFamilyHandle* DefaultColumnFamily() const override {
    return nullptr;
  }

  virtual void GetColumnFamilyMetaData(
      ColumnFamilyHandle* column_family,
      ColumnFamilyMetaData* metadata) override {}

 private:
  class ModelIter: public Iterator {
   public:
    ModelIter(const KVMap* map, bool owned)
        : map_(map), owned_(owned), iter_(map_->end()) {
    }
    ~ModelIter() {
      if (owned_) delete map_;
    }
    virtual bool Valid() const override { return iter_ != map_->end(); }
    virtual void SeekToFirst() override { iter_ = map_->begin(); }
    virtual void SeekToLast() override {
      if (map_->empty()) {
        iter_ = map_->end();
      } else {
        iter_ = map_->find(map_->rbegin()->first);
      }
    }
    virtual void Seek(const Slice& k) override {
      iter_ = map_->lower_bound(k.ToString());
    }
    virtual void Next() override { ++iter_; }
    virtual void Prev() override {
      if (iter_ == map_->begin()) {
        iter_ = map_->end();
        return;
      }
      --iter_;
    }

    virtual Slice key() const override { return iter_->first; }
    virtual Slice value() const override { return iter_->second; }
    virtual Status status() const override { return Status::OK(); }

   private:
    const KVMap* const map_;
    const bool owned_;  // Do we own map_
    KVMap::const_iterator iter_;
  };
  const Options options_;
  KVMap map_;
  std::string name_ = "";
};

static std::string RandomKey(Random* rnd, int minimum = 0) {
  int len;
  do {
    len = (rnd->OneIn(3)
           ? 1                // Short sometimes to encourage collisions
           : (rnd->OneIn(100) ? rnd->Skewed(10) : rnd->Uniform(10)));
  } while (len < minimum);
  return test::RandomKey(rnd, len);
}

static bool CompareIterators(int step,
                             DB* model,
                             DB* db,
                             const Snapshot* model_snap,
                             const Snapshot* db_snap) {
  ReadOptions options;
  options.snapshot = model_snap;
  Iterator* miter = model->NewIterator(options);
  options.snapshot = db_snap;
  Iterator* dbiter = db->NewIterator(options);
  bool ok = true;
  int count = 0;
  for (miter->SeekToFirst(), dbiter->SeekToFirst();
       ok && miter->Valid() && dbiter->Valid();
       miter->Next(), dbiter->Next()) {
    count++;
    if (miter->key().compare(dbiter->key()) != 0) {
      fprintf(stderr, "step %d: Key mismatch: '%s' vs. '%s'\n",
              step,
              EscapeString(miter->key()).c_str(),
              EscapeString(dbiter->key()).c_str());
      ok = false;
      break;
    }

    if (miter->value().compare(dbiter->value()) != 0) {
      fprintf(stderr, "step %d: Value mismatch for key '%s': '%s' vs. '%s'\n",
              step,
              EscapeString(miter->key()).c_str(),
              EscapeString(miter->value()).c_str(),
              EscapeString(miter->value()).c_str());
      ok = false;
    }
  }

  if (ok) {
    if (miter->Valid() != dbiter->Valid()) {
      fprintf(stderr, "step %d: Mismatch at end of iterators: %d vs. %d\n",
              step, miter->Valid(), dbiter->Valid());
      ok = false;
    }
  }
  delete miter;
  delete dbiter;
  return ok;
}

TEST_F(DBTest, Randomized) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Random rnd(test::RandomSeed());
  do {
    ModelDB model(CurrentOptions(options_override));
    const int N = 10000;
    const Snapshot* model_snap = nullptr;
    const Snapshot* db_snap = nullptr;
    std::string k, v;
    for (int step = 0; step < N; step++) {
      // TODO(sanjay): Test Get() works
      int p = rnd.Uniform(100);
      int minimum = 0;
      if (option_config_ == kHashSkipList ||
          option_config_ == kHashLinkList ||
          option_config_ == kHashCuckoo ||
          option_config_ == kPlainTableFirstBytePrefix ||
          option_config_ == kBlockBasedTableWithWholeKeyHashIndex ||
          option_config_ == kBlockBasedTableWithPrefixHashIndex) {
        minimum = 1;
      }
      if (p < 45) {                               // Put
        k = RandomKey(&rnd, minimum);
        v = RandomString(&rnd,
                         rnd.OneIn(20)
                         ? 100 + rnd.Uniform(100)
                         : rnd.Uniform(8));
        ASSERT_OK(model.Put(WriteOptions(), k, v));
        ASSERT_OK(db_->Put(WriteOptions(), k, v));

      } else if (p < 90) {                        // Delete
        k = RandomKey(&rnd, minimum);
        ASSERT_OK(model.Delete(WriteOptions(), k));
        ASSERT_OK(db_->Delete(WriteOptions(), k));


      } else {                                    // Multi-element batch
        WriteBatch b;
        const int num = rnd.Uniform(8);
        for (int i = 0; i < num; i++) {
          if (i == 0 || !rnd.OneIn(10)) {
            k = RandomKey(&rnd, minimum);
          } else {
            // Periodically re-use the same key from the previous iter, so
            // we have multiple entries in the write batch for the same key
          }
          if (rnd.OneIn(2)) {
            v = RandomString(&rnd, rnd.Uniform(10));
            b.Put(k, v);
          } else {
            b.Delete(k);
          }
        }
        ASSERT_OK(model.Write(WriteOptions(), &b));
        ASSERT_OK(db_->Write(WriteOptions(), &b));
      }

      if ((step % 100) == 0) {
        // For DB instances that use the hash index + block-based table, the
        // iterator will be invalid right when seeking a non-existent key, right
        // than return a key that is close to it.
        if (option_config_ != kBlockBasedTableWithWholeKeyHashIndex &&
            option_config_ != kBlockBasedTableWithPrefixHashIndex) {
          ASSERT_TRUE(CompareIterators(step, &model, db_, nullptr, nullptr));
          ASSERT_TRUE(CompareIterators(step, &model, db_, model_snap, db_snap));
        }

        // Save a snapshot from each DB this time that we'll use next
        // time we compare things, to make sure the current state is
        // preserved with the snapshot
        if (model_snap != nullptr) model.ReleaseSnapshot(model_snap);
        if (db_snap != nullptr) db_->ReleaseSnapshot(db_snap);


        auto options = CurrentOptions(options_override);
        Reopen(options);
        ASSERT_TRUE(CompareIterators(step, &model, db_, nullptr, nullptr));

        model_snap = model.GetSnapshot();
        db_snap = db_->GetSnapshot();
      }

      if ((step % 2000) == 0) {
        fprintf(stderr,
                "DBTest.Randomized, option ID: %d, step: %d out of %d\n",
                option_config_, step, N);
      }
    }
    if (model_snap != nullptr) model.ReleaseSnapshot(model_snap);
    if (db_snap != nullptr) db_->ReleaseSnapshot(db_snap);
    // skip cuckoo hash as it does not support snapshot.
  } while (ChangeOptions(kSkipDeletesFilterFirst | kSkipNoSeekToLast |
                         kSkipHashCuckoo));
}

TEST_F(DBTest, MultiGetSimple) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    std::vector<Slice> keys({"k1", "k2", "k3", "k4", "k5", "no_key"});

    std::vector<std::string> values(20, "Temporary data to be overwritten");
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);

    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(values[0], "v1");
    ASSERT_EQ(values[1], "v2");
    ASSERT_EQ(values[2], "v3");
    ASSERT_EQ(values[4], "v5");

    ASSERT_OK(s[0]);
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_TRUE(s[3].IsNotFound());
    ASSERT_OK(s[4]);
    ASSERT_TRUE(s[5].IsNotFound());
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, MultiGetEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    // Empty Key Set
    std::vector<Slice> keys;
    std::vector<std::string> values;
    std::vector<ColumnFamilyHandle*> cfs;
    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Empty Key Set
    Options options = CurrentOptions();
    options.create_if_missing = true;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Search for Keys
    keys.resize(2);
    keys[0] = "a";
    keys[1] = "b";
    cfs.push_back(handles_[0]);
    cfs.push_back(handles_[1]);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ((int)s.size(), 2);
    ASSERT_TRUE(s[0].IsNotFound() && s[1].IsNotFound());
  } while (ChangeCompactOptions());
}

namespace {
void PrefixScanInit(DBTest *dbtest) {
  char buf[100];
  std::string keystr;
  const int small_range_sstfiles = 5;
  const int big_range_sstfiles = 5;

  // Generate 11 sst files with the following prefix ranges.
  // GROUP 0: [0,10]                              (level 1)
  // GROUP 1: [1,2], [2,3], [3,4], [4,5], [5, 6]  (level 0)
  // GROUP 2: [0,6], [0,7], [0,8], [0,9], [0,10]  (level 0)
  //
  // A seek with the previous API would do 11 random I/Os (to all the
  // files).  With the new API and a prefix filter enabled, we should
  // only do 2 random I/O, to the 2 files containing the key.

  // GROUP 0
  snprintf(buf, sizeof(buf), "%02d______:start", 0);
  keystr = std::string(buf);
  ASSERT_OK(dbtest->Put(keystr, keystr));
  snprintf(buf, sizeof(buf), "%02d______:end", 10);
  keystr = std::string(buf);
  ASSERT_OK(dbtest->Put(keystr, keystr));
  dbtest->Flush();
  dbtest->dbfull()->CompactRange(nullptr, nullptr); // move to level 1

  // GROUP 1
  for (int i = 1; i <= small_range_sstfiles; i++) {
    snprintf(buf, sizeof(buf), "%02d______:start", i);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    snprintf(buf, sizeof(buf), "%02d______:end", i+1);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    dbtest->Flush();
  }

  // GROUP 2
  for (int i = 1; i <= big_range_sstfiles; i++) {
    snprintf(buf, sizeof(buf), "%02d______:start", 0);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    snprintf(buf, sizeof(buf), "%02d______:end",
             small_range_sstfiles+i+1);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    dbtest->Flush();
  }
}
}  // namespace

TEST_F(DBTest, PrefixScan) {
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip,
             kSkipNoPrefix);
  while (ChangeFilterOptions()) {
    int count;
    Slice prefix;
    Slice key;
    char buf[100];
    Iterator* iter;
    snprintf(buf, sizeof(buf), "03______:");
    prefix = Slice(buf, 8);
    key = Slice(buf, 9);
    // db configs
    env_->count_random_reads_ = true;
    Options options = CurrentOptions();
    options.env = env_;
    options.prefix_extractor.reset(NewFixedPrefixTransform(8));
    options.disable_auto_compactions = true;
    options.max_background_compactions = 2;
    options.create_if_missing = true;
    options.memtable_factory.reset(NewHashSkipListRepFactory(16));

    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    table_options.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // 11 RAND I/Os
    DestroyAndReopen(options);
    PrefixScanInit(this);
    count = 0;
    env_->random_read_counter_.Reset();
    iter = db_->NewIterator(ReadOptions());
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
      if (! iter->key().starts_with(prefix)) {
        break;
      }
      count++;
    }
    ASSERT_OK(iter->status());
    delete iter;
    ASSERT_EQ(count, 2);
    ASSERT_EQ(env_->random_read_counter_.Read(), 2);
    Close();
  }  // end of while
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip, 0);
}

TEST_F(DBTest, TailingIteratorSingle) {
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  ASSERT_TRUE(!iter->Valid());

  // add a record and check that iter can see it
  ASSERT_OK(db_->Put(WriteOptions(), "mirko", "fodor"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "mirko");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
}

TEST_F(DBTest, TailingIteratorKeepAdding) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  std::string value(1024, 'a');

  const int num_records = 10000;
  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%016d", i);

    Slice key(buf, 16);
    ASSERT_OK(Put(1, key, value));

    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST_F(DBTest, TailingIteratorSeekToNext) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  std::string value(1024, 'a');

  const int num_records = 1000;
  for (int i = 1; i < num_records; ++i) {
    char buf1[32];
    char buf2[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
    }

    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
  for (int i = 2 * num_records; i > 0; --i) {
    char buf1[32];
    char buf2[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
    }

    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST_F(DBTest, TailingIteratorDeletes) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));

  // write a single record, read it using the iterator, then delete it
  ASSERT_OK(Put(1, "0test", "test"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0test");
  ASSERT_OK(Delete(1, "0test"));

  // write many more records
  const int num_records = 10000;
  std::string value(1024, 'A');

  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "1%015d", i);

    Slice key(buf, 16);
    ASSERT_OK(Put(1, key, value));
  }

  // force a flush to make sure that no records are read from memtable
  ASSERT_OK(Flush(1));

  // skip "0test"
  iter->Next();

  // make sure we can read all new records using the existing iterator
  int count = 0;
  for (; iter->Valid(); iter->Next(), ++count) ;

  ASSERT_EQ(count, num_records);
}

TEST_F(DBTest, TailingIteratorPrefixSeek) {
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip,
             kSkipNoPrefix);
  ReadOptions read_options;
  read_options.tailing = true;

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_factory.reset(NewHashSkipListRepFactory(16));
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  ASSERT_OK(Put(1, "0101", "test"));

  ASSERT_OK(Flush(1));

  ASSERT_OK(Put(1, "0202", "test"));

  // Seek(0102) shouldn't find any records since 0202 has a different prefix
  iter->Seek("0102");
  ASSERT_TRUE(!iter->Valid());

  iter->Seek("0202");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0202");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip, 0);
}

TEST_F(DBTest, TailingIteratorIncomplete) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.read_tier = kBlockCacheTier;

  std::string key("key");
  std::string value("value");

  ASSERT_OK(db_->Put(WriteOptions(), key, value));

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  // we either see the entry or it's not in cache
  ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());

  ASSERT_OK(db_->CompactRange(nullptr, nullptr));
  iter->SeekToFirst();
  // should still be true after compaction
  ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());
}

TEST_F(DBTest, TailingIteratorSeekToSame) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 1000;
  CreateAndReopenWithCF({"pikachu"}, options);

  ReadOptions read_options;
  read_options.tailing = true;

  const int NROWS = 10000;
  // Write rows with keys 00000, 00002, 00004 etc.
  for (int i = 0; i < NROWS; ++i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%05d", 2*i);
    std::string key(buf);
    std::string value("value");
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
  }

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  // Seek to 00001.  We expect to find 00002.
  std::string start_key = "00001";
  iter->Seek(start_key);
  ASSERT_TRUE(iter->Valid());

  std::string found = iter->key().ToString();
  ASSERT_EQ("00002", found);

  // Now seek to the same key.  The iterator should remain in the same
  // position.
  iter->Seek(found);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(found, iter->key().ToString());
}

TEST_F(DBTest, ManagedTailingIteratorSingle) {
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  ASSERT_TRUE(!iter->Valid());

  // add a record and check that iter can see it
  ASSERT_OK(db_->Put(WriteOptions(), "mirko", "fodor"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "mirko");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
}

TEST_F(DBTest, ManagedTailingIteratorKeepAdding) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  std::string value(1024, 'a');

  const int num_records = 10000;
  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%016d", i);

    Slice key(buf, 16);
    ASSERT_OK(Put(1, key, value));

    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST_F(DBTest, ManagedTailingIteratorSeekToNext) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  std::string value(1024, 'a');

  const int num_records = 1000;
  for (int i = 1; i < num_records; ++i) {
    char buf1[32];
    char buf2[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
    }

    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
  for (int i = 2 * num_records; i > 0; --i) {
    char buf1[32];
    char buf2[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
    }

    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST_F(DBTest, ManagedTailingIteratorDeletes) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));

  // write a single record, read it using the iterator, then delete it
  ASSERT_OK(Put(1, "0test", "test"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0test");
  ASSERT_OK(Delete(1, "0test"));

  // write many more records
  const int num_records = 10000;
  std::string value(1024, 'A');

  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "1%015d", i);

    Slice key(buf, 16);
    ASSERT_OK(Put(1, key, value));
  }

  // force a flush to make sure that no records are read from memtable
  ASSERT_OK(Flush(1));

  // skip "0test"
  iter->Next();

  // make sure we can read all new records using the existing iterator
  int count = 0;
  for (; iter->Valid(); iter->Next(), ++count) {
  }

  ASSERT_EQ(count, num_records);
}

TEST_F(DBTest, ManagedTailingIteratorPrefixSeek) {
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip,
             kSkipNoPrefix);
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_factory.reset(NewHashSkipListRepFactory(16));
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  ASSERT_OK(Put(1, "0101", "test"));

  ASSERT_OK(Flush(1));

  ASSERT_OK(Put(1, "0202", "test"));

  // Seek(0102) shouldn't find any records since 0202 has a different prefix
  iter->Seek("0102");
  ASSERT_TRUE(!iter->Valid());

  iter->Seek("0202");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0202");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip, 0);
}

TEST_F(DBTest, ManagedTailingIteratorIncomplete) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;
  read_options.read_tier = kBlockCacheTier;

  std::string key = "key";
  std::string value = "value";

  ASSERT_OK(db_->Put(WriteOptions(), key, value));

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  // we either see the entry or it's not in cache
  ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());

  ASSERT_OK(db_->CompactRange(nullptr, nullptr));
  iter->SeekToFirst();
  // should still be true after compaction
  ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());
}

TEST_F(DBTest, ManagedTailingIteratorSeekToSame) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 1000;
  CreateAndReopenWithCF({"pikachu"}, options);

  ReadOptions read_options;
  read_options.tailing = true;
  read_options.managed = true;

  const int NROWS = 10000;
  // Write rows with keys 00000, 00002, 00004 etc.
  for (int i = 0; i < NROWS; ++i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%05d", 2 * i);
    std::string key(buf);
    std::string value("value");
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
  }

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  // Seek to 00001.  We expect to find 00002.
  std::string start_key = "00001";
  iter->Seek(start_key);
  ASSERT_TRUE(iter->Valid());

  std::string found = iter->key().ToString();
  ASSERT_EQ("00002", found);

  // Now seek to the same key.  The iterator should remain in the same
  // position.
  iter->Seek(found);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(found, iter->key().ToString());
}

TEST_F(DBTest, BlockBasedTablePrefixIndexTest) {
  // create a DB with block prefix index
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));


  Reopen(options);
  ASSERT_OK(Put("k1", "v1"));
  Flush();
  ASSERT_OK(Put("k2", "v2"));

  // Reopen it without prefix extractor, make sure everything still works.
  // RocksDB should just fall back to the binary index.
  table_options.index_type = BlockBasedTableOptions::kBinarySearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset();

  Reopen(options);
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("v2", Get("k2"));
}

TEST_F(DBTest, ChecksumTest) {
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Flush());  // table with crc checksum

  table_options.checksum = kxxHash;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_OK(Put("e", "f"));
  ASSERT_OK(Put("g", "h"));
  ASSERT_OK(Flush());  // table with xxhash checksum

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_EQ("b", Get("a"));
  ASSERT_EQ("d", Get("c"));
  ASSERT_EQ("f", Get("e"));
  ASSERT_EQ("h", Get("g"));

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_EQ("b", Get("a"));
  ASSERT_EQ("d", Get("c"));
  ASSERT_EQ("f", Get("e"));
  ASSERT_EQ("h", Get("g"));
}

TEST_F(DBTest, FIFOCompactionTest) {
  for (int iter = 0; iter < 2; ++iter) {
    // first iteration -- auto compaction
    // second iteration -- manual compaction
    Options options;
    options.compaction_style = kCompactionStyleFIFO;
    options.write_buffer_size = 100 << 10;                             // 100KB
    options.compaction_options_fifo.max_table_files_size = 500 << 10;  // 500KB
    options.compression = kNoCompression;
    options.create_if_missing = true;
    if (iter == 1) {
      options.disable_auto_compactions = true;
    }
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    Random rnd(301);
    for (int i = 0; i < 6; ++i) {
      for (int j = 0; j < 100; ++j) {
        ASSERT_OK(Put(ToString(i * 100 + j), RandomString(&rnd, 1024)));
      }
      // flush should happen here
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    if (iter == 0) {
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    } else {
      ASSERT_OK(db_->CompactRange(nullptr, nullptr));
    }
    // only 5 files should survive
    ASSERT_EQ(NumTableFilesAtLevel(0), 5);
    for (int i = 0; i < 50; ++i) {
      // these keys should be deleted in previous compaction
      ASSERT_EQ("NOT_FOUND", Get(ToString(i)));
    }
  }
}

TEST_F(DBTest, SimpleWriteTimeoutTest) {
  // Block compaction thread, which will also block the flushes because
  // max_background_flushes == 0, so flushes are getting executed by the
  // compaction thread
  env_->SetBackgroundThreads(1, Env::LOW);
  SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.write_buffer_size = 100000;
  options.max_background_flushes = 0;
  options.max_write_buffer_number = 2;
  options.max_total_wal_size = std::numeric_limits<uint64_t>::max();
  WriteOptions write_opt;
  write_opt.timeout_hint_us = 0;
  DestroyAndReopen(options);
  // fill the two write buffers
  ASSERT_OK(Put(Key(1), Key(1) + std::string(100000, 'v'), write_opt));
  ASSERT_OK(Put(Key(2), Key(2) + std::string(100000, 'v'), write_opt));
  // As the only two write buffers are full in this moment, the third
  // Put is expected to be timed-out.
  write_opt.timeout_hint_us = 50;
  ASSERT_TRUE(
      Put(Key(3), Key(3) + std::string(100000, 'v'), write_opt).IsTimedOut());

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

// Multi-threaded Timeout Test
namespace {

static const int kValueSize = 1000;
static const int kWriteBufferSize = 100000;

struct TimeoutWriterState {
  int id;
  DB* db;
  std::atomic<bool> done;
  std::map<int, std::string> success_kvs;
};

static void RandomTimeoutWriter(void* arg) {
  TimeoutWriterState* state = reinterpret_cast<TimeoutWriterState*>(arg);
  static const uint64_t kTimerBias = 50;
  int thread_id = state->id;
  DB* db = state->db;

  Random rnd(1000 + thread_id);
  WriteOptions write_opt;
  write_opt.timeout_hint_us = 500;
  int timeout_count = 0;
  int num_keys = kNumKeys * 5;

  for (int k = 0; k < num_keys; ++k) {
    int key = k + thread_id * num_keys;
    std::string value = RandomString(&rnd, kValueSize);
    // only the second-half is randomized
    if (k > num_keys / 2) {
      switch (rnd.Next() % 5) {
        case 0:
          write_opt.timeout_hint_us = 500 * thread_id;
          break;
        case 1:
          write_opt.timeout_hint_us = num_keys - k;
          break;
        case 2:
          write_opt.timeout_hint_us = 1;
          break;
        default:
          write_opt.timeout_hint_us = 0;
          state->success_kvs.insert({key, value});
      }
    }

    uint64_t time_before_put = db->GetEnv()->NowMicros();
    Status s = db->Put(write_opt, Key(key), value);
    uint64_t put_duration = db->GetEnv()->NowMicros() - time_before_put;
    if (write_opt.timeout_hint_us == 0 ||
        put_duration + kTimerBias < write_opt.timeout_hint_us) {
      ASSERT_OK(s);
    }
    if (s.IsTimedOut()) {
      timeout_count++;
      ASSERT_GT(put_duration + kTimerBias, write_opt.timeout_hint_us);
    }
  }

  state->done = true;
}

TEST_F(DBTest, MTRandomTimeoutTest) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.max_write_buffer_number = 2;
  options.compression = kNoCompression;
  options.level0_slowdown_writes_trigger = 10;
  options.level0_stop_writes_trigger = 20;
  options.write_buffer_size = kWriteBufferSize;
  DestroyAndReopen(options);

  TimeoutWriterState thread_states[kNumThreads];
  for (int tid = 0; tid < kNumThreads; ++tid) {
    thread_states[tid].id = tid;
    thread_states[tid].db = db_;
    thread_states[tid].done = false;
    env_->StartThread(RandomTimeoutWriter, &thread_states[tid]);
  }

  for (int tid = 0; tid < kNumThreads; ++tid) {
    while (thread_states[tid].done == false) {
      env_->SleepForMicroseconds(100000);
    }
  }

  Flush();

  for (int tid = 0; tid < kNumThreads; ++tid) {
    auto& success_kvs = thread_states[tid].success_kvs;
    for (auto it = success_kvs.begin(); it != success_kvs.end(); ++it) {
      ASSERT_EQ(Get(Key(it->first)), it->second);
    }
  }
}

TEST_F(DBTest, Level0StopWritesTest) {
  Options options = CurrentOptions();
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 4;
  options.disable_auto_compactions = true;
  options.max_mem_compaction_level = 0;
  Reopen(options);

  // create 4 level0 tables
  for (int i = 0; i < 4; ++i) {
    Put("a", "b");
    Flush();
  }

  WriteOptions woptions;
  woptions.timeout_hint_us = 30 * 1000;  // 30 ms
  Status s = Put("a", "b", woptions);
  ASSERT_TRUE(s.IsTimedOut());
}

}  // anonymous namespace

/*
 * This test is not reliable enough as it heavily depends on disk behavior.
 */
TEST_F(DBTest, RateLimitingTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 1 << 20;         // 1MB
  options.level0_file_num_compaction_trigger = 2;
  options.target_file_size_base = 1 << 20;     // 1MB
  options.max_bytes_for_level_base = 4 << 20;  // 4MB
  options.max_bytes_for_level_multiplier = 4;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options.env = env_;
  options.IncreaseParallelism(4);
  DestroyAndReopen(options);

  WriteOptions wo;
  wo.disableWAL = true;

  // # no rate limiting
  Random rnd(301);
  uint64_t start = env_->NowMicros();
  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(Put(RandomString(&rnd, 32),
                  RandomString(&rnd, (1 << 10) + 1), wo));
  }
  uint64_t elapsed = env_->NowMicros() - start;
  double raw_rate = env_->bytes_written_ * 1000000 / elapsed;
  Close();

  // # rate limiting with 0.7 x threshold
  options.rate_limiter.reset(
    NewGenericRateLimiter(static_cast<int64_t>(0.7 * raw_rate)));
  env_->bytes_written_ = 0;
  DestroyAndReopen(options);

  start = env_->NowMicros();
  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(Put(RandomString(&rnd, 32),
                  RandomString(&rnd, (1 << 10) + 1), wo));
  }
  elapsed = env_->NowMicros() - start;
  Close();
  ASSERT_TRUE(options.rate_limiter->GetTotalBytesThrough() ==
              env_->bytes_written_);
  double ratio = env_->bytes_written_ * 1000000 / elapsed / raw_rate;
  fprintf(stderr, "write rate ratio = %.2lf, expected 0.7\n", ratio);
  ASSERT_TRUE(ratio < 0.8);

  // # rate limiting with half of the raw_rate
  options.rate_limiter.reset(
    NewGenericRateLimiter(static_cast<int64_t>(raw_rate / 2)));
  env_->bytes_written_ = 0;
  DestroyAndReopen(options);

  start = env_->NowMicros();
  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(Put(RandomString(&rnd, 32),
                  RandomString(&rnd, (1 << 10) + 1), wo));
  }
  elapsed = env_->NowMicros() - start;
  Close();
  ASSERT_TRUE(options.rate_limiter->GetTotalBytesThrough() ==
              env_->bytes_written_);
  ratio = env_->bytes_written_ * 1000000 / elapsed / raw_rate;
  fprintf(stderr, "write rate ratio = %.2lf, expected 0.5\n", ratio);
  ASSERT_TRUE(ratio < 0.6);
}

namespace {
  bool HaveOverlappingKeyRanges(
      const Comparator* c,
      const SstFileMetaData& a, const SstFileMetaData& b) {
    if (c->Compare(a.smallestkey, b.smallestkey) >= 0) {
      if (c->Compare(a.smallestkey, b.largestkey) <= 0) {
        // b.smallestkey <= a.smallestkey <= b.largestkey
        return true;
      }
    } else if (c->Compare(a.largestkey, b.smallestkey) >= 0) {
      // a.smallestkey < b.smallestkey <= a.largestkey
      return true;
    }
    if (c->Compare(a.largestkey, b.largestkey) <= 0) {
      if (c->Compare(a.largestkey, b.smallestkey) >= 0) {
        // b.smallestkey <= a.largestkey <= b.largestkey
        return true;
      }
    } else if (c->Compare(a.smallestkey, b.largestkey) <= 0) {
      // a.smallestkey <= b.largestkey < a.largestkey
      return true;
    }
    return false;
  }

  // Identifies all files between level "min_level" and "max_level"
  // which has overlapping key range with "input_file_meta".
  void GetOverlappingFileNumbersForLevelCompaction(
      const ColumnFamilyMetaData& cf_meta,
      const Comparator* comparator,
      int min_level, int max_level,
      const SstFileMetaData* input_file_meta,
      std::set<std::string>* overlapping_file_names) {
    std::set<const SstFileMetaData*> overlapping_files;
    overlapping_files.insert(input_file_meta);
    for (int m = min_level; m <= max_level; ++m) {
      for (auto& file : cf_meta.levels[m].files) {
        for (auto* included_file : overlapping_files) {
          if (HaveOverlappingKeyRanges(
                  comparator, *included_file, file)) {
            overlapping_files.insert(&file);
            overlapping_file_names->insert(file.name);
            break;
          }
        }
      }
    }
  }

  void VerifyCompactionResult(
      const ColumnFamilyMetaData& cf_meta,
      const std::set<std::string>& overlapping_file_numbers) {
#ifndef NDEBUG
    for (auto& level : cf_meta.levels) {
      for (auto& file : level.files) {
        assert(overlapping_file_numbers.find(file.name) ==
               overlapping_file_numbers.end());
      }
    }
#endif
  }

  const SstFileMetaData* PickFileRandomly(
      const ColumnFamilyMetaData& cf_meta,
      Random* rand,
      int* level = nullptr) {
    auto file_id = rand->Uniform(static_cast<int>(
        cf_meta.file_count)) + 1;
    for (auto& level_meta : cf_meta.levels) {
      if (file_id <= level_meta.files.size()) {
        if (level != nullptr) {
          *level = level_meta.level;
        }
        auto result = rand->Uniform(file_id);
        return &(level_meta.files[result]);
      }
      file_id -= level_meta.files.size();
    }
    assert(false);
    return nullptr;
  }
}  // namespace

// TODO t6534343 -- Don't run two level 0 CompactFiles concurrently
TEST_F(DBTest, DISABLED_CompactFilesOnLevelCompaction) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 100;
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base = options.target_file_size_base * 2;
  options.level0_stop_writes_trigger = 2;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  for (int key = 64 * kEntriesPerBuffer; key >= 0; --key) {
    ASSERT_OK(Put(1, ToString(key), RandomString(&rnd, kTestValueSize)));
  }
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  dbfull()->TEST_WaitForCompact();

  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  int output_level = static_cast<int>(cf_meta.levels.size()) - 1;
  for (int file_picked = 5; file_picked > 0; --file_picked) {
    std::set<std::string> overlapping_file_names;
    std::vector<std::string> compaction_input_file_names;
    for (int f = 0; f < file_picked; ++f) {
      int level;
      auto file_meta = PickFileRandomly(cf_meta, &rnd, &level);
      compaction_input_file_names.push_back(file_meta->name);
      GetOverlappingFileNumbersForLevelCompaction(
          cf_meta, options.comparator, level, output_level,
          file_meta, &overlapping_file_names);
    }

    ASSERT_OK(dbfull()->CompactFiles(
        CompactionOptions(), handles_[1],
        compaction_input_file_names,
        output_level));

    // Make sure all overlapping files do not exist after compaction
    dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
    VerifyCompactionResult(cf_meta, overlapping_file_names);
  }

  // make sure all key-values are still there.
  for (int key = 64 * kEntriesPerBuffer; key >= 0; --key) {
    ASSERT_NE(Get(1, ToString(key)), "NOT_FOUND");
  }
}

TEST_F(DBTest, CompactFilesOnUniversalCompaction) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 10;

  ChangeCompactOptions();
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.num_levels = 1;
  options.target_file_size_base = options.write_buffer_size;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(options.compaction_style, kCompactionStyleUniversal);
  Random rnd(301);
  for (int key = 1024 * kEntriesPerBuffer; key >= 0; --key) {
    ASSERT_OK(Put(1, ToString(key), RandomString(&rnd, kTestValueSize)));
  }
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  dbfull()->TEST_WaitForCompact();
  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  std::vector<std::string> compaction_input_file_names;
  for (auto file : cf_meta.levels[0].files) {
    if (rnd.OneIn(2)) {
      compaction_input_file_names.push_back(file.name);
    }
  }

  if (compaction_input_file_names.size() == 0) {
    compaction_input_file_names.push_back(
        cf_meta.levels[0].files[0].name);
  }

  // expect fail since universal compaction only allow L0 output
  ASSERT_TRUE(!dbfull()->CompactFiles(
      CompactionOptions(), handles_[1],
      compaction_input_file_names, 1).ok());

  // expect ok and verify the compacted files no longer exist.
  ASSERT_OK(dbfull()->CompactFiles(
      CompactionOptions(), handles_[1],
      compaction_input_file_names, 0));

  dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  VerifyCompactionResult(
      cf_meta,
      std::set<std::string>(compaction_input_file_names.begin(),
          compaction_input_file_names.end()));

  compaction_input_file_names.clear();

  // Pick the first and the last file, expect everything is
  // compacted into one single file.
  compaction_input_file_names.push_back(
      cf_meta.levels[0].files[0].name);
  compaction_input_file_names.push_back(
      cf_meta.levels[0].files[
          cf_meta.levels[0].files.size() - 1].name);
  ASSERT_OK(dbfull()->CompactFiles(
      CompactionOptions(), handles_[1],
      compaction_input_file_names, 0));

  dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  ASSERT_EQ(cf_meta.levels[0].files.size(), 1U);
}

TEST_F(DBTest, TableOptionsSanitizeTest) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  ASSERT_EQ(db_->GetOptions().allow_mmap_reads, false);

  options.table_factory.reset(new PlainTableFactory());
  options.prefix_extractor.reset(NewNoopTransform());
  Destroy(options);
  ASSERT_TRUE(TryReopen(options).IsNotSupported());

  // Test for check of prefix_extractor when hash index is used for
  // block-based table
  BlockBasedTableOptions to;
  to.index_type = BlockBasedTableOptions::kHashSearch;
  options = CurrentOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(to));
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  ASSERT_OK(TryReopen(options));
}

TEST_F(DBTest, SanitizeNumThreads) {
  for (int attempt = 0; attempt < 2; attempt++) {
    const size_t kTotalTasks = 8;
    SleepingBackgroundTask sleeping_tasks[kTotalTasks];

    Options options = CurrentOptions();
    if (attempt == 0) {
      options.max_background_compactions = 3;
      options.max_background_flushes = 2;
    }
    options.create_if_missing = true;
    DestroyAndReopen(options);

    for (size_t i = 0; i < kTotalTasks; i++) {
      // Insert 5 tasks to low priority queue and 5 tasks to high priority queue
      env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_tasks[i],
                     (i < 4) ? Env::Priority::LOW : Env::Priority::HIGH);
    }

    // Wait 100 milliseconds for they are scheduled.
    env_->SleepForMicroseconds(100000);

    // pool size 3, total task 4. Queue size should be 1.
    ASSERT_EQ(1U, options.env->GetThreadPoolQueueLen(Env::Priority::LOW));
    // pool size 2, total task 4. Queue size should be 2.
    ASSERT_EQ(2U, options.env->GetThreadPoolQueueLen(Env::Priority::HIGH));

    for (size_t i = 0; i < kTotalTasks; i++) {
      sleeping_tasks[i].WakeUp();
      sleeping_tasks[i].WaitUntilDone();
    }

    ASSERT_OK(Put("abc", "def"));
    ASSERT_EQ("def", Get("abc"));
    Flush();
    ASSERT_EQ("def", Get("abc"));
  }
}

TEST_F(DBTest, DBIteratorBoundTest) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;

  options.prefix_extractor = nullptr;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing basic case with no iterate_upper_bound and no prefix_extractor
  {
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("g1")), 0);
  }

  // testing iterate_upper_bound and forward iterator
  // to make sure it stops at bound
  {
    ReadOptions ro;
    // iterate_upper_bound points beyond the last expected entry
    Slice prefix("foo2");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("foo1")), 0);

    iter->Next();
    // should stop here...
    ASSERT_TRUE(!iter->Valid());
  }

  // prefix is the first letter of the key
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));

  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing with iterate_upper_bound and prefix_extractor
  // Seek target and iterate_upper_bound are not is same prefix
  // This should be an error
  {
    ReadOptions ro;
    Slice prefix("g1");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(!iter->Valid());
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  // testing that iterate_upper_bound prevents iterating over deleted items
  // if the bound has already reached
  {
    options.prefix_extractor = nullptr;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_OK(Put("b", "0"));
    ASSERT_OK(Put("b1", "0"));
    ASSERT_OK(Put("c", "0"));
    ASSERT_OK(Put("d", "0"));
    ASSERT_OK(Put("e", "0"));
    ASSERT_OK(Delete("c"));
    ASSERT_OK(Delete("d"));

    // base case with no bound
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    perf_context.Reset();
    iter->Next();

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(static_cast<int>(perf_context.internal_delete_skipped_count), 2);

    // now testing with iterate_bound
    Slice prefix("c");
    ro.iterate_upper_bound = &prefix;

    iter.reset(db_->NewIterator(ro));

    perf_context.Reset();

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    iter->Next();
    // the iteration should stop as soon as the the bound key is reached
    // even though the key is deleted
    // hence internal_delete_skipped_count should be 0
    ASSERT_TRUE(!iter->Valid());
    ASSERT_EQ(static_cast<int>(perf_context.internal_delete_skipped_count), 0);
  }
}

TEST_F(DBTest, WriteSingleThreadEntry) {
  std::vector<std::thread> threads;
  dbfull()->TEST_LockMutex();
  auto w = dbfull()->TEST_BeginWrite();
  threads.emplace_back([&] { Put("a", "b"); });
  env_->SleepForMicroseconds(10000);
  threads.emplace_back([&] { Flush(); });
  env_->SleepForMicroseconds(10000);
  dbfull()->TEST_UnlockMutex();
  dbfull()->TEST_LockMutex();
  dbfull()->TEST_EndWrite(w);
  dbfull()->TEST_UnlockMutex();

  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(DBTest, DisableDataSyncTest) {
  env_->sync_counter_.store(0);
  // iter 0 -- no sync
  // iter 1 -- sync
  for (int iter = 0; iter < 2; ++iter) {
    Options options = CurrentOptions();
    options.disableDataSync = iter == 0;
    options.create_if_missing = true;
    options.env = env_;
    Reopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    MakeTables(10, "a", "z");
    Compact("a", "z");

    if (iter == 0) {
      ASSERT_EQ(env_->sync_counter_.load(), 0);
    } else {
      ASSERT_GT(env_->sync_counter_.load(), 0);
    }
    Destroy(options);
  }
}

TEST_F(DBTest, DynamicMemtableOptions) {
  const uint64_t k64KB = 1 << 16;
  const uint64_t k128KB = 1 << 17;
  const uint64_t k5KB = 5 * 1024;
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.max_background_compactions = 1;
  options.max_mem_compaction_level = 0;
  options.write_buffer_size = k64KB;
  options.max_write_buffer_number = 2;
  // Don't trigger compact/slowdown/stop
  options.level0_file_num_compaction_trigger = 1024;
  options.level0_slowdown_writes_trigger = 1024;
  options.level0_stop_writes_trigger = 1024;
  DestroyAndReopen(options);

  auto gen_l0_kb = [this](int size) {
    Random rnd(301);
    for (int i = 0; i < size; i++) {
      ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
    }
    dbfull()->TEST_WaitForFlushMemTable();
  };

  // Test write_buffer_size
  gen_l0_kb(64);
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  ASSERT_LT(SizeAtLevel(0), k64KB + k5KB);
  ASSERT_GT(SizeAtLevel(0), k64KB - k5KB);

  // Clean up L0
  dbfull()->CompactRange(nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  // Increase buffer size
  ASSERT_OK(dbfull()->SetOptions({
    {"write_buffer_size", "131072"},
  }));

  // The existing memtable is still 64KB in size, after it becomes immutable,
  // the next memtable will be 128KB in size. Write 256KB total, we should
  // have a 64KB L0 file, a 128KB L0 file, and a memtable with 64KB data
  gen_l0_kb(256);
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);
  ASSERT_LT(SizeAtLevel(0), k128KB + k64KB + 2 * k5KB);
  ASSERT_GT(SizeAtLevel(0), k128KB + k64KB - 2 * k5KB);

  // Test max_write_buffer_number
  // Block compaction thread, which will also block the flushes because
  // max_background_flushes == 0, so flushes are getting executed by the
  // compaction thread
  env_->SetBackgroundThreads(1, Env::LOW);
  SleepingBackgroundTask sleeping_task_low1;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low1,
                 Env::Priority::LOW);
  // Start from scratch and disable compaction/flush. Flush can only happen
  // during compaction but trigger is pretty high
  options.max_background_flushes = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Put until timeout, bounded by 256 puts. We should see timeout at ~128KB
  int count = 0;
  Random rnd(301);
  WriteOptions wo;
  wo.timeout_hint_us = 100000;  // Reasonabley long timeout to make sure sleep
                                // triggers but not forever.

  std::atomic<int> sleep_count(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DelayWrite:TimedWait",
      [&](void* arg) { sleep_count.fetch_add(1); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  while (Put(Key(count), RandomString(&rnd, 1024), wo).ok() && count < 256) {
    count++;
  }
  ASSERT_GT(sleep_count.load(), 0);
  ASSERT_GT(static_cast<double>(count), 128 * 0.8);
  ASSERT_LT(static_cast<double>(count), 128 * 1.2);

  sleeping_task_low1.WakeUp();
  sleeping_task_low1.WaitUntilDone();

  // Increase
  ASSERT_OK(dbfull()->SetOptions({
    {"max_write_buffer_number", "8"},
  }));
  // Clean up memtable and L0
  dbfull()->CompactRange(nullptr, nullptr);

  SleepingBackgroundTask sleeping_task_low2;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low2,
                 Env::Priority::LOW);
  count = 0;
  sleep_count.store(0);
  while (Put(Key(count), RandomString(&rnd, 1024), wo).ok() && count < 1024) {
    count++;
  }
  ASSERT_GT(sleep_count.load(), 0);
  ASSERT_GT(static_cast<double>(count), 512 * 0.8);
  ASSERT_LT(static_cast<double>(count), 512 * 1.2);
  sleeping_task_low2.WakeUp();
  sleeping_task_low2.WaitUntilDone();

  // Decrease
  ASSERT_OK(dbfull()->SetOptions({
    {"max_write_buffer_number", "4"},
  }));
  // Clean up memtable and L0
  dbfull()->CompactRange(nullptr, nullptr);

  SleepingBackgroundTask sleeping_task_low3;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low3,
                 Env::Priority::LOW);

  count = 0;
  sleep_count.store(0);
  while (Put(Key(count), RandomString(&rnd, 1024), wo).ok() && count < 1024) {
    count++;
  }
  ASSERT_GT(sleep_count.load(), 0);
  ASSERT_GT(static_cast<double>(count), 256 * 0.8);
  ASSERT_LT(static_cast<double>(count), 266 * 1.2);
  sleeping_task_low3.WakeUp();
  sleeping_task_low3.WaitUntilDone();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

#if ROCKSDB_USING_THREAD_STATUS
namespace {
void VerifyOperationCount(Env* env, ThreadStatus::OperationType op_type,
                          int expected_count) {
  int op_count = 0;
  std::vector<ThreadStatus> thread_list;
  ASSERT_OK(env->GetThreadList(&thread_list));
  for (auto thread : thread_list) {
    if (thread.operation_type == op_type) {
      op_count++;
    }
  }
  ASSERT_EQ(op_count, expected_count);
}
}  // namespace

TEST_F(DBTest, GetThreadStatus) {
  Options options;
  options.env = env_;
  options.enable_thread_tracking = true;
  TryReopen(options);

  std::vector<ThreadStatus> thread_list;
  Status s = env_->GetThreadList(&thread_list);

  for (int i = 0; i < 2; ++i) {
    // repeat the test with differet number of high / low priority threads
    const int kTestCount = 3;
    const unsigned int kHighPriCounts[kTestCount] = {3, 2, 5};
    const unsigned int kLowPriCounts[kTestCount] = {10, 15, 3};
    for (int test = 0; test < kTestCount; ++test) {
      // Change the number of threads in high / low priority pool.
      env_->SetBackgroundThreads(kHighPriCounts[test], Env::HIGH);
      env_->SetBackgroundThreads(kLowPriCounts[test], Env::LOW);
      // Wait to ensure the all threads has been registered
      env_->SleepForMicroseconds(100000);
      s = env_->GetThreadList(&thread_list);
      ASSERT_OK(s);
      unsigned int thread_type_counts[ThreadStatus::NUM_THREAD_TYPES];
      memset(thread_type_counts, 0, sizeof(thread_type_counts));
      for (auto thread : thread_list) {
        ASSERT_LT(thread.thread_type, ThreadStatus::NUM_THREAD_TYPES);
        thread_type_counts[thread.thread_type]++;
      }
      // Verify the total number of threades
      ASSERT_EQ(
          thread_type_counts[ThreadStatus::HIGH_PRIORITY] +
              thread_type_counts[ThreadStatus::LOW_PRIORITY],
          kHighPriCounts[test] + kLowPriCounts[test]);
      // Verify the number of high-priority threads
      ASSERT_EQ(
          thread_type_counts[ThreadStatus::HIGH_PRIORITY],
          kHighPriCounts[test]);
      // Verify the number of low-priority threads
      ASSERT_EQ(
          thread_type_counts[ThreadStatus::LOW_PRIORITY],
          kLowPriCounts[test]);
    }
    if (i == 0) {
      // repeat the test with multiple column families
      CreateAndReopenWithCF({"pikachu", "about-to-remove"}, options);
      env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(
          handles_, true);
    }
  }
  db_->DropColumnFamily(handles_[2]);
  delete handles_[2];
  handles_.erase(handles_.begin() + 2);
  env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(
      handles_, true);
  Close();
  env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(
      handles_, true);
}

TEST_F(DBTest, DisableThreadStatus) {
  Options options;
  options.env = env_;
  options.enable_thread_tracking = false;
  TryReopen(options);
  CreateAndReopenWithCF({"pikachu", "about-to-remove"}, options);
  // Verify non of the column family info exists
  env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(
      handles_, false);
}

TEST_F(DBTest, ThreadStatusFlush) {
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  options.enable_thread_tracking = true;
  options = CurrentOptions(options);

  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"FlushJob::FlushJob()", "DBTest::ThreadStatusFlush:1"},
      {"DBTest::ThreadStatusFlush:2", "FlushJob::~FlushJob()"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu"}, options);
  VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 0);

  ASSERT_OK(Put(1, "foo", "v1"));
  ASSERT_EQ("v1", Get(1, "foo"));
  VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 0);

  Put(1, "k1", std::string(100000, 'x'));  // Fill memtable
  VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 0);
  Put(1, "k2", std::string(100000, 'y'));  // Trigger flush
  // wait for flush to be scheduled
  env_->SleepForMicroseconds(250000);
  TEST_SYNC_POINT("DBTest::ThreadStatusFlush:1");
  VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 1);
  TEST_SYNC_POINT("DBTest::ThreadStatusFlush:2");

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest, ThreadStatusSingleCompaction) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 100;
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base = options.target_file_size_base * 2;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  options.env = env_;
  options.enable_thread_tracking = true;
  const int kNumL0Files = 4;
  options.level0_file_num_compaction_trigger = kNumL0Files;

  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DBTest::ThreadStatusSingleCompaction:0", "DBImpl::BGWorkCompaction"},
      {"CompactionJob::Run():Start", "DBTest::ThreadStatusSingleCompaction:1"},
      {"DBTest::ThreadStatusSingleCompaction:2", "CompactionJob::Run():End"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  for (int tests = 0; tests < 2; ++tests) {
    DestroyAndReopen(options);

    Random rnd(301);
    // The Put Phase.
    for (int file = 0; file < kNumL0Files; ++file) {
      for (int key = 0; key < kEntriesPerBuffer; ++key) {
        ASSERT_OK(Put(ToString(key + file * kEntriesPerBuffer),
                      RandomString(&rnd, kTestValueSize)));
      }
      Flush();
    }
    // This makes sure a compaction won't be scheduled until
    // we have done with the above Put Phase.
    TEST_SYNC_POINT("DBTest::ThreadStatusSingleCompaction:0");
    ASSERT_GE(NumTableFilesAtLevel(0),
              options.level0_file_num_compaction_trigger);

    // This makes sure at least one compaction is running.
    TEST_SYNC_POINT("DBTest::ThreadStatusSingleCompaction:1");

    if (options.enable_thread_tracking) {
      // expecting one single L0 to L1 compaction
      VerifyOperationCount(env_, ThreadStatus::OP_COMPACTION, 1);
    } else {
      // If thread tracking is not enabled, compaction count should be 0.
      VerifyOperationCount(env_, ThreadStatus::OP_COMPACTION, 0);
    }
    // TODO(yhchiang): adding assert to verify each compaction stage.
    TEST_SYNC_POINT("DBTest::ThreadStatusSingleCompaction:2");

    // repeat the test with disabling thread tracking.
    options.enable_thread_tracking = false;
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest, PreShutdownManualCompaction) {
  Options options = CurrentOptions();
  options.max_background_flushes = 0;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(dbfull()->MaxMemCompactionLevel(), 2)
      << "Need to update this test to match kMaxMemCompactLevel";

  // iter - 0 with 7 levels
  // iter - 1 with 3 levels
  for (int iter = 0; iter < 2; ++iter) {
    MakeTables(3, "p", "q", 1);
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls before files
    Compact(1, "", "c");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls after files
    Compact(1, "r", "z");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range overlaps files
    Compact(1, "p1", "p9");
    ASSERT_EQ("0,0,1", FilesPerLevel(1));

    // Populate a different range
    MakeTables(3, "c", "e", 1);
    ASSERT_EQ("1,1,2", FilesPerLevel(1));

    // Compact just the new range
    Compact(1, "b", "f");
    ASSERT_EQ("0,0,2", FilesPerLevel(1));

    // Compact all
    MakeTables(1, "a", "z", 1);
    ASSERT_EQ("0,1,2", FilesPerLevel(1));
    CancelAllBackgroundWork(db_);
    db_->CompactRange(handles_[1], nullptr, nullptr);
    ASSERT_EQ("0,1,2", FilesPerLevel(1));

    if (iter == 0) {
      options = CurrentOptions();
      options.max_background_flushes = 0;
      options.num_levels = 3;
      options.create_if_missing = true;
      DestroyAndReopen(options);
      CreateAndReopenWithCF({"pikachu"}, options);
    }
  }
}

TEST_F(DBTest, PreShutdownMultipleCompaction) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 10;
  const int kNumL0Files = 4;

  const int kHighPriCount = 3;
  const int kLowPriCount = 5;
  env_->SetBackgroundThreads(kHighPriCount, Env::HIGH);
  env_->SetBackgroundThreads(kLowPriCount, Env::LOW);

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base =
      options.target_file_size_base * kNumL0Files;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  options.env = env_;
  options.enable_thread_tracking = true;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.max_bytes_for_level_multiplier = 2;
  options.max_background_compactions = kLowPriCount;
  options.level0_stop_writes_trigger = 1 << 10;
  options.level0_slowdown_writes_trigger = 1 << 10;

  TryReopen(options);
  Random rnd(301);

  std::vector<ThreadStatus> thread_list;
  // Delay both flush and compaction
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"FlushJob::FlushJob()", "CompactionJob::Run():Start"},
       {"CompactionJob::Run():Start",
        "DBTest::PreShutdownMultipleCompaction:Preshutdown"},
       {"DBTest::PreShutdownMultipleCompaction:Preshutdown",
        "CompactionJob::Run():End"},
       {"CompactionJob::Run():End",
        "DBTest::PreShutdownMultipleCompaction:VerifyPreshutdown"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Make rocksdb busy
  int key = 0;
  int max_operation_count[ThreadStatus::NUM_OP_TYPES] = {0};
  // check how many threads are doing compaction using GetThreadList
  int operation_count[ThreadStatus::NUM_OP_TYPES] = {0};
  for (int file = 0; file < 8 * kNumL0Files; ++file) {
    for (int k = 0; k < kEntriesPerBuffer; ++k) {
      ASSERT_OK(Put(ToString(key++), RandomString(&rnd, kTestValueSize)));
    }

    Status s = env_->GetThreadList(&thread_list);
    for (auto thread : thread_list) {
      operation_count[thread.operation_type]++;
    }

    // Record the max number of compactions at a time.
    for (int i = 0; i < ThreadStatus::NUM_OP_TYPES; ++i) {
      if (max_operation_count[i] < operation_count[i]) {
        max_operation_count[i] = operation_count[i];
      }
    }
    // Speed up the test
    if (max_operation_count[ThreadStatus::OP_FLUSH] > 1 &&
        max_operation_count[ThreadStatus::OP_COMPACTION] >
            0.6 * options.max_background_compactions) {
      break;
    }
  }

  TEST_SYNC_POINT("DBTest::PreShutdownMultipleCompaction:Preshutdown");
  ASSERT_GE(max_operation_count[ThreadStatus::OP_COMPACTION], 1);
  CancelAllBackgroundWork(db_);
  TEST_SYNC_POINT("DBTest::PreShutdownMultipleCompaction:VerifyPreshutdown");
  dbfull()->TEST_WaitForCompact();
  // Record the number of compactions at a time.
  for (int i = 0; i < ThreadStatus::NUM_OP_TYPES; ++i) {
    operation_count[i] = 0;
  }
  Status s = env_->GetThreadList(&thread_list);
  for (auto thread : thread_list) {
    operation_count[thread.operation_type]++;
  }
  ASSERT_EQ(operation_count[ThreadStatus::OP_COMPACTION], 0);
}

TEST_F(DBTest, PreShutdownCompactionMiddle) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 10;
  const int kNumL0Files = 4;

  const int kHighPriCount = 3;
  const int kLowPriCount = 5;
  env_->SetBackgroundThreads(kHighPriCount, Env::HIGH);
  env_->SetBackgroundThreads(kLowPriCount, Env::LOW);

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base =
      options.target_file_size_base * kNumL0Files;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  options.env = env_;
  options.enable_thread_tracking = true;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.max_bytes_for_level_multiplier = 2;
  options.max_background_compactions = kLowPriCount;
  options.level0_stop_writes_trigger = 1 << 10;
  options.level0_slowdown_writes_trigger = 1 << 10;

  TryReopen(options);
  Random rnd(301);

  std::vector<ThreadStatus> thread_list;
  // Delay both flush and compaction
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBTest::PreShutdownMultipleCompaction:Preshutdown",
        "CompactionJob::Run():Inprogress"},
       {"CompactionJob::Run():Inprogress", "CompactionJob::Run():End"},
       {"CompactionJob::Run():End",
        "DBTest::PreShutdownMultipleCompaction:VerifyPreshutdown"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Make rocksdb busy
  int key = 0;
  int max_operation_count[ThreadStatus::NUM_OP_TYPES] = {0};
  // check how many threads are doing compaction using GetThreadList
  int operation_count[ThreadStatus::NUM_OP_TYPES] = {0};
  for (int file = 0; file < 8 * kNumL0Files; ++file) {
    for (int k = 0; k < kEntriesPerBuffer; ++k) {
      ASSERT_OK(Put(ToString(key++), RandomString(&rnd, kTestValueSize)));
    }

    Status s = env_->GetThreadList(&thread_list);
    for (auto thread : thread_list) {
      operation_count[thread.operation_type]++;
    }

    // Record the max number of compactions at a time.
    for (int i = 0; i < ThreadStatus::NUM_OP_TYPES; ++i) {
      if (max_operation_count[i] < operation_count[i]) {
        max_operation_count[i] = operation_count[i];
      }
    }
    // Speed up the test
    if (max_operation_count[ThreadStatus::OP_FLUSH] > 1 &&
        max_operation_count[ThreadStatus::OP_COMPACTION] >
            0.6 * options.max_background_compactions) {
      break;
    }
  }

  ASSERT_GE(max_operation_count[ThreadStatus::OP_COMPACTION], 1);
  CancelAllBackgroundWork(db_);
  TEST_SYNC_POINT("DBTest::PreShutdownMultipleCompaction:Preshutdown");
  TEST_SYNC_POINT("DBTest::PreShutdownMultipleCompaction:VerifyPreshutdown");
  dbfull()->TEST_WaitForCompact();
  // Record the number of compactions at a time.
  for (int i = 0; i < ThreadStatus::NUM_OP_TYPES; ++i) {
    operation_count[i] = 0;
  }
  Status s = env_->GetThreadList(&thread_list);
  for (auto thread : thread_list) {
    operation_count[thread.operation_type]++;
  }
  ASSERT_EQ(operation_count[ThreadStatus::OP_COMPACTION], 0);
}

#endif  // ROCKSDB_USING_THREAD_STATUS

TEST_F(DBTest, DynamicLevelMaxBytesBase) {
  // Use InMemoryEnv, or it would be too slow.
  unique_ptr<Env> env(new MockEnv(env_));

  const int kNKeys = 1000;
  int keys[kNKeys];

  auto verify_func = [&]() {
    for (int i = 0; i < kNKeys; i++) {
      ASSERT_NE("NOT_FOUND", Get(Key(i)));
      ASSERT_NE("NOT_FOUND", Get(Key(kNKeys * 2 + i)));
      if (i < kNKeys / 10) {
        ASSERT_EQ("NOT_FOUND", Get(Key(kNKeys + keys[i])));
      } else {
        ASSERT_NE("NOT_FOUND", Get(Key(kNKeys + keys[i])));
      }
    }
  };

  Random rnd(301);
  for (int ordered_insert = 0; ordered_insert <= 1; ordered_insert++) {
    for (int i = 0; i < kNKeys; i++) {
      keys[i] = i;
    }
    if (ordered_insert == 0) {
      std::random_shuffle(std::begin(keys), std::end(keys));
    }
    for (int max_background_compactions = 1; max_background_compactions < 4;
         max_background_compactions += 2) {
      Options options;
      options.env = env.get();
      options.create_if_missing = true;
      options.db_write_buffer_size = 2048;
      options.write_buffer_size = 2048;
      options.max_write_buffer_number = 2;
      options.level0_file_num_compaction_trigger = 2;
      options.level0_slowdown_writes_trigger = 2;
      options.level0_stop_writes_trigger = 2;
      options.target_file_size_base = 2048;
      options.level_compaction_dynamic_level_bytes = true;
      options.max_bytes_for_level_base = 10240;
      options.max_bytes_for_level_multiplier = 4;
      options.hard_rate_limit = 1.1;
      options.max_background_compactions = max_background_compactions;
      options.num_levels = 5;

      options.compression_per_level.resize(3);
      options.compression_per_level[0] = kNoCompression;
      options.compression_per_level[1] = kLZ4Compression;
      options.compression_per_level[2] = kSnappyCompression;

      DestroyAndReopen(options);

      for (int i = 0; i < kNKeys; i++) {
        int key = keys[i];
        ASSERT_OK(Put(Key(kNKeys + key), RandomString(&rnd, 102)));
        ASSERT_OK(Put(Key(key), RandomString(&rnd, 102)));
        ASSERT_OK(Put(Key(kNKeys * 2 + key), RandomString(&rnd, 102)));
        ASSERT_OK(Delete(Key(kNKeys + keys[i / 10])));
        env_->SleepForMicroseconds(5000);
      }

      uint64_t int_prop;
      ASSERT_TRUE(db_->GetIntProperty("rocksdb.background-errors", &int_prop));
      ASSERT_EQ(0U, int_prop);

      // Verify DB
      for (int j = 0; j < 2; j++) {
        verify_func();
        if (j == 0) {
          Reopen(options);
        }
      }

      // Test compact range works
      dbfull()->CompactRange(nullptr, nullptr);
      // All data should be in the last level.
      ColumnFamilyMetaData cf_meta;
      db_->GetColumnFamilyMetaData(&cf_meta);
      ASSERT_EQ(5U, cf_meta.levels.size());
      for (int i = 0; i < 4; i++) {
        ASSERT_EQ(0U, cf_meta.levels[i].files.size());
      }
      ASSERT_GT(cf_meta.levels[4U].files.size(), 0U);
      verify_func();

      Close();
    }
  }

  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
}

// Test specific cases in dynamic max bytes
TEST_F(DBTest, DynamicLevelMaxBytesBase2) {
  Random rnd(301);
  int kMaxKey = 1000000;

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.db_write_buffer_size = 2048;
  options.write_buffer_size = 2048;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 9999;
  options.level0_stop_writes_trigger = 9999;
  options.target_file_size_base = 2048;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = 10240;
  options.max_bytes_for_level_multiplier = 4;
  options.max_background_compactions = 2;
  options.num_levels = 5;
  options.expanded_compaction_factor = 0;  // Force not expanding in compactions
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "true"},
  }));

  uint64_t int_prop;
  std::string str_prop;

  // Initial base level is the last level
  ASSERT_TRUE(db_->GetIntProperty("rocksdb.base-level", &int_prop));
  ASSERT_EQ(4U, int_prop);

  // Put about 7K to L0
  for (int i = 0; i < 70; i++) {
    ASSERT_OK(Put(Key(static_cast<int>(rnd.Uniform(kMaxKey))),
                  RandomString(&rnd, 80)));
  }
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "false"},
  }));
  Flush();
  dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(db_->GetIntProperty("rocksdb.base-level", &int_prop));
  ASSERT_EQ(4U, int_prop);

  // Insert extra about 3.5K to L0. After they are compacted to L4, base level
  // should be changed to L3.
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "true"},
  }));
  for (int i = 0; i < 70; i++) {
    ASSERT_OK(Put(Key(static_cast<int>(rnd.Uniform(kMaxKey))),
                  RandomString(&rnd, 80)));
  }

  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "false"},
  }));
  Flush();
  dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(db_->GetIntProperty("rocksdb.base-level", &int_prop));
  ASSERT_EQ(3U, int_prop);
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level3", &str_prop));
  ASSERT_EQ("0", str_prop);

  // Trigger parallel compaction, and the first one would change the base
  // level.
  // Hold compaction jobs to make sure
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start",
      [&](void* arg) { env_->SleepForMicroseconds(100000); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "true"},
  }));
  // Write about 10K more
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(static_cast<int>(rnd.Uniform(kMaxKey))),
                  RandomString(&rnd, 80)));
  }
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "false"},
  }));
  Flush();
  // Wait for 200 milliseconds before proceeding compactions to make sure two
  // parallel ones are executed.
  env_->SleepForMicroseconds(200000);
  dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(db_->GetIntProperty("rocksdb.base-level", &int_prop));
  ASSERT_EQ(3U, int_prop);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  // Trigger a condition that the compaction changes base level and L0->Lbase
  // happens at the same time.
  // We try to make last levels' targets to be 10K, 40K, 160K, add triggers
  // another compaction from 40K->160K.
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "true"},
  }));
  // Write about 150K more
  for (int i = 0; i < 1350; i++) {
    ASSERT_OK(Put(Key(static_cast<int>(rnd.Uniform(kMaxKey))),
                  RandomString(&rnd, 80)));
  }
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "false"},
  }));
  Flush();
  dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(db_->GetIntProperty("rocksdb.base-level", &int_prop));
  ASSERT_EQ(2U, int_prop);

  // Keep Writing data until base level changed 2->1. There will be L0->L2
  // compaction going on at the same time.
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  for (int attempt = 0; attempt <= 20; attempt++) {
    // Write about 5K more data with two flushes. It should be flush to level 2
    // but when it is applied, base level is already 1.
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(static_cast<int>(rnd.Uniform(kMaxKey))),
                    RandomString(&rnd, 80)));
    }
    Flush();

    ASSERT_TRUE(db_->GetIntProperty("rocksdb.base-level", &int_prop));
    if (int_prop == 2U) {
      env_->SleepForMicroseconds(50000);
    } else {
      break;
    }
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

  env_->SleepForMicroseconds(200000);

  ASSERT_TRUE(db_->GetIntProperty("rocksdb.base-level", &int_prop));
  ASSERT_EQ(1U, int_prop);
}

TEST_F(DBTest, DynamicLevelMaxBytesBaseInc) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.db_write_buffer_size = 2048;
  options.write_buffer_size = 2048;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.target_file_size_base = 2048;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = 10240;
  options.max_bytes_for_level_multiplier = 4;
  options.hard_rate_limit = 1.1;
  options.max_background_compactions = 2;
  options.num_levels = 5;

  DestroyAndReopen(options);

  int non_trivial = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* arg) { non_trivial++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  const int total_keys = 3000;
  const int random_part_size = 100;
  for (int i = 0; i < total_keys; i++) {
    std::string value = RandomString(&rnd, random_part_size);
    PutFixed32(&value, static_cast<uint32_t>(i));
    ASSERT_OK(Put(Key(i), value));
  }
  Flush();
  dbfull()->TEST_WaitForCompact();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  ASSERT_EQ(non_trivial, 0);

  for (int i = 0; i < total_keys; i++) {
    std::string value = Get(Key(i));
    ASSERT_EQ(DecodeFixed32(value.c_str() + random_part_size),
              static_cast<uint32_t>(i));
  }

  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
}


TEST_F(DBTest, DynamicLevelCompressionPerLevel) {
  if (!Snappy_Supported()) {
    return;
  }
  const int kNKeys = 120;
  int keys[kNKeys];
  for (int i = 0; i < kNKeys; i++) {
    keys[i] = i;
  }
  std::random_shuffle(std::begin(keys), std::end(keys));

  Random rnd(301);
  Options options;
  options.create_if_missing = true;
  options.db_write_buffer_size = 20480;
  options.write_buffer_size = 20480;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.target_file_size_base = 2048;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = 102400;
  options.max_bytes_for_level_multiplier = 4;
  options.max_background_compactions = 1;
  options.num_levels = 5;

  options.compression_per_level.resize(3);
  options.compression_per_level[0] = kNoCompression;
  options.compression_per_level[1] = kNoCompression;
  options.compression_per_level[2] = kSnappyCompression;

  DestroyAndReopen(options);

  // Insert more than 80K. L4 should be base level. Neither L0 nor L4 should
  // be compressed, so total data size should be more than 80K.
  for (int i = 0; i < 20; i++) {
    ASSERT_OK(Put(Key(keys[i]), CompressibleString(&rnd, 4000)));
  }
  Flush();
  dbfull()->TEST_WaitForCompact();

  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_EQ(NumTableFilesAtLevel(3), 0);
  ASSERT_GT(SizeAtLevel(0) + SizeAtLevel(4), 20U * 4000U);

  // Insert 400KB. Some data will be compressed
  for (int i = 21; i < 120; i++) {
    ASSERT_OK(Put(Key(keys[i]), CompressibleString(&rnd, 4000)));
  }
  Flush();
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_LT(SizeAtLevel(0) + SizeAtLevel(3) + SizeAtLevel(4), 120U * 4000U);
  // Make sure data in files in L3 is not compacted by removing all files
  // in L4 and calculate number of rows
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "true"},
  }));
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  for (auto file : cf_meta.levels[4].files) {
    ASSERT_OK(dbfull()->DeleteFile(file.name));
  }
  int num_keys = 0;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    num_keys++;
  }
  ASSERT_OK(iter->status());
  ASSERT_GT(SizeAtLevel(0) + SizeAtLevel(3), num_keys * 4000U);
}

TEST_F(DBTest, DynamicLevelCompressionPerLevel2) {
  const int kNKeys = 500;
  int keys[kNKeys];
  for (int i = 0; i < kNKeys; i++) {
    keys[i] = i;
  }
  std::random_shuffle(std::begin(keys), std::end(keys));

  Random rnd(301);
  Options options;
  options.create_if_missing = true;
  options.db_write_buffer_size = 6000;
  options.write_buffer_size = 6000;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.hard_rate_limit = 1.1;

  // Use file size to distinguish levels
  // L1: 10, L2: 20, L3 40, L4 80
  // L0 is less than 30
  options.target_file_size_base = 10;
  options.target_file_size_multiplier = 2;

  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = 200;
  options.max_bytes_for_level_multiplier = 8;
  options.max_background_compactions = 1;
  options.num_levels = 5;
  std::shared_ptr<mock::MockTableFactory> mtf(new mock::MockTableFactory);
  options.table_factory = mtf;

  options.compression_per_level.resize(3);
  options.compression_per_level[0] = kNoCompression;
  options.compression_per_level[1] = kLZ4Compression;
  options.compression_per_level[2] = kZlibCompression;

  DestroyAndReopen(options);
  // When base level is L4, L4 is LZ4.
  std::atomic<int> num_zlib(0);
  std::atomic<int> num_lz4(0);
  std::atomic<int> num_no(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        if (compaction->output_level() == 4) {
          ASSERT_TRUE(compaction->OutputCompressionType() == kLZ4Compression);
          num_lz4.fetch_add(1);
        }
      });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:output_compression", [&](void* arg) {
        auto* compression = reinterpret_cast<CompressionType*>(arg);
        ASSERT_TRUE(*compression == kNoCompression);
        num_no.fetch_add(1);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(keys[i]), RandomString(&rnd, 200)));
  }
  Flush();
  dbfull()->TEST_WaitForCompact();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_EQ(NumTableFilesAtLevel(3), 0);
  ASSERT_GT(NumTableFilesAtLevel(4), 0);
  ASSERT_GT(num_no.load(), 2);
  ASSERT_GT(num_lz4.load(), 0);
  int prev_num_files_l4 = NumTableFilesAtLevel(4);

  // After base level turn L4->L3, L3 becomes LZ4 and L4 becomes Zlib
  num_lz4.store(0);
  num_no.store(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        if (compaction->output_level() == 4 && compaction->start_level() == 3) {
          ASSERT_TRUE(compaction->OutputCompressionType() == kZlibCompression);
          num_zlib.fetch_add(1);
        } else {
          ASSERT_TRUE(compaction->OutputCompressionType() == kLZ4Compression);
          num_lz4.fetch_add(1);
        }
      });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:output_compression", [&](void* arg) {
        auto* compression = reinterpret_cast<CompressionType*>(arg);
        ASSERT_TRUE(*compression == kNoCompression);
        num_no.fetch_add(1);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 101; i < 500; i++) {
    ASSERT_OK(Put(Key(keys[i]), RandomString(&rnd, 200)));
    if (i % 100 == 99) {
      Flush();
      dbfull()->TEST_WaitForCompact();
    }
  }

  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_GT(NumTableFilesAtLevel(3), 0);
  ASSERT_GT(NumTableFilesAtLevel(4), prev_num_files_l4);
  ASSERT_GT(num_no.load(), 2);
  ASSERT_GT(num_lz4.load(), 0);
  ASSERT_GT(num_zlib.load(), 0);
}

TEST_F(DBTest, DynamicCompactionOptions) {
  // minimum write buffer size is enforced at 64KB
  const uint64_t k32KB = 1 << 15;
  const uint64_t k64KB = 1 << 16;
  const uint64_t k128KB = 1 << 17;
  const uint64_t k1MB = 1 << 20;
  const uint64_t k4KB = 1 << 12;
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.hard_rate_limit = 1.1;
  options.write_buffer_size = k64KB;
  options.max_write_buffer_number = 2;
  // Compaction related options
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 4;
  options.level0_stop_writes_trigger = 8;
  options.max_grandparent_overlap_factor = 10;
  options.expanded_compaction_factor = 25;
  options.source_compaction_factor = 1;
  options.target_file_size_base = k64KB;
  options.target_file_size_multiplier = 1;
  options.max_bytes_for_level_base = k128KB;
  options.max_bytes_for_level_multiplier = 4;

  // Block flush thread and disable compaction thread
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  DestroyAndReopen(options);

  auto gen_l0_kb = [this](int start, int size, int stride) {
    Random rnd(301);
    for (int i = 0; i < size; i++) {
      ASSERT_OK(Put(Key(start + stride * i), RandomString(&rnd, 1024)));
    }
    dbfull()->TEST_WaitForFlushMemTable();
  };

  // Write 3 files that have the same key range.
  // Since level0_file_num_compaction_trigger is 3, compaction should be
  // triggered. The compaction should result in one L1 file
  gen_l0_kb(0, 64, 1);
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  gen_l0_kb(0, 64, 1);
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);
  gen_l0_kb(0, 64, 1);
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,1", FilesPerLevel());
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(1U, metadata.size());
  ASSERT_LE(metadata[0].size, k64KB + k4KB);
  ASSERT_GE(metadata[0].size, k64KB - k4KB);

  // Test compaction trigger and target_file_size_base
  // Reduce compaction trigger to 2, and reduce L1 file size to 32KB.
  // Writing to 64KB L0 files should trigger a compaction. Since these
  // 2 L0 files have the same key range, compaction merge them and should
  // result in 2 32KB L1 files.
  ASSERT_OK(dbfull()->SetOptions({
    {"level0_file_num_compaction_trigger", "2"},
    {"target_file_size_base", ToString(k32KB) }
  }));

  gen_l0_kb(0, 64, 1);
  ASSERT_EQ("1,1", FilesPerLevel());
  gen_l0_kb(0, 64, 1);
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,2", FilesPerLevel());
  metadata.clear();
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(2U, metadata.size());
  ASSERT_LE(metadata[0].size, k32KB + k4KB);
  ASSERT_GE(metadata[0].size, k32KB - k4KB);
  ASSERT_LE(metadata[1].size, k32KB + k4KB);
  ASSERT_GE(metadata[1].size, k32KB - k4KB);

  // Test max_bytes_for_level_base
  // Increase level base size to 256KB and write enough data that will
  // fill L1 and L2. L1 size should be around 256KB while L2 size should be
  // around 256KB x 4.
  ASSERT_OK(dbfull()->SetOptions({
    {"max_bytes_for_level_base", ToString(k1MB) }
  }));

  // writing 96 x 64KB => 6 * 1024KB
  // (L1 + L2) = (1 + 4) * 1024KB
  for (int i = 0; i < 96; ++i) {
    gen_l0_kb(i, 64, 96);
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_GT(SizeAtLevel(1), k1MB / 2);
  ASSERT_LT(SizeAtLevel(1), k1MB + k1MB / 2);

  // Within (0.5, 1.5) of 4MB.
  ASSERT_GT(SizeAtLevel(2), 2 * k1MB);
  ASSERT_LT(SizeAtLevel(2), 6 * k1MB);

  // Test max_bytes_for_level_multiplier and
  // max_bytes_for_level_base. Now, reduce both mulitplier and level base,
  // After filling enough data that can fit in L1 - L3, we should see L1 size
  // reduces to 128KB from 256KB which was asserted previously. Same for L2.
  ASSERT_OK(dbfull()->SetOptions({
    {"max_bytes_for_level_multiplier", "2"},
    {"max_bytes_for_level_base", ToString(k128KB) }
  }));

  // writing 20 x 64KB = 10 x 128KB
  // (L1 + L2 + L3) = (1 + 2 + 4) * 128KB
  for (int i = 0; i < 20; ++i) {
    gen_l0_kb(i, 64, 32);
  }
  dbfull()->TEST_WaitForCompact();
  uint64_t total_size =
    SizeAtLevel(1) + SizeAtLevel(2) + SizeAtLevel(3);
  ASSERT_TRUE(total_size < k128KB * 7 * 1.5);

  // Test level0_stop_writes_trigger.
  // Clean up memtable and L0. Block compaction threads. If continue to write
  // and flush memtables. We should see put timeout after 8 memtable flushes
  // since level0_stop_writes_trigger = 8
  dbfull()->CompactRange(nullptr, nullptr);
  // Block compaction
  SleepingBackgroundTask sleeping_task_low1;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low1,
                 Env::Priority::LOW);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  int count = 0;
  Random rnd(301);
  WriteOptions wo;
  wo.timeout_hint_us = 10000;
  while (Put(Key(count), RandomString(&rnd, 1024), wo).ok() && count < 64) {
    dbfull()->TEST_FlushMemTable(true);
    count++;
  }
  // Stop trigger = 8
  ASSERT_EQ(count, 8);
  // Unblock
  sleeping_task_low1.WakeUp();
  sleeping_task_low1.WaitUntilDone();

  // Now reduce level0_stop_writes_trigger to 6. Clear up memtables and L0.
  // Block compaction thread again. Perform the put and memtable flushes
  // until we see timeout after 6 memtable flushes.
  ASSERT_OK(dbfull()->SetOptions({
    {"level0_stop_writes_trigger", "6"}
  }));
  dbfull()->CompactRange(nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  // Block compaction
  SleepingBackgroundTask sleeping_task_low2;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low2,
                 Env::Priority::LOW);
  count = 0;
  while (Put(Key(count), RandomString(&rnd, 1024), wo).ok() && count < 64) {
    dbfull()->TEST_FlushMemTable(true);
    count++;
  }
  ASSERT_EQ(count, 6);
  // Unblock
  sleeping_task_low2.WakeUp();
  sleeping_task_low2.WaitUntilDone();

  // Test disable_auto_compactions
  // Compaction thread is unblocked but auto compaction is disabled. Write
  // 4 L0 files and compaction should be triggered. If auto compaction is
  // disabled, then TEST_WaitForCompact will be waiting for nothing. Number of
  // L0 files do not change after the call.
  ASSERT_OK(dbfull()->SetOptions({
    {"disable_auto_compactions", "true"}
  }));
  dbfull()->CompactRange(nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
    // Wait for compaction so that put won't timeout
    dbfull()->TEST_FlushMemTable(true);
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(NumTableFilesAtLevel(0), 4);

  // Enable auto compaction and perform the same test, # of L0 files should be
  // reduced after compaction.
  ASSERT_OK(dbfull()->SetOptions({
    {"disable_auto_compactions", "false"}
  }));
  dbfull()->CompactRange(nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
    // Wait for compaction so that put won't timeout
    dbfull()->TEST_FlushMemTable(true);
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_LT(NumTableFilesAtLevel(0), 4);

  // Test for hard_rate_limit.
  // First change max_bytes_for_level_base to a big value and populate
  // L1 - L3. Then thrink max_bytes_for_level_base and disable auto compaction
  // at the same time, we should see some level with score greater than 2.
  ASSERT_OK(dbfull()->SetOptions({
    {"max_bytes_for_level_base", ToString(k1MB) }
  }));
  // writing 40 x 64KB = 10 x 256KB
  // (L1 + L2 + L3) = (1 + 2 + 4) * 256KB
  for (int i = 0; i < 40; ++i) {
    gen_l0_kb(i, 64, 32);
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE((SizeAtLevel(1) > k1MB * 0.8 &&
               SizeAtLevel(1) < k1MB * 1.2) ||
              (SizeAtLevel(2) > 2 * k1MB * 0.8 &&
               SizeAtLevel(2) < 2 * k1MB * 1.2) ||
              (SizeAtLevel(3) > 4 * k1MB * 0.8 &&
               SizeAtLevel(3) < 4 * k1MB * 1.2));
  // Reduce max_bytes_for_level_base and disable compaction at the same time
  // This should cause score to increase
  ASSERT_OK(dbfull()->SetOptions({
    {"disable_auto_compactions", "true"},
    {"max_bytes_for_level_base", "65536"},
  }));
  ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024)));
  dbfull()->TEST_FlushMemTable(true);

  // Check score is above 2
  ASSERT_TRUE(SizeAtLevel(1) / k64KB > 2 ||
              SizeAtLevel(2) / k64KB > 4 ||
              SizeAtLevel(3) / k64KB > 8);

  // Enfoce hard rate limit. Now set hard_rate_limit to 2,
  // we should start to see put delay (1000 us) and timeout as a result
  // (L0 score is not regulated by this limit).
  ASSERT_OK(dbfull()->SetOptions({
    {"hard_rate_limit", "2"},
    {"level0_slowdown_writes_trigger", "18"},
    {"level0_stop_writes_trigger", "20"}
  }));
  ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024)));
  dbfull()->TEST_FlushMemTable(true);

  std::atomic<int> sleep_count(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DelayWrite:Sleep", [&](void* arg) { sleep_count.fetch_add(1); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Hard rate limit slow down for 1000 us, so default 10ms should be ok
  ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024), wo));
  sleep_count.store(0);
  ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024), wo));
  ASSERT_GT(sleep_count.load(), 0);

  // Lift the limit and no timeout
  ASSERT_OK(dbfull()->SetOptions({
    {"hard_rate_limit", "200"},
  }));
  dbfull()->TEST_FlushMemTable(true);
  sleep_count.store(0);
  ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024), wo));
  // Technically, time out is still possible for timing issue.
  ASSERT_EQ(sleep_count.load(), 0);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();


  // Test max_mem_compaction_level.
  // Destory DB and start from scratch
  options.max_background_compactions = 1;
  options.max_background_flushes = 0;
  options.max_mem_compaction_level = 2;
  DestroyAndReopen(options);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);

  ASSERT_OK(Put("max_mem_compaction_level_key", RandomString(&rnd, 8)));
  dbfull()->TEST_FlushMemTable(true);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 1);

  ASSERT_TRUE(Put("max_mem_compaction_level_key",
              RandomString(&rnd, 8)).ok());
  // Set new value and it becomes effective in this flush
  ASSERT_OK(dbfull()->SetOptions({
    {"max_mem_compaction_level", "1"}
  }));
  dbfull()->TEST_FlushMemTable(true);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  ASSERT_EQ(NumTableFilesAtLevel(2), 1);

  ASSERT_TRUE(Put("max_mem_compaction_level_key",
              RandomString(&rnd, 8)).ok());
  // Set new value and it becomes effective in this flush
  ASSERT_OK(dbfull()->SetOptions({
    {"max_mem_compaction_level", "0"}
  }));
  dbfull()->TEST_FlushMemTable(true);
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  ASSERT_EQ(NumTableFilesAtLevel(2), 1);
}

TEST_F(DBTest, FileCreationRandomFailure) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.write_buffer_size = 100000;  // Small write buffer
  options.target_file_size_base = 200000;
  options.max_bytes_for_level_base = 1000000;
  options.max_bytes_for_level_multiplier = 2;

  DestroyAndReopen(options);
  Random rnd(301);

  const int kTestSize = kCDTKeysPerBuffer * 4096;
  const int kTotalIteration = 100;
  // the second half of the test involves in random failure
  // of file creation.
  const int kRandomFailureTest = kTotalIteration / 2;
  std::vector<std::string> values;
  for (int i = 0; i < kTestSize; ++i) {
    values.push_back("NOT_FOUND");
  }
  for (int j = 0; j < kTotalIteration; ++j) {
    if (j == kRandomFailureTest) {
      env_->non_writeable_rate_.store(90);
    }
    for (int k = 0; k < kTestSize; ++k) {
      // here we expect some of the Put fails.
      std::string value = RandomString(&rnd, 100);
      Status s = Put(Key(k), Slice(value));
      if (s.ok()) {
        // update the latest successful put
        values[k] = value;
      }
      // But everything before we simulate the failure-test should succeed.
      if (j < kRandomFailureTest) {
        ASSERT_OK(s);
      }
    }
  }

  // If rocksdb does not do the correct job, internal assert will fail here.
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();

  // verify we have the latest successful update
  for (int k = 0; k < kTestSize; ++k) {
    auto v = Get(Key(k));
    ASSERT_EQ(v, values[k]);
  }

  // reopen and reverify we have the latest successful update
  env_->non_writeable_rate_.store(0);
  Reopen(options);
  for (int k = 0; k < kTestSize; ++k) {
    auto v = Get(Key(k));
    ASSERT_EQ(v, values[k]);
  }
}

TEST_F(DBTest, PartialCompactionFailure) {
  Options options;
  const int kKeySize = 16;
  const int kKvSize = 1000;
  const int kKeysPerBuffer = 100;
  const int kNumL1Files = 5;
  options.create_if_missing = true;
  options.write_buffer_size = kKeysPerBuffer * kKvSize;
  options.max_write_buffer_number = 2;
  options.target_file_size_base =
      options.write_buffer_size *
      (options.max_write_buffer_number - 1);
  options.level0_file_num_compaction_trigger = kNumL1Files;
  options.max_bytes_for_level_base =
      options.level0_file_num_compaction_trigger *
      options.target_file_size_base;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;

  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  // stop the compaction thread until we simulate the file creation failure.
  SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  options.env = env_;

  DestroyAndReopen(options);

  const int kNumInsertedKeys =
      options.level0_file_num_compaction_trigger *
      (options.max_write_buffer_number - 1) *
      kKeysPerBuffer;

  Random rnd(301);
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (int k = 0; k < kNumInsertedKeys; ++k) {
    keys.emplace_back(RandomString(&rnd, kKeySize));
    values.emplace_back(RandomString(&rnd, kKvSize - kKeySize));
    ASSERT_OK(Put(Slice(keys[k]), Slice(values[k])));
  }

  dbfull()->TEST_FlushMemTable(true);
  // Make sure the number of L0 files can trigger compaction.
  ASSERT_GE(NumTableFilesAtLevel(0),
            options.level0_file_num_compaction_trigger);

  auto previous_num_level0_files = NumTableFilesAtLevel(0);

  // Fail the first file creation.
  env_->non_writable_count_ = 1;
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  // Expect compaction to fail here as one file will fail its
  // creation.
  ASSERT_TRUE(!dbfull()->TEST_WaitForCompact().ok());

  // Verify L0 -> L1 compaction does fail.
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);

  // Verify all L0 files are still there.
  ASSERT_EQ(NumTableFilesAtLevel(0), previous_num_level0_files);

  // All key-values must exist after compaction fails.
  for (int k = 0; k < kNumInsertedKeys; ++k) {
    ASSERT_EQ(values[k], Get(keys[k]));
  }

  env_->non_writable_count_ = 0;

  // Make sure RocksDB will not get into corrupted state.
  Reopen(options);

  // Verify again after reopen.
  for (int k = 0; k < kNumInsertedKeys; ++k) {
    ASSERT_EQ(values[k], Get(keys[k]));
  }
}

TEST_F(DBTest, DynamicMiscOptions) {
  // Test max_sequential_skip_in_iterations
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.max_sequential_skip_in_iterations = 16;
  options.compression = kNoCompression;
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(options);

  auto assert_reseek_count = [this, &options](int key_start, int num_reseek) {
    int key0 = key_start;
    int key1 = key_start + 1;
    int key2 = key_start + 2;
    Random rnd(301);
    ASSERT_OK(Put(Key(key0), RandomString(&rnd, 8)));
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(Put(Key(key1), RandomString(&rnd, 8)));
    }
    ASSERT_OK(Put(Key(key2), RandomString(&rnd, 8)));
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek(Key(key1));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Key(key1)), 0);
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Key(key2)), 0);
    ASSERT_EQ(num_reseek,
              TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION));
  };
  // No reseek
  assert_reseek_count(100, 0);

  ASSERT_OK(dbfull()->SetOptions({
    {"max_sequential_skip_in_iterations", "4"}
  }));
  // Clear memtable and make new option effective
  dbfull()->TEST_FlushMemTable(true);
  // Trigger reseek
  assert_reseek_count(200, 1);

  ASSERT_OK(dbfull()->SetOptions({
    {"max_sequential_skip_in_iterations", "16"}
  }));
  // Clear memtable and make new option effective
  dbfull()->TEST_FlushMemTable(true);
  // No reseek
  assert_reseek_count(300, 1);
}

TEST_F(DBTest, DontDeletePendingOutputs) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  // Every time we write to a table file, call FOF/POF with full DB scan. This
  // will make sure our pending_outputs_ protection work correctly
  std::function<void()> purge_obsolete_files_function = [&]() {
    JobContext job_context(0);
    dbfull()->TEST_LockMutex();
    dbfull()->FindObsoleteFiles(&job_context, true /*force*/);
    dbfull()->TEST_UnlockMutex();
    dbfull()->PurgeObsoleteFiles(job_context);
  };

  env_->table_write_callback_ = &purge_obsolete_files_function;

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(Put("a", "begin"));
    ASSERT_OK(Put("z", "end"));
    ASSERT_OK(Flush());
  }

  // If pending output guard does not work correctly, PurgeObsoleteFiles() will
  // delete the file that Compaction is trying to create, causing this: error
  // db/db_test.cc:975: IO error:
  // /tmp/rocksdbtest-1552237650/db_test/000009.sst: No such file or directory
  Compact("a", "b");
}

TEST_F(DBTest, DontDeleteMovedFile) {
  // This test triggers move compaction and verifies that the file is not
  // deleted when it's part of move compaction
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
  options.level0_file_num_compaction_trigger =
      2;  // trigger compaction when we have 2 files
  DestroyAndReopen(options);

  Random rnd(301);
  // Create two 1MB sst files
  for (int i = 0; i < 2; ++i) {
    // Create 1MB sst file
    for (int j = 0; j < 100; ++j) {
      ASSERT_OK(Put(Key(i * 50 + j), RandomString(&rnd, 10 * 1024)));
    }
    ASSERT_OK(Flush());
  }
  // this should execute both L0->L1 and L1->(move)->L2 compactions
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  // If the moved file is actually deleted (the move-safeguard in
  // ~Version::Version() is not there), we get this failure:
  // Corruption: Can't access /000009.sst
  Reopen(options);
}

TEST_F(DBTest, DeleteMovedFileAfterCompaction) {
  // iter 1 -- delete_obsolete_files_period_micros == 0
  for (int iter = 0; iter < 2; ++iter) {
    // This test triggers move compaction and verifies that the file is not
    // deleted when it's part of move compaction
    Options options = CurrentOptions();
    options.env = env_;
    if (iter == 1) {
      options.delete_obsolete_files_period_micros = 0;
    }
    options.create_if_missing = true;
    options.level0_file_num_compaction_trigger =
        2;  // trigger compaction when we have 2 files
    DestroyAndReopen(options);

    Random rnd(301);
    // Create two 1MB sst files
    for (int i = 0; i < 2; ++i) {
      // Create 1MB sst file
      for (int j = 0; j < 100; ++j) {
        ASSERT_OK(Put(Key(i * 50 + j), RandomString(&rnd, 10 * 1024)));
      }
      ASSERT_OK(Flush());
    }
    // this should execute L0->L1
    dbfull()->TEST_WaitForCompact();
    ASSERT_EQ("0,1", FilesPerLevel(0));

    // block compactions
    SleepingBackgroundTask sleeping_task;
    env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                   Env::Priority::LOW);

    options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
    Reopen(options);
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
    ASSERT_EQ("0,1", FilesPerLevel(0));
    // let compactions go
    sleeping_task.WakeUp();
    sleeping_task.WaitUntilDone();

    // this should execute L1->L2 (move)
    dbfull()->TEST_WaitForCompact();

    ASSERT_EQ("0,0,1", FilesPerLevel(0));

    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    ASSERT_EQ(metadata.size(), 1U);
    auto moved_file_name = metadata[0].name;

    // Create two more 1MB sst files
    for (int i = 0; i < 2; ++i) {
      // Create 1MB sst file
      for (int j = 0; j < 100; ++j) {
        ASSERT_OK(Put(Key(i * 50 + j + 100), RandomString(&rnd, 10 * 1024)));
      }
      ASSERT_OK(Flush());
    }
    // this should execute both L0->L1 and L1->L2 (merge with previous file)
    dbfull()->TEST_WaitForCompact();

    ASSERT_EQ("0,0,2", FilesPerLevel(0));

    // iterator is holding the file
    ASSERT_TRUE(env_->FileExists(dbname_ + "/" + moved_file_name));

    iterator.reset();

    // this file should have been compacted away
    ASSERT_TRUE(!env_->FileExists(dbname_ + "/" + moved_file_name));
  }
}

TEST_F(DBTest, OptimizeFiltersForHits) {
  Options options = CurrentOptions();
  options.write_buffer_size = 256 * 1024;
  options.target_file_size_base = 256 * 1024;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 4;
  options.max_bytes_for_level_base = 256 * 1024;
  options.max_write_buffer_number = 2;
  options.max_background_compactions = 8;
  options.max_background_flushes = 8;
  options.compaction_style = kCompactionStyleLevel;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, true));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.optimize_filters_for_hits = true;
  options.statistics = rocksdb::CreateDBStatistics();
  CreateAndReopenWithCF({"mypikachu"}, options);

  int numkeys = 200000;
  for (int i = 0; i < 20; i += 2) {
    for (int j = i; j < numkeys; j += 20) {
      ASSERT_OK(Put(1, Key(j), "val"));
    }
  }


  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  for (int i = 1; i < numkeys; i += 2) {
    ASSERT_EQ(Get(1, Key(i)), "NOT_FOUND");
  }

  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L0));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L1));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L2_AND_UP));

  // When the skip_filters_on_last_level is ON, the last level which has
  // most of the keys does not use bloom filters. We end up using
  // bloom filters in a very small number of cases. Without the flag.
  // this number would be close to 150000 (all the key at the last level) +
  // some use in the upper levels
  //
  ASSERT_GT(90000, TestGetTickerCount(options, BLOOM_FILTER_USEFUL));

  for (int i = 0; i < numkeys; i += 2) {
    ASSERT_EQ(Get(1, Key(i)), "val");
  }
}

TEST_F(DBTest, L0L1L2AndUpHitCounter) {
  Options options = CurrentOptions();
  options.write_buffer_size = 32 * 1024;
  options.target_file_size_base = 32 * 1024;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 4;
  options.max_bytes_for_level_base = 64 * 1024;
  options.max_write_buffer_number = 2;
  options.max_background_compactions = 8;
  options.max_background_flushes = 8;
  options.statistics = rocksdb::CreateDBStatistics();
  CreateAndReopenWithCF({"mypikachu"}, options);

  int numkeys = 20000;
  for (int i = 0; i < numkeys; i++) {
    ASSERT_OK(Put(1, Key(i), "val"));
  }
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L0));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L1));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L2_AND_UP));

  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  for (int i = 0; i < numkeys; i++) {
    ASSERT_EQ(Get(1, Key(i)), "val");
  }

  ASSERT_GT(TestGetTickerCount(options, GET_HIT_L0), 100);
  ASSERT_GT(TestGetTickerCount(options, GET_HIT_L1), 100);
  ASSERT_GT(TestGetTickerCount(options, GET_HIT_L2_AND_UP), 100);

  ASSERT_EQ(numkeys, TestGetTickerCount(options, GET_HIT_L0) +
                         TestGetTickerCount(options, GET_HIT_L1) +
                         TestGetTickerCount(options, GET_HIT_L2_AND_UP));
}

TEST_F(DBTest, EncodeDecompressedBlockSizeTest) {
  // iter 0 -- zlib
  // iter 1 -- bzip2
  // iter 2 -- lz4
  // iter 3 -- lz4HC
  CompressionType compressions[] = {kZlibCompression, kBZip2Compression,
                                    kLZ4Compression,  kLZ4HCCompression};
  for (int iter = 0; iter < 4; ++iter) {
    // first_table_version 1 -- generate with table_version == 1, read with
    // table_version == 2
    // first_table_version 2 -- generate with table_version == 2, read with
    // table_version == 1
    for (int first_table_version = 1; first_table_version <= 2;
         ++first_table_version) {
      BlockBasedTableOptions table_options;
      table_options.format_version = first_table_version;
      table_options.filter_policy.reset(NewBloomFilterPolicy(10));
      Options options = CurrentOptions();
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      options.create_if_missing = true;
      options.compression = compressions[iter];
      DestroyAndReopen(options);

      int kNumKeysWritten = 100000;

      Random rnd(301);
      for (int i = 0; i < kNumKeysWritten; ++i) {
        // compressible string
        ASSERT_OK(Put(Key(i), RandomString(&rnd, 128) + std::string(128, 'a')));
      }

      table_options.format_version = first_table_version == 1 ? 2 : 1;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      Reopen(options);
      for (int i = 0; i < kNumKeysWritten; ++i) {
        auto r = Get(Key(i));
        ASSERT_EQ(r.substr(128), std::string(128, 'a'));
      }
    }
  }
}

TEST_F(DBTest, MutexWaitStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  CreateAndReopenWithCF({"pikachu"}, options);
  const int64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(
      ThreadStatus::STATE_MUTEX_WAIT, kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  ASSERT_GE(TestGetTickerCount(
            options, DB_MUTEX_WAIT_MICROS), kMutexWaitDelay);
  ThreadStatusUtil::TEST_SetStateDelay(
      ThreadStatus::STATE_MUTEX_WAIT, 0);
}

// This reproduces a bug where we don't delete a file because when it was
// supposed to be deleted, it was blocked by pending_outputs
// Consider:
// 1. current file_number is 13
// 2. compaction (1) starts, blocks deletion of all files starting with 13
// (pending outputs)
// 3. file 13 is created by compaction (2)
// 4. file 13 is consumed by compaction (3) and file 15 was created. Since file
// 13 has no references, it is put into VersionSet::obsolete_files_
// 5. FindObsoleteFiles() gets file 13 from VersionSet::obsolete_files_. File 13
// is deleted from obsolete_files_ set.
// 6. PurgeObsoleteFiles() tries to delete file 13, but this file is blocked by
// pending outputs since compaction (1) is still running. It is not deleted and
// it is not present in obsolete_files_ anymore. Therefore, we never delete it.
TEST_F(DBTest, DeleteObsoleteFilesPendingOutputs) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 2 * 1024 * 1024;     // 2 MB
  options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
  options.level0_file_num_compaction_trigger =
      2;  // trigger compaction when we have 2 files
  options.max_background_flushes = 2;
  options.max_background_compactions = 2;
  Reopen(options);

  Random rnd(301);
  // Create two 1MB sst files
  for (int i = 0; i < 2; ++i) {
    // Create 1MB sst file
    for (int j = 0; j < 100; ++j) {
      ASSERT_OK(Put(Key(i * 50 + j), RandomString(&rnd, 10 * 1024)));
    }
    ASSERT_OK(Flush());
  }
  // this should execute both L0->L1 and L1->(move)->L2 compactions
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  SleepingBackgroundTask blocking_thread;
  port::Mutex mutex_;
  bool already_blocked(false);

  // block the flush
  std::function<void()> block_first_time = [&]() {
    bool blocking = false;
    {
      MutexLock l(&mutex_);
      if (!already_blocked) {
        blocking = true;
        already_blocked = true;
      }
    }
    if (blocking) {
      blocking_thread.DoSleep();
    }
  };
  env_->table_write_callback_ = &block_first_time;
  // Create 1MB sst file
  for (int j = 0; j < 256; ++j) {
    ASSERT_OK(Put(Key(j), RandomString(&rnd, 10 * 1024)));
  }
  // this should trigger a flush, which is blocked with block_first_time
  // pending_file is protecting all the files created after

  ASSERT_OK(dbfull()->TEST_CompactRange(2, nullptr, nullptr));

  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  auto file_on_L2 = metadata[0].name;

  ASSERT_OK(dbfull()->TEST_CompactRange(3, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,1", FilesPerLevel(0));

  // finish the flush!
  blocking_thread.WakeUp();
  blocking_thread.WaitUntilDone();
  dbfull()->TEST_WaitForFlushMemTable();
  ASSERT_EQ("1,0,0,0,1", FilesPerLevel(0));

  metadata.clear();
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 2U);

  // This file should have been deleted
  ASSERT_TRUE(!env_->FileExists(dbname_ + "/" + file_on_L2));
}

TEST_F(DBTest, CloseSpeedup) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_write_buffer_number = 16;

  // Block background threads
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&SleepingBackgroundTask::DoSleepTask, &sleeping_task_high,
                 Env::Priority::HIGH);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);
  // Delete archival files.
  for (size_t i = 0; i < filenames.size(); ++i) {
    env_->DeleteFile(dbname_ + "/" + filenames[i]);
  }
  env_->DeleteDir(dbname_);
  DestroyAndReopen(options);

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are not going to level 2
  // After that, (100K, 200K)
  for (int num = 0; num < 5; num++) {
    GenerateNewFile(&rnd, &key_idx, true);
  }

  ASSERT_EQ(0, GetSstFileCount(dbname_));

  Close();
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // Unblock background threads
  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  Destroy(options);
}

class DelayedMergeOperator : public AssociativeMergeOperator {
 private:
  DBTest* db_test_;

 public:
  explicit DelayedMergeOperator(DBTest* d) : db_test_(d) {}
  virtual bool Merge(const Slice& key, const Slice* existing_value,
                     const Slice& value, std::string* new_value,
                     Logger* logger) const override {
    db_test_->env_->addon_time_ += 1000;
    return true;
  }

  virtual const char* Name() const override { return "DelayedMergeOperator"; }
};

TEST_F(DBTest, MergeTestTime) {
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);

  // Enable time profiling
  SetPerfLevel(kEnableTime);
  this->env_->addon_time_ = 0;
  Options options;
  options = CurrentOptions(options);
  options.statistics = rocksdb::CreateDBStatistics();
  options.merge_operator.reset(new DelayedMergeOperator(this));
  DestroyAndReopen(options);

  ASSERT_EQ(TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME), 0);
  db_->Put(WriteOptions(), "foo", one);
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "foo", two));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "foo", three));
  ASSERT_OK(Flush());

  ReadOptions opt;
  opt.verify_checksums = true;
  opt.snapshot = nullptr;
  std::string result;
  db_->Get(opt, "foo", &result);

  ASSERT_LT(TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME), 2800000);
  ASSERT_GT(TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME), 1200000);

  ReadOptions read_options;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ++count;
  }

  ASSERT_EQ(1, count);

  ASSERT_LT(TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME), 6000000);
  ASSERT_GT(TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME), 3200000);
}

TEST_F(DBTest, MergeCompactionTimeTest) {
  SetPerfLevel(kEnableTime);
  Options options;
  options = CurrentOptions(options);
  options.compaction_filter_factory = std::make_shared<KeepFilterFactory>();
  options.statistics = rocksdb::CreateDBStatistics();
  options.merge_operator.reset(new DelayedMergeOperator(this));
  options.compaction_style = kCompactionStyleUniversal;
  DestroyAndReopen(options);

  for (int i = 0; i < 1000; i++) {
    ASSERT_OK(db_->Merge(WriteOptions(), "foo", "TEST"));
    ASSERT_OK(Flush());
  }
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();

  ASSERT_NE(TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME), 0);
}

TEST_F(DBTest, FilterCompactionTimeTest) {
  Options options;
  options.compaction_filter_factory =
      std::make_shared<DelayFilterFactory>(this);
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options = CurrentOptions(options);
  DestroyAndReopen(options);

  // put some data
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      Put(ToString(table * 100 + i), "val");
    }
    Flush();
  }

  ASSERT_OK(db_->CompactRange(nullptr, nullptr));
  ASSERT_EQ(0U, CountLiveFiles());

  Reopen(options);

  Iterator* itr = db_->NewIterator(ReadOptions());
  itr->SeekToFirst();
  ASSERT_NE(TestGetTickerCount(options, FILTER_OPERATION_TOTAL_TIME), 0);
  delete itr;
}

TEST_F(DBTest, TestLogCleanup) {
  Options options = CurrentOptions();
  options.write_buffer_size = 64 * 1024;  // very small
  // only two memtables allowed ==> only two log files
  options.max_write_buffer_number = 2;
  Reopen(options);

  for (int i = 0; i < 100000; ++i) {
    Put(Key(i), "val");
    // only 2 memtables will be alive, so logs_to_free needs to always be below
    // 2
    ASSERT_LT(dbfull()->TEST_LogsToFreeSize(), static_cast<size_t>(3));
  }
}

TEST_F(DBTest, EmptyCompactedDB) {
  Options options;
  options.max_open_files = -1;
  options = CurrentOptions(options);
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  Status s = Put("new", "value");
  ASSERT_TRUE(s.IsNotSupported());
  Close();
}

TEST_F(DBTest, CompressLevelCompaction) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  // First two levels have no compression, so that a trivial move between
  // them will be allowed. Level 2 has Zlib compression so that a trivial
  // move to level 3 will not be allowed
  options.compression_per_level = {kNoCompression, kNoCompression,
                                   kZlibCompression};
  int matches = 0, didnt_match = 0, trivial_move = 0, non_trivial = 0;

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Compaction::InputCompressionMatchesOutput:Matches",
      [&](void* arg) { matches++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Compaction::InputCompressionMatchesOutput:DidntMatch",
      [&](void* arg) { didnt_match++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* arg) { non_trivial++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* arg) { trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are going to level 0
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to fill up level 0
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(4, GetSstFileCount(dbname_));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4", FilesPerLevel(0));

  // (1, 4, 1)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,1", FilesPerLevel(0));

  // (1, 4, 2)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,2", FilesPerLevel(0));

  // (1, 4, 3)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,3", FilesPerLevel(0));

  // (1, 4, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,4", FilesPerLevel(0));

  // (1, 4, 5)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,5", FilesPerLevel(0));

  // (1, 4, 6)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,6", FilesPerLevel(0));

  // (1, 4, 7)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,7", FilesPerLevel(0));

  // (1, 4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,8", FilesPerLevel(0));

  ASSERT_EQ(matches, 12);
  ASSERT_EQ(didnt_match, 8);
  ASSERT_EQ(trivial_move, 12);
  ASSERT_EQ(non_trivial, 8);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Destroy(options);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
