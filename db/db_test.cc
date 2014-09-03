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
#include <unordered_set>
#include <utility>

#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
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
#include "rocksdb/utilities/write_batch_with_index.h"
#include "table/block_based_table_factory.h"
#include "table/plain_table_factory.h"
#include "util/hash.h"
#include "util/hash_linklist_rep.h"
#include "utilities/merge_operators.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/rate_limiter.h"
#include "util/statistics.h"
#include "util/testharness.h"
#include "util/sync_point.h"
#include "util/testutil.h"

namespace rocksdb {

static bool SnappyCompressionSupported(const CompressionOptions& options) {
  std::string out;
  Slice in = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  return port::Snappy_Compress(options, in.data(), in.size(), &out);
}

static bool ZlibCompressionSupported(const CompressionOptions& options) {
  std::string out;
  Slice in = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  return port::Zlib_Compress(options, in.data(), in.size(), &out);
}

static bool BZip2CompressionSupported(const CompressionOptions& options) {
  std::string out;
  Slice in = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  return port::BZip2_Compress(options, in.data(), in.size(), &out);
}

static bool LZ4CompressionSupported(const CompressionOptions &options) {
  std::string out;
  Slice in = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  return port::LZ4_Compress(options, in.data(), in.size(), &out);
}

static bool LZ4HCCompressionSupported(const CompressionOptions &options) {
  std::string out;
  Slice in = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  return port::LZ4HC_Compress(options, in.data(), in.size(), &out);
}

static std::string RandomString(Random *rnd, int len) {
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
  // sstable Sync() calls are blocked while this pointer is non-nullptr.
  port::AtomicPointer delay_sstable_sync_;

  // Simulate no-space errors while this pointer is non-nullptr.
  port::AtomicPointer no_space_;

  // Simulate non-writable file system while this pointer is non-nullptr
  port::AtomicPointer non_writable_;

  // Force sync of manifest files to fail while this pointer is non-nullptr
  port::AtomicPointer manifest_sync_error_;

  // Force write to manifest files to fail while this pointer is non-nullptr
  port::AtomicPointer manifest_write_error_;

  // Force write to log files to fail while this pointer is non-nullptr
  port::AtomicPointer log_write_error_;

  bool count_random_reads_;
  anon::AtomicCounter random_read_counter_;

  bool count_sequential_reads_;
  anon::AtomicCounter sequential_read_counter_;

  anon::AtomicCounter sleep_counter_;

  std::atomic<int64_t> bytes_written_;

  explicit SpecialEnv(Env* base) : EnvWrapper(base) {
    delay_sstable_sync_.Release_Store(nullptr);
    no_space_.Release_Store(nullptr);
    non_writable_.Release_Store(nullptr);
    count_random_reads_ = false;
    count_sequential_reads_ = false;
    manifest_sync_error_.Release_Store(nullptr);
    manifest_write_error_.Release_Store(nullptr);
    log_write_error_.Release_Store(nullptr);
    bytes_written_ = 0;
  }

  Status NewWritableFile(const std::string& f, unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) {
    class SSTableFile : public WritableFile {
     private:
      SpecialEnv* env_;
      unique_ptr<WritableFile> base_;

     public:
      SSTableFile(SpecialEnv* env, unique_ptr<WritableFile>&& base)
          : env_(env),
            base_(std::move(base)) {
      }
      Status Append(const Slice& data) {
        if (env_->no_space_.Acquire_Load() != nullptr) {
          // Drop writes on the floor
          return Status::OK();
        } else {
          env_->bytes_written_ += data.size();
          return base_->Append(data);
        }
      }
      Status Close() { return base_->Close(); }
      Status Flush() { return base_->Flush(); }
      Status Sync() {
        while (env_->delay_sstable_sync_.Acquire_Load() != nullptr) {
          env_->SleepForMicroseconds(100000);
        }
        return base_->Sync();
      }
      void SetIOPriority(Env::IOPriority pri) {
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
      Status Append(const Slice& data) {
        if (env_->manifest_write_error_.Acquire_Load() != nullptr) {
          return Status::IOError("simulated writer error");
        } else {
          return base_->Append(data);
        }
      }
      Status Close() { return base_->Close(); }
      Status Flush() { return base_->Flush(); }
      Status Sync() {
        if (env_->manifest_sync_error_.Acquire_Load() != nullptr) {
          return Status::IOError("simulated sync error");
        } else {
          return base_->Sync();
        }
      }
    };
    class LogFile : public WritableFile {
     private:
      SpecialEnv* env_;
      unique_ptr<WritableFile> base_;
     public:
      LogFile(SpecialEnv* env, unique_ptr<WritableFile>&& b)
          : env_(env), base_(std::move(b)) { }
      Status Append(const Slice& data) {
        if (env_->log_write_error_.Acquire_Load() != nullptr) {
          return Status::IOError("simulated writer error");
        } else {
          return base_->Append(data);
        }
      }
      Status Close() { return base_->Close(); }
      Status Flush() { return base_->Flush(); }
      Status Sync() { return base_->Sync(); }
    };

    if (non_writable_.Acquire_Load() != nullptr) {
      return Status::IOError("simulated write error");
    }

    Status s = target()->NewWritableFile(f, r, soptions);
    if (s.ok()) {
      if (strstr(f.c_str(), ".sst") != nullptr) {
        r->reset(new SSTableFile(this, std::move(*r)));
      } else if (strstr(f.c_str(), "MANIFEST") != nullptr) {
        r->reset(new ManifestFile(this, std::move(*r)));
      } else if (strstr(f.c_str(), "log") != nullptr) {
        r->reset(new LogFile(this, std::move(*r)));
      }
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& soptions) {
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
                          char* scratch) const {
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
                           const EnvOptions& soptions) {
    class CountingFile : public SequentialFile {
     private:
      unique_ptr<SequentialFile> target_;
      anon::AtomicCounter* counter_;

     public:
      CountingFile(unique_ptr<SequentialFile>&& target,
                   anon::AtomicCounter* counter)
          : target_(std::move(target)), counter_(counter) {}
      virtual Status Read(size_t n, Slice* result, char* scratch) {
        counter_->Increment();
        return target_->Read(n, result, scratch);
      }
      virtual Status Skip(uint64_t n) { return target_->Skip(n); }
    };

    Status s = target()->NewSequentialFile(f, r, soptions);
    if (s.ok() && count_sequential_reads_) {
      r->reset(new CountingFile(std::move(*r), &sequential_read_counter_));
    }
    return s;
  }

  virtual void SleepForMicroseconds(int micros) {
    sleep_counter_.Increment();
    target()->SleepForMicroseconds(micros);
  }
};

class DBTest {
 protected:
  // Sequence of option configurations to try
  enum OptionConfig {
    kDefault = 0,
    kBlockBasedTableWithPrefixHashIndex = 1,
    kBlockBasedTableWithWholeKeyHashIndex = 2,
    kPlainTableFirstBytePrefix = 3,
    kPlainTableAllBytesPrefix = 4,
    kVectorRep = 5,
    kHashLinkList = 6,
    kHashCuckoo = 7,
    kMergePut = 8,
    kFilter = 9,
    kUncompressed = 10,
    kNumLevel_3 = 11,
    kDBLogDir = 12,
    kWalDir = 13,
    kManifestFileSize = 14,
    kCompactOnFlush = 15,
    kPerfOptions = 16,
    kDeletesFilterFirst = 17,
    kHashSkipList = 18,
    kUniversalCompaction = 19,
    kCompressedBlockCache = 20,
    kInfiniteMaxOpenFiles = 21,
    kxxHashChecksum = 22,
    kFIFOCompaction = 23,
    kEnd = 24
  };
  int option_config_;

 public:
  std::string dbname_;
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
  };


  DBTest() : option_config_(kDefault),
             env_(new SpecialEnv(Env::Default())) {
    dbname_ = test::TmpDir() + "/db_test";
    ASSERT_OK(DestroyDB(dbname_, Options()));
    db_ = nullptr;
    Reopen();
  }

  ~DBTest() {
    Close();
    Options options;
    options.db_paths.emplace_back(dbname_, 0);
    options.db_paths.emplace_back(dbname_ + "_2", 0);
    options.db_paths.emplace_back(dbname_ + "_3", 0);
    options.db_paths.emplace_back(dbname_ + "_4", 0);
    ASSERT_OK(DestroyDB(dbname_, options));
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
          option_config_ == kUniversalCompaction) {
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
      if ((skip_mask & kSkipPlainTable)
          && (option_config_ == kPlainTableAllBytesPrefix
              || option_config_ == kPlainTableFirstBytePrefix)) {
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
      break;
    }

    if (option_config_ >= kEnd) {
      Destroy(&last_options_);
      return false;
    } else {
      DestroyAndReopen();
      return true;
    }
  }

  // Switch between different compaction styles (we have only 2 now).
  bool ChangeCompactOptions(Options* prev_options = nullptr) {
    if (option_config_ == kDefault) {
      option_config_ = kUniversalCompaction;
      if (prev_options == nullptr) {
        prev_options = &last_options_;
      }
      Destroy(prev_options);
      TryReopen();
      return true;
    } else {
      return false;
    }
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
        table_options.filter_policy.reset(NewBloomFilterPolicy(10));
        break;
      case kUncompressed:
        options.compression = kNoCompression;
        break;
      case kNumLevel_3:
        options.num_levels = 3;
        break;
      case kDBLogDir:
        options.db_log_dir = test::TmpDir();
        break;
      case kWalDir:
        options.wal_dir = test::TmpDir() + "/wal";
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
      default:
        break;
    }

    if (options_override.filter_policy) {
      table_options.filter_policy = options_override.filter_policy;
    }
    if (set_block_based_table_factory) {
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    }
    return options;
  }

  DBImpl* dbfull() {
    return reinterpret_cast<DBImpl*>(db_);
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const ColumnFamilyOptions* options = nullptr) {
    ColumnFamilyOptions cf_opts;
    if (options != nullptr) {
      cf_opts = ColumnFamilyOptions(*options);
    } else {
      cf_opts = ColumnFamilyOptions(CurrentOptions());
    }
    int cfi = handles_.size();
    handles_.resize(cfi + cfs.size());
    for (auto cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
    }
  }

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const Options* options = nullptr) {
    CreateColumnFamilies(cfs, options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    ReopenWithColumnFamilies(cfs_plus_default, options);
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const std::vector<const Options*>& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const Options* options = nullptr) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  Status TryReopenWithColumnFamilies(
      const std::vector<std::string>& cfs,
      const std::vector<const Options*>& options) {
    Close();
    ASSERT_EQ(cfs.size(), options.size());
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(cfs[i], *options[i]));
    }
    DBOptions db_opts = DBOptions(*options[0]);
    return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const Options* options = nullptr) {
    Close();
    Options opts = (options == nullptr) ? CurrentOptions() : *options;
    std::vector<const Options*> v_opts(cfs.size(), &opts);
    return TryReopenWithColumnFamilies(cfs, v_opts);
  }

  void Reopen(Options* options = nullptr) {
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

  void DestroyAndReopen(Options* options = nullptr) {
    //Destroy using last options
    Destroy(&last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(Options* options) {
    Close();
    ASSERT_OK(DestroyDB(dbname_, *options));
  }

  Status ReadOnlyReopen(Options* options) {
    return DB::OpenForReadOnly(*options, dbname_, &db_);
  }

  Status TryReopen(Options* options = nullptr) {
    Close();
    Options opts;
    if (options != nullptr) {
      opts = *options;
    } else {
      opts = CurrentOptions();
      opts.create_if_missing = true;
    }
    last_options_ = opts;
    return DB::Open(opts, dbname_, &db_);
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
      ASSERT_LT(matched, forward.size());
      ASSERT_EQ(IterStatus(iter), forward[forward.size() - matched - 1]);
      matched++;
    }
    ASSERT_EQ(matched, forward.size());

    delete iter;
    return result;
  }

  std::string AllEntriesFor(const Slice& user_key, int cf = 0) {
    Iterator* iter;
    if (cf == 0) {
      iter = dbfull()->TEST_NewInternalIterator();
    } else {
      iter = dbfull()->TEST_NewInternalIterator(handles_[cf]);
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
    delete iter;
    return result;
  }

  int NumTableFilesAtLevel(int level, int cf = 0) {
    std::string property;
    if (cf == 0) {
      // default cfd
      ASSERT_TRUE(db_->GetProperty(
          "rocksdb.num-files-at-level" + NumberToString(level), &property));
    } else {
      ASSERT_TRUE(db_->GetProperty(
          handles_[cf], "rocksdb.num-files-at-level" + NumberToString(level),
          &property));
    }
    return atoi(property.c_str());
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
    int last_non_zero_offset = 0;
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

  int CountFiles() {
    std::vector<std::string> files;
    env_->GetChildren(dbname_, &files);

    std::vector<std::string> logfiles;
    if (dbname_ != last_options_.wal_dir) {
      env_->GetChildren(last_options_.wal_dir, &logfiles);
    }

    return static_cast<int>(files.size() + logfiles.size());
  }

  int CountLiveFiles() {
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

  void GenerateNewFile(Random* rnd, int* key_idx) {
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(*key_idx), RandomString(rnd, (i == 10) ? 1 : 10000)));
      (*key_idx)++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
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
    ASSERT_OK(status);
    ASSERT_TRUE(iter->Valid());
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
    Iterator* iter;
    if (cf != 0) {
      iter = dbfull()->TEST_NewInternalIterator(handles_[cf]);
    } else {
      iter = dbfull()->TEST_NewInternalIterator();
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
    delete iter;
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
}  // namespace

TEST(DBTest, Empty) {
  do {
    Options options;
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

    std::string num;
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("0", num);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("1", num);

    env_->delay_sstable_sync_.Release_Store(env_);  // Block sync calls
    Put(1, "k1", std::string(100000, 'x'));         // Fill memtable
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("2", num);

    Put(1, "k2", std::string(100000, 'y'));         // Trigger compaction
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("1", num);

    ASSERT_EQ("v1", Get(1, "foo"));
    env_->delay_sstable_sync_.Release_Store(nullptr);   // Release sync calls

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

TEST(DBTest, ReadOnlyDB) {
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  Close();

  Options options;
  ASSERT_OK(ReadOnlyReopen(&options));
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
  Reopen();
  Flush();
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(ReadOnlyReopen(&options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
}

// Make sure that when options.block_cache is set, after a new table is
// created its index/filter blocks are added to block cache.
TEST(DBTest, IndexAndFilterBlocksOfNewTableAddedToCache) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, GetPropertiesOfAllTablesTest) {
  Options options = CurrentOptions();
  Reopen(&options);
  // Create 4 tables
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      db_->Put(WriteOptions(), std::to_string(table * 100 + i), "val");
    }
    db_->Flush(FlushOptions());
  }

  // 1. Read table properties directly from file
  Reopen(&options);
  VerifyTableProperties(db_, 10 + 11 + 12 + 13);

  // 2. Put two tables to table cache and
  Reopen(&options);
  // fetch key from 1st and 2nd table, which will internally place that table to
  // the table cache.
  for (int i = 0; i < 2; ++i) {
    Get(std::to_string(i * 100 + 0));
  }

  VerifyTableProperties(db_, 10 + 11 + 12 + 13);

  // 3. Put all tables to table cache
  Reopen(&options);
  // fetch key from 1st and 2nd table, which will internally place that table to
  // the table cache.
  for (int i = 0; i < 4; ++i) {
    Get(std::to_string(i * 100 + 0));
  }
  VerifyTableProperties(db_, 10 + 11 + 12 + 13);
}

TEST(DBTest, LevelLimitReopen) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, &options);

  const std::string value(1024 * 1024, ' ');
  int i = 0;
  while (NumTableFilesAtLevel(2, 1) == 0) {
    ASSERT_OK(Put(1, Key(i++), value));
  }

  options.num_levels = 1;
  options.max_bytes_for_level_multiplier_additional.resize(1, 1);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, &options);
  ASSERT_EQ(s.IsInvalidArgument(), true);
  ASSERT_EQ(s.ToString(),
            "Invalid argument: db has more levels than options.num_levels");

  options.num_levels = 10;
  options.max_bytes_for_level_multiplier_additional.resize(10, 1);
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, &options));
}

TEST(DBTest, Preallocation) {
  const std::string src = dbname_ + "/alloc_test";
  unique_ptr<WritableFile> srcfile;
  const EnvOptions soptions;
  ASSERT_OK(env_->NewWritableFile(src, &srcfile, soptions));
  srcfile->SetPreallocationBlockSize(1024 * 1024);

  // No writes should mean no preallocation
  size_t block_size, last_allocated_block;
  srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
  ASSERT_EQ(last_allocated_block, 0UL);

  // Small write should preallocate one block
  srcfile->Append("test");
  srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
  ASSERT_EQ(last_allocated_block, 1UL);

  // Write an entire preallocation block, make sure we increased by two.
  std::string buf(block_size, ' ');
  srcfile->Append(buf);
  srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
  ASSERT_EQ(last_allocated_block, 2UL);

  // Write five more blocks at once, ensure we're where we need to be.
  buf = std::string(block_size * 5, ' ');
  srcfile->Append(buf);
  srcfile->GetPreallocationStatus(&block_size, &last_allocated_block);
  ASSERT_EQ(last_allocated_block, 7UL);
}

TEST(DBTest, PutDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
  } while (ChangeOptions());
}


TEST(DBTest, GetFromImmutableLayer) {
  do {
    Options options;
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));

    env_->delay_sstable_sync_.Release_Store(env_);   // Block sync calls
    Put(1, "k1", std::string(100000, 'x'));          // Fill memtable
    Put(1, "k2", std::string(100000, 'y'));          // Trigger flush
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
    env_->delay_sstable_sync_.Release_Store(nullptr);   // Release sync calls
  } while (ChangeOptions());
}

TEST(DBTest, GetFromVersions) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  } while (ChangeOptions());
}

TEST(DBTest, GetSnapshot) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    // Try with both a short key and a long key
    for (int i = 0; i < 2; i++) {
      std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
      ASSERT_OK(Put(1, key, "v1"));
      const Snapshot* s1 = db_->GetSnapshot();
      ASSERT_OK(Put(1, key, "v2"));
      ASSERT_EQ("v2", Get(1, key));
      ASSERT_EQ("v1", Get(1, key, s1));
      ASSERT_OK(Flush(1));
      ASSERT_EQ("v2", Get(1, key));
      ASSERT_EQ("v1", Get(1, key, s1));
      db_->ReleaseSnapshot(s1);
    }
    // skip as HashCuckooRep does not support snapshot
  } while (ChangeOptions(kSkipHashCuckoo));
}

TEST(DBTest, GetLevel0Ordering) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, GetOrderedByLevels) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    ASSERT_OK(Put(1, "foo", "v1"));
    Compact(1, "a", "z");
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v2", Get(1, "foo"));
  } while (ChangeOptions());
}

TEST(DBTest, GetPicksCorrectFile) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, GetEncountersEmptyLevel) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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
TEST(DBTest, KeyMayExist) {
  do {
    ReadOptions ropts;
    std::string value;
    anon::OptionsOverride options_override;
    options_override.filter_policy.reset(NewBloomFilterPolicy(20));
    Options options = CurrentOptions(options_override);
    options.statistics = rocksdb::CreateDBStatistics();
    CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, NonBlockingIteration) {
  do {
    ReadOptions non_blocking_opts, regular_opts;
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    non_blocking_opts.read_tier = kBlockCacheTier;
    CreateAndReopenWithCF({"pikachu"}, &options);
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
  } while (ChangeOptions(kSkipPlainTable | kSkipNoSeekToLast |
                         kSkipHashCuckoo));
}

// A delete is skipped for key if KeyMayExist(key) returns False
// Tests Writebatch consistency and proper delete behaviour
TEST(DBTest, FilterDeletes) {
  do {
    anon::OptionsOverride options_override;
    options_override.filter_policy.reset(NewBloomFilterPolicy(20));
    Options options = CurrentOptions(options_override);
    options.filter_deletes = true;
    CreateAndReopenWithCF({"pikachu"}, &options);
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


TEST(DBTest, IterSeekBeforePrev) {
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

TEST(DBTest, IterLongKeys) {
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


TEST(DBTest, IterNextWithNewerSeq) {
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

TEST(DBTest, IterPrevWithNewerSeq) {
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

TEST(DBTest, IterPrevWithNewerSeq2) {
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

TEST(DBTest, IterEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, IterSingle) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, IterMulti) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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
TEST(DBTest, IterReseek) {
  Options options = CurrentOptions();
  options.max_sequential_skip_in_iterations = 3;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(&options);
  CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, IterSmallAndLargeMix) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, IterMultiWithDelete) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, IterPrevMaxSkip) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, IterWithSnapshot) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, Recover) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_EQ("v1", Get(1, "foo"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v5", Get(1, "baz"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Put(1, "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v4"));
    ASSERT_EQ("v4", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v5", Get(1, "baz"));
  } while (ChangeOptions());
}

TEST(DBTest, RecoverWithTableHandle) {
  do {
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 100;
    options.disable_auto_compactions = true;
    options = CurrentOptions(options);
    DestroyAndReopen(&options);
    CreateAndReopenWithCF({"pikachu"}, &options);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "foo", "v3"));
    ASSERT_OK(Put(1, "bar", "v4"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "big", std::string(100, 'a')));
    ReopenWithColumnFamilies({"default", "pikachu"});

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

TEST(DBTest, IgnoreRecoveredLog) {
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
    DestroyAndReopen(&options);

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
    Reopen(&options);
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
    Reopen(&options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));
    Close();
    Destroy(&options);
    Reopen(&options);
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
    Reopen(&options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));

    // Recovery will fail if DB directory doesn't exist.
    Destroy(&options);
    // copy the logs from backup back to wal dir
    env_->CreateDirIfMissing(options.wal_dir);
    for (auto& log : logs) {
      if (log != ".." && log != ".") {
        CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
        // we won't be needing this file no more
        env_->DeleteFile(backup_logs + "/" + log);
      }
    }
    Status s = TryReopen(&options);
    ASSERT_TRUE(!s.ok());
  } while (ChangeOptions(kSkipHashCuckoo));
}

TEST(DBTest, RollLog) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"});
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"});
    }
    ASSERT_OK(Put(1, "foo", "v4"));
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"});
    }
  } while (ChangeOptions());
}

TEST(DBTest, WAL) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));

    ReopenWithColumnFamilies({"default", "pikachu"});
    // Both value's should be present.
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v2", Get(1, "foo"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"});
    // again both values should be present.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));
  } while (ChangeCompactOptions());
}

TEST(DBTest, CheckLock) {
  do {
    DB* localdb;
    Options options = CurrentOptions();
    ASSERT_OK(TryReopen(&options));

    // second open should fail
    ASSERT_TRUE(!(DB::Open(options, dbname_, &localdb)).ok());
  } while (ChangeCompactOptions());
}

TEST(DBTest, FlushMultipleMemtable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    CreateAndReopenWithCF({"pikachu"}, &options);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));
    ASSERT_OK(Flush(1));
  } while (ChangeCompactOptions());
}

TEST(DBTest, NumImmutableMemTable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    options.write_buffer_size = 1000000;
    CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, GetProperty) {
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
  Reopen(&options);

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
  ASSERT_EQ(num, "4");
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
  ASSERT_EQ(int_num, 4U);

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
  Reopen(&options);
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
}

TEST(DBTest, FLUSH) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_EQ("v2", Get(1, "bar"));
    perf_context.Reset();
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_TRUE((int) perf_context.get_from_output_files_time > 0);

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"});
    // 'foo' should be there because its put
    // has WAL enabled.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST(DBTest, RecoveryWithEmptyLog) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ReopenWithColumnFamilies({"default", "pikachu"});
    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_OK(Put(1, "foo", "v3"));
    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_EQ("v3", Get(1, "foo"));
  } while (ChangeOptions());
}

// Check that writes done during a memtable compaction are recovered
// if the database is shutdown during the memtable compaction.
TEST(DBTest, RecoverDuringMemtableCompaction) {
  do {
    Options options;
    options.env = env_;
    options.write_buffer_size = 1000000;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

    // Trigger a long memtable compaction and reopen the database during it
    ASSERT_OK(Put(1, "foo", "v1"));  // Goes to 1st log file
    ASSERT_OK(Put(1, "big1", std::string(10000000, 'x')));  // Fills memtable
    ASSERT_OK(Put(1, "big2", std::string(1000, 'y')));  // Triggers compaction
    ASSERT_OK(Put(1, "bar", "v2"));                     // Goes to new log file

    ReopenWithColumnFamilies({"default", "pikachu"}, &options);
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ(std::string(10000000, 'x'), Get(1, "big1"));
    ASSERT_EQ(std::string(1000, 'y'), Get(1, "big2"));
  } while (ChangeOptions());
}

TEST(DBTest, MinorCompactionsHappen) {
  do {
    Options options;
    options.write_buffer_size = 10000;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

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

    ReopenWithColumnFamilies({"default", "pikachu"}, &options);

    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(1, Key(i)));
    }
  } while (ChangeCompactOptions());
}

TEST(DBTest, ManifestRollOver) {
  do {
    Options options;
    options.max_manifest_file_size = 10 ;  // 10 bytes
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);
    {
      ASSERT_OK(Put(1, "manifest_key1", std::string(1000, '1')));
      ASSERT_OK(Put(1, "manifest_key2", std::string(1000, '2')));
      ASSERT_OK(Put(1, "manifest_key3", std::string(1000, '3')));
      uint64_t manifest_before_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_OK(Flush(1));  // This should trigger LogAndApply.
      uint64_t manifest_after_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_GT(manifest_after_flush, manifest_before_flush);
      ReopenWithColumnFamilies({"default", "pikachu"}, &options);
      ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(), manifest_after_flush);
      // check if a new manifest file got inserted or not.
      ASSERT_EQ(std::string(1000, '1'), Get(1, "manifest_key1"));
      ASSERT_EQ(std::string(1000, '2'), Get(1, "manifest_key2"));
      ASSERT_EQ(std::string(1000, '3'), Get(1, "manifest_key3"));
    }
  } while (ChangeCompactOptions());
}

TEST(DBTest, IdentityAcrossRestarts) {
  do {
    std::string id1;
    ASSERT_OK(db_->GetDbIdentity(id1));

    Options options = CurrentOptions();
    Reopen(&options);
    std::string id2;
    ASSERT_OK(db_->GetDbIdentity(id2));
    // id1 should match id2 because identity was not regenerated
    ASSERT_EQ(id1.compare(id2), 0);

    std::string idfilename = IdentityFileName(dbname_);
    ASSERT_OK(env_->DeleteFile(idfilename));
    Reopen(&options);
    std::string id3;
    ASSERT_OK(db_->GetDbIdentity(id3));
    // id1 should NOT match id3 because identity was regenerated
    ASSERT_NE(id1.compare(id3), 0);
  } while (ChangeCompactOptions());
}

TEST(DBTest, RecoverWithLargeLog) {
  do {
    {
      Options options = CurrentOptions();
      CreateAndReopenWithCF({"pikachu"}, &options);
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
    ReopenWithColumnFamilies({"default", "pikachu"}, &options);
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 3);
    ASSERT_EQ(std::string(200000, '1'), Get(1, "big1"));
    ASSERT_EQ(std::string(200000, '2'), Get(1, "big2"));
    ASSERT_EQ(std::string(10, '3'), Get(1, "small3"));
    ASSERT_EQ(std::string(10, '4'), Get(1, "small4"));
    ASSERT_GT(NumTableFilesAtLevel(0, 1), 1);
  } while (ChangeCompactOptions());
}

TEST(DBTest, CompactionsGenerateMultipleFiles) {
  Options options;
  options.write_buffer_size = 100000000;        // Large write buffer
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, &options);

  Random rnd(301);

  // Write 8MB (80 values, each 100K)
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  std::vector<std::string> values;
  for (int i = 0; i < 80; i++) {
    values.push_back(RandomString(&rnd, 100000));
    ASSERT_OK(Put(1, Key(i), values[i]));
  }

  // Reopening moves updates to level-0
  ReopenWithColumnFamilies({"default", "pikachu"}, &options);
  dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);

  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_GT(NumTableFilesAtLevel(1, 1), 1);
  for (int i = 0; i < 80; i++) {
    ASSERT_EQ(Get(1, Key(i)), values[i]);
  }
}

TEST(DBTest, CompactionTrigger) {
  Options options;
  options.write_buffer_size = 100<<10; //100KB
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.level0_file_num_compaction_trigger = 3;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, CompactionDeletionTrigger) {
  Options options = DeletionTriggerOptions();
  options.create_if_missing = true;

  for (int tid = 0; tid < 2; ++tid) {
    uint64_t db_size[2];

    DestroyAndReopen(&options);
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

    // repeat the test with universal compaction
    options.compaction_style = kCompactionStyleUniversal;
    options.num_levels = 1;
  }
}

TEST(DBTest, CompactionDeletionTriggerReopen) {
  for (int tid = 0; tid < 2; ++tid) {
    uint64_t db_size[3];
    Options options = DeletionTriggerOptions();
    options.create_if_missing = true;

    DestroyAndReopen(&options);
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
    Reopen(&options);

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
    Reopen(&options);
    // insert relatively small amount of data to trigger auto compaction.
    for (int k = 0; k < kTestSize / 10; ++k) {
      ASSERT_OK(Put(Key(k), values[k]));
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
    db_size[2] = Size(Key(0), Key(kTestSize - 1));
    // this time we're expecting significant drop in size.
    ASSERT_GT(db_size[0] / 3, db_size[2]);

    // repeat the test with universal compaction
    options.compaction_style = kCompactionStyleUniversal;
    options.num_levels = 1;
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
      ASSERT_EQ(expect_full_compaction_.load(), context.is_full_compaction);
      ASSERT_EQ(expect_manual_compaction_.load(), context.is_manual_compaction);
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

class ChangeFilterFactory : public CompactionFilterFactory {
 public:
  explicit ChangeFilterFactory() {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    return std::unique_ptr<CompactionFilter>(new ChangeFilter());
  }

  virtual const char* Name() const override { return "ChangeFilterFactory"; }
};

// TODO(kailiu) The tests on UniversalCompaction has some issues:
//  1. A lot of magic numbers ("11" or "12").
//  2. Made assumption on the memtable flush conidtions, which may change from
//     time to time.
TEST(DBTest, UniversalCompactionTrigger) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
  KeepFilterFactory* filter = new KeepFilterFactory(true);
  filter->expect_manual_compaction_.store(false);
  options.compaction_filter_factory.reset(filter);

  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, &options);

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
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), num + 1);
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
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i, 1), 0);
  }

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
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 0.4, 1, 1.
  // After comapction, we should have 2 files, with size 4, 2.4.
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 2);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i, 1), 0);
  }

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
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 12; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 2.4, 1, 1.
  // After comapction, we should have 3 files, with size 4, 2.4, 2.
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 3);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i, 1), 0);
  }

  // Stage 4:
  //   Now we have 3 files at level 0, with size 4, 2.4, 2. Let's generate a
  //   new file of size 1.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Level-0 compaction is triggered, but no file will be picked up.
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 4);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i, 1), 0);
  }

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
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i, 1), 0);
  }
}

TEST(DBTest, UniversalCompactionSizeAmplification) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 3;
  CreateAndReopenWithCF({"pikachu"}, &options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, &options);

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
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), num + 1);
  }
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 2);

  // Flush whatever is remaining in memtable. This is typically
  // small, which should not trigger size ratio based compaction
  // but will instead trigger size amplification.
  ASSERT_OK(Flush(1));

  dbfull()->TEST_WaitForCompact();

  // Verify that size amplification did occur
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);
}

TEST(DBTest, UniversalCompactionOptions) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 1;
  options.compaction_options_universal.compression_size_percent = -1;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, &options);

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
      ASSERT_EQ(NumTableFilesAtLevel(0, 1), num + 1);
    }
  }

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i, 1), 0);
  }
}

TEST(DBTest, UniversalCompactionStopStyleSimilarSize) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
  options.compaction_options_universal.size_ratio = 10;
  options.compaction_options_universal.stop_style = kCompactionStopStyleSimilarSize;
  options.num_levels=1;
  Reopen(&options);

  Random rnd(301);
  int key_idx = 0;

  // Stage 1:
  //   Generate a set of files at level 0, but don't trigger level-0
  //   compaction.
  for (int num = 0;
       num < options.level0_file_num_compaction_trigger-1;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumTableFilesAtLevel(0), num + 1);
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
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);

  // Stage 2:
  //   Now we have one file at level 0, with size 4. We also have some data in
  //   mem table. Let's continue generating new files at level 0, but don't
  //   trigger level-0 compaction.
  //   First, clean up memtable before inserting new data. This will generate
  //   a level-0 file, with size around 0.4 (according to previously written
  //   data amount).
  dbfull()->Flush(FlushOptions());
  for (int num = 0;
       num < options.level0_file_num_compaction_trigger-3;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumTableFilesAtLevel(0), num + 3);
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
  ASSERT_EQ(NumTableFilesAtLevel(0), 3);
  // Stage 3:
  //   Now we have 3 files at level 0, with size 4, 0.4, 2. Generate one
  //   more file at level-0, which should trigger level-0 compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Level-0 compaction is triggered, but no file will be picked up.
  ASSERT_EQ(NumTableFilesAtLevel(0), 4);
}

#if defined(SNAPPY)
TEST(DBTest, CompressedCache) {
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
    CreateAndReopenWithCF({"pikachu"}, &options);
    // default column family doesn't have block cache
    Options no_block_cache_opts;
    no_block_cache_opts.statistics = options.statistics;
    BlockBasedTableOptions table_options_no_bc;
    table_options_no_bc.no_block_cache = true;
    no_block_cache_opts.table_factory.reset(
        NewBlockBasedTableFactory(table_options_no_bc));
    ReopenWithColumnFamilies({"default", "pikachu"},
                             {&no_block_cache_opts, &options});

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
    DestroyAndReopen(&options);
  }
}

static std::string CompressibleString(Random* rnd, int len) {
  std::string r;
  test::CompressibleString(rnd, 0.8, len, &r);
  return r;
}

TEST(DBTest, UniversalCompactionCompressRatio1) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 1;
  options.compaction_options_universal.compression_size_percent = 70;
  options = CurrentOptions(options);
  Reopen(&options);

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
  ASSERT_LT((int)dbfull()->TEST_GetLevel0TotalSize(), 110000 * 2 * 0.9);

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
  ASSERT_LT((int)dbfull()->TEST_GetLevel0TotalSize(), 110000 * 4 * 0.9);

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
  ASSERT_LT((int)dbfull()->TEST_GetLevel0TotalSize(), 110000 * 6 * 0.9);

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
  ASSERT_GT((int)dbfull()->TEST_GetLevel0TotalSize(),
            110000 * 11 * 0.8 + 110000 * 2);
}

TEST(DBTest, UniversalCompactionCompressRatio2) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 1;
  options.compaction_options_universal.compression_size_percent = 95;
  options = CurrentOptions(options);
  Reopen(&options);

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
  ASSERT_LT((int)dbfull()->TEST_GetLevel0TotalSize(),
            120000 * 12 * 0.8 + 120000 * 2);
}

TEST(DBTest, FailMoreDbPaths) {
  Options options;
  options.db_paths.emplace_back(dbname_, 10000000);
  options.db_paths.emplace_back(dbname_ + "_2", 1000000);
  options.db_paths.emplace_back(dbname_ + "_3", 1000000);
  options.db_paths.emplace_back(dbname_ + "_4", 1000000);
  options.db_paths.emplace_back(dbname_ + "_5", 1000000);
  ASSERT_TRUE(TryReopen(&options).IsNotSupported());
}

TEST(DBTest, UniversalCompactionSecondPathRatio) {
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
  Reopen(&options);

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

  Reopen(&options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Destroy(&options);
}

TEST(DBTest, UniversalCompactionFourPaths) {
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
  Reopen(&options);

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

  Reopen(&options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 10000);
  }

  Destroy(&options);
}
#endif

TEST(DBTest, ConvertCompactionStyle) {
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
  CreateAndReopenWithCF({"pikachu"}, &options);

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
  options = CurrentOptions(options);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, &options);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Stage 3: compact into a single file and move the file to level 0
  options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = INT_MAX;
  options.target_file_size_multiplier = 1;
  options.max_bytes_for_level_base = INT_MAX;
  options.max_bytes_for_level_multiplier = 1;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, &options);

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
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 3;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, &options);

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

  if (SnappyCompressionSupported(CompressionOptions(wbits, lev, strategy))) {
    type = kSnappyCompression;
    fprintf(stderr, "using snappy\n");
  } else if (ZlibCompressionSupported(
               CompressionOptions(wbits, lev, strategy))) {
    type = kZlibCompression;
    fprintf(stderr, "using zlib\n");
  } else if (BZip2CompressionSupported(
               CompressionOptions(wbits, lev, strategy))) {
    type = kBZip2Compression;
    fprintf(stderr, "using bzip2\n");
  } else if (LZ4CompressionSupported(
                 CompressionOptions(wbits, lev, strategy))) {
    type = kLZ4Compression;
    fprintf(stderr, "using lz4\n");
  } else if (LZ4HCCompressionSupported(
                 CompressionOptions(wbits, lev, strategy))) {
    type = kLZ4HCCompression;
    fprintf(stderr, "using lz4hc\n");
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

TEST(DBTest, MinLevelToCompress1) {
  Options options = CurrentOptions();
  CompressionType type;
  if (!MinLevelToCompress(type, options, -14, -1, 0)) {
    return;
  }
  Reopen(&options);
  MinLevelHelper(this, options);

  // do not compress L0 and L1
  for (int i = 0; i < 2; i++) {
    options.compression_per_level[i] = kNoCompression;
  }
  for (int i = 2; i < options.num_levels; i++) {
    options.compression_per_level[i] = type;
  }
  DestroyAndReopen(&options);
  MinLevelHelper(this, options);
}

TEST(DBTest, MinLevelToCompress2) {
  Options options = CurrentOptions();
  CompressionType type;
  if (!MinLevelToCompress(type, options, 15, -1, 0)) {
    return;
  }
  Reopen(&options);
  MinLevelHelper(this, options);

  // do not compress L0 and L1
  for (int i = 0; i < 2; i++) {
    options.compression_per_level[i] = kNoCompression;
  }
  for (int i = 2; i < options.num_levels; i++) {
    options.compression_per_level[i] = type;
  }
  DestroyAndReopen(&options);
  MinLevelHelper(this, options);
}

TEST(DBTest, RepeatedWritesToSameKey) {
  do {
    Options options;
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

    // We must have at most one file per level except for level-0,
    // which may have up to kL0_StopWritesTrigger files.
    const int kMaxFiles =
        options.num_levels + options.level0_stop_writes_trigger;

    Random rnd(301);
    std::string value = RandomString(&rnd, 2 * options.write_buffer_size);
    for (int i = 0; i < 5 * kMaxFiles; i++) {
      ASSERT_OK(Put(1, "key", value));
      ASSERT_LE(TotalTableFiles(1), kMaxFiles);
    }
  } while (ChangeCompactOptions());
}

TEST(DBTest, InPlaceUpdate) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, InPlaceUpdateLargeNewValue) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

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


TEST(DBTest, InPlaceUpdateCallbackSmallerSize) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceSmallerSize;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, InPlaceUpdateCallbackSmallerVarintSize) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceSmallerVarintSize;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, InPlaceUpdateCallbackLargeNewValue) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceLargerSize;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, InPlaceUpdateCallbackNoAction) {
  do {
    Options options;
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceNoAction;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

    // Callback function requests no actions from db
    ASSERT_OK(Put(1, "key", DummyString(1, 'a')));
    ASSERT_EQ(Get(1, "key"), "NOT_FOUND");

  } while (ChangeCompactOptions());
}

TEST(DBTest, CompactionFilter) {
  Options options = CurrentOptions();
  options.max_open_files = -1;
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.compaction_filter_factory = std::make_shared<KeepFilterFactory>();
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, &options);

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
  Iterator* iter = dbfull()->TEST_NewInternalIterator(handles_[1]);
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
  ASSERT_EQ(total, 100000);
  ASSERT_EQ(count, 1);
  delete iter;

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
  DestroyAndReopen(&options);
  CreateAndReopenWithCF({"pikachu"}, &options);

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

  // Scan the entire database to ensure that nothing is left
  iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  count = 0;
  while (iter->Valid()) {
    count++;
    iter->Next();
  }
  ASSERT_EQ(count, 0);
  delete iter;

  // The sequence number of the remaining record
  // is not zeroed out even though it is at the
  // level Lmax because this record is at the tip
  // TODO: remove the following or design a different
  // test
  count = 0;
  iter = dbfull()->TEST_NewInternalIterator(handles_[1]);
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
  delete iter;
}

// Tests the edge case where compaction does not produce any output -- all
// entries are deleted. The compaction should create bunch of 'DeleteFile'
// entries in VersionEdit, but none of the 'AddFile's.
TEST(DBTest, CompactionFilterDeletesAll) {
  Options options;
  options.compaction_filter_factory = std::make_shared<DeleteFilterFactory>();
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  DestroyAndReopen(&options);

  // put some data
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      Put(std::to_string(table * 100 + i), "val");
    }
    Flush();
  }

  // this will produce empty file (delete compaction filter)
  ASSERT_OK(db_->CompactRange(nullptr, nullptr));
  ASSERT_EQ(0, CountLiveFiles());

  Reopen(&options);

  Iterator* itr = db_->NewIterator(ReadOptions());
  itr->SeekToFirst();
  // empty db
  ASSERT_TRUE(!itr->Valid());

  delete itr;
}

TEST(DBTest, CompactionFilterWithValueChange) {
  do {
    Options options;
    options.num_levels = 3;
    options.max_mem_compaction_level = 0;
    options.compaction_filter_factory =
      std::make_shared<ChangeFilterFactory>();
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, CompactionFilterContextManual) {
  KeepFilterFactory* filter = new KeepFilterFactory();

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_filter_factory.reset(filter);
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 8;
  Reopen(&options);
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
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);

  // Verify total number of keys is correct after manual compaction.
  int count = 0;
  int total = 0;
  Iterator* iter = dbfull()->TEST_NewInternalIterator();
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
  delete iter;
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

TEST(DBTest, CompactionFilterV2) {
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
  Reopen(&options);

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

  ASSERT_EQ(NumTableFilesAtLevel(0), 1);

  // All the files are in the lowest level.
  int count = 0;
  int total = 0;
  Iterator* iter = dbfull()->TEST_NewInternalIterator();
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

  ASSERT_EQ(total, 100000);
  // 1 snapshot only. Since we are using universal compacton,
  // the sequence no is cleared for better compression
  ASSERT_EQ(count, 1);
  delete iter;

  // create a new database with the compaction
  // filter in such a way that it deletes all keys
  options.compaction_filter_factory_v2 =
    std::make_shared<DeleteFilterFactoryV2>(prefix_extractor.get());
  options.create_if_missing = true;
  DestroyAndReopen(&options);

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
  iter = db_->NewIterator(ReadOptions());
  iter->SeekToFirst();
  count = 0;
  while (iter->Valid()) {
    count++;
    iter->Next();
  }

  ASSERT_EQ(count, 0);
  delete iter;
}

TEST(DBTest, CompactionFilterV2WithValueChange) {
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
  Reopen(&options);

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

TEST(DBTest, CompactionFilterV2NULLPrefix) {
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
  Reopen(&options);

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
    std::string newvalue = Get(key);
    ASSERT_EQ(newvalue.compare(NEW_VALUE), 0);
  }
}

TEST(DBTest, SparseMerge) {
  do {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, ApproximateSizes) {
  do {
    Options options;
    options.write_buffer_size = 100000000;        // Large write buffer
    options.compression = kNoCompression;
    options = CurrentOptions(options);
    DestroyAndReopen();
    CreateAndReopenWithCF({"pikachu"}, &options);

    ASSERT_TRUE(Between(Size("", "xyz", 1), 0, 0));
    ReopenWithColumnFamilies({"default", "pikachu"}, &options);
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
      ReopenWithColumnFamilies({"default", "pikachu"}, &options);

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

TEST(DBTest, ApproximateSizes_MixOfSmallAndLarge) {
  do {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    CreateAndReopenWithCF({"pikachu"}, &options);

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
      ReopenWithColumnFamilies({"default", "pikachu"}, &options);

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

TEST(DBTest, IteratorPinsRef) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, Snapshot) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    Put(0, "foo", "0v1");
    Put(1, "foo", "1v1");
    const Snapshot* s1 = db_->GetSnapshot();
    Put(0, "foo", "0v2");
    Put(1, "foo", "1v2");
    const Snapshot* s2 = db_->GetSnapshot();
    Put(0, "foo", "0v3");
    Put(1, "foo", "1v3");
    const Snapshot* s3 = db_->GetSnapshot();

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

    db_->ReleaseSnapshot(s2);
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));
  } while (ChangeOptions(kSkipHashCuckoo));
}

TEST(DBTest, HiddenValuesAreRemoved) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, CompactBetweenSnapshots) {
  do {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, DeletionMarkers1) {
  CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, DeletionMarkers2) {
  CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, OverlapInLevel0) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, L0_CompactionBug_Issue44_a) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    ASSERT_OK(Put(1, "b", "v"));
    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_OK(Delete(1, "b"));
    ASSERT_OK(Delete(1, "a"));
    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_OK(Delete(1, "a"));
    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_OK(Put(1, "a", "v"));
    ReopenWithColumnFamilies({"default", "pikachu"});
    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_EQ("(a->v)", Contents(1));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(a->v)", Contents(1));
  } while (ChangeCompactOptions());
}

TEST(DBTest, L0_CompactionBug_Issue44_b) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    Put(1, "", "");
    ReopenWithColumnFamilies({"default", "pikachu"});
    Delete(1, "e");
    Put(1, "", "");
    ReopenWithColumnFamilies({"default", "pikachu"});
    Put(1, "c", "cv");
    ReopenWithColumnFamilies({"default", "pikachu"});
    Put(1, "", "");
    ReopenWithColumnFamilies({"default", "pikachu"});
    Put(1, "", "");
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ReopenWithColumnFamilies({"default", "pikachu"});
    Put(1, "d", "dv");
    ReopenWithColumnFamilies({"default", "pikachu"});
    Put(1, "", "");
    ReopenWithColumnFamilies({"default", "pikachu"});
    Delete(1, "d");
    Delete(1, "b");
    ReopenWithColumnFamilies({"default", "pikachu"});
    ASSERT_EQ("(->)(c->cv)", Contents(1));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(->)(c->cv)", Contents(1));
  } while (ChangeCompactOptions());
}

TEST(DBTest, ComparatorCheck) {
  class NewComparator : public Comparator {
   public:
    virtual const char* Name() const { return "rocksdb.NewComparator"; }
    virtual int Compare(const Slice& a, const Slice& b) const {
      return BytewiseComparator()->Compare(a, b);
    }
    virtual void FindShortestSeparator(std::string* s, const Slice& l) const {
      BytewiseComparator()->FindShortestSeparator(s, l);
    }
    virtual void FindShortSuccessor(std::string* key) const {
      BytewiseComparator()->FindShortSuccessor(key);
    }
  };
  Options new_options, options;
  NewComparator cmp;
  do {
    CreateAndReopenWithCF({"pikachu"});
    options = CurrentOptions();
    new_options = CurrentOptions();
    new_options.comparator = &cmp;
    // only the non-default column family has non-matching comparator
    Status s = TryReopenWithColumnFamilies({"default", "pikachu"},
                                           {&options, &new_options});
    ASSERT_TRUE(!s.ok());
    ASSERT_TRUE(s.ToString().find("comparator") != std::string::npos)
        << s.ToString();
  } while (ChangeCompactOptions(&new_options));
}

TEST(DBTest, CustomComparator) {
  class NumberComparator : public Comparator {
   public:
    virtual const char* Name() const { return "test.NumberComparator"; }
    virtual int Compare(const Slice& a, const Slice& b) const {
      return ToNumber(a) - ToNumber(b);
    }
    virtual void FindShortestSeparator(std::string* s, const Slice& l) const {
      ToNumber(*s);     // Check format
      ToNumber(l);      // Check format
    }
    virtual void FindShortSuccessor(std::string* key) const {
      ToNumber(*key);   // Check format
    }
   private:
    static int ToNumber(const Slice& x) {
      // Check that there are no extra characters.
      ASSERT_TRUE(x.size() >= 2 && x[0] == '[' && x[x.size()-1] == ']')
          << EscapeString(x);
      int val;
      char ignored;
      ASSERT_TRUE(sscanf(x.ToString().c_str(), "[%i]%c", &val, &ignored) == 1)
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
    DestroyAndReopen(&new_options);
    CreateAndReopenWithCF({"pikachu"}, &new_options);
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
  } while (ChangeCompactOptions(&new_options));
}

TEST(DBTest, ManualCompaction) {
  CreateAndReopenWithCF({"pikachu"});
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
      Options options = CurrentOptions();
      options.num_levels = 3;
      options.create_if_missing = true;
      DestroyAndReopen(&options);
      CreateAndReopenWithCF({"pikachu"}, &options);
    }
  }

}

TEST(DBTest, ManualCompactionOutputPathId) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.db_paths.emplace_back(dbname_, 1000000000);
  options.db_paths.emplace_back(dbname_ + "_2", 1000000000);
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = 10;
  Destroy(&options);
  DestroyAndReopen(&options);
  CreateAndReopenWithCF({"pikachu"}, &options);
  MakeTables(3, "p", "q", 1);
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("3", FilesPerLevel(1));
  ASSERT_EQ(3, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[1].path));

  // Full compaction to DB path 0
  db_->CompactRange(handles_[1], nullptr, nullptr, false, -1, 1);
  ASSERT_EQ("1", FilesPerLevel(1));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, &options);
  ASSERT_EQ("1", FilesPerLevel(1));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  MakeTables(1, "p", "q", 1);
  ASSERT_EQ("2", FilesPerLevel(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, &options);
  ASSERT_EQ("2", FilesPerLevel(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  // Full compaction to DB path 0
  db_->CompactRange(handles_[1], nullptr, nullptr, false, -1, 0);
  ASSERT_EQ("1", FilesPerLevel(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[1].path));

  // Fail when compacting to an invalid path ID
  ASSERT_TRUE(db_->CompactRange(handles_[1], nullptr, nullptr, false, -1, 2)
                  .IsInvalidArgument());
}

TEST(DBTest, DBOpen_Options) {
  std::string dbname = test::TmpDir() + "/db_options_test";
  ASSERT_OK(DestroyDB(dbname, Options()));

  // Does not exist, and create_if_missing == false: error
  DB* db = nullptr;
  Options opts;
  opts.create_if_missing = false;
  Status s = DB::Open(opts, dbname, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "does not exist") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does not exist, and create_if_missing == true: OK
  opts.create_if_missing = true;
  s = DB::Open(opts, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;

  // Does exist, and error_if_exists == true: error
  opts.create_if_missing = false;
  opts.error_if_exists = true;
  s = DB::Open(opts, dbname, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "exists") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does exist, and error_if_exists == false: OK
  opts.create_if_missing = true;
  opts.error_if_exists = false;
  s = DB::Open(opts, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;
}

TEST(DBTest, DBOpen_Change_NumLevels) {
  Options opts;
  opts.create_if_missing = true;
  DestroyAndReopen(&opts);
  ASSERT_TRUE(db_ != nullptr);
  CreateAndReopenWithCF({"pikachu"}, &opts);

  ASSERT_OK(Put(1, "a", "123"));
  ASSERT_OK(Put(1, "b", "234"));
  db_->CompactRange(handles_[1], nullptr, nullptr);
  Close();

  opts.create_if_missing = false;
  opts.num_levels = 2;
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, &opts);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "Invalid argument") != nullptr);
  ASSERT_TRUE(db_ == nullptr);
}

TEST(DBTest, DestroyDBMetaDatabase) {
  std::string dbname = test::TmpDir() + "/db_meta";
  std::string metadbname = MetaDatabaseName(dbname, 0);
  std::string metametadbname = MetaDatabaseName(metadbname, 0);

  // Destroy previous versions if they exist. Using the long way.
  ASSERT_OK(DestroyDB(metametadbname, Options()));
  ASSERT_OK(DestroyDB(metadbname, Options()));
  ASSERT_OK(DestroyDB(dbname, Options()));

  // Setup databases
  Options opts;
  opts.create_if_missing = true;
  DB* db = nullptr;
  ASSERT_OK(DB::Open(opts, dbname, &db));
  delete db;
  db = nullptr;
  ASSERT_OK(DB::Open(opts, metadbname, &db));
  delete db;
  db = nullptr;
  ASSERT_OK(DB::Open(opts, metametadbname, &db));
  delete db;
  db = nullptr;

  // Delete databases
  ASSERT_OK(DestroyDB(dbname, Options()));

  // Check if deletion worked.
  opts.create_if_missing = false;
  ASSERT_TRUE(!(DB::Open(opts, dbname, &db)).ok());
  ASSERT_TRUE(!(DB::Open(opts, metadbname, &db)).ok());
  ASSERT_TRUE(!(DB::Open(opts, metametadbname, &db)).ok());
}

// Check that number of files does not grow when we are out of space
TEST(DBTest, NoSpace) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.paranoid_checks = false;
    Reopen(&options);

    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    Compact("a", "z");
    const int num_files = CountFiles();
    env_->no_space_.Release_Store(env_);   // Force out-of-space errors
    env_->sleep_counter_.Reset();
    for (int i = 0; i < 5; i++) {
      for (int level = 0; level < dbfull()->NumberLevels()-1; level++) {
        dbfull()->TEST_CompactRange(level, nullptr, nullptr);
      }
    }

    std::string property_value;
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("5", property_value);

    env_->no_space_.Release_Store(nullptr);
    ASSERT_LT(CountFiles(), num_files + 3);

    // Check that compaction attempts slept after errors
    ASSERT_GE(env_->sleep_counter_.Read(), 5);
  } while (ChangeCompactOptions());
}

// Check background error counter bumped on flush failures.
TEST(DBTest, NoSpaceFlush) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.max_background_flushes = 1;
    Reopen(&options);

    ASSERT_OK(Put("foo", "v1"));
    env_->no_space_.Release_Store(env_);  // Force out-of-space errors

    std::string property_value;
    // Background error count is 0 now.
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("0", property_value);

    dbfull()->TEST_FlushMemTable(false);

    // Wait 300 milliseconds or background-errors turned 1 from 0.
    int time_to_sleep_limit = 300000;
    while (time_to_sleep_limit > 0) {
      int to_sleep = (time_to_sleep_limit > 1000) ? 1000 : time_to_sleep_limit;
      time_to_sleep_limit -= to_sleep;
      env_->SleepForMicroseconds(to_sleep);

      ASSERT_TRUE(
          db_->GetProperty("rocksdb.background-errors", &property_value));
      if (property_value == "1") {
        break;
      }
    }
    ASSERT_EQ("1", property_value);

    env_->no_space_.Release_Store(nullptr);
  } while (ChangeCompactOptions());
}

TEST(DBTest, NonWritableFileSystem) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 1000;
    options.env = env_;
    Reopen(&options);
    ASSERT_OK(Put("foo", "v1"));
    env_->non_writable_.Release_Store(env_); // Force errors for new files
    std::string big(100000, 'x');
    int errors = 0;
    for (int i = 0; i < 20; i++) {
      if (!Put("foo", big).ok()) {
        errors++;
        env_->SleepForMicroseconds(100000);
      }
    }
    ASSERT_GT(errors, 0);
    env_->non_writable_.Release_Store(nullptr);
  } while (ChangeCompactOptions());
}

TEST(DBTest, ManifestWriteError) {
  // Test for the following problem:
  // (a) Compaction produces file F
  // (b) Log record containing F is written to MANIFEST file, but Sync() fails
  // (c) GC deletes F
  // (d) After reopening DB, reads fail since deleted F is named in log record

  // We iterate twice.  In the second iteration, everything is the
  // same except the log record never makes it to the MANIFEST file.
  for (int iter = 0; iter < 2; iter++) {
    port::AtomicPointer* error_type = (iter == 0)
        ? &env_->manifest_sync_error_
        : &env_->manifest_write_error_;

    // Insert foo=>bar mapping
    Options options = CurrentOptions();
    options.env = env_;
    options.create_if_missing = true;
    options.error_if_exists = false;
    DestroyAndReopen(&options);
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_EQ("bar", Get("foo"));

    // Memtable compaction (will succeed)
    Flush();
    ASSERT_EQ("bar", Get("foo"));
    const int last = dbfull()->MaxMemCompactionLevel();
    ASSERT_EQ(NumTableFilesAtLevel(last), 1);   // foo=>bar is now in last level

    // Merging compaction (will fail)
    error_type->Release_Store(env_);
    dbfull()->TEST_CompactRange(last, nullptr, nullptr);  // Should fail
    ASSERT_EQ("bar", Get("foo"));

    // Recovery: should not lose data
    error_type->Release_Store(nullptr);
    Reopen(&options);
    ASSERT_EQ("bar", Get("foo"));
  }
}

TEST(DBTest, PutFailsParanoid) {
  // Test the following:
  // (a) A random put fails in paranoid mode (simulate by sync fail)
  // (b) All other puts have to fail, even if writes would succeed
  // (c) All of that should happen ONLY if paranoid_checks = true

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  DestroyAndReopen(&options);
  CreateAndReopenWithCF({"pikachu"}, &options);
  Status s;

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.Release_Store(env_);
  s = Put(1, "foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.Release_Store(nullptr);
  s = Put(1, "foo3", "bar3");
  // the next put should fail, too
  ASSERT_TRUE(!s.ok());
  // but we're still able to read
  ASSERT_EQ("bar", Get(1, "foo"));

  // do the same thing with paranoid checks off
  options.paranoid_checks = false;
  DestroyAndReopen(&options);
  CreateAndReopenWithCF({"pikachu"}, &options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.Release_Store(env_);
  s = Put(1, "foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.Release_Store(nullptr);
  s = Put(1, "foo3", "bar3");
  // the next put should NOT fail
  ASSERT_TRUE(s.ok());
}

TEST(DBTest, FilesDeletedAfterCompaction) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    ASSERT_OK(Put(1, "foo", "v2"));
    Compact(1, "a", "z");
    const int num_files = CountLiveFiles();
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(Put(1, "foo", "v2"));
      Compact(1, "a", "z");
    }
    ASSERT_EQ(CountLiveFiles(), num_files);
  } while (ChangeCompactOptions());
}

TEST(DBTest, BloomFilter) {
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

    CreateAndReopenWithCF({"pikachu"}, &options);

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
    env_->delay_sstable_sync_.Release_Store(env_);

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

    env_->delay_sstable_sync_.Release_Store(nullptr);
    Close();
  } while (ChangeCompactOptions());
}

TEST(DBTest, SnapshotFiles) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 100000000;        // Large write buffer
    CreateAndReopenWithCF({"pikachu"}, &options);

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
    std::string mkdir = "mkdir -p " + snapdir;
    ASSERT_EQ(system(mkdir.c_str()), 0);

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

TEST(DBTest, CompactOnFlush) {
  do {
    Options options = CurrentOptions();
    options.purge_redundant_kvs_while_flush = true;
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, &options);

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
  std::vector<uint64_t> log_files;
  env->GetChildren(path, &files);
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < files.size(); ++i) {
    if (ParseFileName(files[i], &number, &type)) {
      if (type == expected_file_type) {
        log_files.push_back(number);
      }
    }
  }
  return std::move(log_files);
}

std::vector<std::uint64_t> ListLogFiles(Env* env, const std::string& path) {
  return ListSpecificFiles(env, path, kLogFile);
}

std::vector<std::uint64_t> ListTableFiles(Env* env, const std::string& path) {
  return ListSpecificFiles(env, path, kTableFile);
}
}  // namespace

TEST(DBTest, FlushOneColumnFamily) {
  Options options;
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        &options);

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "pikachu", "pikachu"));
  ASSERT_OK(Put(2, "ilya", "ilya"));
  ASSERT_OK(Put(3, "muromec", "muromec"));
  ASSERT_OK(Put(4, "dobrynia", "dobrynia"));
  ASSERT_OK(Put(5, "nikitich", "nikitich"));
  ASSERT_OK(Put(6, "alyosha", "alyosha"));
  ASSERT_OK(Put(7, "popovich", "popovich"));

  for (size_t i = 0; i < 8; ++i) {
    Flush(i);
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), i + 1U);
  }
}

TEST(DBTest, WALArchivalTtl) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.WAL_ttl_seconds = 1000;
    DestroyAndReopen(&options);

    //  TEST : Create DB with a ttl and no size limit.
    //  Put some keys. Count the log files present in the DB just after insert.
    //  Re-open db. Causes deletion/archival to take place.
    //  Assert that the files moved under "/archive".
    //  Reopen db with small ttl.
    //  Assert that archive was removed.

    std::string archiveDir = ArchivalDirectory(dbname_);

    for (int i = 0; i < 10; ++i) {
      for (int j = 0; j < 10; ++j) {
        ASSERT_OK(Put(Key(10 * i + j), DummyString(1024)));
      }

      std::vector<uint64_t> log_files = ListLogFiles(env_, dbname_);

      options.create_if_missing = false;
      Reopen(&options);

      std::vector<uint64_t> logs = ListLogFiles(env_, archiveDir);
      std::set<uint64_t> archivedFiles(logs.begin(), logs.end());

      for (auto& log : log_files) {
        ASSERT_TRUE(archivedFiles.find(log) != archivedFiles.end());
      }
    }

    std::vector<uint64_t> log_files = ListLogFiles(env_, archiveDir);
    ASSERT_TRUE(log_files.size() > 0);

    options.WAL_ttl_seconds = 1;
    env_->SleepForMicroseconds(2 * 1000 * 1000);
    Reopen(&options);

    log_files = ListLogFiles(env_, archiveDir);
    ASSERT_TRUE(log_files.empty());
  } while (ChangeCompactOptions());
}

namespace {
uint64_t GetLogDirSize(std::string dir_path, SpecialEnv* env) {
  uint64_t dir_size = 0;
  std::vector<std::string> files;
  env->GetChildren(dir_path, &files);
  for (auto& f : files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kLogFile) {
      std::string const file_path = dir_path + "/" + f;
      uint64_t file_size;
      env->GetFileSize(file_path, &file_size);
      dir_size += file_size;
    }
  }
  return dir_size;
}
}  // namespace

TEST(DBTest, WALArchivalSizeLimit) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.WAL_ttl_seconds = 0;
    options.WAL_size_limit_MB = 1000;

    // TEST : Create DB with huge size limit and no ttl.
    // Put some keys. Count the archived log files present in the DB
    // just after insert. Assert that there are many enough.
    // Change size limit. Re-open db.
    // Assert that archive is not greater than WAL_size_limit_MB.
    // Set ttl and time_to_check_ to small values. Re-open db.
    // Assert that there are no archived logs left.

    DestroyAndReopen(&options);
    for (int i = 0; i < 128 * 128; ++i) {
      ASSERT_OK(Put(Key(i), DummyString(1024)));
    }
    Reopen(&options);

    std::string archive_dir = ArchivalDirectory(dbname_);
    std::vector<std::uint64_t> log_files = ListLogFiles(env_, archive_dir);
    ASSERT_TRUE(log_files.size() > 2);

    options.WAL_size_limit_MB = 8;
    Reopen(&options);
    dbfull()->TEST_PurgeObsoleteteWAL();

    uint64_t archive_size = GetLogDirSize(archive_dir, env_);
    ASSERT_TRUE(archive_size <= options.WAL_size_limit_MB * 1024 * 1024);

    options.WAL_ttl_seconds = 1;
    dbfull()->TEST_SetDefaultTimeToCheck(1);
    env_->SleepForMicroseconds(2 * 1000 * 1000);
    Reopen(&options);
    dbfull()->TEST_PurgeObsoleteteWAL();

    log_files = ListLogFiles(env_, archive_dir);
    ASSERT_TRUE(log_files.empty());
  } while (ChangeCompactOptions());
}

TEST(DBTest, PurgeInfoLogs) {
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
      Reopen(&options);
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

    Destroy(&options);
    // For mode (1), test DestoryDB() to delete all the logs under DB dir.
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
    ASSERT_TRUE(res.sequence > lastSequence);
    ++count;
    lastSequence = res.sequence;
    ASSERT_OK(iter->status());
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

TEST(DBTest, TransactionLogIterator) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(&options);
    CreateAndReopenWithCF({"pikachu"}, &options);
    Put(0, "key1", DummyString(1024));
    Put(1, "key2", DummyString(1024));
    Put(1, "key2", DummyString(1024));
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3U);
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(3, iter);
    }
    ReopenWithColumnFamilies({"default", "pikachu"}, &options);
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
TEST(DBTest, TransactionLogIteratorRace) {
  static const int LOG_ITERATOR_RACE_TEST_COUNT = 2;
  static const char* sync_points[LOG_ITERATOR_RACE_TEST_COUNT][4] =
    { { "DBImpl::GetSortedWalFiles:1", "DBImpl::PurgeObsoleteFiles:1",
        "DBImpl::PurgeObsoleteFiles:2", "DBImpl::GetSortedWalFiles:2" },
      { "DBImpl::GetSortedWalsOfType:1", "DBImpl::PurgeObsoleteFiles:1",
        "DBImpl::PurgeObsoleteFiles:2", "DBImpl::GetSortedWalsOfType:2" }};
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
      DestroyAndReopen(&options);
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

TEST(DBTest, TransactionLogIteratorMoveOverZeroFiles) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(&options);
    CreateAndReopenWithCF({"pikachu"}, &options);
    // Do a plain Reopen.
    Put(1, "key1", DummyString(1024));
    // Two reopens should create a zero record WAL file.
    ReopenWithColumnFamilies({"default", "pikachu"}, &options);
    ReopenWithColumnFamilies({"default", "pikachu"}, &options);

    Put(1, "key2", DummyString(1024));

    auto iter = OpenTransactionLogIter(0);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST(DBTest, TransactionLogIteratorStallAtLastRecord) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(&options);
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

TEST(DBTest, TransactionLogIteratorJustEmptyFile) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(&options);
    unique_ptr<TransactionLogIterator> iter;
    Status status = dbfull()->GetUpdatesSince(0, &iter);
    // Check that an empty iterator is returned
    ASSERT_TRUE(!iter->Valid());
  } while (ChangeCompactOptions());
}

TEST(DBTest, TransactionLogIteratorCheckAfterRestart) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(&options);
    Put("key1", DummyString(1024));
    Put("key2", DummyString(1023));
    dbfull()->Flush(FlushOptions());
    Reopen(&options);
    auto iter = OpenTransactionLogIter(0);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST(DBTest, TransactionLogIteratorCorruptedLog) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(&options);
    for (int i = 0; i < 1024; i++) {
      Put("key"+std::to_string(i), DummyString(10));
    }
    dbfull()->Flush(FlushOptions());
    // Corrupt this log to create a gap
    rocksdb::VectorLogPtr wal_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
    const auto logfilePath = dbname_ + "/" + wal_files.front()->PathName();
    ASSERT_EQ(
      0,
      truncate(logfilePath.c_str(), wal_files.front()->SizeFileBytes() / 2));
    // Insert a new entry to a new log file
    Put("key1025", DummyString(10));
    // Try to read from the beginning. Should stop before the gap and read less
    // than 1025 entries
    auto iter = OpenTransactionLogIter(0);
    int count;
    int last_sequence_read = ReadRecords(iter, count);
    ASSERT_LT(last_sequence_read, 1025);
    // Try to read past the gap, should be able to seek to key1025
    auto iter2 = OpenTransactionLogIter(last_sequence_read + 1);
    ExpectRecords(1, iter2);
  } while (ChangeCompactOptions());
}

TEST(DBTest, TransactionLogIteratorBatchOperations) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(&options);
    CreateAndReopenWithCF({"pikachu"}, &options);
    WriteBatch batch;
    batch.Put(handles_[1], "key1", DummyString(1024));
    batch.Put(handles_[0], "key2", DummyString(1024));
    batch.Put(handles_[1], "key3", DummyString(1024));
    batch.Delete(handles_[0], "key2");
    dbfull()->Write(WriteOptions(), &batch);
    Flush(1);
    Flush(0);
    ReopenWithColumnFamilies({"default", "pikachu"}, &options);
    Put(1, "key4", DummyString(1024));
    auto iter = OpenTransactionLogIter(3);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST(DBTest, TransactionLogIteratorBlobs) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(&options);
  CreateAndReopenWithCF({"pikachu"}, &options);
  {
    WriteBatch batch;
    batch.Put(handles_[1], "key1", DummyString(1024));
    batch.Put(handles_[0], "key2", DummyString(1024));
    batch.PutLogData(Slice("blob1"));
    batch.Put(handles_[1], "key3", DummyString(1024));
    batch.PutLogData(Slice("blob2"));
    batch.Delete(handles_[0], "key2");
    dbfull()->Write(WriteOptions(), &batch);
    ReopenWithColumnFamilies({"default", "pikachu"}, &options);
  }

  auto res = OpenTransactionLogIter(0)->GetBatch();
  struct Handler : public WriteBatch::Handler {
    std::string seen;
    virtual Status PutCF(uint32_t cf, const Slice& key, const Slice& value) {
      seen += "Put(" + std::to_string(cf) + ", " + key.ToString() + ", " +
              std::to_string(value.size()) + ")";
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) {
      seen += "Merge(" + std::to_string(cf) + ", " + key.ToString() + ", " +
              std::to_string(value.size()) + ")";
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) {
      seen += "LogData(" + blob.ToString() + ")";
    }
    virtual Status DeleteCF(uint32_t cf, const Slice& key) {
      seen += "Delete(" + std::to_string(cf) + ", " + key.ToString() + ")";
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

TEST(DBTest, ReadFirstRecordCache) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  DestroyAndReopen(&options);

  std::string path = dbname_ + "/000001.log";
  unique_ptr<WritableFile> file;
  ASSERT_OK(env_->NewWritableFile(path, &file, EnvOptions()));

  SequenceNumber s;
  ASSERT_OK(dbfull()->TEST_ReadFirstLine(path, &s));
  ASSERT_EQ(s, 0U);

  ASSERT_OK(dbfull()->TEST_ReadFirstRecord(kAliveLogFile, 1, &s));
  ASSERT_EQ(s, 0U);

  log::Writer writer(std::move(file));
  WriteBatch batch;
  batch.Put("foo", "bar");
  WriteBatchInternal::SetSequence(&batch, 10);
  writer.AddRecord(WriteBatchInternal::Contents(&batch));

  env_->count_sequential_reads_ = true;
  // sequential_read_counter_ sanity test
  ASSERT_EQ(env_->sequential_read_counter_.Read(), 0);

  ASSERT_OK(dbfull()->TEST_ReadFirstRecord(kAliveLogFile, 1, &s));
  ASSERT_EQ(s, 10U);
  // did a read
  ASSERT_EQ(env_->sequential_read_counter_.Read(), 1);

  ASSERT_OK(dbfull()->TEST_ReadFirstRecord(kAliveLogFile, 1, &s));
  ASSERT_EQ(s, 10U);
  // no new reads since the value is cached
  ASSERT_EQ(env_->sequential_read_counter_.Read(), 1);
}

// Multi-threaded test:
namespace {

static const int kColumnFamilies = 10;
static const int kNumThreads = 10;
static const int kTestSeconds = 10;
static const int kNumKeys = 1000;

struct MTState {
  DBTest* test;
  port::AtomicPointer stop;
  port::AtomicPointer counter[kNumThreads];
  port::AtomicPointer thread_done[kNumThreads];
};

struct MTThread {
  MTState* state;
  int id;
};

static void MTThreadBody(void* arg) {
  MTThread* t = reinterpret_cast<MTThread*>(arg);
  int id = t->id;
  DB* db = t->state->test->db_;
  uintptr_t counter = 0;
  fprintf(stderr, "... starting thread %d\n", id);
  Random rnd(1000 + id);
  char valbuf[1500];
  while (t->state->stop.Acquire_Load() == nullptr) {
    t->state->counter[id].Release_Store(reinterpret_cast<void*>(counter));

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
          ASSERT_LE((unsigned int)c, reinterpret_cast<uintptr_t>(
                                         t->state->counter[w].Acquire_Load()));
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
  t->state->thread_done[id].Release_Store(t);
  fprintf(stderr, "... stopping thread %d after %d ops\n", id, int(counter));
}

}  // namespace

TEST(DBTest, MultiThreaded) {
  do {
    std::vector<std::string> cfs;
    for (int i = 1; i < kColumnFamilies; ++i) {
      cfs.push_back(std::to_string(i));
    }
    CreateAndReopenWithCF(cfs);
    // Initialize state
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    for (int id = 0; id < kNumThreads; id++) {
      mt.counter[id].Release_Store(0);
      mt.thread_done[id].Release_Store(0);
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
    mt.stop.Release_Store(&mt);
    for (int id = 0; id < kNumThreads; id++) {
      while (mt.thread_done[id].Acquire_Load() == nullptr) {
        env_->SleepForMicroseconds(100000);
      }
    }
    // skip as HashCuckooRep does not support snapshot
  } while (ChangeOptions(kSkipHashCuckoo));
}

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
    std::string kv(std::to_string(i + id * kGCNumKeys));
    ASSERT_OK(db->Put(wo, kv, kv));
  }
  t->done = true;
}

}  // namespace

TEST(DBTest, GroupCommitTest) {
  do {
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    Reopen(&options);

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
    ASSERT_GT(TestGetTickerCount(options, WRITE_DONE_BY_OTHER), 0);

    std::vector<std::string> expected_db;
    for (int i = 0; i < kGCNumThreads * kGCNumKeys; ++i) {
      expected_db.push_back(std::to_string(i));
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
  };

  explicit ModelDB(const Options& options) : options_(options) {}
  using DB::Put;
  virtual Status Put(const WriteOptions& o, ColumnFamilyHandle* cf,
                     const Slice& k, const Slice& v) {
    WriteBatch batch;
    batch.Put(cf, k, v);
    return Write(o, &batch);
  }
  using DB::Merge;
  virtual Status Merge(const WriteOptions& o, ColumnFamilyHandle* cf,
                       const Slice& k, const Slice& v) {
    WriteBatch batch;
    batch.Merge(cf, k, v);
    return Write(o, &batch);
  }
  using DB::Delete;
  virtual Status Delete(const WriteOptions& o, ColumnFamilyHandle* cf,
                        const Slice& key) {
    WriteBatch batch;
    batch.Delete(cf, key);
    return Write(o, &batch);
  }
  using DB::Get;
  virtual Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                     const Slice& key, std::string* value) {
    return Status::NotSupported(key);
  }

  using DB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys, std::vector<std::string>* values) {
    std::vector<Status> s(keys.size(),
                          Status::NotSupported("Not implemented."));
    return s;
  }

  using DB::GetPropertiesOfAllTables;
  virtual Status GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                          TablePropertiesCollection* props) {
    return Status();
  }

  using DB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value, bool* value_found = nullptr) {
    if (value_found != nullptr) {
      *value_found = false;
    }
    return true; // Not Supported directly
  }
  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) {
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
      std::vector<Iterator*>* iterators) {
    return Status::NotSupported("Not supported yet");
  }
  virtual const Snapshot* GetSnapshot() {
    ModelSnapshot* snapshot = new ModelSnapshot;
    snapshot->map_ = map_;
    return snapshot;
  }

  virtual void ReleaseSnapshot(const Snapshot* snapshot) {
    delete reinterpret_cast<const ModelSnapshot*>(snapshot);
  }

  virtual Status Write(const WriteOptions& options, WriteBatch* batch) {
    class Handler : public WriteBatch::Handler {
     public:
      KVMap* map_;
      virtual void Put(const Slice& key, const Slice& value) {
        (*map_)[key.ToString()] = value.ToString();
      }
      virtual void Merge(const Slice& key, const Slice& value) {
        // ignore merge for now
        //(*map_)[key.ToString()] = value.ToString();
      }
      virtual void Delete(const Slice& key) {
        map_->erase(key.ToString());
      }
    };
    Handler handler;
    handler.map_ = &map_;
    return batch->Iterate(&handler);
  }

  using DB::GetProperty;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const Slice& property, std::string* value) {
    return false;
  }
  using DB::GetIntProperty;
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const Slice& property, uint64_t* value) override {
    return false;
  }
  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(ColumnFamilyHandle* column_family,
                                   const Range* range, int n, uint64_t* sizes) {
    for (int i = 0; i < n; i++) {
      sizes[i] = 0;
    }
  }
  using DB::CompactRange;
  virtual Status CompactRange(ColumnFamilyHandle* column_family,
                              const Slice* start, const Slice* end,
                              bool reduce_level, int target_level,
                              uint32_t output_path_id) {
    return Status::NotSupported("Not supported operation.");
  }

  using DB::NumberLevels;
  virtual int NumberLevels(ColumnFamilyHandle* column_family) { return 1; }

  using DB::MaxMemCompactionLevel;
  virtual int MaxMemCompactionLevel(ColumnFamilyHandle* column_family) {
    return 1;
  }

  using DB::Level0StopWriteTrigger;
  virtual int Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
    return -1;
  }

  virtual const std::string& GetName() const {
    return name_;
  }

  virtual Env* GetEnv() const {
    return nullptr;
  }

  using DB::GetOptions;
  virtual const Options& GetOptions(ColumnFamilyHandle* column_family) const {
    return options_;
  }

  using DB::Flush;
  virtual Status Flush(const rocksdb::FlushOptions& options,
                       ColumnFamilyHandle* column_family) {
    Status ret;
    return ret;
  }

  virtual Status DisableFileDeletions() {
    return Status::OK();
  }
  virtual Status EnableFileDeletions(bool force) {
    return Status::OK();
  }
  virtual Status GetLiveFiles(std::vector<std::string>&, uint64_t* size,
                              bool flush_memtable = true) {
    return Status::OK();
  }

  virtual Status GetSortedWalFiles(VectorLogPtr& files) {
    return Status::OK();
  }

  virtual Status DeleteFile(std::string name) {
    return Status::OK();
  }

  virtual Status GetDbIdentity(std::string& identity) {
    return Status::OK();
  }

  virtual SequenceNumber GetLatestSequenceNumber() const {
    return 0;
  }
  virtual Status GetUpdatesSince(
      rocksdb::SequenceNumber, unique_ptr<rocksdb::TransactionLogIterator>*,
      const TransactionLogIterator::ReadOptions&
          read_options = TransactionLogIterator::ReadOptions()) {
    return Status::NotSupported("Not supported in Model DB");
  }

  virtual ColumnFamilyHandle* DefaultColumnFamily() const { return nullptr; }

 private:
  class ModelIter: public Iterator {
   public:
    ModelIter(const KVMap* map, bool owned)
        : map_(map), owned_(owned), iter_(map_->end()) {
    }
    ~ModelIter() {
      if (owned_) delete map_;
    }
    virtual bool Valid() const { return iter_ != map_->end(); }
    virtual void SeekToFirst() { iter_ = map_->begin(); }
    virtual void SeekToLast() {
      if (map_->empty()) {
        iter_ = map_->end();
      } else {
        iter_ = map_->find(map_->rbegin()->first);
      }
    }
    virtual void Seek(const Slice& k) {
      iter_ = map_->lower_bound(k.ToString());
    }
    virtual void Next() { ++iter_; }
    virtual void Prev() {
      if (iter_ == map_->begin()) {
        iter_ = map_->end();
        return;
      }
      --iter_;
    }

    virtual Slice key() const { return iter_->first; }
    virtual Slice value() const { return iter_->second; }
    virtual Status status() const { return Status::OK(); }
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

TEST(DBTest, Randomized) {
  Random rnd(test::RandomSeed());
  do {
    ModelDB model(CurrentOptions());
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

        Reopen();
        ASSERT_TRUE(CompareIterators(step, &model, db_, nullptr, nullptr));

        model_snap = model.GetSnapshot();
        db_snap = db_->GetSnapshot();
      }

      if ((step % 2000) == 0) {
        fprintf(stdout,
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

TEST(DBTest, MultiGetSimple) {
  do {
    CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, MultiGetEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"});
    // Empty Key Set
    std::vector<Slice> keys;
    std::vector<std::string> values;
    std::vector<ColumnFamilyHandle*> cfs;
    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Empty Key Set
    DestroyAndReopen();
    CreateAndReopenWithCF({"pikachu"});
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
    std::string keystr;
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

TEST(DBTest, PrefixScan) {
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
  DestroyAndReopen(&options);
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
}

TEST(DBTest, TailingIteratorSingle) {
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

TEST(DBTest, TailingIteratorKeepAdding) {
  CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, TailingIteratorSeekToNext) {
  CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, TailingIteratorDeletes) {
  CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, TailingIteratorPrefixSeek) {
  ReadOptions read_options;
  read_options.tailing = true;

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_factory.reset(NewHashSkipListRepFactory(16));
  DestroyAndReopen(&options);
  CreateAndReopenWithCF({"pikachu"}, &options);

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
}

TEST(DBTest, TailingIteratorIncomplete) {
  CreateAndReopenWithCF({"pikachu"});
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

TEST(DBTest, TailingIteratorSeekToSame) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 1000;
  CreateAndReopenWithCF({"pikachu"}, &options);

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

TEST(DBTest, BlockBasedTablePrefixIndexTest) {
  // create a DB with block prefix index
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));


  Reopen(&options);
  ASSERT_OK(Put("k1", "v1"));
  Flush();
  ASSERT_OK(Put("k2", "v2"));

  // Reopen it without prefix extractor, make sure everything still works.
  // RocksDB should just fall back to the binary index.
  table_options.index_type = BlockBasedTableOptions::kBinarySearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset();

  Reopen(&options);
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("v2", Get("k2"));
}

TEST(DBTest, ChecksumTest) {
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(&options);
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Flush());  // table with crc checksum

  table_options.checksum = kxxHash;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(&options);
  ASSERT_OK(Put("e", "f"));
  ASSERT_OK(Put("g", "h"));
  ASSERT_OK(Flush());  // table with xxhash checksum

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(&options);
  ASSERT_EQ("b", Get("a"));
  ASSERT_EQ("d", Get("c"));
  ASSERT_EQ("f", Get("e"));
  ASSERT_EQ("h", Get("g"));

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(&options);
  ASSERT_EQ("b", Get("a"));
  ASSERT_EQ("d", Get("c"));
  ASSERT_EQ("f", Get("e"));
  ASSERT_EQ("h", Get("g"));
}

TEST(DBTest, FIFOCompactionTest) {
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
    DestroyAndReopen(&options);

    Random rnd(301);
    for (int i = 0; i < 6; ++i) {
      for (int j = 0; j < 100; ++j) {
        ASSERT_OK(Put(std::to_string(i * 100 + j), RandomString(&rnd, 1024)));
      }
      // flush should happen here
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
      ASSERT_EQ("NOT_FOUND", Get(std::to_string(i)));
    }
  }
}

TEST(DBTest, SimpleWriteTimeoutTest) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.write_buffer_size = 100000;
  options.max_background_flushes = 0;
  options.max_write_buffer_number = 2;
  options.min_write_buffer_number_to_merge = 3;
  options.max_total_wal_size = std::numeric_limits<uint64_t>::max();
  WriteOptions write_opt = WriteOptions();
  write_opt.timeout_hint_us = 0;
  DestroyAndReopen(&options);
  // fill the two write buffer
  ASSERT_OK(Put(Key(1), Key(1) + std::string(100000, 'v'), write_opt));
  ASSERT_OK(Put(Key(2), Key(2) + std::string(100000, 'v'), write_opt));
  // As the only two write buffers are full in this moment, the third
  // Put is expected to be timed-out.
  write_opt.timeout_hint_us = 50;
  ASSERT_TRUE(
      Put(Key(3), Key(3) + std::string(100000, 'v'), write_opt).IsTimedOut());
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
  WriteOptions write_opt = WriteOptions();
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
      std::string result;
    }
    if (s.IsTimedOut()) {
      timeout_count++;
      ASSERT_GT(put_duration + kTimerBias, write_opt.timeout_hint_us);
    }
  }

  state->done = true;
}

TEST(DBTest, MTRandomTimeoutTest) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.max_write_buffer_number = 2;
  options.compression = kNoCompression;
  options.level0_slowdown_writes_trigger = 10;
  options.level0_stop_writes_trigger = 20;
  options.write_buffer_size = kWriteBufferSize;
  DestroyAndReopen(&options);

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

}  // anonymous namespace

/*
 * This test is not reliable enough as it heavily depends on disk behavior.
 */
TEST(DBTest, RateLimitingTest) {
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
  DestroyAndReopen(&options);

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
  DestroyAndReopen(&options);

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
  DestroyAndReopen(&options);

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

TEST(DBTest, TableOptionsSanitizeTest) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  DestroyAndReopen(&options);
  ASSERT_EQ(db_->GetOptions().allow_mmap_reads, false);

  options.table_factory.reset(new PlainTableFactory());
  options.prefix_extractor.reset(NewNoopTransform());
  Destroy(&options);
  ASSERT_TRUE(TryReopen(&options).IsNotSupported());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
