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
#include "rocksdb/table_properties.h"
#include "table/block_based_table_factory.h"
#include "table/plain_table_factory.h"
#include "util/hash.h"
#include "util/hash_linklist_rep.h"
#include "utilities/merge_operators.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/statistics.h"
#include "util/testharness.h"
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

  anon::AtomicCounter sleep_counter_;

  explicit SpecialEnv(Env* base) : EnvWrapper(base) {
    delay_sstable_sync_.Release_Store(nullptr);
    no_space_.Release_Store(nullptr);
    non_writable_.Release_Store(nullptr);
    count_random_reads_ = false;
    manifest_sync_error_.Release_Store(nullptr);
    manifest_write_error_.Release_Store(nullptr);
    log_write_error_.Release_Store(nullptr);
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

  virtual void SleepForMicroseconds(int micros) {
    sleep_counter_.Increment();
    target()->SleepForMicroseconds(micros);
  }
};

class DBTest {
 private:
  const FilterPolicy* filter_policy_;

 protected:
  // Sequence of option configurations to try
  enum OptionConfig {
    kDefault,
    kPlainTableFirstBytePrefix,
    kPlainTableAllBytesPrefix,
    kVectorRep,
    kHashLinkList,
    kMergePut,
    kFilter,
    kUncompressed,
    kNumLevel_3,
    kDBLogDir,
    kWalDir,
    kManifestFileSize,
    kCompactOnFlush,
    kPerfOptions,
    kDeletesFilterFirst,
    kHashSkipList,
    kUniversalCompaction,
    kCompressedBlockCache,
    kInfiniteMaxOpenFiles,
    kEnd
  };
  int option_config_;

 public:
  std::string dbname_;
  SpecialEnv* env_;
  DB* db_;

  Options last_options_;

  // Skip some options, as they may not be applicable to a specific test.
  // To add more skip constants, use values 4, 8, 16, etc.
  enum OptionSkip {
    kNoSkip = 0,
    kSkipDeletesFilterFirst = 1,
    kSkipUniversalCompaction = 2,
    kSkipMergePut = 4,
    kSkipPlainTable = 8
  };

  DBTest() : option_config_(kDefault),
             env_(new SpecialEnv(Env::Default())) {
    filter_policy_ = NewBloomFilterPolicy(10);
    dbname_ = test::TmpDir() + "/db_test";
    ASSERT_OK(DestroyDB(dbname_, Options()));
    db_ = nullptr;
    Reopen();
  }

  ~DBTest() {
    delete db_;
    ASSERT_OK(DestroyDB(dbname_, Options()));
    delete env_;
    delete filter_policy_;
  }

  // Switch to a fresh database with the next option configuration to
  // test.  Return false if there are no more configurations to test.
  bool ChangeOptions(int skip_mask = kNoSkip) {
    // skip some options
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
      if ((skip_mask & kSkipPlainTable)
          && (option_config_ == kPlainTableAllBytesPrefix
              || option_config_ == kPlainTableFirstBytePrefix)) {
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
  Options CurrentOptions() {
    Options options;
    switch (option_config_) {
      case kHashSkipList:
        options.prefix_extractor.reset(NewFixedPrefixTransform(1));
        options.memtable_factory.reset(NewHashSkipListRepFactory());
        break;
      case kPlainTableFirstBytePrefix:
        options.table_factory.reset(new PlainTableFactory());
        options.prefix_extractor.reset(NewFixedPrefixTransform(1));
        options.allow_mmap_reads = true;
        options.max_sequential_skip_in_iterations = 999999;
        break;
      case kPlainTableAllBytesPrefix:
        options.table_factory.reset(new PlainTableFactory());
        options.prefix_extractor.reset(NewNoopTransform());
        options.allow_mmap_reads = true;
        options.max_sequential_skip_in_iterations = 999999;
        break;
      case kMergePut:
        options.merge_operator = MergeOperators::CreatePutOperator();
        break;
      case kFilter:
        options.filter_policy = filter_policy_;
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
        options.wal_dir = "/tmp/wal";
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
        options.memtable_factory.reset(NewHashLinkListRepFactory(4));
        break;
      case kUniversalCompaction:
        options.compaction_style = kCompactionStyleUniversal;
        break;
      case kCompressedBlockCache:
        options.block_cache_compressed = NewLRUCache(8*1024*1024);
        break;
      case kInfiniteMaxOpenFiles:
        options.max_open_files = -1;
        break;
      default:
        break;
    }
    return options;
  }

  DBImpl* dbfull() {
    return reinterpret_cast<DBImpl*>(db_);
  }

  void Reopen(Options* options = nullptr) {
    ASSERT_OK(TryReopen(options));
  }

  void Close() {
    delete db_;
    db_ = nullptr;
  }

  void DestroyAndReopen(Options* options = nullptr) {
    //Destroy using last options
    Destroy(&last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(Options* options) {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, *options));
  }

  Status PureReopen(Options* options, DB** db) {
    return DB::Open(*options, dbname_, db);
  }

  Status ReadOnlyReopen(Options* options) {
    return DB::OpenForReadOnly(*options, dbname_, &db_);
  }

  Status TryReopen(Options* options = nullptr) {
    delete db_;
    db_ = nullptr;
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

  Status Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions()) {
    if (kMergePut == option_config_ ) {
      return db_->Merge(wo, k, v);
    } else {
      return db_->Put(wo, k, v);
    }
  }

  Status Delete(const std::string& k) {
    return db_->Delete(WriteOptions(), k);
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

  // Return a string that contains all key,value pairs in order,
  // formatted like "(k1->v1)(k2->v2)".
  std::string Contents() {
    std::vector<std::string> forward;
    std::string result;
    Iterator* iter = db_->NewIterator(ReadOptions());
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

  std::string AllEntriesFor(const Slice& user_key) {
    Iterator* iter = dbfull()->TEST_NewInternalIterator();
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

  int NumTableFilesAtLevel(int level) {
    std::string property;
    ASSERT_TRUE(
        db_->GetProperty("rocksdb.num-files-at-level" + NumberToString(level),
                         &property));
    return atoi(property.c_str());
  }

  int TotalTableFiles() {
    int result = 0;
    for (int level = 0; level < db_->NumberLevels(); level++) {
      result += NumTableFilesAtLevel(level);
    }
    return result;
  }

  // Return spread of files per level
  std::string FilesPerLevel() {
    std::string result;
    int last_non_zero_offset = 0;
    for (int level = 0; level < db_->NumberLevels(); level++) {
      int f = NumTableFilesAtLevel(level);
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
    std::vector<std::string> files;
    uint64_t manifest_file_size;
    db_->GetLiveFiles(files, &manifest_file_size);
    return files.size();
  }

  uint64_t Size(const Slice& start, const Slice& limit) {
    Range r(start, limit);
    uint64_t size;
    db_->GetApproximateSizes(&r, 1, &size);
    return size;
  }

  void Compact(const Slice& start, const Slice& limit) {
    db_->CompactRange(&start, &limit);
  }

  // Do n memtable compactions, each of which produces an sstable
  // covering the range [small,large].
  void MakeTables(int n, const std::string& small, const std::string& large) {
    for (int i = 0; i < n; i++) {
      Put(small, "begin");
      Put(large, "end");
      dbfull()->TEST_FlushMemTable();
    }
  }

  // Prevent pushing of new sstables into deeper levels by adding
  // tables that cover a specified range to all levels.
  void FillLevels(const std::string& smallest, const std::string& largest) {
    MakeTables(db_->NumberLevels(), smallest, largest);
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

  void VerifyIterLast(std::string expected_key) {
    Iterator* iter = db_->NewIterator(ReadOptions());
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
  void validateNumberOfEntries(int numValues) {
      Iterator* iter = dbfull()->TEST_NewInternalIterator();
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

static std::string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key%06d", i);
  return std::string(buf);
}

static long TestGetTickerCount(const Options& options, Tickers ticker_type) {
  return options.statistics->getTickerCount(ticker_type);
}

// A helper function that ensures the table properties returned in
// `GetPropertiesOfAllTablesTest` is correct.
// This test assumes entries size is differnt for each of the tables.
void VerifyTableProperties(DB* db, uint64_t expected_entries_size) {
  TablePropertiesCollection props;
  ASSERT_OK(db->GetPropertiesOfAllTables(&props));

  assert(props.size() == 4);
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

std::unordered_map<std::string, size_t> GetMemoryUsage(MemTable* memtable) {
  const auto& arena = memtable->TEST_GetArena();
  return {{"memtable.approximate.usage", memtable->ApproximateMemoryUsage()},
          {"arena.approximate.usage", arena.ApproximateMemoryUsage()},
          {"arena.allocated.memory", arena.MemoryAllocatedBytes()},
          {"arena.unused.bytes", arena.AllocatedAndUnused()},
          {"irregular.blocks", arena.IrregularBlockNum()}};
}

void PrintMemoryUsage(const std::unordered_map<std::string, size_t>& usage) {
  for (const auto& item : usage) {
    std::cout << "\t" << item.first << ": " << item.second << std::endl;
  }
}

void AddRandomKV(MemTable* memtable, Random* rnd, size_t arena_block_size) {
  memtable->Add(0, kTypeValue, RandomString(rnd, 20) /* key */,
                // make sure we will be able to generate some over sized entries
                RandomString(rnd, rnd->Uniform(arena_block_size / 4) * 1.15 +
                                      10) /* value */);
}

TEST(DBTest, Empty) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    Reopen(&options);

    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));

    env_->delay_sstable_sync_.Release_Store(env_);   // Block sync calls
    Put("k1", std::string(100000, 'x'));             // Fill memtable
    Put("k2", std::string(100000, 'y'));             // Trigger compaction
    ASSERT_EQ("v1", Get("foo"));
    env_->delay_sstable_sync_.Release_Store(nullptr);   // Release sync calls
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
}

// Make sure that when options.block_cache is set, after a new table is
// created its index/filter blocks are added to block cache.
TEST(DBTest, IndexAndFilterBlocksOfNewTableAddedToCache) {
  Options options = CurrentOptions();
  std::unique_ptr<const FilterPolicy> filter_policy(NewBloomFilterPolicy(20));
  options.filter_policy = filter_policy.get();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  DestroyAndReopen(&options);

  ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
  // Create a new talbe.
  ASSERT_OK(dbfull()->Flush(FlushOptions()));

  // index/filter blocks added to block cache right after table creation.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(2, /* only index/filter were added */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));

  // Make sure filter block is in cache.
  std::string value;
  ReadOptions ropt;
  db_->KeyMayExist(ReadOptions(), "key", &value);

  // Miss count should remain the same.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  db_->KeyMayExist(ReadOptions(), "key", &value);
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Make sure index block is in cache.
  auto index_block_hit = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  value = Get("key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(index_block_hit + 1,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  value = Get("key");
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
  Reopen(&options);

  const std::string value(1024 * 1024, ' ');
  int i = 0;
  while (NumTableFilesAtLevel(2) == 0) {
    ASSERT_OK(Put(Key(i++), value));
  }

  options.num_levels = 1;
  options.max_bytes_for_level_multiplier_additional.resize(1, 1);
  Status s = TryReopen(&options);
  ASSERT_EQ(s.IsInvalidArgument(), true);
  ASSERT_EQ(s.ToString(),
            "Invalid argument: db has more levels than options.num_levels");

  options.num_levels = 10;
  options.max_bytes_for_level_multiplier_additional.resize(10, 1);
  ASSERT_OK(TryReopen(&options));
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
    ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_OK(db_->Put(WriteOptions(), "foo", "v2"));
    ASSERT_EQ("v2", Get("foo"));
    ASSERT_OK(db_->Delete(WriteOptions(), "foo"));
    ASSERT_EQ("NOT_FOUND", Get("foo"));
  } while (ChangeOptions());
}


TEST(DBTest, GetFromImmutableLayer) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    Reopen(&options);

    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));

    env_->delay_sstable_sync_.Release_Store(env_);   // Block sync calls
    Put("k1", std::string(100000, 'x'));             // Fill memtable
    Put("k2", std::string(100000, 'y'));             // Trigger compaction
    ASSERT_EQ("v1", Get("foo"));
    env_->delay_sstable_sync_.Release_Store(nullptr);   // Release sync calls
  } while (ChangeOptions());
}

TEST(DBTest, GetFromVersions) {
  do {
    ASSERT_OK(Put("foo", "v1"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_EQ("v1", Get("foo"));
  } while (ChangeOptions());
}

TEST(DBTest, GetSnapshot) {
  do {
    // Try with both a short key and a long key
    for (int i = 0; i < 2; i++) {
      std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
      ASSERT_OK(Put(key, "v1"));
      const Snapshot* s1 = db_->GetSnapshot();
      ASSERT_OK(Put(key, "v2"));
      ASSERT_EQ("v2", Get(key));
      ASSERT_EQ("v1", Get(key, s1));
      dbfull()->TEST_FlushMemTable();
      ASSERT_EQ("v2", Get(key));
      ASSERT_EQ("v1", Get(key, s1));
      db_->ReleaseSnapshot(s1);
    }
  } while (ChangeOptions());
}

TEST(DBTest, GetLevel0Ordering) {
  do {
    // Check that we process level-0 files in correct order.  The code
    // below generates two level-0 files where the earlier one comes
    // before the later one in the level-0 file list since the earlier
    // one has a smaller "smallest" key.
    ASSERT_OK(Put("bar", "b"));
    ASSERT_OK(Put("foo", "v1"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_OK(Put("foo", "v2"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_EQ("v2", Get("foo"));
  } while (ChangeOptions());
}

TEST(DBTest, GetOrderedByLevels) {
  do {
    ASSERT_OK(Put("foo", "v1"));
    Compact("a", "z");
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_OK(Put("foo", "v2"));
    ASSERT_EQ("v2", Get("foo"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_EQ("v2", Get("foo"));
  } while (ChangeOptions());
}

TEST(DBTest, GetPicksCorrectFile) {
  do {
    // Arrange to have multiple files in a non-level-0 level.
    ASSERT_OK(Put("a", "va"));
    Compact("a", "b");
    ASSERT_OK(Put("x", "vx"));
    Compact("x", "y");
    ASSERT_OK(Put("f", "vf"));
    Compact("f", "g");
    ASSERT_EQ("va", Get("a"));
    ASSERT_EQ("vf", Get("f"));
    ASSERT_EQ("vx", Get("x"));
  } while (ChangeOptions());
}

TEST(DBTest, GetEncountersEmptyLevel) {
  do {
    // Arrange for the following to happen:
    //   * sstable A in level 0
    //   * nothing in level 1
    //   * sstable B in level 2
    // Then do enough Get() calls to arrange for an automatic compaction
    // of sstable A.  A bug would cause the compaction to be marked as
    // occuring at level 1 (instead of the correct level 0).

    // Step 1: First place sstables in levels 0 and 2
    int compaction_count = 0;
    while (NumTableFilesAtLevel(0) == 0 ||
           NumTableFilesAtLevel(2) == 0) {
      ASSERT_LE(compaction_count, 100) << "could not fill levels 0 and 2";
      compaction_count++;
      Put("a", "begin");
      Put("z", "end");
      dbfull()->TEST_FlushMemTable();
    }

    // Step 2: clear level 1 if necessary.
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);
    ASSERT_EQ(NumTableFilesAtLevel(0), 1);
    ASSERT_EQ(NumTableFilesAtLevel(1), 0);
    ASSERT_EQ(NumTableFilesAtLevel(2), 1);

    // Step 3: read a bunch of times
    for (int i = 0; i < 1000; i++) {
      ASSERT_EQ("NOT_FOUND", Get("missing"));
    }

    // Step 4: Wait for compaction to finish
    env_->SleepForMicroseconds(1000000);

    ASSERT_EQ(NumTableFilesAtLevel(0), 1); // XXX
  } while (ChangeOptions(kSkipUniversalCompaction));
}

// KeyMayExist can lead to a few false positives, but not false negatives.
// To make test deterministic, use a much larger number of bits per key-20 than
// bits in the key, so that false positives are eliminated
TEST(DBTest, KeyMayExist) {
  do {
    ReadOptions ropts;
    std::string value;
    Options options = CurrentOptions();
    options.filter_policy = NewBloomFilterPolicy(20);
    options.statistics = rocksdb::CreateDBStatistics();
    Reopen(&options);

    ASSERT_TRUE(!db_->KeyMayExist(ropts, "a", &value));

    ASSERT_OK(db_->Put(WriteOptions(), "a", "b"));
    bool value_found = false;
    ASSERT_TRUE(db_->KeyMayExist(ropts, "a", &value, &value_found));
    ASSERT_TRUE(value_found);
    ASSERT_EQ("b", value);

    dbfull()->Flush(FlushOptions());
    value.clear();

    long numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    long cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(db_->KeyMayExist(ropts, "a", &value, &value_found));
    ASSERT_TRUE(!value_found);
    // assert that no new files were opened and no new blocks were
    // read into block cache.
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(db_->Delete(WriteOptions(), "a"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    dbfull()->Flush(FlushOptions());
    dbfull()->CompactRange(nullptr, nullptr);

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(db_->Delete(WriteOptions(), "c"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, "c", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    delete options.filter_policy;

    // KeyMayExist function only checks data in block caches, which is not used
    // by plain table format.
  } while (ChangeOptions(kSkipPlainTable));
}

TEST(DBTest, NonBlockingIteration) {
  do {
    ReadOptions non_blocking_opts, regular_opts;
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    non_blocking_opts.read_tier = kBlockCacheTier;
    Reopen(&options);
    // write one kv to the database.
    ASSERT_OK(db_->Put(WriteOptions(), "a", "b"));

    // scan using non-blocking iterator. We should find it because
    // it is in memtable.
    Iterator* iter = db_->NewIterator(non_blocking_opts);
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_EQ(count, 1);
    delete iter;

    // flush memtable to storage. Now, the key should not be in the
    // memtable neither in the block cache.
    dbfull()->Flush(FlushOptions());

    // verify that a non-blocking iterator does not find any
    // kvs. Neither does it do any IOs to storage.
    long numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    long cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts);
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
    ASSERT_EQ(Get("a"), "b");

    // verify that we can find it via a non-blocking scan
    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts);
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
  } while (ChangeOptions(kSkipPlainTable));
}

// A delete is skipped for key if KeyMayExist(key) returns False
// Tests Writebatch consistency and proper delete behaviour
TEST(DBTest, FilterDeletes) {
  do {
    Options options = CurrentOptions();
    options.filter_policy = NewBloomFilterPolicy(20);
    options.filter_deletes = true;
    Reopen(&options);
    WriteBatch batch;

    batch.Delete("a");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(AllEntriesFor("a"), "[ ]"); // Delete skipped
    batch.Clear();

    batch.Put("a", "b");
    batch.Delete("a");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(Get("a"), "NOT_FOUND");
    ASSERT_EQ(AllEntriesFor("a"), "[ DEL, b ]"); // Delete issued
    batch.Clear();

    batch.Delete("c");
    batch.Put("c", "d");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(Get("c"), "d");
    ASSERT_EQ(AllEntriesFor("c"), "[ d ]"); // Delete skipped
    batch.Clear();

    dbfull()->Flush(FlushOptions()); // A stray Flush

    batch.Delete("c");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(AllEntriesFor("c"), "[ DEL, d ]"); // Delete issued
    batch.Clear();

    delete options.filter_policy;
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

TEST(DBTest, IterEmpty) {
  do {
    Iterator* iter = db_->NewIterator(ReadOptions());

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
    ASSERT_OK(Put("a", "va"));
    Iterator* iter = db_->NewIterator(ReadOptions());

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
    ASSERT_OK(Put("a", "va"));
    ASSERT_OK(Put("b", "vb"));
    ASSERT_OK(Put("c", "vc"));
    Iterator* iter = db_->NewIterator(ReadOptions());

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
    ASSERT_OK(Put("a",  "va2"));
    ASSERT_OK(Put("a2", "va3"));
    ASSERT_OK(Put("b",  "vb2"));
    ASSERT_OK(Put("c",  "vc2"));
    ASSERT_OK(Delete("b"));
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

  // insert two keys with same userkey and verify that
  // reseek is not invoked. For each of these test cases,
  // verify that we can find the next key "b".
  ASSERT_OK(Put("a",  "one"));
  ASSERT_OK(Put("a",  "two"));
  ASSERT_OK(Put("b",  "bone"));
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "a->two");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of three keys with same userkey and verify
  // that reseek is still not invoked.
  ASSERT_OK(Put("a",  "three"));
  iter = db_->NewIterator(ReadOptions());
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->three");
  iter->Next();
  ASSERT_EQ(TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of four keys with same userkey and verify
  // that reseek is invoked.
  ASSERT_OK(Put("a",  "four"));
  iter = db_->NewIterator(ReadOptions());
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
  ASSERT_OK(Put("b",  "btwo"));
  iter = db_->NewIterator(ReadOptions());
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
  ASSERT_OK(Put("b",  "bthree"));
  ASSERT_OK(Put("b",  "bfour"));
  iter = db_->NewIterator(ReadOptions());
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
    ASSERT_OK(Put("a", "va"));
    ASSERT_OK(Put("b", std::string(100000, 'b')));
    ASSERT_OK(Put("c", "vc"));
    ASSERT_OK(Put("d", std::string(100000, 'd')));
    ASSERT_OK(Put("e", std::string(100000, 'e')));

    Iterator* iter = db_->NewIterator(ReadOptions());

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
    ASSERT_OK(Put("a", "va"));
    ASSERT_OK(Put("b", "vb"));
    ASSERT_OK(Put("c", "vc"));
    ASSERT_OK(Delete("b"));
    ASSERT_EQ("NOT_FOUND", Get("b"));

    Iterator* iter = db_->NewIterator(ReadOptions());
    iter->Seek("c");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      iter->Prev();
      ASSERT_EQ(IterStatus(iter), "a->va");
    }
    delete iter;
  } while (ChangeOptions());
}

TEST(DBTest, IterPrevMaxSkip) {
  do {
    for (int i = 0; i < 2; i++) {
      db_->Put(WriteOptions(), "key1", "v1");
      db_->Put(WriteOptions(), "key2", "v2");
      db_->Put(WriteOptions(), "key3", "v3");
      db_->Put(WriteOptions(), "key4", "v4");
      db_->Put(WriteOptions(), "key5", "v5");
    }

    VerifyIterLast("key5->v5");

    ASSERT_OK(db_->Delete(WriteOptions(), "key5"));
    VerifyIterLast("key4->v4");

    ASSERT_OK(db_->Delete(WriteOptions(), "key4"));
    VerifyIterLast("key3->v3");

    ASSERT_OK(db_->Delete(WriteOptions(), "key3"));
    VerifyIterLast("key2->v2");

    ASSERT_OK(db_->Delete(WriteOptions(), "key2"));
    VerifyIterLast("key1->v1");

    ASSERT_OK(db_->Delete(WriteOptions(), "key1"));
    VerifyIterLast("(invalid)");
  } while (ChangeOptions(kSkipMergePut));
}

TEST(DBTest, IterWithSnapshot) {
  do {
    ASSERT_OK(Put("key1", "val1"));
    ASSERT_OK(Put("key2", "val2"));
    ASSERT_OK(Put("key3", "val3"));
    ASSERT_OK(Put("key4", "val4"));
    ASSERT_OK(Put("key5", "val5"));

    const Snapshot *snapshot = db_->GetSnapshot();
    ReadOptions options;
    options.snapshot = snapshot;
    Iterator* iter = db_->NewIterator(options);

    // Put more values after the snapshot
    ASSERT_OK(Put("key100", "val100"));
    ASSERT_OK(Put("key101", "val101"));

    iter->Seek("key5");
    ASSERT_EQ(IterStatus(iter), "key5->val5");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      iter->Prev();
      ASSERT_EQ(IterStatus(iter), "key4->val4");
      iter->Prev();
      ASSERT_EQ(IterStatus(iter), "key3->val3");

      iter->Next();
      ASSERT_EQ(IterStatus(iter), "key4->val4");
      iter->Next();
      ASSERT_EQ(IterStatus(iter), "key5->val5");
      iter->Next();
      ASSERT_TRUE(!iter->Valid());
    }
    db_->ReleaseSnapshot(snapshot);
    delete iter;
  } while (ChangeOptions());
}

TEST(DBTest, Recover) {
  do {
    ASSERT_OK(Put("foo", "v1"));
    ASSERT_OK(Put("baz", "v5"));

    Reopen();
    ASSERT_EQ("v1", Get("foo"));

    ASSERT_EQ("v1", Get("foo"));
    ASSERT_EQ("v5", Get("baz"));
    ASSERT_OK(Put("bar", "v2"));
    ASSERT_OK(Put("foo", "v3"));

    Reopen();
    ASSERT_EQ("v3", Get("foo"));
    ASSERT_OK(Put("foo", "v4"));
    ASSERT_EQ("v4", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));
    ASSERT_EQ("v5", Get("baz"));
  } while (ChangeOptions());
}

TEST(DBTest, RecoverWithTableHandle) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.write_buffer_size = 100;
    options.disable_auto_compactions = true;
    DestroyAndReopen(&options);

    ASSERT_OK(Put("foo", "v1"));
    ASSERT_OK(Put("bar", "v2"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_OK(Put("foo", "v3"));
    ASSERT_OK(Put("bar", "v4"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_OK(Put("big", std::string(100, 'a')));
    Reopen();

    std::vector<std::vector<FileMetaData>> files;
    dbfull()->TEST_GetFilesMetaData(&files);
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

    // copy the logs from backup back to wal dir
    env_->CreateDirIfMissing(options.wal_dir);
    for (auto& log : logs) {
      if (log != ".." && log != ".") {
        CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
        // we won't be needing this file no more
        env_->DeleteFile(backup_logs + "/" + log);
      }
    }
    // assert that we successfully recovered only from logs, even though we
    // destroyed the DB
    Reopen(&options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));
    Close();
  } while (ChangeOptions());
}

TEST(DBTest, RollLog) {
  do {
    ASSERT_OK(Put("foo", "v1"));
    ASSERT_OK(Put("baz", "v5"));

    Reopen();
    for (int i = 0; i < 10; i++) {
      Reopen();
    }
    ASSERT_OK(Put("foo", "v4"));
    for (int i = 0; i < 10; i++) {
      Reopen();
    }
  } while (ChangeOptions());
}

TEST(DBTest, WAL) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v1"));
    ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v1"));

    Reopen();
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_EQ("v1", Get("bar"));

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v2"));
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v2"));

    Reopen();
    // Both value's should be present.
    ASSERT_EQ("v2", Get("bar"));
    ASSERT_EQ("v2", Get("foo"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v3"));
    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v3"));

    Reopen();
    // again both values should be present.
    ASSERT_EQ("v3", Get("foo"));
    ASSERT_EQ("v3", Get("bar"));
  } while (ChangeCompactOptions());
}

TEST(DBTest, CheckLock) {
  do {
    DB* localdb;
    Options options = CurrentOptions();
    ASSERT_OK(TryReopen(&options));

    // second open should fail
    ASSERT_TRUE(!(PureReopen(&options, &localdb)).ok());
  } while (ChangeCompactOptions());
}

TEST(DBTest, FlushMultipleMemtable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    Reopen(&options);
    ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v1"));
    dbfull()->Flush(FlushOptions());
    ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v1"));

    ASSERT_EQ("v1", Get("foo"));
    ASSERT_EQ("v1", Get("bar"));
    dbfull()->Flush(FlushOptions());
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
    Reopen(&options);

    std::string big_value(1000000 * 2, 'x');
    std::string num;
    SetPerfLevel(kEnableTime);;

    ASSERT_OK(dbfull()->Put(writeOpt, "k1", big_value));
    ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "0");
    perf_context.Reset();
    Get("k1");
    ASSERT_EQ(1, (int) perf_context.get_from_memtable_count);

    ASSERT_OK(dbfull()->Put(writeOpt, "k2", big_value));
    ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "1");
    perf_context.Reset();
    Get("k1");
    ASSERT_EQ(2, (int) perf_context.get_from_memtable_count);
    perf_context.Reset();
    Get("k2");
    ASSERT_EQ(1, (int) perf_context.get_from_memtable_count);

    ASSERT_OK(dbfull()->Put(writeOpt, "k3", big_value));
    ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "2");
    perf_context.Reset();
    Get("k2");
    ASSERT_EQ(2, (int) perf_context.get_from_memtable_count);
    perf_context.Reset();
    Get("k3");
    ASSERT_EQ(1, (int) perf_context.get_from_memtable_count);
    perf_context.Reset();
    Get("k1");
    ASSERT_EQ(3, (int) perf_context.get_from_memtable_count);

    dbfull()->Flush(FlushOptions());
    ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "0");
    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST(DBTest, FLUSH) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    SetPerfLevel(kEnableTime);;
    ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v1"));
    // this will now also flush the last 2 writes
    dbfull()->Flush(FlushOptions());
    ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v1"));

    perf_context.Reset();
    Get("foo");
    ASSERT_TRUE((int) perf_context.get_from_output_files_time > 0);

    Reopen();
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_EQ("v1", Get("bar"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v2"));
    ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v2"));
    dbfull()->Flush(FlushOptions());

    Reopen();
    ASSERT_EQ("v2", Get("bar"));
    perf_context.Reset();
    ASSERT_EQ("v2", Get("foo"));
    ASSERT_TRUE((int) perf_context.get_from_output_files_time > 0);

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v3"));
    ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v3"));
    dbfull()->Flush(FlushOptions());

    Reopen();
    // 'foo' should be there because its put
    // has WAL enabled.
    ASSERT_EQ("v3", Get("foo"));
    ASSERT_EQ("v3", Get("bar"));

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST(DBTest, RecoveryWithEmptyLog) {
  do {
    ASSERT_OK(Put("foo", "v1"));
    ASSERT_OK(Put("foo", "v2"));
    Reopen();
    Reopen();
    ASSERT_OK(Put("foo", "v3"));
    Reopen();
    ASSERT_EQ("v3", Get("foo"));
  } while (ChangeOptions());
}

// Check that writes done during a memtable compaction are recovered
// if the database is shutdown during the memtable compaction.
TEST(DBTest, RecoverDuringMemtableCompaction) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.write_buffer_size = 1000000;
    Reopen(&options);

    // Trigger a long memtable compaction and reopen the database during it
    ASSERT_OK(Put("foo", "v1"));                         // Goes to 1st log file
    ASSERT_OK(Put("big1", std::string(10000000, 'x')));  // Fills memtable
    ASSERT_OK(Put("big2", std::string(1000, 'y')));      // Triggers compaction
    ASSERT_OK(Put("bar", "v2"));                         // Goes to new log file

    Reopen(&options);
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));
    ASSERT_EQ(std::string(10000000, 'x'), Get("big1"));
    ASSERT_EQ(std::string(1000, 'y'), Get("big2"));
  } while (ChangeOptions());
}

TEST(DBTest, MinorCompactionsHappen) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 10000;
    Reopen(&options);

    const int N = 500;

    int starting_num_tables = TotalTableFiles();
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(Key(i), Key(i) + std::string(1000, 'v')));
    }
    int ending_num_tables = TotalTableFiles();
    ASSERT_GT(ending_num_tables, starting_num_tables);

    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(Key(i)));
    }

    Reopen();

    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(Key(i)));
    }
  } while (ChangeCompactOptions());
}

TEST(DBTest, ManifestRollOver) {
  do {
    Options options = CurrentOptions();
    options.max_manifest_file_size = 10 ;  // 10 bytes
    Reopen(&options);
    {
      ASSERT_OK(Put("manifest_key1", std::string(1000, '1')));
      ASSERT_OK(Put("manifest_key2", std::string(1000, '2')));
      ASSERT_OK(Put("manifest_key3", std::string(1000, '3')));
      uint64_t manifest_before_flush =
        dbfull()->TEST_Current_Manifest_FileNo();
      dbfull()->Flush(FlushOptions()); // This should trigger LogAndApply.
      uint64_t manifest_after_flush =
        dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_GT(manifest_after_flush, manifest_before_flush);
      Reopen(&options);
      ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(),
                manifest_after_flush);
      // check if a new manifest file got inserted or not.
      ASSERT_EQ(std::string(1000, '1'), Get("manifest_key1"));
      ASSERT_EQ(std::string(1000, '2'), Get("manifest_key2"));
      ASSERT_EQ(std::string(1000, '3'), Get("manifest_key3"));
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
      Reopen(&options);
      ASSERT_OK(Put("big1", std::string(200000, '1')));
      ASSERT_OK(Put("big2", std::string(200000, '2')));
      ASSERT_OK(Put("small3", std::string(10, '3')));
      ASSERT_OK(Put("small4", std::string(10, '4')));
      ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    }

    // Make sure that if we re-open with a small write buffer size that
    // we flush table files in the middle of a large log file.
    Options options = CurrentOptions();
    options.write_buffer_size = 100000;
    Reopen(&options);
    ASSERT_EQ(NumTableFilesAtLevel(0), 3);
    ASSERT_EQ(std::string(200000, '1'), Get("big1"));
    ASSERT_EQ(std::string(200000, '2'), Get("big2"));
    ASSERT_EQ(std::string(10, '3'), Get("small3"));
    ASSERT_EQ(std::string(10, '4'), Get("small4"));
    ASSERT_GT(NumTableFilesAtLevel(0), 1);
  } while (ChangeCompactOptions());
}

TEST(DBTest, CompactionsGenerateMultipleFiles) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;        // Large write buffer
  Reopen(&options);

  Random rnd(301);

  // Write 8MB (80 values, each 100K)
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  std::vector<std::string> values;
  for (int i = 0; i < 80; i++) {
    values.push_back(RandomString(&rnd, 100000));
    ASSERT_OK(Put(Key(i), values[i]));
  }

  // Reopening moves updates to level-0
  Reopen(&options);
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);

  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_GT(NumTableFilesAtLevel(1), 1);
  for (int i = 0; i < 80; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
}

TEST(DBTest, CompactionTrigger) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100<<10; //100KB
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.level0_file_num_compaction_trigger = 3;
  Reopen(&options);

  Random rnd(301);

  for (int num = 0;
       num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    std::vector<std::string> values;
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      values.push_back(RandomString(&rnd, 10000));
      ASSERT_OK(Put(Key(i), values[i]));
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumTableFilesAtLevel(0), num + 1);
  }

  //generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < 12; i++) {
    values.push_back(RandomString(&rnd, 10000));
    ASSERT_OK(Put(Key(i), values[i]));
  }
  dbfull()->TEST_WaitForCompact();

  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
}

// TODO(kailiu) The tests on UniversalCompaction has some issues:
//  1. A lot of magic numbers ("11" or "12").
//  2. Made assumption on the memtable flush conidtions, which may change from
//     time to time.
TEST(DBTest, UniversalCompactionTrigger) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
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
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i), 0);
  }

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
  // After comapction, we should have 2 files, with size 4, 2.4.
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i), 0);
  }

  // Stage 3:
  //   Now we have 2 files at level 0, with size 4 and 2.4. Continue
  //   generating new files at level 0.
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
  for (int i = 0; i < 12; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 2.4, 1, 1.
  // After comapction, we should have 3 files, with size 4, 2.4, 2.
  ASSERT_EQ(NumTableFilesAtLevel(0), 3);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i), 0);
  }

  // Stage 4:
  //   Now we have 3 files at level 0, with size 4, 2.4, 2. Let's generate a
  //   new file of size 1.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Level-0 compaction is triggered, but no file will be picked up.
  ASSERT_EQ(NumTableFilesAtLevel(0), 4);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i), 0);
  }

  // Stage 5:
  //   Now we have 4 files at level 0, with size 4, 2.4, 2, 1. Let's generate
  //   a new file of size 1.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // All files at level 0 will be compacted into a single one.
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i), 0);
  }
}

TEST(DBTest, UniversalCompactionSizeAmplification) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 3;

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.
    max_size_amplification_percent = 110;
  Reopen(&options);

  Random rnd(301);
  int key_idx = 0;

  //   Generate two files in Level 0. Both files are approx the same size.
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
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);

  // Flush whatever is remaining in memtable. This is typically
  // small, which should not trigger size ratio based compaction
  // but will instead trigger size amplification.
  dbfull()->Flush(FlushOptions());

  dbfull()->TEST_WaitForCompact();

  // Verify that size amplification did occur
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
}

TEST(DBTest, UniversalCompactionOptions) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 1;
  options.compaction_options_universal.compression_size_percent = -1;
  Reopen(&options);

  Random rnd(301);
  int key_idx = 0;

  for (int num = 0;
       num < options.level0_file_num_compaction_trigger;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();

    if (num < options.level0_file_num_compaction_trigger - 1) {
      ASSERT_EQ(NumTableFilesAtLevel(0), num + 1);
    }
  }

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  for (int i = 1; i < options.num_levels ; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i), 0);
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

#if defined(SNAPPY) && defined(ZLIB) && defined(BZIP2)
TEST(DBTest, CompressedCache) {
  int num_iter = 80;

  // Run this test three iterations.
  // Iteration 1: only a uncompressed block cache
  // Iteration 2: only a compressed block cache
  // Iteration 3: both block cache and compressed cache
  for (int iter = 0; iter < 3; iter++) {
    Options options = CurrentOptions();
    options.write_buffer_size = 64*1024;        // small write buffer
    options.statistics = rocksdb::CreateDBStatistics();

    switch (iter) {
      case 0:
        // only uncompressed block cache
        options.block_cache = NewLRUCache(8*1024);
        options.block_cache_compressed = nullptr;
        break;
      case 1:
        // no block cache, only compressed cache
        options.no_block_cache = true;
        options.block_cache = nullptr;
        options.block_cache_compressed = NewLRUCache(8*1024);
        break;
      case 2:
        // both compressed and uncompressed block cache
        options.block_cache = NewLRUCache(1024);
        options.block_cache_compressed = NewLRUCache(8*1024);
        break;
      default:
        ASSERT_TRUE(false);
    }
    Reopen(&options);

    Random rnd(301);

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    std::vector<std::string> values;
    std::string str;
    for (int i = 0; i < num_iter; i++) {
      if (i % 4 == 0) {        // high compression ratio
        str = RandomString(&rnd, 1000);
      }
      values.push_back(str);
      ASSERT_OK(Put(Key(i), values[i]));
    }

    // flush all data from memtable so that reads are from block cache
    dbfull()->Flush(FlushOptions());

    for (int i = 0; i < num_iter; i++) {
      ASSERT_EQ(Get(Key(i)), values[i]);
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
      default:
        ASSERT_TRUE(false);
    }
  }
}

static std::string CompressibleString(Random* rnd, int len) {
  std::string r;
  test::CompressibleString(rnd, 0.8, len, &r);
  return r;
}

TEST(DBTest, UniversalCompactionCompressRatio1) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 1;
  options.compaction_options_universal.compression_size_percent = 70;
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
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100<<10; //100KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 1;
  options.compaction_options_universal.compression_size_percent = 95;
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
#endif

TEST(DBTest, ConvertCompactionStyle) {
  Random rnd(301);
  int max_key_level_insert = 200;
  int max_key_universal_insert = 600;

  // Stage 1: generate a db with level compaction
  Options options = CurrentOptions();
  options.write_buffer_size = 100<<10; //100KB
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.max_bytes_for_level_base = 500<<10; // 500KB
  options.max_bytes_for_level_multiplier = 1;
  options.target_file_size_base = 200<<10; // 200KB
  options.target_file_size_multiplier = 1;
  Reopen(&options);

  for (int i = 0; i <= max_key_level_insert; i++) {
    // each value is 10K
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 10000)));
  }
  dbfull()->Flush(FlushOptions());
  dbfull()->TEST_WaitForCompact();

  ASSERT_GT(TotalTableFiles(), 1);
  int non_level0_num_files = 0;
  for (int i = 1; i < dbfull()->NumberLevels(); i++) {
    non_level0_num_files += NumTableFilesAtLevel(i);
  }
  ASSERT_GT(non_level0_num_files, 0);

  // Stage 2: reopen with universal compaction - should fail
  options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  Status s = TryReopen(&options);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Stage 3: compact into a single file and move the file to level 0
  options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = INT_MAX;
  options.target_file_size_multiplier = 1;
  options.max_bytes_for_level_base = INT_MAX;
  options.max_bytes_for_level_multiplier = 1;
  Reopen(&options);

  dbfull()->CompactRange(nullptr, nullptr,
                         true /* reduce level */,
                         0    /* reduce to level 0 */);

  for (int i = 0; i < dbfull()->NumberLevels(); i++) {
    int num = NumTableFilesAtLevel(i);
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
  Reopen(&options);

  for (int i = max_key_level_insert / 2; i <= max_key_universal_insert; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 10000)));
  }
  dbfull()->Flush(FlushOptions());
  dbfull()->TEST_WaitForCompact();

  for (int i = 1; i < dbfull()->NumberLevels(); i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i), 0);
  }

  // verify keys inserted in both level compaction style and universal
  // compaction style
  std::string keys_in_db;
  Iterator* iter = dbfull()->NewIterator(ReadOptions());
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
    Options options = CurrentOptions();
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    Reopen(&options);

    // We must have at most one file per level except for level-0,
    // which may have up to kL0_StopWritesTrigger files.
    const int kMaxFiles = dbfull()->NumberLevels() +
      dbfull()->Level0StopWriteTrigger();

    Random rnd(301);
    std::string value = RandomString(&rnd, 2 * options.write_buffer_size);
    for (int i = 0; i < 5 * kMaxFiles; i++) {
      Put("key", value);
      ASSERT_LE(TotalTableFiles(), kMaxFiles);
    }
  } while (ChangeCompactOptions());
}

TEST(DBTest, InPlaceUpdate) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    Reopen(&options);

    // Update key with values of smaller size
    int numValues = 10;
    for (int i = numValues; i > 0; i--) {
      std::string value = DummyString(i, 'a');
      ASSERT_OK(Put("key", value));
      ASSERT_EQ(value, Get("key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1);

  } while (ChangeCompactOptions());
}

TEST(DBTest, InPlaceUpdateLargeNewValue) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;
    options.env = env_;
    options.write_buffer_size = 100000;
    Reopen(&options);

    // Update key with values of larger size
    int numValues = 10;
    for (int i = 0; i < numValues; i++) {
      std::string value = DummyString(i, 'a');
      ASSERT_OK(Put("key", value));
      ASSERT_EQ(value, Get("key"));
    }

    // All 10 updates exist in the internal iterator
    validateNumberOfEntries(numValues);

  } while (ChangeCompactOptions());
}


TEST(DBTest, InPlaceUpdateCallbackSmallerSize) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceSmallerSize;
    Reopen(&options);

    // Update key with values of smaller size
    int numValues = 10;
    ASSERT_OK(Put("key", DummyString(numValues, 'a')));
    ASSERT_EQ(DummyString(numValues, 'c'), Get("key"));

    for (int i = numValues; i > 0; i--) {
      ASSERT_OK(Put("key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(i - 1, 'b'), Get("key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1);

  } while (ChangeCompactOptions());
}

TEST(DBTest, InPlaceUpdateCallbackSmallerVarintSize) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceSmallerVarintSize;
    Reopen(&options);

    // Update key with values of smaller varint size
    int numValues = 265;
    ASSERT_OK(Put("key", DummyString(numValues, 'a')));
    ASSERT_EQ(DummyString(numValues, 'c'), Get("key"));

    for (int i = numValues; i > 0; i--) {
      ASSERT_OK(Put("key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(1, 'b'), Get("key"));
    }

    // Only 1 instance for that key.
    validateNumberOfEntries(1);

  } while (ChangeCompactOptions());
}

TEST(DBTest, InPlaceUpdateCallbackLargeNewValue) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceLargerSize;
    Reopen(&options);

    // Update key with values of larger size
    int numValues = 10;
    for (int i = 0; i < numValues; i++) {
      ASSERT_OK(Put("key", DummyString(i, 'a')));
      ASSERT_EQ(DummyString(i, 'c'), Get("key"));
    }

    // No inplace updates. All updates are puts with new seq number
    // All 10 updates exist in the internal iterator
    validateNumberOfEntries(numValues);

  } while (ChangeCompactOptions());
}

TEST(DBTest, InPlaceUpdateCallbackNoAction) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.inplace_update_support = true;

    options.env = env_;
    options.write_buffer_size = 100000;
    options.inplace_callback =
      rocksdb::DBTest::updateInPlaceNoAction;
    Reopen(&options);

    // Callback function requests no actions from db
    ASSERT_OK(Put("key", DummyString(1, 'a')));
    ASSERT_EQ(Get("key"), "NOT_FOUND");

  } while (ChangeCompactOptions());
}

// This is a static filter used for filtering
// kvs during the compaction process.
static int cfilter_count;
static std::string NEW_VALUE = "NewValue";

class KeepFilter : public CompactionFilter {
 public:
  virtual bool Filter(int level, const Slice& key,
                      const Slice& value, std::string* new_value,
                      bool* value_changed) const override {
    cfilter_count++;
    return false;
  }

  virtual const char* Name() const override {
    return "KeepFilter";
  }

};

class DeleteFilter : public CompactionFilter {
 public:
  virtual bool Filter(int level, const Slice& key,
                      const Slice& value, std::string* new_value,
                      bool* value_changed) const override {
    cfilter_count++;
    return true;
  }

  virtual const char* Name() const override {
    return "DeleteFilter";
  }
};

class ChangeFilter : public CompactionFilter {
 public:
  explicit ChangeFilter() {}

  virtual bool Filter(int level, const Slice& key,
                      const Slice& value, std::string* new_value,
                      bool* value_changed) const override {
    assert(new_value != nullptr);
    *new_value = NEW_VALUE;
    *value_changed = true;
    return false;
  }

  virtual const char* Name() const override {
    return "ChangeFilter";
  }
};

class KeepFilterFactory : public CompactionFilterFactory {
  public:
    virtual std::unique_ptr<CompactionFilter>
    CreateCompactionFilter(const CompactionFilter::Context& context) override {
      return std::unique_ptr<CompactionFilter>(new KeepFilter());
    }

    virtual const char* Name() const override {
      return "KeepFilterFactory";
    }
};

class DeleteFilterFactory : public CompactionFilterFactory {
  public:
    virtual std::unique_ptr<CompactionFilter>
    CreateCompactionFilter(const CompactionFilter::Context& context) override {
      if (context.is_manual_compaction) {
        return std::unique_ptr<CompactionFilter>(new DeleteFilter());
      } else {
        return std::unique_ptr<CompactionFilter>(nullptr);
      }
    }

    virtual const char* Name() const override {
      return "DeleteFilterFactory";
    }
};

class ChangeFilterFactory : public CompactionFilterFactory {
  public:
    explicit ChangeFilterFactory() {}

    virtual std::unique_ptr<CompactionFilter>
    CreateCompactionFilter(const CompactionFilter::Context& context) override {
      return std::unique_ptr<CompactionFilter>(new ChangeFilter());
    }

    virtual const char* Name() const override {
      return "ChangeFilterFactory";
    }
};

TEST(DBTest, CompactionFilter) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.compaction_filter_factory = std::make_shared<KeepFilterFactory>();
  Reopen(&options);

  // Write 100K keys, these are written to a few files in L0.
  const std::string value(10, 'x');
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    Put(key, value);
  }
  dbfull()->TEST_FlushMemTable();

  // Push all files to the highest level L2. Verify that
  // the compaction is each level invokes the filter for
  // all the keys in that level.
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  ASSERT_EQ(cfilter_count, 100000);
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(1, nullptr, nullptr);
  ASSERT_EQ(cfilter_count, 100000);

  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_NE(NumTableFilesAtLevel(2), 0);
  cfilter_count = 0;

  // All the files are in the lowest level.
  // Verify that all but the 100001st record
  // has sequence number zero. The 100001st record
  // is at the tip of this snapshot and cannot
  // be zeroed out.
  // TODO: figure out sequence number squashtoo
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
  ASSERT_EQ(count, 1);
  delete iter;

  // overwrite all the 100K keys once again.
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    Put(key, value);
  }
  dbfull()->TEST_FlushMemTable();

  // push all files to the highest level L2. This
  // means that all keys should pass at least once
  // via the compaction filter
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  ASSERT_EQ(cfilter_count, 100000);
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(1, nullptr, nullptr);
  ASSERT_EQ(cfilter_count, 100000);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_NE(NumTableFilesAtLevel(2), 0);

  // create a new database with the compaction
  // filter in such a way that it deletes all keys
  options.compaction_filter_factory = std::make_shared<DeleteFilterFactory>();
  options.create_if_missing = true;
  DestroyAndReopen(&options);

  // write all the keys once again.
  for (int i = 0; i < 100000; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    Put(key, value);
  }
  dbfull()->TEST_FlushMemTable();
  ASSERT_NE(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);

  // Push all files to the highest level L2. This
  // triggers the compaction filter to delete all keys,
  // verify that at the end of the compaction process,
  // nothing is left.
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  ASSERT_EQ(cfilter_count, 100000);
  cfilter_count = 0;
  dbfull()->TEST_CompactRange(1, nullptr, nullptr);
  ASSERT_EQ(cfilter_count, 0);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
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

  // The sequence number of the remaining record
  // is not zeroed out even though it is at the
  // level Lmax because this record is at the tip
  // TODO: remove the following or design a different
  // test
  count = 0;
  iter = dbfull()->TEST_NewInternalIterator();
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

TEST(DBTest, CompactionFilterWithValueChange) {
  do {
    Options options = CurrentOptions();
    options.num_levels = 3;
    options.max_mem_compaction_level = 0;
    options.compaction_filter_factory =
      std::make_shared<ChangeFilterFactory>();
    Reopen(&options);

    // Write 100K+1 keys, these are written to a few files
    // in L0. We do this so that the current snapshot points
    // to the 100001 key.The compaction filter is  not invoked
    // on keys that are visible via a snapshot because we
    // anyways cannot delete it.
    const std::string value(10, 'x');
    for (int i = 0; i < 100001; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%010d", i);
      Put(key, value);
    }

    // push all files to  lower levels
    dbfull()->TEST_FlushMemTable();
    dbfull()->TEST_CompactRange(0, nullptr, nullptr);
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);

    // re-write all data again
    for (int i = 0; i < 100001; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%010d", i);
      Put(key, value);
    }

    // push all files to  lower levels. This should
    // invoke the compaction filter for all 100000 keys.
    dbfull()->TEST_FlushMemTable();
    dbfull()->TEST_CompactRange(0, nullptr, nullptr);
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);

    // verify that all keys now have the new value that
    // was set by the compaction process.
    for (int i = 0; i < 100000; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%010d", i);
      std::string newvalue = Get(key);
      ASSERT_EQ(newvalue.compare(NEW_VALUE), 0);
    }
  } while (ChangeCompactOptions());
}

TEST(DBTest, SparseMerge) {
  do {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    Reopen(&options);

    FillLevels("A", "Z");

    // Suppose there is:
    //    small amount of data with prefix A
    //    large amount of data with prefix B
    //    small amount of data with prefix C
    // and that recent updates have made small changes to all three prefixes.
    // Check that we do not do a compaction that merges all of B in one shot.
    const std::string value(1000, 'x');
    Put("A", "va");
    // Write approximately 100MB of "B" values
    for (int i = 0; i < 100000; i++) {
      char key[100];
      snprintf(key, sizeof(key), "B%010d", i);
      Put(key, value);
    }
    Put("C", "vc");
    dbfull()->TEST_FlushMemTable();
    dbfull()->TEST_CompactRange(0, nullptr, nullptr);

    // Make sparse update
    Put("A",    "va2");
    Put("B100", "bvalue2");
    Put("C",    "vc2");
    dbfull()->TEST_FlushMemTable();

    // Compactions should not cause us to create a situation where
    // a file overlaps too much data at the next level.
    ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20*1048576);
    dbfull()->TEST_CompactRange(0, nullptr, nullptr);
    ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20*1048576);
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);
    ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20*1048576);
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
    Options options = CurrentOptions();
    options.write_buffer_size = 100000000;        // Large write buffer
    options.compression = kNoCompression;
    DestroyAndReopen();

    ASSERT_TRUE(Between(Size("", "xyz"), 0, 0));
    Reopen(&options);
    ASSERT_TRUE(Between(Size("", "xyz"), 0, 0));

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    const int N = 80;
    static const int S1 = 100000;
    static const int S2 = 105000;  // Allow some expansion from metadata
    Random rnd(301);
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(Key(i), RandomString(&rnd, S1)));
    }

    // 0 because GetApproximateSizes() does not account for memtable space
    ASSERT_TRUE(Between(Size("", Key(50)), 0, 0));

    // Check sizes across recovery by reopening a few times
    for (int run = 0; run < 3; run++) {
      Reopen(&options);

      for (int compact_start = 0; compact_start < N; compact_start += 10) {
        for (int i = 0; i < N; i += 10) {
          ASSERT_TRUE(Between(Size("", Key(i)), S1*i, S2*i));
          ASSERT_TRUE(Between(Size("", Key(i)+".suffix"), S1*(i+1), S2*(i+1)));
          ASSERT_TRUE(Between(Size(Key(i), Key(i+10)), S1*10, S2*10));
        }
        ASSERT_TRUE(Between(Size("", Key(50)), S1*50, S2*50));
        ASSERT_TRUE(Between(Size("", Key(50)+".suffix"), S1*50, S2*50));

        std::string cstart_str = Key(compact_start);
        std::string cend_str = Key(compact_start + 9);
        Slice cstart = cstart_str;
        Slice cend = cend_str;
        dbfull()->TEST_CompactRange(0, &cstart, &cend);
      }

      ASSERT_EQ(NumTableFilesAtLevel(0), 0);
      ASSERT_GT(NumTableFilesAtLevel(1), 0);
    }
    // ApproximateOffsetOf() is not yet implemented in plain table format.
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipPlainTable));
}

TEST(DBTest, ApproximateSizes_MixOfSmallAndLarge) {
  do {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    Reopen();

    Random rnd(301);
    std::string big1 = RandomString(&rnd, 100000);
    ASSERT_OK(Put(Key(0), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(Key(1), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(Key(2), big1));
    ASSERT_OK(Put(Key(3), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(Key(4), big1));
    ASSERT_OK(Put(Key(5), RandomString(&rnd, 10000)));
    ASSERT_OK(Put(Key(6), RandomString(&rnd, 300000)));
    ASSERT_OK(Put(Key(7), RandomString(&rnd, 10000)));

    // Check sizes across recovery by reopening a few times
    for (int run = 0; run < 3; run++) {
      Reopen(&options);

      ASSERT_TRUE(Between(Size("", Key(0)), 0, 0));
      ASSERT_TRUE(Between(Size("", Key(1)), 10000, 11000));
      ASSERT_TRUE(Between(Size("", Key(2)), 20000, 21000));
      ASSERT_TRUE(Between(Size("", Key(3)), 120000, 121000));
      ASSERT_TRUE(Between(Size("", Key(4)), 130000, 131000));
      ASSERT_TRUE(Between(Size("", Key(5)), 230000, 231000));
      ASSERT_TRUE(Between(Size("", Key(6)), 240000, 241000));
      ASSERT_TRUE(Between(Size("", Key(7)), 540000, 541000));
      ASSERT_TRUE(Between(Size("", Key(8)), 550000, 560000));

      ASSERT_TRUE(Between(Size(Key(3), Key(5)), 110000, 111000));

      dbfull()->TEST_CompactRange(0, nullptr, nullptr);
    }
    // ApproximateOffsetOf() is not yet implemented in plain table format.
  } while (ChangeOptions(kSkipPlainTable));
}

TEST(DBTest, IteratorPinsRef) {
  do {
    Put("foo", "hello");

    // Get iterator that will yield the current contents of the DB.
    Iterator* iter = db_->NewIterator(ReadOptions());

    // Write to force compactions
    Put("foo", "newvalue1");
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(Put(Key(i), Key(i) + std::string(100000, 'v'))); // 100K values
    }
    Put("foo", "newvalue2");

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
    Put("foo", "v1");
    const Snapshot* s1 = db_->GetSnapshot();
    Put("foo", "v2");
    const Snapshot* s2 = db_->GetSnapshot();
    Put("foo", "v3");
    const Snapshot* s3 = db_->GetSnapshot();

    Put("foo", "v4");
    ASSERT_EQ("v1", Get("foo", s1));
    ASSERT_EQ("v2", Get("foo", s2));
    ASSERT_EQ("v3", Get("foo", s3));
    ASSERT_EQ("v4", Get("foo"));

    db_->ReleaseSnapshot(s3);
    ASSERT_EQ("v1", Get("foo", s1));
    ASSERT_EQ("v2", Get("foo", s2));
    ASSERT_EQ("v4", Get("foo"));

    db_->ReleaseSnapshot(s1);
    ASSERT_EQ("v2", Get("foo", s2));
    ASSERT_EQ("v4", Get("foo"));

    db_->ReleaseSnapshot(s2);
    ASSERT_EQ("v4", Get("foo"));
  } while (ChangeOptions());
}

TEST(DBTest, HiddenValuesAreRemoved) {
  do {
    Random rnd(301);
    FillLevels("a", "z");

    std::string big = RandomString(&rnd, 50000);
    Put("foo", big);
    Put("pastfoo", "v");
    const Snapshot* snapshot = db_->GetSnapshot();
    Put("foo", "tiny");
    Put("pastfoo2", "v2");        // Advance sequence number one more

    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_GT(NumTableFilesAtLevel(0), 0);

    ASSERT_EQ(big, Get("foo", snapshot));
    ASSERT_TRUE(Between(Size("", "pastfoo"), 50000, 60000));
    db_->ReleaseSnapshot(snapshot);
    ASSERT_EQ(AllEntriesFor("foo"), "[ tiny, " + big + " ]");
    Slice x("x");
    dbfull()->TEST_CompactRange(0, nullptr, &x);
    ASSERT_EQ(AllEntriesFor("foo"), "[ tiny ]");
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    ASSERT_GE(NumTableFilesAtLevel(1), 1);
    dbfull()->TEST_CompactRange(1, nullptr, &x);
    ASSERT_EQ(AllEntriesFor("foo"), "[ tiny ]");

    ASSERT_TRUE(Between(Size("", "pastfoo"), 0, 1000));
    // ApproximateOffsetOf() is not yet implemented in plain table format,
    // which is used by Size().
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipPlainTable));
}

TEST(DBTest, CompactBetweenSnapshots) {
  do {
    Random rnd(301);
    FillLevels("a", "z");

    Put("foo", "first");
    const Snapshot* snapshot1 = db_->GetSnapshot();
    Put("foo", "second");
    Put("foo", "third");
    Put("foo", "fourth");
    const Snapshot* snapshot2 = db_->GetSnapshot();
    Put("foo", "fifth");
    Put("foo", "sixth");

    // All entries (including duplicates) exist
    // before any compaction is triggered.
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_EQ("sixth", Get("foo"));
    ASSERT_EQ("fourth", Get("foo", snapshot2));
    ASSERT_EQ("first", Get("foo", snapshot1));
    ASSERT_EQ(AllEntriesFor("foo"),
              "[ sixth, fifth, fourth, third, second, first ]");

    // After a compaction, "second", "third" and "fifth" should
    // be removed
    FillLevels("a", "z");
    dbfull()->CompactRange(nullptr, nullptr);
    ASSERT_EQ("sixth", Get("foo"));
    ASSERT_EQ("fourth", Get("foo", snapshot2));
    ASSERT_EQ("first", Get("foo", snapshot1));
    ASSERT_EQ(AllEntriesFor("foo"), "[ sixth, fourth, first ]");

    // after we release the snapshot1, only two values left
    db_->ReleaseSnapshot(snapshot1);
    FillLevels("a", "z");
    dbfull()->CompactRange(nullptr, nullptr);

    // We have only one valid snapshot snapshot2. Since snapshot1 is
    // not valid anymore, "first" should be removed by a compaction.
    ASSERT_EQ("sixth", Get("foo"));
    ASSERT_EQ("fourth", Get("foo", snapshot2));
    ASSERT_EQ(AllEntriesFor("foo"), "[ sixth, fourth ]");

    // after we release the snapshot2, only one value should be left
    db_->ReleaseSnapshot(snapshot2);
    FillLevels("a", "z");
    dbfull()->CompactRange(nullptr, nullptr);
    ASSERT_EQ("sixth", Get("foo"));
    ASSERT_EQ(AllEntriesFor("foo"), "[ sixth ]");

  } while (ChangeOptions());
}

TEST(DBTest, DeletionMarkers1) {
  Put("foo", "v1");
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  const int last = dbfull()->MaxMemCompactionLevel();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);   // foo => v1 is now in last level

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put("a", "begin");
  Put("z", "end");
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last-1), 1);

  Delete("foo");
  Put("foo", "v2");
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
  ASSERT_OK(dbfull()->TEST_FlushMemTable());  // Moves to level last-2
  if (CurrentOptions().purge_redundant_kvs_while_flush) {
    ASSERT_EQ(AllEntriesFor("foo"), "[ v2, v1 ]");
  } else {
    ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
  }
  Slice z("z");
  dbfull()->TEST_CompactRange(last-2, nullptr, &z);
  // DEL eliminated, but v1 remains because we aren't compacting that level
  // (DEL can be eliminated because v2 hides v1).
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2, v1 ]");
  dbfull()->TEST_CompactRange(last-1, nullptr, nullptr);
  // Merging last-1 w/ last, so we are the base level for "foo", so
  // DEL is removed.  (as is v1).
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2 ]");
}

TEST(DBTest, DeletionMarkers2) {
  Put("foo", "v1");
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  const int last = dbfull()->MaxMemCompactionLevel();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);   // foo => v1 is now in last level

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put("a", "begin");
  Put("z", "end");
  dbfull()->TEST_FlushMemTable();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last-1), 1);

  Delete("foo");
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
  ASSERT_OK(dbfull()->TEST_FlushMemTable());  // Moves to level last-2
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
  dbfull()->TEST_CompactRange(last-2, nullptr, nullptr);
  // DEL kept: "last" file overlaps
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
  dbfull()->TEST_CompactRange(last-1, nullptr, nullptr);
  // Merging last-1 w/ last, so we are the base level for "foo", so
  // DEL is removed.  (as is v1).
  ASSERT_EQ(AllEntriesFor("foo"), "[ ]");
}

TEST(DBTest, OverlapInLevel0) {
  do {
    int tmp = dbfull()->MaxMemCompactionLevel();
    ASSERT_EQ(tmp, 2) << "Fix test to match config";

    //Fill levels 1 and 2 to disable the pushing of new memtables to levels > 0.
    ASSERT_OK(Put("100", "v100"));
    ASSERT_OK(Put("999", "v999"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_OK(Delete("100"));
    ASSERT_OK(Delete("999"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_EQ("0,1,1", FilesPerLevel());

    // Make files spanning the following ranges in level-0:
    //  files[0]  200 .. 900
    //  files[1]  300 .. 500
    // Note that files are sorted by smallest key.
    ASSERT_OK(Put("300", "v300"));
    ASSERT_OK(Put("500", "v500"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_OK(Put("200", "v200"));
    ASSERT_OK(Put("600", "v600"));
    ASSERT_OK(Put("900", "v900"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_EQ("2,1,1", FilesPerLevel());

    // Compact away the placeholder files we created initially
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);
    dbfull()->TEST_CompactRange(2, nullptr, nullptr);
    ASSERT_EQ("2", FilesPerLevel());

    // Do a memtable compaction.  Before bug-fix, the compaction would
    // not detect the overlap with level-0 files and would incorrectly place
    // the deletion in a deeper level.
    ASSERT_OK(Delete("600"));
    dbfull()->TEST_FlushMemTable();
    ASSERT_EQ("3", FilesPerLevel());
    ASSERT_EQ("NOT_FOUND", Get("600"));
  } while (ChangeOptions(kSkipUniversalCompaction));
}

TEST(DBTest, L0_CompactionBug_Issue44_a) {
  do {
    Reopen();
    ASSERT_OK(Put("b", "v"));
    Reopen();
    ASSERT_OK(Delete("b"));
    ASSERT_OK(Delete("a"));
    Reopen();
    ASSERT_OK(Delete("a"));
    Reopen();
    ASSERT_OK(Put("a", "v"));
    Reopen();
    Reopen();
    ASSERT_EQ("(a->v)", Contents());
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(a->v)", Contents());
  } while (ChangeCompactOptions());
}

TEST(DBTest, L0_CompactionBug_Issue44_b) {
  do {
    Reopen();
    Put("","");
    Reopen();
    Delete("e");
    Put("","");
    Reopen();
    Put("c", "cv");
    Reopen();
    Put("","");
    Reopen();
    Put("","");
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    Reopen();
    Put("d","dv");
    Reopen();
    Put("","");
    Reopen();
    Delete("d");
    Delete("b");
    Reopen();
    ASSERT_EQ("(->)(c->cv)", Contents());
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(->)(c->cv)", Contents());
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
  Options new_options;
  NewComparator cmp;
  do {
    new_options = CurrentOptions();
    new_options.comparator = &cmp;
    Status s = TryReopen(&new_options);
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
    new_options.filter_policy = nullptr;     // Cannot use bloom filters
    new_options.write_buffer_size = 1000;  // Compact more often
    DestroyAndReopen(&new_options);
    ASSERT_OK(Put("[10]", "ten"));
    ASSERT_OK(Put("[0x14]", "twenty"));
    for (int i = 0; i < 2; i++) {
      ASSERT_EQ("ten", Get("[10]"));
      ASSERT_EQ("ten", Get("[0xa]"));
      ASSERT_EQ("twenty", Get("[20]"));
      ASSERT_EQ("twenty", Get("[0x14]"));
      ASSERT_EQ("NOT_FOUND", Get("[15]"));
      ASSERT_EQ("NOT_FOUND", Get("[0xf]"));
      Compact("[0]", "[9999]");
    }

    for (int run = 0; run < 2; run++) {
      for (int i = 0; i < 1000; i++) {
        char buf[100];
        snprintf(buf, sizeof(buf), "[%d]", i*10);
        ASSERT_OK(Put(buf, buf));
      }
      Compact("[0]", "[1000000]");
    }
  } while (ChangeCompactOptions(&new_options));
}

TEST(DBTest, ManualCompaction) {
  ASSERT_EQ(dbfull()->MaxMemCompactionLevel(), 2)
      << "Need to update this test to match kMaxMemCompactLevel";

  // iter - 0 with 7 levels
  // iter - 1 with 3 levels
  for (int iter = 0; iter < 2; ++iter) {
    MakeTables(3, "p", "q");
    ASSERT_EQ("1,1,1", FilesPerLevel());

    // Compaction range falls before files
    Compact("", "c");
    ASSERT_EQ("1,1,1", FilesPerLevel());

    // Compaction range falls after files
    Compact("r", "z");
    ASSERT_EQ("1,1,1", FilesPerLevel());

    // Compaction range overlaps files
    Compact("p1", "p9");
    ASSERT_EQ("0,0,1", FilesPerLevel());

    // Populate a different range
    MakeTables(3, "c", "e");
    ASSERT_EQ("1,1,2", FilesPerLevel());

    // Compact just the new range
    Compact("b", "f");
    ASSERT_EQ("0,0,2", FilesPerLevel());

    // Compact all
    MakeTables(1, "a", "z");
    ASSERT_EQ("0,1,2", FilesPerLevel());
    db_->CompactRange(nullptr, nullptr);
    ASSERT_EQ("0,0,1", FilesPerLevel());

    if (iter == 0) {
      Options options = CurrentOptions();
      options.num_levels = 3;
      options.create_if_missing = true;
      DestroyAndReopen(&options);
    }
  }

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
  std::string dbname = test::TmpDir() + "/db_change_num_levels";
  ASSERT_OK(DestroyDB(dbname, Options()));
  Options opts;
  Status s;
  DB* db = nullptr;
  opts.create_if_missing = true;
  s = DB::Open(opts, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  db->Put(WriteOptions(), "a", "123");
  db->Put(WriteOptions(), "b", "234");
  db->CompactRange(nullptr, nullptr);
  delete db;
  db = nullptr;

  opts.create_if_missing = false;
  opts.num_levels = 2;
  s = DB::Open(opts, dbname, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "Invalid argument") != nullptr);
  ASSERT_TRUE(db == nullptr);
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
    env_->no_space_.Release_Store(nullptr);
    ASSERT_LT(CountFiles(), num_files + 3);

    // Check that compaction attempts slept after errors
    ASSERT_GE(env_->sleep_counter_.Read(), 5);
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
    dbfull()->TEST_FlushMemTable();
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
  Status s;

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  // simulate error
  env_->log_write_error_.Release_Store(env_);
  s = Put("foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.Release_Store(nullptr);
  s = Put("foo3", "bar3");
  // the next put should fail, too
  ASSERT_TRUE(!s.ok());
  // but we're still able to read
  ASSERT_EQ("bar", Get("foo"));

  // do the same thing with paranoid checks off
  options.paranoid_checks = false;
  DestroyAndReopen(&options);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  // simulate error
  env_->log_write_error_.Release_Store(env_);
  s = Put("foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.Release_Store(nullptr);
  s = Put("foo3", "bar3");
  // the next put should NOT fail
  ASSERT_TRUE(s.ok());
}

TEST(DBTest, FilesDeletedAfterCompaction) {
  do {
    ASSERT_OK(Put("foo", "v2"));
    Compact("a", "z");
    const int num_files = CountLiveFiles();
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(Put("foo", "v2"));
      Compact("a", "z");
    }
    ASSERT_EQ(CountLiveFiles(), num_files);
  } while (ChangeCompactOptions());
}

TEST(DBTest, BloomFilter) {
  do {
    env_->count_random_reads_ = true;
    Options options = CurrentOptions();
    options.env = env_;
    options.no_block_cache = true;
    options.filter_policy = NewBloomFilterPolicy(10);
    Reopen(&options);

    // Populate multiple layers
    const int N = 10000;
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(Key(i), Key(i)));
    }
    Compact("a", "z");
    for (int i = 0; i < N; i += 100) {
      ASSERT_OK(Put(Key(i), Key(i)));
    }
    dbfull()->TEST_FlushMemTable();

    // Prevent auto compactions triggered by seeks
    env_->delay_sstable_sync_.Release_Store(env_);

    // Lookup present keys.  Should rarely read from small sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i), Get(Key(i)));
    }
    int reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d present => %d reads\n", N, reads);
    ASSERT_GE(reads, N);
    ASSERT_LE(reads, N + 2*N/100);

    // Lookup present keys.  Should rarely read from either sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ("NOT_FOUND", Get(Key(i) + ".missing"));
    }
    reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d missing => %d reads\n", N, reads);
    ASSERT_LE(reads, 3*N/100);

    env_->delay_sstable_sync_.Release_Store(nullptr);
    Close();
    delete options.filter_policy;
  } while (ChangeCompactOptions());
}

TEST(DBTest, SnapshotFiles) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 100000000;        // Large write buffer
    Reopen(&options);

    Random rnd(301);

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    std::vector<std::string> values;
    for (int i = 0; i < 80; i++) {
      values.push_back(RandomString(&rnd, 100000));
      ASSERT_OK(Put(Key(i), values[i]));
    }

    // assert that nothing makes it to disk yet.
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);

    // get a file snapshot
    uint64_t manifest_number = 0;
    uint64_t manifest_size = 0;
    std::vector<std::string> files;
    dbfull()->DisableFileDeletions();
    dbfull()->GetLiveFiles(files, &manifest_size);

    // CURRENT, MANIFEST, *.sst files
    ASSERT_EQ(files.size(), 3U);

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
      ASSERT_OK(Put(Key(i), extras[i]));
    }

    // verify that data in the snapshot are correct
    Options opts;
    DB* snapdb;
    opts.create_if_missing = false;
    Status stat = DB::Open(opts, snapdir, &snapdb);
    ASSERT_OK(stat);

    ReadOptions roptions;
    std::string val;
    for (unsigned int i = 0; i < 80; i++) {
      stat = snapdb->Get(roptions, Key(i), &val);
      ASSERT_EQ(values[i].compare(val), 0);
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
    Reopen(&options);

    Put("foo", "v1");
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_EQ(AllEntriesFor("foo"), "[ v1 ]");

    // Write two new keys
    Put("a", "begin");
    Put("z", "end");
    dbfull()->TEST_FlushMemTable();

    // Case1: Delete followed by a put
    Delete("foo");
    Put("foo", "v2");
    ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");

    // After the current memtable is flushed, the DEL should
    // have been removed
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_EQ(AllEntriesFor("foo"), "[ v2, v1 ]");

    dbfull()->CompactRange(nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo"), "[ v2 ]");

    // Case 2: Delete followed by another delete
    Delete("foo");
    Delete("foo");
    ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, DEL, v2 ]");
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v2 ]");
    dbfull()->CompactRange(nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo"), "[ ]");

    // Case 3: Put followed by a delete
    Put("foo", "v3");
    Delete("foo");
    ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v3 ]");
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_EQ(AllEntriesFor("foo"), "[ DEL ]");
    dbfull()->CompactRange(nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo"), "[ ]");

    // Case 4: Put followed by another Put
    Put("foo", "v4");
    Put("foo", "v5");
    ASSERT_EQ(AllEntriesFor("foo"), "[ v5, v4 ]");
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_EQ(AllEntriesFor("foo"), "[ v5 ]");
    dbfull()->CompactRange(nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo"), "[ v5 ]");

    // clear database
    Delete("foo");
    dbfull()->CompactRange(nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo"), "[ ]");

    // Case 5: Put followed by snapshot followed by another Put
    // Both puts should remain.
    Put("foo", "v6");
    const Snapshot* snapshot = db_->GetSnapshot();
    Put("foo", "v7");
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_EQ(AllEntriesFor("foo"), "[ v7, v6 ]");
    db_->ReleaseSnapshot(snapshot);

    // clear database
    Delete("foo");
    dbfull()->CompactRange(nullptr, nullptr);
    ASSERT_EQ(AllEntriesFor("foo"), "[ ]");

    // Case 5: snapshot followed by a put followed by another Put
    // Only the last put should remain.
    const Snapshot* snapshot1 = db_->GetSnapshot();
    Put("foo", "v8");
    Put("foo", "v9");
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_EQ(AllEntriesFor("foo"), "[ v9 ]");
    db_->ReleaseSnapshot(snapshot1);
  } while (ChangeCompactOptions());
}

std::vector<std::uint64_t> ListLogFiles(Env* env, const std::string& path) {
  std::vector<std::string> files;
  std::vector<uint64_t> log_files;
  env->GetChildren(path, &files);
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < files.size(); ++i) {
    if (ParseFileName(files[i], &number, &type)) {
      if (type == kLogFile) {
        log_files.push_back(number);
      }
    }
  }
  return std::move(log_files);
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

TEST(DBTest, TransactionLogIterator) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(&options);
    Put("key1", DummyString(1024));
    Put("key2", DummyString(1024));
    Put("key2", DummyString(1024));
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3U);
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(3, iter);
    }
    Reopen(&options);
      env_->SleepForMicroseconds(2 * 1000 * 1000);{
      Put("key4", DummyString(1024));
      Put("key5", DummyString(1024));
      Put("key6", DummyString(1024));
    }
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(6, iter);
    }
  } while (ChangeCompactOptions());
}

TEST(DBTest, TransactionLogIteratorMoveOverZeroFiles) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(&options);
    // Do a plain Reopen.
    Put("key1", DummyString(1024));
    // Two reopens should create a zero record WAL file.
    Reopen(&options);
    Reopen(&options);

    Put("key2", DummyString(1024));

    auto iter = OpenTransactionLogIter(0);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

// TODO(kailiu) disable the in non-linux platforms to temporarily solve
// // the unit test failure.
#ifdef OS_LINUX
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
#endif

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
    WriteBatch batch;
    batch.Put("key1", DummyString(1024));
    batch.Put("key2", DummyString(1024));
    batch.Put("key3", DummyString(1024));
    batch.Delete("key2");
    dbfull()->Write(WriteOptions(), &batch);
    dbfull()->Flush(FlushOptions());
    Reopen(&options);
    Put("key4", DummyString(1024));
    auto iter = OpenTransactionLogIter(3);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST(DBTest, TransactionLogIteratorBlobs) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(&options);
  {
    WriteBatch batch;
    batch.Put("key1", DummyString(1024));
    batch.Put("key2", DummyString(1024));
    batch.PutLogData(Slice("blob1"));
    batch.Put("key3", DummyString(1024));
    batch.PutLogData(Slice("blob2"));
    batch.Delete("key2");
    dbfull()->Write(WriteOptions(), &batch);
    Reopen(&options);
  }

  auto res = OpenTransactionLogIter(0)->GetBatch();
  struct Handler : public WriteBatch::Handler {
    std::string seen;
    virtual void Put(const Slice& key, const Slice& value) {
      seen += "Put(" + key.ToString() + ", " + std::to_string(value.size()) +
        ")";
    }
    virtual void Merge(const Slice& key, const Slice& value) {
      seen += "Merge(" + key.ToString() + ", " + std::to_string(value.size()) +
        ")";
    }
    virtual void LogData(const Slice& blob) {
      seen += "LogData(" + blob.ToString() + ")";
    }
    virtual void Delete(const Slice& key) {
      seen += "Delete(" + key.ToString() + ")";
    }
  } handler;
  res.writeBatchPtr->Iterate(&handler);
  ASSERT_EQ("Put(key1, 1024)"
            "Put(key2, 1024)"
            "LogData(blob1)"
            "Put(key3, 1024)"
            "LogData(blob2)"
            "Delete(key2)", handler.seen);
}

TEST(DBTest, ReadCompaction) {
  std::string value(4096, '4'); // a string of size 4K
  {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.max_open_files = 20; // only 10 file in file-cache
    options.target_file_size_base = 512;
    options.write_buffer_size = 64 * 1024;
    options.filter_policy = nullptr;
    options.block_size = 4096;
    options.no_block_cache = true;

    Reopen(&options);

    // Write 8MB (2000 values, each 4K)
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    std::vector<std::string> values;
    for (int i = 0; i < 2000; i++) {
      ASSERT_OK(Put(Key(i), value));
    }

    // clear level 0 and 1 if necessary.
    dbfull()->TEST_FlushMemTable();
    dbfull()->TEST_CompactRange(0, nullptr, nullptr);
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    ASSERT_EQ(NumTableFilesAtLevel(1), 0);

    // write some new keys into level 0
    for (int i = 0; i < 2000; i = i + 16) {
      ASSERT_OK(Put(Key(i), value));
    }
    dbfull()->Flush(FlushOptions());

    // Wait for any write compaction to finish
    dbfull()->TEST_WaitForCompact();

    // remember number of files in each level
    int l1 = NumTableFilesAtLevel(0);
    int l2 = NumTableFilesAtLevel(1);
    int l3 = NumTableFilesAtLevel(3);
    ASSERT_NE(NumTableFilesAtLevel(0), 0);
    ASSERT_NE(NumTableFilesAtLevel(1), 0);
    ASSERT_NE(NumTableFilesAtLevel(2), 0);

    // read a bunch of times, trigger read compaction
    for (int j = 0; j < 100; j++) {
      for (int i = 0; i < 2000; i++) {
        Get(Key(i));
      }
    }
    // wait for read compaction to finish
    env_->SleepForMicroseconds(1000000);

    // verify that the number of files have decreased
    // in some level, indicating that there was a compaction
    ASSERT_TRUE(NumTableFilesAtLevel(0) < l1 ||
                NumTableFilesAtLevel(1) < l2 ||
                NumTableFilesAtLevel(2) < l3);
  }
}

// Multi-threaded test:
namespace {

static const int kNumThreads = 4;
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
  std::string value;
  char valbuf[1500];
  while (t->state->stop.Acquire_Load() == nullptr) {
    t->state->counter[id].Release_Store(reinterpret_cast<void*>(counter));

    int key = rnd.Uniform(kNumKeys);
    char keybuf[20];
    snprintf(keybuf, sizeof(keybuf), "%016d", key);

    if (rnd.OneIn(2)) {
      // Write values of the form <key, my id, counter>.
      // We add some padding for force compactions.
      snprintf(valbuf, sizeof(valbuf), "%d.%d.%-1000d",
               key, id, static_cast<int>(counter));
      ASSERT_OK(t->state->test->Put(Slice(keybuf), Slice(valbuf)));
    } else {
      // Read a value and verify that it matches the pattern written above.
      Status s = db->Get(ReadOptions(), Slice(keybuf), &value);
      if (s.IsNotFound()) {
        // Key has not yet been written
      } else {
        // Check that the writer thread counter is >= the counter in the value
        ASSERT_OK(s);
        int k, w, c;
        ASSERT_EQ(3, sscanf(value.c_str(), "%d.%d.%d", &k, &w, &c)) << value;
        ASSERT_EQ(k, key);
        ASSERT_GE(w, 0);
        ASSERT_LT(w, kNumThreads);
        ASSERT_LE((unsigned int)c, reinterpret_cast<uintptr_t>(
            t->state->counter[w].Acquire_Load()));
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
  } while (ChangeOptions());
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

  } while (ChangeOptions());
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

  explicit ModelDB(const Options& options): options_(options) { }
  virtual Status Put(const WriteOptions& o, const Slice& k, const Slice& v) {
    return DB::Put(o, k, v);
  }
  virtual Status Merge(const WriteOptions& o, const Slice& k, const Slice& v) {
    return DB::Merge(o, k, v);
  }
  virtual Status Delete(const WriteOptions& o, const Slice& key) {
    return DB::Delete(o, key);
  }
  virtual Status Get(const ReadOptions& options,
                     const Slice& key, std::string* value) {
    return Status::NotSupported(key);
  }

  virtual std::vector<Status> MultiGet(const ReadOptions& options,
                                       const std::vector<Slice>& keys,
                                       std::vector<std::string>* values) {
    std::vector<Status> s(keys.size(),
                          Status::NotSupported("Not implemented."));
    return s;
  }
  virtual Status GetPropertiesOfAllTables(TablePropertiesCollection* props) {
    return Status();
  }
  virtual bool KeyMayExist(const ReadOptions& options,
                           const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) {
    if (value_found != nullptr) {
      *value_found = false;
    }
    return true; // Not Supported directly
  }
  virtual Iterator* NewIterator(const ReadOptions& options) {
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

  virtual bool GetProperty(const Slice& property, std::string* value) {
    return false;
  }
  virtual void GetApproximateSizes(const Range* r, int n, uint64_t* sizes) {
    for (int i = 0; i < n; i++) {
      sizes[i] = 0;
    }
  }
  virtual Status CompactRange(const Slice* start, const Slice* end,
                              bool reduce_level, int target_level) {
    return Status::NotSupported("Not supported operation.");
  }

  virtual int NumberLevels()
  {
  return 1;
  }

  virtual int MaxMemCompactionLevel()
  {
  return 1;
  }

  virtual int Level0StopWriteTrigger()
  {
  return -1;
  }

  virtual const std::string& GetName() const {
    return name_;
  }

  virtual Env* GetEnv() const {
    return nullptr;
  }

  virtual const Options& GetOptions() const {
    return options_;
  }

  virtual Status Flush(const rocksdb::FlushOptions& options) {
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
    virtual void Prev() { --iter_; }
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
          option_config_ == kPlainTableFirstBytePrefix) {
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
        ASSERT_TRUE(CompareIterators(step, &model, db_, nullptr, nullptr));
        ASSERT_TRUE(CompareIterators(step, &model, db_, model_snap, db_snap));
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
    }
    if (model_snap != nullptr) model.ReleaseSnapshot(model_snap);
    if (db_snap != nullptr) db_->ReleaseSnapshot(db_snap);
  } while (ChangeOptions(kSkipDeletesFilterFirst));
}

TEST(DBTest, MultiGetSimple) {
  do {
    ASSERT_OK(db_->Put(WriteOptions(),"k1","v1"));
    ASSERT_OK(db_->Put(WriteOptions(),"k2","v2"));
    ASSERT_OK(db_->Put(WriteOptions(),"k3","v3"));
    ASSERT_OK(db_->Put(WriteOptions(),"k4","v4"));
    ASSERT_OK(db_->Delete(WriteOptions(),"k4"));
    ASSERT_OK(db_->Put(WriteOptions(),"k5","v5"));
    ASSERT_OK(db_->Delete(WriteOptions(),"no_key"));

    std::vector<Slice> keys(6);
    keys[0] = "k1";
    keys[1] = "k2";
    keys[2] = "k3";
    keys[3] = "k4";
    keys[4] = "k5";
    keys[5] = "no_key";

    std::vector<std::string> values(20,"Temporary data to be overwritten");

    std::vector<Status> s =  db_->MultiGet(ReadOptions(),keys,&values);
    ASSERT_EQ(values.size(),keys.size());
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
    // Empty Key Set
    std::vector<Slice> keys;
    std::vector<std::string> values;
    std::vector<Status> s = db_->MultiGet(ReadOptions(),keys,&values);
    ASSERT_EQ((int)s.size(),0);

    // Empty Database, Empty Key Set
    DestroyAndReopen();
    s = db_->MultiGet(ReadOptions(), keys, &values);
    ASSERT_EQ((int)s.size(),0);

    // Empty Database, Search for Keys
    keys.resize(2);
    keys[0] = "a";
    keys[1] = "b";
    s = db_->MultiGet(ReadOptions(),keys,&values);
    ASSERT_EQ((int)s.size(), 2);
    ASSERT_TRUE(s[0].IsNotFound() && s[1].IsNotFound());
  } while (ChangeCompactOptions());
}

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
  dbtest->dbfull()->TEST_FlushMemTable();
  dbtest->dbfull()->CompactRange(nullptr, nullptr); // move to level 1

  // GROUP 1
  for (int i = 1; i <= small_range_sstfiles; i++) {
    snprintf(buf, sizeof(buf), "%02d______:start", i);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    snprintf(buf, sizeof(buf), "%02d______:end", i+1);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    dbtest->dbfull()->TEST_FlushMemTable();
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
    dbtest->dbfull()->TEST_FlushMemTable();
  }
}

TEST(DBTest, PrefixScan) {
  ReadOptions ro = ReadOptions();
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
  options.no_block_cache = true;
  options.filter_policy = NewBloomFilterPolicy(10);
  options.prefix_extractor.reset(NewFixedPrefixTransform(8));
  options.whole_key_filtering = false;
  options.disable_auto_compactions = true;
  options.max_background_compactions = 2;
  options.create_if_missing = true;
  options.disable_seek_compaction = true;
  // Tricky: options.prefix_extractor will be released by
  // NewHashSkipListRepFactory after use.
  options.memtable_factory.reset(NewHashSkipListRepFactory());

  // prefix specified, with blooms: 2 RAND I/Os
  // SeekToFirst
  DestroyAndReopen(&options);
  PrefixScanInit(this);
  count = 0;
  env_->random_read_counter_.Reset();
  ro.prefix = &prefix;
  iter = db_->NewIterator(ro);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    assert(iter->key().starts_with(prefix));
    count++;
  }
  ASSERT_OK(iter->status());
  delete iter;
  ASSERT_EQ(count, 2);
  ASSERT_EQ(env_->random_read_counter_.Read(), 2);

  // prefix specified, with blooms: 2 RAND I/Os
  // Seek
  DestroyAndReopen(&options);
  PrefixScanInit(this);
  count = 0;
  env_->random_read_counter_.Reset();
  ro.prefix = &prefix;
  iter = db_->NewIterator(ro);
  for (iter->Seek(key); iter->Valid(); iter->Next()) {
    assert(iter->key().starts_with(prefix));
    count++;
  }
  ASSERT_OK(iter->status());
  delete iter;
  ASSERT_EQ(count, 2);
  ASSERT_EQ(env_->random_read_counter_.Read(), 2);

  // no prefix specified: 11 RAND I/Os
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
  ASSERT_EQ(env_->random_read_counter_.Read(), 11);
  Close();
  delete options.filter_policy;
}

std::string MakeKey(unsigned int num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%016u", num);
  return std::string(buf);
}

void BM_LogAndApply(int iters, int num_base_files) {
  std::string dbname = test::TmpDir() + "/rocksdb_test_benchmark";
  ASSERT_OK(DestroyDB(dbname, Options()));

  DB* db = nullptr;
  Options opts;
  opts.create_if_missing = true;
  Status s = DB::Open(opts, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;

  Env* env = Env::Default();

  port::Mutex mu;
  MutexLock l(&mu);

  InternalKeyComparator cmp(BytewiseComparator());
  Options options;
  EnvOptions sopt;
  VersionSet vset(dbname, &options, sopt, nullptr, &cmp);
  ASSERT_OK(vset.Recover());
  VersionEdit vbase;
  uint64_t fnum = 1;
  for (int i = 0; i < num_base_files; i++) {
    InternalKey start(MakeKey(2*fnum), 1, kTypeValue);
    InternalKey limit(MakeKey(2*fnum+1), 1, kTypeDeletion);
    vbase.AddFile(2, fnum++, 1 /* file size */, start, limit, 1, 1);
  }
  ASSERT_OK(vset.LogAndApply(&vbase, &mu));

  uint64_t start_micros = env->NowMicros();

  for (int i = 0; i < iters; i++) {
    VersionEdit vedit;
    vedit.DeleteFile(2, fnum);
    InternalKey start(MakeKey(2*fnum), 1, kTypeValue);
    InternalKey limit(MakeKey(2*fnum+1), 1, kTypeDeletion);
    vedit.AddFile(2, fnum++, 1 /* file size */, start, limit, 1, 1);
    vset.LogAndApply(&vedit, &mu);
  }
  uint64_t stop_micros = env->NowMicros();
  unsigned int us = stop_micros - start_micros;
  char buf[16];
  snprintf(buf, sizeof(buf), "%d", num_base_files);
  fprintf(stderr,
          "BM_LogAndApply/%-6s   %8d iters : %9u us (%7.0f us / iter)\n",
          buf, iters, us, ((float)us) / iters);
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
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  std::string value(1024, 'a');

  const int num_records = 10000;
  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%016d", i);

    Slice key(buf, 16);
    ASSERT_OK(db_->Put(WriteOptions(), key, value));

    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST(DBTest, TailingIteratorDeletes) {
  ReadOptions read_options;
  read_options.tailing = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

  // write a single record, read it using the iterator, then delete it
  ASSERT_OK(db_->Put(WriteOptions(), "0test", "test"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0test");
  ASSERT_OK(db_->Delete(WriteOptions(), "0test"));

  // write many more records
  const int num_records = 10000;
  std::string value(1024, 'A');

  for (int i = 0; i < num_records; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "1%015d", i);

    Slice key(buf, 16);
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
  }

  // force a flush to make sure that no records are read from memtable
  dbfull()->TEST_FlushMemTable();

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
  read_options.prefix_seek = true;

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_factory.reset(NewHashSkipListRepFactory());
  DestroyAndReopen(&options);

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  ASSERT_OK(db_->Put(WriteOptions(), "0101", "test"));

  dbfull()->TEST_FlushMemTable();

  ASSERT_OK(db_->Put(WriteOptions(), "0202", "test"));

  // Seek(0102) shouldn't find any records since 0202 has a different prefix
  iter->Seek("0102");
  ASSERT_TRUE(!iter->Valid());

  iter->Seek("0202");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "0202");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
}


}  // namespace rocksdb

int main(int argc, char** argv) {
  if (argc > 1 && std::string(argv[1]) == "--benchmark") {
    rocksdb::BM_LogAndApply(1000, 1);
    rocksdb::BM_LogAndApply(1000, 100);
    rocksdb::BM_LogAndApply(1000, 10000);
    rocksdb::BM_LogAndApply(100, 100000);
    return 0;
  }

  return rocksdb::test::RunAllTests();
}
