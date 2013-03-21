// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <set>

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/storage_options.h"

namespace leveldb {

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

    if (non_writable_.Acquire_Load() != nullptr) {
      return Status::IOError("simulated write error");
    }

    Status s = target()->NewWritableFile(f, r, soptions);
    if (s.ok()) {
      if (strstr(f.c_str(), ".sst") != nullptr) {
        r->reset(new SSTableFile(this, std::move(*r)));
      } else if (strstr(f.c_str(), "MANIFEST") != nullptr) {
        r->reset(new ManifestFile(this, std::move(*r)));
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

  // Sequence of option configurations to try
  enum OptionConfig {
    kDefault,
    kFilter,
    kUncompressed,
    kNumLevel_3,
    kDBLogDir,
    kManifestFileSize,
    kCompactOnFlush,
    kPerfOptions,
    kEnd
  };
  int option_config_;

 public:
  std::string dbname_;
  SpecialEnv* env_;
  DB* db_;

  Options last_options_;

  DBTest() : option_config_(kDefault),
             env_(new SpecialEnv(Env::Default())) {
    filter_policy_ = NewBloomFilterPolicy(10);
    dbname_ = test::TmpDir() + "/db_test";
    DestroyDB(dbname_, Options());
    db_ = nullptr;
    Reopen();
  }

  ~DBTest() {
    delete db_;
    DestroyDB(dbname_, Options());
    delete env_;
    delete filter_policy_;
  }

  // Switch to a fresh database with the next option configuration to
  // test.  Return false if there are no more configurations to test.
  bool ChangeOptions() {
    option_config_++;
    if (option_config_ >= kEnd) {
      return false;
    } else {
      DestroyAndReopen();
      return true;
    }
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    switch (option_config_) {
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
      case kManifestFileSize:
        options.max_manifest_file_size = 50; // 50 bytes
      case kCompactOnFlush:
        options.purge_redundant_kvs_while_flush = !options.purge_redundant_kvs_while_flush;
        break;
      case kPerfOptions:
        options.rate_limit = 2.0;
        options.rate_limit_delay_milliseconds = 2;
        // TODO -- test more options
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
    delete db_;
    db_ = nullptr;
    DestroyDB(dbname_, Options());
    ASSERT_OK(TryReopen(options));
  }

  Status PureReopen(Options* options, DB** db) {
    return DB::Open(*options, dbname_, db);
  }

  Status TryReopen(Options* options) {
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

  Status Put(const std::string& k, const std::string& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  Status Delete(const std::string& k) {
    return db_->Delete(WriteOptions(), k);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
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
        ParsedInternalKey ikey;
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
            case kTypeDeletion:
              result += "DEL";
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
        db_->GetProperty("leveldb.num-files-at-level" + NumberToString(level),
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
    return static_cast<int>(files.size());
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
      dbfull()->TEST_CompactMemTable();
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
    db_->GetProperty("leveldb.sstables", &property);
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
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(iter->Valid());
    return std::move(iter);
  }

  std::string DummyString(size_t len, char c = 'a') {
    return std::string(len, c);
  }
};

TEST(DBTest, Empty) {
  do {
    ASSERT_TRUE(db_ != nullptr);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
  } while (ChangeOptions());
}

TEST(DBTest, ReadWrite) {
  do {
    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_OK(Put("bar", "v2"));
    ASSERT_OK(Put("foo", "v3"));
    ASSERT_EQ("v3", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));
  } while (ChangeOptions());
}

static std::string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key%06d", i);
  return std::string(buf);
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
  Status s = TryReopen(&options);
  ASSERT_EQ(s.IsCorruption(), true);
  ASSERT_EQ(s.ToString(),
            "Corruption: VersionEdit: db already has "
            "more levels than options.num_levels");

  options.num_levels = 10;
  ASSERT_OK(TryReopen(&options));
}

TEST(DBTest, Preallocation) {
  const std::string src = dbname_ + "/alloc_test";
  unique_ptr<WritableFile> srcfile;
  const StorageOptions soptions;
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
    dbfull()->TEST_CompactMemTable();
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
      dbfull()->TEST_CompactMemTable();
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
    dbfull()->TEST_CompactMemTable();
    ASSERT_OK(Put("foo", "v2"));
    dbfull()->TEST_CompactMemTable();
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
    dbfull()->TEST_CompactMemTable();
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
      dbfull()->TEST_CompactMemTable();
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
  } while (ChangeOptions());
}

TEST(DBTest, IterEmpty) {
  Iterator* iter = db_->NewIterator(ReadOptions());

  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("foo");
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;
}

TEST(DBTest, IterSingle) {
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
}

TEST(DBTest, IterMulti) {
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
}

TEST(DBTest, IterSmallAndLargeMix) {
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
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
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
}

TEST(DBTest, CheckLock) {
  DB* localdb;
  Options options = CurrentOptions();
  ASSERT_TRUE(TryReopen(&options).ok());
  ASSERT_TRUE(!(PureReopen(&options, &localdb).ok())); // second open should fail
}

TEST(DBTest, FLUSH) {
  Options options = CurrentOptions();
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v1"));
  // this will now also flush the last 2 writes
  dbfull()->Flush(FlushOptions());
  ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v1"));

  Reopen();
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_EQ("v1", Get("bar"));

  writeOpt.disableWAL = true;
  ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v2"));
  ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v2"));
  dbfull()->Flush(FlushOptions());

  Reopen();
  ASSERT_EQ("v2", Get("bar"));
  ASSERT_EQ("v2", Get("foo"));

  writeOpt.disableWAL = false;
  ASSERT_OK(dbfull()->Put(writeOpt, "bar", "v3"));
  ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v3"));
  dbfull()->Flush(FlushOptions());

  Reopen();
  // 'foo' should be there because its put
  // has WAL enabled.
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v3", Get("bar"));
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
}

TEST(DBTest, ManifestRollOver) {
  Options options = CurrentOptions();
  options.max_manifest_file_size = 10 ;  // 10 bytes
  Reopen(&options);
  {
    ASSERT_OK(Put("manifest_key1", std::string(1000, '1')));
    ASSERT_OK(Put("manifest_key2", std::string(1000, '2')));
    ASSERT_OK(Put("manifest_key3", std::string(1000, '3')));
    uint64_t manifest_before_fulsh =
      dbfull()->TEST_Current_Manifest_FileNo();
    dbfull()->Flush(FlushOptions()); // This should trigger LogAndApply.
    uint64_t manifest_after_flush =
      dbfull()->TEST_Current_Manifest_FileNo();
    ASSERT_GT(manifest_after_flush, manifest_before_fulsh);
    Reopen(&options);
    ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(),
              manifest_after_flush);
    // check if a new manifest file got inserted or not.
    ASSERT_EQ(std::string(1000, '1'), Get("manifest_key1"));
    ASSERT_EQ(std::string(1000, '2'), Get("manifest_key2"));
    ASSERT_EQ(std::string(1000, '3'), Get("manifest_key3"));
  }
}


TEST(DBTest, RecoverWithLargeLog) {
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
    dbfull()->TEST_WaitForCompactMemTable();
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
    self->dbfull()->TEST_WaitForCompactMemTable();
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
}

// This is a static filter used for filtering
// kvs during the compaction process.
static int cfilter_count;
static std::string NEW_VALUE = "NewValue";
static bool keep_filter(void* arg, int level, const Slice& key,
  const Slice& value, Slice** new_value) {
  assert(arg == nullptr);
  cfilter_count++;
  return false;
}
static bool delete_filter(void*argv, int level, const Slice& key,
  const Slice& value, Slice** new_value) {
  assert(argv == nullptr);
  cfilter_count++;
  return true;
}
static bool change_filter(void*argv, int level, const Slice& key,
  const Slice& value, Slice** new_value) {
  assert(argv == (void*)100);
  assert(new_value != nullptr);
  *new_value = new Slice(NEW_VALUE);
  return false;
}

TEST(DBTest, CompactionFilter) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.CompactionFilter = keep_filter;
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
  dbfull()->TEST_CompactMemTable();

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
  int count = 0;
  int total = 0;
  Iterator* iter = dbfull()->TEST_NewInternalIterator();
  iter->SeekToFirst();
  ASSERT_EQ(iter->status().ok(), true);
  while (iter->Valid()) {
    ParsedInternalKey ikey;
    ikey.sequence = -1;
    ASSERT_EQ(ParseInternalKey(iter->key(), &ikey), true);
    total++;
    if (ikey.sequence != 0) {
      count++;
    }
    iter->Next();
  }
  ASSERT_EQ(total, 100001);
  ASSERT_EQ(count, 1);
  delete iter;

  // overwrite all the 100K+1 keys once again.
  for (int i = 0; i < 100001; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    Put(key, value);
  }
  dbfull()->TEST_CompactMemTable();

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
  options.CompactionFilter = delete_filter;
  options.create_if_missing = true;
  DestroyAndReopen(&options);

  // write all the keys once again.
  for (int i = 0; i < 100001; i++) {
    char key[100];
    snprintf(key, sizeof(key), "B%010d", i);
    Put(key, value);
  }
  dbfull()->TEST_CompactMemTable();
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

  // Scan the entire database to ensure that only the
  // 100001th key is left in the db. The 100001th key
  // is part of the default-most-current snapshot and
  // cannot be deleted.
  iter = db_->NewIterator(ReadOptions());
  iter->SeekToFirst();
  count = 0;
  while (iter->Valid()) {
    count++;
    iter->Next();
  }
  ASSERT_EQ(count, 1);
  delete iter;

  // The sequence number of the remaining record
  // is not zeroed out even though it is at the
  // level Lmax because this record is at the tip
  count = 0;
  iter = dbfull()->TEST_NewInternalIterator();
  iter->SeekToFirst();
  ASSERT_EQ(iter->status().ok(), true);
  while (iter->Valid()) {
    ParsedInternalKey ikey;
    ASSERT_EQ(ParseInternalKey(iter->key(), &ikey), true);
    ASSERT_NE(ikey.sequence, (unsigned)0);
    count++;
    iter->Next();
  }
  ASSERT_EQ(count, 1);
  delete iter;
}

TEST(DBTest, CompactionFilterWithValueChange) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.compaction_filter_args = (void *)100;
  options.CompactionFilter = change_filter;
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
  dbfull()->TEST_CompactMemTable();
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
  dbfull()->TEST_CompactMemTable();
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
}

TEST(DBTest, SparseMerge) {
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
  dbfull()->TEST_CompactMemTable();
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);

  // Make sparse update
  Put("A",    "va2");
  Put("B100", "bvalue2");
  Put("C",    "vc2");
  dbfull()->TEST_CompactMemTable();

  // Compactions should not cause us to create a situation where
  // a file overlaps too much data at the next level.
  ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20*1048576);
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20*1048576);
  dbfull()->TEST_CompactRange(1, nullptr, nullptr);
  ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20*1048576);
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
  } while (ChangeOptions());
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
  } while (ChangeOptions());
}

TEST(DBTest, IteratorPinsRef) {
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

    ASSERT_OK(dbfull()->TEST_CompactMemTable());
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
  } while (ChangeOptions());
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
    ASSERT_OK(dbfull()->TEST_CompactMemTable());
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
  ASSERT_OK(dbfull()->TEST_CompactMemTable());
  const int last = dbfull()->MaxMemCompactionLevel();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);   // foo => v1 is now in last level

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put("a", "begin");
  Put("z", "end");
  dbfull()->TEST_CompactMemTable();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last-1), 1);

  Delete("foo");
  Put("foo", "v2");
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
  ASSERT_OK(dbfull()->TEST_CompactMemTable());  // Moves to level last-2
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
  ASSERT_OK(dbfull()->TEST_CompactMemTable());
  const int last = dbfull()->MaxMemCompactionLevel();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);   // foo => v1 is now in last level

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put("a", "begin");
  Put("z", "end");
  dbfull()->TEST_CompactMemTable();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last-1), 1);

  Delete("foo");
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
  ASSERT_OK(dbfull()->TEST_CompactMemTable());  // Moves to level last-2
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

    // Fill levels 1 and 2 to disable the pushing of new memtables to levels > 0.
    ASSERT_OK(Put("100", "v100"));
    ASSERT_OK(Put("999", "v999"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_OK(Delete("100"));
    ASSERT_OK(Delete("999"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("0,1,1", FilesPerLevel());

    // Make files spanning the following ranges in level-0:
    //  files[0]  200 .. 900
    //  files[1]  300 .. 500
    // Note that files are sorted by smallest key.
    ASSERT_OK(Put("300", "v300"));
    ASSERT_OK(Put("500", "v500"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_OK(Put("200", "v200"));
    ASSERT_OK(Put("600", "v600"));
    ASSERT_OK(Put("900", "v900"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("2,1,1", FilesPerLevel());

    // Compact away the placeholder files we created initially
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);
    dbfull()->TEST_CompactRange(2, nullptr, nullptr);
    ASSERT_EQ("2", FilesPerLevel());

    // Do a memtable compaction.  Before bug-fix, the compaction would
    // not detect the overlap with level-0 files and would incorrectly place
    // the deletion in a deeper level.
    ASSERT_OK(Delete("600"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("3", FilesPerLevel());
    ASSERT_EQ("NOT_FOUND", Get("600"));
  } while (ChangeOptions());
}

TEST(DBTest, L0_CompactionBug_Issue44_a) {
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
}

TEST(DBTest, L0_CompactionBug_Issue44_b) {
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
}

TEST(DBTest, ComparatorCheck) {
  class NewComparator : public Comparator {
   public:
    virtual const char* Name() const { return "leveldb.NewComparator"; }
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
  NewComparator cmp;
  Options new_options = CurrentOptions();
  new_options.comparator = &cmp;
  Status s = TryReopen(&new_options);
  ASSERT_TRUE(!s.ok());
  ASSERT_TRUE(s.ToString().find("comparator") != std::string::npos)
      << s.ToString();
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
  NumberComparator cmp;
  Options new_options = CurrentOptions();
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
}

TEST(DBTest, ManualCompaction) {
  ASSERT_EQ(dbfull()->MaxMemCompactionLevel(), 2)
      << "Need to update this test to match kMaxMemCompactLevel";

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
}

TEST(DBTest, DBOpen_Options) {
  std::string dbname = test::TmpDir() + "/db_options_test";
  DestroyDB(dbname, Options());

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
  DestroyDB(dbname, Options());
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
  ASSERT_TRUE(strstr(s.ToString().c_str(), "Corruption") != nullptr);
  ASSERT_TRUE(db == nullptr);
}

TEST(DBTest, DestroyDBMetaDatabase) {
  std::string dbname = test::TmpDir() + "/db_meta";
  std::string metadbname = MetaDatabaseName(dbname, 0);
  std::string metametadbname = MetaDatabaseName(metadbname, 0);

  // Destroy previous versions if they exist. Using the long way.
  DestroyDB(metametadbname, Options());
  DestroyDB(metadbname, Options());
  DestroyDB(dbname, Options());

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
  DestroyDB(dbname, Options());

  // Check if deletion worked.
  opts.create_if_missing = false;
  ASSERT_TRUE(!DB::Open(opts, dbname, &db).ok());
  ASSERT_TRUE(!DB::Open(opts, metadbname, &db).ok());
  ASSERT_TRUE(!DB::Open(opts, metametadbname, &db).ok());
}


// Check that number of files does not grow when we are out of space
TEST(DBTest, NoSpace) {
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
}

TEST(DBTest, NonWritableFileSystem)
{
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
    dbfull()->TEST_CompactMemTable();
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

TEST(DBTest, FilesDeletedAfterCompaction) {
  ASSERT_OK(Put("foo", "v2"));
  Compact("a", "z");
  const int num_files = CountLiveFiles();
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("foo", "v2"));
    Compact("a", "z");
  }
  ASSERT_EQ(CountLiveFiles(), num_files);
}

TEST(DBTest, BloomFilter) {
  env_->count_random_reads_ = true;
  Options options = CurrentOptions();
  options.env = env_;
  options.block_cache = NewLRUCache(0);  // Prevent cache hits
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
  dbfull()->TEST_CompactMemTable();

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
}

TEST(DBTest, SnapshotFiles) {
  Options options = CurrentOptions();
  const StorageOptions soptions;
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
    std::string src = dbname_ + "/" + files[i];
    std::string dest = snapdir + "/" + files[i];

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
    unique_ptr<SequentialFile> srcfile;
    ASSERT_OK(env_->NewSequentialFile(src, &srcfile, soptions));
    unique_ptr<WritableFile> destfile;
    ASSERT_OK(env_->NewWritableFile(dest, &destfile, soptions));

    char buffer[4096];
    Slice slice;
    while (size > 0) {
      uint64_t one = std::min(sizeof(buffer), size);
      ASSERT_OK(srcfile->Read(one, &slice, buffer));
      ASSERT_OK(destfile->Append(slice));
      size -= slice.size();
    }
    ASSERT_OK(destfile->Close());
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
  ASSERT_TRUE(stat.ok());

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
}

TEST(DBTest, CompactOnFlush) {
  Options options = CurrentOptions();
  options.purge_redundant_kvs_while_flush = true;
  options.disable_auto_compactions = true;
  Reopen(&options);

  Put("foo", "v1");
  ASSERT_OK(dbfull()->TEST_CompactMemTable());
  ASSERT_EQ(AllEntriesFor("foo"), "[ v1 ]");

  // Write two new keys
  Put("a", "begin");
  Put("z", "end");
  dbfull()->TEST_CompactMemTable();

  // Case1: Delete followed by a put
  Delete("foo");
  Put("foo", "v2");
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");

  // After the current memtable is flushed, the DEL should
  // have been removed
  ASSERT_OK(dbfull()->TEST_CompactMemTable());
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2, v1 ]");

  dbfull()->CompactRange(nullptr, nullptr);
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2 ]");

  // Case 2: Delete followed by another delete
  Delete("foo");
  Delete("foo");
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, DEL, v2 ]");
  ASSERT_OK(dbfull()->TEST_CompactMemTable());
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v2 ]");
  dbfull()->CompactRange(nullptr, nullptr);
  ASSERT_EQ(AllEntriesFor("foo"), "[ ]");

  // Case 3: Put followed by a delete
  Put("foo", "v3");
  Delete("foo");
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v3 ]");
  ASSERT_OK(dbfull()->TEST_CompactMemTable());
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL ]");
  dbfull()->CompactRange(nullptr, nullptr);
  ASSERT_EQ(AllEntriesFor("foo"), "[ ]");

  // Case 4: Put followed by another Put
  Put("foo", "v4");
  Put("foo", "v5");
  ASSERT_EQ(AllEntriesFor("foo"), "[ v5, v4 ]");
  ASSERT_OK(dbfull()->TEST_CompactMemTable());
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
  ASSERT_OK(dbfull()->TEST_CompactMemTable());
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
  ASSERT_OK(dbfull()->TEST_CompactMemTable());
  ASSERT_EQ(AllEntriesFor("foo"), "[ v9 ]");
  db_->ReleaseSnapshot(snapshot1);
}

void ListLogFiles(Env* env,
                  const std::string& path,
                  std::vector<uint64_t>* logFiles) {
  std::vector<std::string> files;
  env->GetChildren(path, &files);
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < files.size(); ++i) {
    if (ParseFileName(files[i], &number, &type)) {
      if (type == kLogFile) {
        logFiles->push_back(number);
      }
    }
  }
}

TEST(DBTest, WALArchival) {
  std::string value(1024, '1');
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.WAL_ttl_seconds = 1000;
  DestroyAndReopen(&options);


  //  TEST : Create DB with a ttl.
  //  Put some keys. Count the log files present in the DB just after insert.
  //  Re-open db. Causes deletion/archival to take place.
  //  Assert that the files moved under "/archive".

  std::string archiveDir = ArchivalDirectory(dbname_);

  for (int i = 0; i < 10; ++i) {

    for (int j = 0; j < 10; ++j) {
      ASSERT_OK(Put(Key(10*i+j), value));
    }

    std::vector<uint64_t> logFiles;
    ListLogFiles(env_, dbname_, &logFiles);

    options.create_if_missing = false;
    Reopen(&options);

    std::vector<uint64_t> logs;
    ListLogFiles(env_, archiveDir, &logs);
    std::set<uint64_t> archivedFiles(logs.begin(), logs.end());

    for (std::vector<uint64_t>::iterator it = logFiles.begin();
         it != logFiles.end();
         ++it) {
      ASSERT_TRUE(archivedFiles.find(*it) != archivedFiles.end());
    }
  }

  // REOPEN database with 0 TTL. all files should have been deleted.
  std::vector<uint64_t> logFiles;
  ListLogFiles(env_, archiveDir, &logFiles);
  ASSERT_TRUE(logFiles.size() > 0);
  options.WAL_ttl_seconds = 1;
  env_->SleepForMicroseconds(2*1000*1000);
  Reopen(&options);

  logFiles.clear();
  ListLogFiles(env_, archiveDir, &logFiles);
  ASSERT_TRUE(logFiles.size() == 0);

}

void ExpectRecords(
  const int expected_no_records,
  std::unique_ptr<TransactionLogIterator>& iter) {
  int i = 0;
  SequenceNumber lastSequence = 0;
  while (iter->Valid()) {
    BatchResult res = iter->GetBatch();
    ASSERT_TRUE(res.sequence > lastSequence);
    ++i;
    lastSequence = res.sequence;
    ASSERT_TRUE(iter->status().ok());
    iter->Next();
  }
  ASSERT_EQ(i, expected_no_records);
}

TEST(DBTest, TransactionLogIterator) {
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
  {
    Put("key4", DummyString(1024));
    Put("key5", DummyString(1024));
    Put("key6", DummyString(1024));
  }
  {
    auto iter = OpenTransactionLogIter(0);
    ExpectRecords(6, iter);
  }
}

TEST(DBTest, TransactionLogIteratorMoveOverZeroFiles) {
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
}
// Disabled currently as does not work with mmaped files.
//
// TEST(DBTest, TransactionLogIteratorStallAtLastRecord) {
//   Options options = OptionsForLogIterTest();
//   DestroyAndReopen(&options);
//   Put("key1", DummyString(1024));
//   auto iter = OpenTransactionLogIter(0);
//   ASSERT_OK(iter->status());
//   ASSERT_TRUE(iter->Valid());
//   iter->Next();
//   ASSERT_TRUE(!iter->Valid());
//   ASSERT_OK(iter->status());
//   Put("key2", DummyString(1024));
//   iter->Next();
//   ASSERT_OK(iter->status());
//   ASSERT_TRUE(iter->Valid());
// }
//
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
    options.block_cache = NewLRUCache(0);  // Prevent cache hits

    Reopen(&options);

    // Write 8MB (2000 values, each 4K)
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    std::vector<std::string> values;
    for (int i = 0; i < 2000; i++) {
      ASSERT_OK(Put(Key(i), value));
    }

    // clear level 0 and 1 if necessary.
    dbfull()->TEST_CompactMemTable();
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
      ASSERT_OK(db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf)));
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
  virtual Status Delete(const WriteOptions& o, const Slice& key) {
    return DB::Delete(o, key);
  }
  virtual Status Get(const ReadOptions& options,
                     const Slice& key, std::string* value) {
    assert(false);      // Not implemented
    return Status::NotFound(key);
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
  virtual void CompactRange(const Slice* start, const Slice* end) {
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

  virtual Status Flush(const leveldb::FlushOptions& options) {
    Status ret;
    return ret;
  }

  virtual Status DisableFileDeletions() {
    return Status::OK();
  }
  virtual Status EnableFileDeletions() {
    return Status::OK();
  }
  virtual Status GetLiveFiles(std::vector<std::string>&, uint64_t* size) {
    return Status::OK();
  }

  virtual SequenceNumber GetLatestSequenceNumber() {
    return 0;
  }
  virtual Status GetUpdatesSince(leveldb::SequenceNumber,
                                 unique_ptr<leveldb::TransactionLogIterator>*) {
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
};

static std::string RandomKey(Random* rnd) {
  int len = (rnd->OneIn(3)
             ? 1                // Short sometimes to encourage collisions
             : (rnd->OneIn(100) ? rnd->Skewed(10) : rnd->Uniform(10)));
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
      if (p < 45) {                               // Put
        k = RandomKey(&rnd);
        v = RandomString(&rnd,
                         rnd.OneIn(20)
                         ? 100 + rnd.Uniform(100)
                         : rnd.Uniform(8));
        ASSERT_OK(model.Put(WriteOptions(), k, v));
        ASSERT_OK(db_->Put(WriteOptions(), k, v));

      } else if (p < 90) {                        // Delete
        k = RandomKey(&rnd);
        ASSERT_OK(model.Delete(WriteOptions(), k));
        ASSERT_OK(db_->Delete(WriteOptions(), k));


      } else {                                    // Multi-element batch
        WriteBatch b;
        const int num = rnd.Uniform(8);
        for (int i = 0; i < num; i++) {
          if (i == 0 || !rnd.OneIn(10)) {
            k = RandomKey(&rnd);
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
  } while (ChangeOptions());
}

std::string MakeKey(unsigned int num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%016u", num);
  return std::string(buf);
}

void BM_LogAndApply(int iters, int num_base_files) {
  std::string dbname = test::TmpDir() + "/leveldb_test_benchmark";
  DestroyDB(dbname, Options());

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
  StorageOptions sopt;
  VersionSet vset(dbname, &options, sopt, nullptr, &cmp);
  ASSERT_OK(vset.Recover());
  VersionEdit vbase(vset.NumberLevels());
  uint64_t fnum = 1;
  for (int i = 0; i < num_base_files; i++) {
    InternalKey start(MakeKey(2*fnum), 1, kTypeValue);
    InternalKey limit(MakeKey(2*fnum+1), 1, kTypeDeletion);
    vbase.AddFile(2, fnum++, 1 /* file size */, start, limit);
  }
  ASSERT_OK(vset.LogAndApply(&vbase, &mu));

  uint64_t start_micros = env->NowMicros();

  for (int i = 0; i < iters; i++) {
    VersionEdit vedit(vset.NumberLevels());
    vedit.DeleteFile(2, fnum);
    InternalKey start(MakeKey(2*fnum), 1, kTypeValue);
    InternalKey limit(MakeKey(2*fnum+1), 1, kTypeDeletion);
    vedit.AddFile(2, fnum++, 1 /* file size */, start, limit);
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

}  // namespace leveldb

int main(int argc, char** argv) {
  if (argc > 1 && std::string(argv[1]) == "--benchmark") {
    leveldb::BM_LogAndApply(1000, 1);
    leveldb::BM_LogAndApply(1000, 100);
    leveldb::BM_LogAndApply(1000, 10000);
    leveldb::BM_LogAndApply(100, 100000);
    return 0;
  }

  return leveldb::test::RunAllTests();
}
