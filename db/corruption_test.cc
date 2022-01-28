//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cinttypes>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/log_format.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/meta_blocks.h"
#include "table/mock_table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

static constexpr int kValueSize = 1000;
namespace {
// A wrapper that allows injection of errors.
class ErrorEnv : public EnvWrapper {
 public:
  bool writable_file_error_;
  int num_writable_file_errors_;

  explicit ErrorEnv(Env* _target)
      : EnvWrapper(_target),
        writable_file_error_(false),
        num_writable_file_errors_(0) {}
  const char* Name() const override { return "ErrorEnv"; }

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& soptions) override {
    result->reset();
    if (writable_file_error_) {
      ++num_writable_file_errors_;
      return Status::IOError(fname, "fake error");
    }
    return target()->NewWritableFile(fname, result, soptions);
  }
};
}  // namespace
class CorruptionTest : public testing::Test {
 public:
  std::shared_ptr<Env> env_guard_;
  ErrorEnv* env_;
  std::string dbname_;
  std::shared_ptr<Cache> tiny_cache_;
  Options options_;
  DB* db_;

  CorruptionTest() {
    // If LRU cache shard bit is smaller than 2 (or -1 which will automatically
    // set it to 0), test SequenceNumberRecovery will fail, likely because of a
    // bug in recovery code. Keep it 4 for now to make the test passes.
    tiny_cache_ = NewLRUCache(100, 4);
    Env* base_env = Env::Default();
    EXPECT_OK(
        test::CreateEnvFromSystem(ConfigOptions(), &base_env, &env_guard_));
    EXPECT_NE(base_env, nullptr);
    env_ = new ErrorEnv(base_env);
    options_.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
    options_.env = env_;
    dbname_ = test::PerThreadDBPath(env_, "corruption_test");
    Status s = DestroyDB(dbname_, options_);
    EXPECT_OK(s);

    db_ = nullptr;
    options_.create_if_missing = true;
    BlockBasedTableOptions table_options;
    table_options.block_size_deviation = 0;  // make unit test pass for now
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen();
    options_.create_if_missing = false;
  }

  ~CorruptionTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency({});
    SyncPoint::GetInstance()->ClearAllCallBacks();
    delete db_;
    db_ = nullptr;
    if (getenv("KEEP_DB")) {
      fprintf(stdout, "db is still at %s\n", dbname_.c_str());
    } else {
      Options opts;
      opts.env = env_->target();
      EXPECT_OK(DestroyDB(dbname_, opts));
    }
    delete env_;
  }

  void CloseDb() {
    delete db_;
    db_ = nullptr;
  }

  Status TryReopen(Options* options = nullptr) {
    delete db_;
    db_ = nullptr;
    Options opt = (options ? *options : options_);
    if (opt.env == Options().env) {
      // If env is not overridden, replace it with ErrorEnv.
      // Otherwise, the test already uses a non-default Env.
      opt.env = env_;
    }
    opt.arena_block_size = 4096;
    BlockBasedTableOptions table_options;
    table_options.block_cache = tiny_cache_;
    table_options.block_size_deviation = 0;
    opt.table_factory.reset(NewBlockBasedTableFactory(table_options));
    return DB::Open(opt, dbname_, &db_);
  }

  void Reopen(Options* options = nullptr) {
    ASSERT_OK(TryReopen(options));
  }

  void RepairDB() {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(::ROCKSDB_NAMESPACE::RepairDB(dbname_, options_));
  }

  void Build(int n, int start, int flush_every) {
    std::string key_space, value_space;
    WriteBatch batch;
    for (int i = 0; i < n; i++) {
      if (flush_every != 0 && i != 0 && i % flush_every == 0) {
        DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
        ASSERT_OK(dbi->TEST_FlushMemTable());
      }
      //if ((i % 100) == 0) fprintf(stderr, "@ %d of %d\n", i, n);
      Slice key = Key(i + start, &key_space);
      batch.Clear();
      ASSERT_OK(batch.Put(key, Value(i + start, &value_space)));
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }
  }

  void Build(int n, int flush_every = 0) { Build(n, 0, flush_every); }

  void Check(int min_expected, int max_expected) {
    uint64_t next_expected = 0;
    uint64_t missed = 0;
    int bad_keys = 0;
    int bad_values = 0;
    int correct = 0;
    std::string value_space;
    // Do not verify checksums. If we verify checksums then the
    // db itself will raise errors because data is corrupted.
    // Instead, we want the reads to be successful and this test
    // will detect whether the appropriate corruptions have
    // occurred.
    Iterator* iter = db_->NewIterator(ReadOptions(false, true));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      uint64_t key;
      Slice in(iter->key());
      if (!ConsumeDecimalNumber(&in, &key) ||
          !in.empty() ||
          key < next_expected) {
        bad_keys++;
        continue;
      }
      missed += (key - next_expected);
      next_expected = key + 1;
      if (iter->value() != Value(static_cast<int>(key), &value_space)) {
        bad_values++;
      } else {
        correct++;
      }
    }
    iter->status().PermitUncheckedError();
    delete iter;

    fprintf(stderr,
      "expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%llu\n",
            min_expected, max_expected, correct, bad_keys, bad_values,
            static_cast<unsigned long long>(missed));
    ASSERT_LE(min_expected, correct);
    ASSERT_GE(max_expected, correct);
  }

  void Corrupt(FileType filetype, int offset, int bytes_to_corrupt) {
    // Pick file to corrupt
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    uint64_t number;
    FileType type;
    std::string fname;
    int picked_number = -1;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type == filetype &&
          static_cast<int>(number) > picked_number) {  // Pick latest file
        fname = dbname_ + "/" + filenames[i];
        picked_number = static_cast<int>(number);
      }
    }
    ASSERT_TRUE(!fname.empty()) << filetype;

    ASSERT_OK(test::CorruptFile(env_, fname, offset, bytes_to_corrupt));
  }

  // corrupts exactly one file at level `level`. if no file found at level,
  // asserts
  void CorruptTableFileAtLevel(int level, int offset, int bytes_to_corrupt) {
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    for (const auto& m : metadata) {
      if (m.level == level) {
        ASSERT_OK(test::CorruptFile(env_, dbname_ + "/" + m.name, offset,
                                    bytes_to_corrupt));
        return;
      }
    }
    FAIL() << "no file found at level";
  }


  int Property(const std::string& name) {
    std::string property;
    int result;
    if (db_->GetProperty(name, &property) &&
        sscanf(property.c_str(), "%d", &result) == 1) {
      return result;
    } else {
      return -1;
    }
  }

  // Return the ith key
  Slice Key(int i, std::string* storage) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%016d", i);
    storage->assign(buf, strlen(buf));
    return Slice(*storage);
  }

  // Return the value to associate with the specified key
  Slice Value(int k, std::string* storage) {
    if (k == 0) {
      // Ugh.  Random seed of 0 used to produce no entropy.  This code
      // preserves the implementation that was in place when all of the
      // magic values in this file were picked.
      *storage = std::string(kValueSize, ' ');
    } else {
      Random r(k);
      *storage = r.RandomString(kValueSize);
    }
    return Slice(*storage);
  }
};

TEST_F(CorruptionTest, Recovery) {
  Build(100);
  Check(100, 100);
#ifdef OS_WIN
  // On Wndows OS Disk cache does not behave properly
  // We do not call FlushBuffers on every Flush. If we do not close
  // the log file prior to the corruption we end up with the first
  // block not corrupted but only the second. However, under the debugger
  // things work just fine but never pass when running normally
  // For that reason people may want to run with unbuffered I/O. That option
  // is not available for WAL though.
  CloseDb();
#endif
  Corrupt(kWalFile, 19, 1);  // WriteBatch tag for first record
  Corrupt(kWalFile, log::kBlockSize + 1000, 1);  // Somewhere in second block
  ASSERT_TRUE(!TryReopen().ok());
  options_.paranoid_checks = false;
  Reopen(&options_);

  // The 64 records in the first two log blocks are completely lost.
  Check(36, 36);
}

TEST_F(CorruptionTest, RecoverWriteError) {
  env_->writable_file_error_ = true;
  Status s = TryReopen();
  ASSERT_TRUE(!s.ok());
}

TEST_F(CorruptionTest, NewFileErrorDuringWrite) {
  // Do enough writing to force minor compaction
  env_->writable_file_error_ = true;
  const int num =
      static_cast<int>(3 + (Options().write_buffer_size / kValueSize));
  std::string value_storage;
  Status s;
  bool failed = false;
  for (int i = 0; i < num; i++) {
    WriteBatch batch;
    ASSERT_OK(batch.Put("a", Value(100, &value_storage)));
    s = db_->Write(WriteOptions(), &batch);
    if (!s.ok()) {
      failed = true;
    }
    ASSERT_TRUE(!failed || !s.ok());
  }
  ASSERT_TRUE(!s.ok());
  ASSERT_GE(env_->num_writable_file_errors_, 1);
  env_->writable_file_error_ = false;
  Reopen();
}

TEST_F(CorruptionTest, TableFile) {
  Build(100);
  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_OK(dbi->TEST_CompactRange(1, nullptr, nullptr));

  Corrupt(kTableFile, 100, 1);
  Check(99, 99);
  ASSERT_NOK(dbi->VerifyChecksum());
}

TEST_F(CorruptionTest, VerifyChecksumReadahead) {
  Options options;
  SpecialEnv senv(env_->target());
  options.env = &senv;
  // Disable block cache as we are going to check checksum for
  // the same file twice and measure number of reads.
  BlockBasedTableOptions table_options_no_bc;
  table_options_no_bc.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options_no_bc));

  Reopen(&options);

  Build(10000);
  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_OK(dbi->TEST_CompactRange(1, nullptr, nullptr));

  senv.count_random_reads_ = true;
  senv.random_read_counter_.Reset();
  ASSERT_OK(dbi->VerifyChecksum());

  // Make sure the counter is enabled.
  ASSERT_GT(senv.random_read_counter_.Read(), 0);

  // The SST file is about 10MB. Default readahead size is 256KB.
  // Give a conservative 20 reads for metadata blocks, The number
  // of random reads should be within 10 MB / 256KB + 20 = 60.
  ASSERT_LT(senv.random_read_counter_.Read(), 60);

  senv.random_read_bytes_counter_ = 0;
  ReadOptions ro;
  ro.readahead_size = size_t{32 * 1024};
  ASSERT_OK(dbi->VerifyChecksum(ro));
  // The SST file is about 10MB. We set readahead size to 32KB.
  // Give 0 to 20 reads for metadata blocks, and allow real read
  // to range from 24KB to 48KB. The lower bound would be:
  //   10MB / 48KB + 0 = 213
  // The higher bound is
  //   10MB / 24KB + 20 = 447.
  ASSERT_GE(senv.random_read_counter_.Read(), 213);
  ASSERT_LE(senv.random_read_counter_.Read(), 447);

  // Test readahead shouldn't break mmap mode (where it should be
  // disabled).
  options.allow_mmap_reads = true;
  Reopen(&options);
  dbi = static_cast<DBImpl*>(db_);
  ASSERT_OK(dbi->VerifyChecksum(ro));

  CloseDb();
}

TEST_F(CorruptionTest, TableFileIndexData) {
  Options options;
  // very big, we'll trigger flushes manually
  options.write_buffer_size = 100 * 1024 * 1024;
  Reopen(&options);
  // build 2 tables, flush at 5000
  Build(10000, 5000);
  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
  ASSERT_OK(dbi->TEST_FlushMemTable());

  // corrupt an index block of an entire file
  Corrupt(kTableFile, -2000, 500);
  options.paranoid_checks = false;
  Reopen(&options);
  dbi = static_cast_with_check<DBImpl>(db_);
  // one full file may be readable, since only one was corrupted
  // the other file should be fully non-readable, since index was corrupted
  Check(0, 5000);
  ASSERT_NOK(dbi->VerifyChecksum());

  // In paranoid mode, the db cannot be opened due to the corrupted file.
  ASSERT_TRUE(TryReopen().IsCorruption());
}

TEST_F(CorruptionTest, MissingDescriptor) {
  Build(1000);
  RepairDB();
  Reopen();
  Check(1000, 1000);
}

TEST_F(CorruptionTest, SequenceNumberRecovery) {
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v3"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v4"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v5"));
  RepairDB();
  Reopen();
  std::string v;
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &v));
  ASSERT_EQ("v5", v);
  // Write something.  If sequence number was not recovered properly,
  // it will be hidden by an earlier write.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v6"));
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &v));
  ASSERT_EQ("v6", v);
  Reopen();
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &v));
  ASSERT_EQ("v6", v);
}

TEST_F(CorruptionTest, CorruptedDescriptor) {
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "hello"));
  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr));

  Corrupt(kDescriptorFile, 0, 1000);
  Status s = TryReopen();
  ASSERT_TRUE(!s.ok());

  RepairDB();
  Reopen();
  std::string v;
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &v));
  ASSERT_EQ("hello", v);
}

TEST_F(CorruptionTest, CompactionInputError) {
  Options options;
  options.env = env_;
  Reopen(&options);
  Build(10);
  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_OK(dbi->TEST_CompactRange(1, nullptr, nullptr));
  ASSERT_EQ(1, Property("rocksdb.num-files-at-level2"));

  Corrupt(kTableFile, 100, 1);
  Check(9, 9);
  ASSERT_NOK(dbi->VerifyChecksum());

  // Force compactions by writing lots of values
  Build(10000);
  Check(10000, 10000);
  ASSERT_NOK(dbi->VerifyChecksum());
}

TEST_F(CorruptionTest, CompactionInputErrorParanoid) {
  Options options;
  options.env = env_;
  options.paranoid_checks = true;
  options.write_buffer_size = 131072;
  options.max_write_buffer_number = 2;
  Reopen(&options);
  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);

  // Fill levels >= 1
  for (int level = 1; level < dbi->NumberLevels(); level++) {
    ASSERT_OK(dbi->Put(WriteOptions(), "", "begin"));
    ASSERT_OK(dbi->Put(WriteOptions(), "~", "end"));
    ASSERT_OK(dbi->TEST_FlushMemTable());
    for (int comp_level = 0; comp_level < dbi->NumberLevels() - level;
         ++comp_level) {
      ASSERT_OK(dbi->TEST_CompactRange(comp_level, nullptr, nullptr));
    }
  }

  Reopen(&options);

  dbi = static_cast_with_check<DBImpl>(db_);
  Build(10);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_WaitForCompact());
  ASSERT_EQ(1, Property("rocksdb.num-files-at-level0"));

  CorruptTableFileAtLevel(0, 100, 1);
  Check(9, 9);
  ASSERT_NOK(dbi->VerifyChecksum());

  // Write must eventually fail because of corrupted table
  Status s;
  std::string tmp1, tmp2;
  bool failed = false;
  for (int i = 0; i < 10000; i++) {
    s = db_->Put(WriteOptions(), Key(i, &tmp1), Value(i, &tmp2));
    if (!s.ok()) {
      failed = true;
    }
    // if one write failed, every subsequent write must fail, too
    ASSERT_TRUE(!failed || !s.ok()) << "write did not fail in a corrupted db";
  }
  ASSERT_TRUE(!s.ok()) << "write did not fail in corrupted paranoid db";
}

TEST_F(CorruptionTest, UnrelatedKeys) {
  Build(10);
  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  Corrupt(kTableFile, 100, 1);
  ASSERT_NOK(dbi->VerifyChecksum());

  std::string tmp1, tmp2;
  ASSERT_OK(db_->Put(WriteOptions(), Key(1000, &tmp1), Value(1000, &tmp2)));
  std::string v;
  ASSERT_OK(db_->Get(ReadOptions(), Key(1000, &tmp1), &v));
  ASSERT_EQ(Value(1000, &tmp2).ToString(), v);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(db_->Get(ReadOptions(), Key(1000, &tmp1), &v));
  ASSERT_EQ(Value(1000, &tmp2).ToString(), v);
}

TEST_F(CorruptionTest, RangeDeletionCorrupted) {
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(static_cast<size_t>(1), metadata.size());
  std::string filename = dbname_ + metadata[0].name;

  FileOptions file_opts;
  const auto& fs = options_.env->GetFileSystem();
  std::unique_ptr<RandomAccessFileReader> file_reader;
  ASSERT_OK(RandomAccessFileReader::Create(fs, filename, file_opts,
                                           &file_reader, nullptr));

  uint64_t file_size;
  ASSERT_OK(
      fs->GetFileSize(filename, file_opts.io_options, &file_size, nullptr));

  BlockHandle range_del_handle;
  ASSERT_OK(FindMetaBlockInFile(
      file_reader.get(), file_size, kBlockBasedTableMagicNumber,
      ImmutableOptions(options_), kRangeDelBlockName, &range_del_handle));

  ASSERT_OK(TryReopen());
  ASSERT_OK(test::CorruptFile(env_, filename,
                              static_cast<int>(range_del_handle.offset()), 1));
  ASSERT_TRUE(TryReopen().IsCorruption());
}

TEST_F(CorruptionTest, FileSystemStateCorrupted) {
  for (int iter = 0; iter < 2; ++iter) {
    Options options;
    options.env = env_;
    options.paranoid_checks = true;
    options.create_if_missing = true;
    Reopen(&options);
    Build(10);
    ASSERT_OK(db_->Flush(FlushOptions()));
    DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
    std::vector<LiveFileMetaData> metadata;
    dbi->GetLiveFilesMetaData(&metadata);
    ASSERT_GT(metadata.size(), 0);
    std::string filename = dbname_ + metadata[0].name;

    delete db_;
    db_ = nullptr;

    if (iter == 0) {  // corrupt file size
      std::unique_ptr<WritableFile> file;
      ASSERT_OK(env_->NewWritableFile(filename, &file, EnvOptions()));
      ASSERT_OK(file->Append(Slice("corrupted sst")));
      file.reset();
      Status x = TryReopen(&options);
      ASSERT_TRUE(x.IsCorruption());
    } else {  // delete the file
      ASSERT_OK(env_->DeleteFile(filename));
      Status x = TryReopen(&options);
      ASSERT_TRUE(x.IsCorruption());
    }

    ASSERT_OK(DestroyDB(dbname_, options_));
  }
}

static const auto& corruption_modes = {
    mock::MockTableFactory::kCorruptNone, mock::MockTableFactory::kCorruptKey,
    mock::MockTableFactory::kCorruptValue,
    mock::MockTableFactory::kCorruptReorderKey};

TEST_F(CorruptionTest, ParanoidFileChecksOnFlush) {
  Options options;
  options.env = env_;
  options.check_flush_compaction_key_order = false;
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  Status s;
  for (const auto& mode : corruption_modes) {
    delete db_;
    db_ = nullptr;
    s = DestroyDB(dbname_, options);
    ASSERT_OK(s);
    std::shared_ptr<mock::MockTableFactory> mock =
        std::make_shared<mock::MockTableFactory>();
    options.table_factory = mock;
    mock->SetCorruptionMode(mode);
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report
    Build(10);
    s = db_->Flush(FlushOptions());
    if (mode == mock::MockTableFactory::kCorruptNone) {
      ASSERT_OK(s);
    } else {
      ASSERT_NOK(s);
    }
  }
}

TEST_F(CorruptionTest, ParanoidFileChecksOnCompact) {
  Options options;
  options.env = env_;
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  options.check_flush_compaction_key_order = false;
  Status s;
  for (const auto& mode : corruption_modes) {
    delete db_;
    db_ = nullptr;
    s = DestroyDB(dbname_, options);
    std::shared_ptr<mock::MockTableFactory> mock =
        std::make_shared<mock::MockTableFactory>();
    options.table_factory = mock;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report
    Build(100, 2);
    // ASSERT_OK(db_->Flush(FlushOptions()));
    DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
    ASSERT_OK(dbi->TEST_FlushMemTable());
    mock->SetCorruptionMode(mode);
    s = dbi->TEST_CompactRange(0, nullptr, nullptr, nullptr, true);
    if (mode == mock::MockTableFactory::kCorruptNone) {
      ASSERT_OK(s);
    } else {
      ASSERT_NOK(s);
    }
  }
}

TEST_F(CorruptionTest, ParanoidFileChecksWithDeleteRangeFirst) {
  Options options;
  options.env = env_;
  options.check_flush_compaction_key_order = false;
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  for (bool do_flush : {true, false}) {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string start, end;
    assert(db_ != nullptr);  // suppress false clang-analyze report
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(3, &start), Key(7, &end)));
    auto snap = db_->GetSnapshot();
    ASSERT_NE(snap, nullptr);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(8, &start), Key(9, &end)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(2, &start), Key(5, &end)));
    Build(10);
    if (do_flush) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    } else {
      DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
      ASSERT_OK(dbi->TEST_FlushMemTable());
      ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
    }
    db_->ReleaseSnapshot(snap);
  }
}

TEST_F(CorruptionTest, ParanoidFileChecksWithDeleteRange) {
  Options options;
  options.env = env_;
  options.check_flush_compaction_key_order = false;
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  for (bool do_flush : {true, false}) {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report
    Build(10, 0, 0);
    std::string start, end;
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(5, &start), Key(15, &end)));
    auto snap = db_->GetSnapshot();
    ASSERT_NE(snap, nullptr);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(8, &start), Key(9, &end)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(12, &start), Key(17, &end)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(2, &start), Key(4, &end)));
    Build(10, 10, 0);
    if (do_flush) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    } else {
      DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
      ASSERT_OK(dbi->TEST_FlushMemTable());
      ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
    }
    db_->ReleaseSnapshot(snap);
  }
}

TEST_F(CorruptionTest, ParanoidFileChecksWithDeleteRangeLast) {
  Options options;
  options.env = env_;
  options.check_flush_compaction_key_order = false;
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  for (bool do_flush : {true, false}) {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report
    std::string start, end;
    Build(10);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(3, &start), Key(7, &end)));
    auto snap = db_->GetSnapshot();
    ASSERT_NE(snap, nullptr);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(6, &start), Key(8, &end)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(2, &start), Key(5, &end)));
    if (do_flush) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    } else {
      DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
      ASSERT_OK(dbi->TEST_FlushMemTable());
      ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
    }
    db_->ReleaseSnapshot(snap);
  }
}

TEST_F(CorruptionTest, LogCorruptionErrorsInCompactionIterator) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.allow_data_in_errors = true;
  auto mode = mock::MockTableFactory::kCorruptKey;
  delete db_;
  db_ = nullptr;
  ASSERT_OK(DestroyDB(dbname_, options));

  std::shared_ptr<mock::MockTableFactory> mock =
      std::make_shared<mock::MockTableFactory>();
  mock->SetCorruptionMode(mode);
  options.table_factory = mock;

  ASSERT_OK(DB::Open(options, dbname_, &db_));
  assert(db_ != nullptr);  // suppress false clang-analyze report
  Build(100, 2);

  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  Status s = dbi->TEST_CompactRange(0, nullptr, nullptr, nullptr, true);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(CorruptionTest, CompactionKeyOrderCheck) {
  Options options;
  options.env = env_;
  options.paranoid_file_checks = false;
  options.create_if_missing = true;
  options.check_flush_compaction_key_order = false;
  delete db_;
  db_ = nullptr;
  ASSERT_OK(DestroyDB(dbname_, options));
  std::shared_ptr<mock::MockTableFactory> mock =
      std::make_shared<mock::MockTableFactory>();
  options.table_factory = mock;
  ASSERT_OK(DB::Open(options, dbname_, &db_));
  assert(db_ != nullptr);  // suppress false clang-analyze report
  mock->SetCorruptionMode(mock::MockTableFactory::kCorruptReorderKey);
  Build(100, 2);
  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);
  ASSERT_OK(dbi->TEST_FlushMemTable());

  mock->SetCorruptionMode(mock::MockTableFactory::kCorruptNone);
  ASSERT_OK(db_->SetOptions({{"check_flush_compaction_key_order", "true"}}));
  ASSERT_NOK(dbi->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
}

TEST_F(CorruptionTest, FlushKeyOrderCheck) {
  Options options;
  options.env = env_;
  options.paranoid_file_checks = false;
  options.create_if_missing = true;
  ASSERT_OK(db_->SetOptions({{"check_flush_compaction_key_order", "true"}}));

  ASSERT_OK(db_->Put(WriteOptions(), "foo1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo2", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo3", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo4", "v1"));

  int cnt = 0;
  // Generate some out of order keys from the memtable
  SyncPoint::GetInstance()->SetCallBack(
      "MemTableIterator::Next:0", [&](void* arg) {
        MemTableRep::Iterator* mem_iter =
            static_cast<MemTableRep::Iterator*>(arg);
        if (++cnt == 3) {
          mem_iter->Prev();
          mem_iter->Prev();
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Status s = static_cast_with_check<DBImpl>(db_)->TEST_FlushMemTable();
  ASSERT_NOK(s);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CorruptionTest, DisableKeyOrderCheck) {
  ASSERT_OK(db_->SetOptions({{"check_flush_compaction_key_order", "false"}}));
  DBImpl* dbi = static_cast_with_check<DBImpl>(db_);

  SyncPoint::GetInstance()->SetCallBack(
      "OutputValidator::Add:order_check",
      [&](void* /*arg*/) { ASSERT_TRUE(false); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "foo1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo3", "v1"));
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(db_->Put(WriteOptions(), "foo2", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo4", "v1"));
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CorruptionTest, VerifyWholeTableChecksum) {
  CloseDb();
  Options options;
  options.env = env_;
  ASSERT_OK(DestroyDB(dbname_, options));
  options.create_if_missing = true;
  options.file_checksum_gen_factory =
      ROCKSDB_NAMESPACE::GetFileChecksumGenCrc32cFactory();
  Reopen(&options);

  Build(10, 5);

  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));
  CloseDb();

  // Corrupt the first byte of each table file, this must be data block.
  Corrupt(kTableFile, 0, 1);

  ASSERT_OK(TryReopen(&options));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  int count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::VerifyFullFileChecksum:mismatch", [&](void* arg) {
        auto* s = reinterpret_cast<Status*>(arg);
        ASSERT_NE(s, nullptr);
        ++count;
        ASSERT_NOK(*s);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(db_->VerifyFileChecksums(ReadOptions()).IsCorruption());
  ASSERT_EQ(1, count);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as RepairDB() is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
