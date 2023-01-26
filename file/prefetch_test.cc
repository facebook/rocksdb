//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "file/file_prefetch_buffer.h"
#include "file/file_util.h"
#include "rocksdb/file_system.h"
#include "test_util/sync_point.h"
#ifdef GFLAGS
#include "tools/io_tracer_parser_tool.h"
#endif
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class MockFS;

class MockRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 public:
  MockRandomAccessFile(std::unique_ptr<FSRandomAccessFile>& file,
                       bool support_prefetch, std::atomic_int& prefetch_count)
      : FSRandomAccessFileOwnerWrapper(std::move(file)),
        support_prefetch_(support_prefetch),
        prefetch_count_(prefetch_count) {}

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override {
    if (support_prefetch_) {
      prefetch_count_.fetch_add(1);
      return target()->Prefetch(offset, n, options, dbg);
    } else {
      return IOStatus::NotSupported("Prefetch not supported");
    }
  }

 private:
  const bool support_prefetch_;
  std::atomic_int& prefetch_count_;
};

class MockFS : public FileSystemWrapper {
 public:
  explicit MockFS(const std::shared_ptr<FileSystem>& wrapped,
                  bool support_prefetch)
      : FileSystemWrapper(wrapped), support_prefetch_(support_prefetch) {}

  static const char* kClassName() { return "MockFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s;
    s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    result->reset(
        new MockRandomAccessFile(file, support_prefetch_, prefetch_count_));
    return s;
  }

  void ClearPrefetchCount() { prefetch_count_ = 0; }

  bool IsPrefetchCalled() { return prefetch_count_ > 0; }

  int GetPrefetchCount() {
    return prefetch_count_.load(std::memory_order_relaxed);
  }

 private:
  const bool support_prefetch_;
  std::atomic_int prefetch_count_{0};
};

class PrefetchTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  PrefetchTest() : DBTestBase("prefetch_test", true) {}

  void SetGenericOptions(Env* env, bool use_direct_io, Options& options) {
    options = CurrentOptions();
    options.write_buffer_size = 1024;
    options.create_if_missing = true;
    options.compression = kNoCompression;
    options.env = env;
    options.disable_auto_compactions = true;
    if (use_direct_io) {
      options.use_direct_reads = true;
      options.use_direct_io_for_flush_and_compaction = true;
    }
  }

  void SetBlockBasedTableOptions(BlockBasedTableOptions& table_options) {
    table_options.no_block_cache = true;
    table_options.cache_index_and_filter_blocks = false;
    table_options.metadata_block_size = 1024;
    table_options.index_type =
        BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  }
};

INSTANTIATE_TEST_CASE_P(PrefetchTest, PrefetchTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()));

std::string BuildKey(int num, std::string postfix = "") {
  return "my_key_" + std::to_string(num) + postfix;
}

// This test verifies the basic functionality of prefetching.
TEST_P(PrefetchTest, Basic) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);

  const int kNumKeys = 1100;
  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  // create first key range
  WriteBatch batch;
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), "value for range 1 key"));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  // create second key range
  batch.Clear();
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i, "key2"), "value for range 2 key"));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  // delete second key range
  batch.Clear();
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Delete(BuildKey(i, "key2")));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  // compact database
  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  // commenting out the line below causes the example to work correctly
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  if (support_prefetch && !use_direct_io) {
    // If underline file system supports prefetch, and directIO is not enabled
    // make sure prefetch() is called and FilePrefetchBuffer is not used.
    ASSERT_TRUE(fs->IsPrefetchCalled());
    fs->ClearPrefetchCount();
    ASSERT_EQ(0, buff_prefetch_count);
  } else {
    // If underline file system doesn't support prefetch, or directIO is
    // enabled, make sure prefetch() is not called and FilePrefetchBuffer is
    // used.
    ASSERT_FALSE(fs->IsPrefetchCalled());
    ASSERT_GT(buff_prefetch_count, 0);
    buff_prefetch_count = 0;
  }

  // count the keys
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      num_keys++;
    }
  }

  // Make sure prefetch is called only if file system support prefetch.
  if (support_prefetch && !use_direct_io) {
    ASSERT_TRUE(fs->IsPrefetchCalled());
    fs->ClearPrefetchCount();
    ASSERT_EQ(0, buff_prefetch_count);
  } else {
    ASSERT_FALSE(fs->IsPrefetchCalled());
    ASSERT_GT(buff_prefetch_count, 0);
    buff_prefetch_count = 0;
  }
  Close();
}

#ifndef ROCKSDB_LITE
// This test verifies BlockBasedTableOptions.max_auto_readahead_size is
// configured dynamically.
TEST_P(PrefetchTest, ConfigureAutoMaxReadaheadSize) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  table_options.max_auto_readahead_size = 0;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });

  // DB open will create table readers unless we reduce the table cache
  // capacity. SanitizeOptions will set max_open_files to minimum of 20. Table
  // cache is allocated with max_open_files - 10 as capacity. So override
  // max_open_files to 10 so table cache capacity will become 0. This will
  // prevent file open during DB open and force the file to be opened during
  // Iteration.
  SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = (int*)arg;
        *max_open_files = 11;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);

  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  Random rnd(309);
  int key_count = 0;
  const int num_keys_per_level = 100;
  // Level 0 : Keys in range [0, 99], Level 1:[100, 199], Level 2:[200, 299].
  for (int level = 2; level >= 0; level--) {
    key_count = level * num_keys_per_level;
    for (int i = 0; i < num_keys_per_level; ++i) {
      ASSERT_OK(Put(Key(key_count++), rnd.RandomString(500)));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(level);
  }
  Close();
  std::vector<int> buff_prefectch_level_count = {0, 0, 0};
  TryReopen(options);
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    fs->ClearPrefetchCount();
    buff_prefetch_count = 0;

    for (int level = 2; level >= 0; level--) {
      key_count = level * num_keys_per_level;
      switch (level) {
        case 0:
          // max_auto_readahead_size is set 0 so data and index blocks are not
          // prefetched.
          ASSERT_OK(db_->SetOptions(
              {{"block_based_table_factory", "{max_auto_readahead_size=0;}"}}));
          break;
        case 1:
          // max_auto_readahead_size is set less than
          // initial_auto_readahead_size. So readahead_size remains equal to
          // max_auto_readahead_size.
          ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                                      "{max_auto_readahead_size=4096;}"}}));
          break;
        case 2:
          ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                                      "{max_auto_readahead_size=65536;}"}}));
          break;
        default:
          assert(false);
      }

      for (int i = 0; i < num_keys_per_level; ++i) {
        iter->Seek(Key(key_count++));
        iter->Next();
      }

      buff_prefectch_level_count[level] = buff_prefetch_count;
      if (support_prefetch && !use_direct_io) {
        if (level == 0) {
          ASSERT_FALSE(fs->IsPrefetchCalled());
        } else {
          ASSERT_TRUE(fs->IsPrefetchCalled());
        }
        fs->ClearPrefetchCount();
      } else {
        ASSERT_FALSE(fs->IsPrefetchCalled());
        if (level == 0) {
          ASSERT_EQ(buff_prefetch_count, 0);
        } else {
          ASSERT_GT(buff_prefetch_count, 0);
        }
        buff_prefetch_count = 0;
      }
    }
  }

  if (!support_prefetch) {
    ASSERT_GT(buff_prefectch_level_count[1], buff_prefectch_level_count[2]);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// This test verifies BlockBasedTableOptions.initial_auto_readahead_size is
// configured dynamically.
TEST_P(PrefetchTest, ConfigureInternalAutoReadaheadSize) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  table_options.initial_auto_readahead_size = 0;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  // DB open will create table readers unless we reduce the table cache
  // capacity. SanitizeOptions will set max_open_files to minimum of 20.
  // Table cache is allocated with max_open_files - 10 as capacity. So
  // override max_open_files to 10 so table cache capacity will become 0.
  // This will prevent file open during DB open and force the file to be
  // opened during Iteration.
  SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = (int*)arg;
        *max_open_files = 11;
      });

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });

  SyncPoint::GetInstance()->EnableProcessing();

  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);

  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  Random rnd(309);
  int key_count = 0;
  const int num_keys_per_level = 100;
  // Level 0 : Keys in range [0, 99], Level 1:[100, 199], Level 2:[200, 299].
  for (int level = 2; level >= 0; level--) {
    key_count = level * num_keys_per_level;
    for (int i = 0; i < num_keys_per_level; ++i) {
      ASSERT_OK(Put(Key(key_count++), rnd.RandomString(500)));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(level);
  }
  Close();

  TryReopen(options);
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    fs->ClearPrefetchCount();
    buff_prefetch_count = 0;
    std::vector<int> buff_prefetch_level_count = {0, 0, 0};

    for (int level = 2; level >= 0; level--) {
      key_count = level * num_keys_per_level;
      switch (level) {
        case 0:
          // initial_auto_readahead_size is set 0 so data and index blocks are
          // not prefetched.
          ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                                      "{initial_auto_readahead_size=0;}"}}));
          break;
        case 1:
          // intial_auto_readahead_size and max_auto_readahead_size are set same
          // so readahead_size remains same.
          ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                                      "{initial_auto_readahead_size=4096;max_"
                                      "auto_readahead_size=4096;}"}}));
          break;
        case 2:
          ASSERT_OK(
              db_->SetOptions({{"block_based_table_factory",
                                "{initial_auto_readahead_size=65536;}"}}));
          break;
        default:
          assert(false);
      }

      for (int i = 0; i < num_keys_per_level; ++i) {
        iter->Seek(Key(key_count++));
        iter->Next();
      }

      buff_prefetch_level_count[level] = buff_prefetch_count;
      if (support_prefetch && !use_direct_io) {
        if (level == 0) {
          ASSERT_FALSE(fs->IsPrefetchCalled());
        } else {
          ASSERT_TRUE(fs->IsPrefetchCalled());
        }
        fs->ClearPrefetchCount();
      } else {
        ASSERT_FALSE(fs->IsPrefetchCalled());
        if (level == 0) {
          ASSERT_EQ(buff_prefetch_count, 0);
        } else {
          ASSERT_GT(buff_prefetch_count, 0);
        }
        buff_prefetch_count = 0;
      }
    }
    if (!support_prefetch) {
      ASSERT_GT(buff_prefetch_level_count[1], buff_prefetch_level_count[2]);
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// This test verifies BlockBasedTableOptions.num_file_reads_for_auto_readahead
// is configured dynamically.
TEST_P(PrefetchTest, ConfigureNumFilesReadsForReadaheadSize) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  const int kNumKeys = 2000;
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  table_options.num_file_reads_for_auto_readahead = 0;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  Close();
  TryReopen(options);

  fs->ClearPrefetchCount();
  buff_prefetch_count = 0;

  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    /*
     * Reseek keys from sequential Data Blocks within same partitioned
     * index. It will prefetch the data block at the first seek since
     * num_file_reads_for_auto_readahead = 0. Data Block size is nearly 4076 so
     * readahead will fetch 8 * 1024 data more initially (2 more data blocks).
     */
    iter->Seek(BuildKey(0));  // Prefetch data + index block since
                              // num_file_reads_for_auto_readahead = 0.
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1000));  // In buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1004));  // In buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1008));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1011));  // In buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));  // In buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));  // In buffer
    ASSERT_TRUE(iter->Valid());
    // Missed 2 blocks but they are already in buffer so no reset.
    iter->Seek(BuildKey(103));  // Already in buffer.
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1033));  // Prefetch Data.
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 4);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 4);
      buff_prefetch_count = 0;
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}
#endif  // !ROCKSDB_LITE

// This test verifies the basic functionality of implicit autoreadahead:
// - Enable implicit autoreadahead and prefetch only if sequential blocks are
//   read,
// - If data is already in buffer and few blocks are not requested to read,
//   don't reset,
// - If data blocks are sequential during read after enabling implicit
//   autoreadahead, reset readahead parameters.
TEST_P(PrefetchTest, PrefetchWhenReseek) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  const int kNumKeys = 2000;
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  fs->ClearPrefetchCount();
  buff_prefetch_count = 0;

  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    /*
     * Reseek keys from sequential Data Blocks within same partitioned
     * index. After 2 sequential reads it will prefetch the data block.
     * Data Block size is nearly 4076 so readahead will fetch 8 * 1024 data more
     * initially (2 more data blocks).
     */
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1000));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1004));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1008));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    // Missed 2 blocks but they are already in buffer so no reset.
    iter->Seek(BuildKey(103));  // Already in buffer.
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1033));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 3);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 3);
      buff_prefetch_count = 0;
    }
  }
  {
    /*
     * Reseek keys from  non sequential data blocks within same partitioned
     * index. buff_prefetch_count will be 0 in that case.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1008));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1033));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1048));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 0);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 0);
      buff_prefetch_count = 0;
    }
  }
  {
    /*
     * Reesek keys from Single Data Block.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(10));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(100));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 0);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 0);
      buff_prefetch_count = 0;
    }
  }
  {
    /*
     * Reseek keys from  sequential data blocks to set implicit auto readahead
     * and prefetch data but after that iterate over different (non sequential)
     * data blocks which won't prefetch any data further. So buff_prefetch_count
     * will be 1 for the first one.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1000));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1004));  // This iteration will prefetch buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1008));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(
        BuildKey(996));  // Reseek won't prefetch any data and
                         // readahead_size will be initiallized to 8*1024.
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(992));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(989));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 1);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 1);
      buff_prefetch_count = 0;
    }

    // Read sequentially to confirm readahead_size is reset to initial value (2
    // more data blocks)
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1022));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1026));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(103));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 2);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 2);
      buff_prefetch_count = 0;
    }
  }
  {
    /* Reseek keys from sequential partitioned index block. Since partitioned
     * index fetch are sequential, buff_prefetch_count will be 1.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1167));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1334));  // This iteration will prefetch buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1499));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1667));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1847));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1999));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 1);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 1);
      buff_prefetch_count = 0;
    }
  }
  {
    /*
     * Reseek over different keys from different blocks. buff_prefetch_count is
     * set 0.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    int i = 0;
    int j = 1000;
    do {
      iter->Seek(BuildKey(i));
      if (!iter->Valid()) {
        break;
      }
      i = i + 100;
      iter->Seek(BuildKey(j));
      j = j + 100;
    } while (i < 1000 && j < kNumKeys && iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 0);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 0);
      buff_prefetch_count = 0;
    }
  }
  {
    /* Iterates sequentially over all keys. It will prefetch the buffer.*/
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    }
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 13);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 13);
      buff_prefetch_count = 0;
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// This test verifies the functionality of implicit autoreadahead when caching
// is enabled:
// - If data is already in buffer and few blocks are not requested to read,
//   don't reset,
// - If block was eligible for prefetching/in buffer but found in cache, don't
// prefetch and reset.
TEST_P(PrefetchTest, PrefetchWhenReseekwithCache) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  const int kNumKeys = 2000;
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);  // 8MB
  table_options.block_cache = cache;
  table_options.no_block_cache = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  fs->ClearPrefetchCount();
  buff_prefetch_count = 0;

  {
    /*
     * Reseek keys from sequential Data Blocks within same partitioned
     * index. After 2 sequential reads it will prefetch the data block.
     * Data Block size is nearly 4076 so readahead will fetch 8 * 1024 data more
     * initially (2 more data blocks).
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    // Warm up the cache
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 1);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 1);
      buff_prefetch_count = 0;
    }
  }
  {
    // After caching, blocks will be read from cache (Sequential blocks)
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1000));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1004));  // Prefetch data (not in cache).
    ASSERT_TRUE(iter->Valid());
    // Missed one sequential block but next is in already in buffer so readahead
    // will not be reset.
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    // Prefetch data but blocks are in cache so no prefetch and reset.
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1022));
    ASSERT_TRUE(iter->Valid());
    // Prefetch data with readahead_size = 4 blocks.
    iter->Seek(BuildKey(1026));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(103));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1033));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1037));
    ASSERT_TRUE(iter->Valid());

    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 3);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 2);
      buff_prefetch_count = 0;
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

#ifndef ROCKSDB_LITE
// This test verifies the functionality of ReadOptions.adaptive_readahead.
TEST_P(PrefetchTest, DBIterLevelReadAhead) {
  const int kNumKeys = 1000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  bool is_adaptive_readahead = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  int total_keys = 0;
  for (int j = 0; j < 5; j++) {
    for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
      ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
      total_keys++;
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);
  int buff_prefetch_count = 0;
  int readahead_carry_over_count = 0;
  int num_sst_files = NumTableFilesAtLevel(2);
  size_t current_readahead_size = 0;

  // Test - Iterate over the keys sequentially.
  {
    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::Prefetch:Start",
        [&](void*) { buff_prefetch_count++; });

    // The callback checks, since reads are sequential, readahead_size doesn't
    // start from 8KB when iterator moves to next file and its called
    // num_sst_files-1 times (excluding for first file).
    SyncPoint::GetInstance()->SetCallBack(
        "BlockPrefetcher::SetReadaheadState", [&](void* arg) {
          readahead_carry_over_count++;
          size_t readahead_size = *reinterpret_cast<size_t*>(arg);
          if (readahead_carry_over_count) {
            ASSERT_GT(readahead_size, 8 * 1024);
          }
        });

    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::TryReadFromCache", [&](void* arg) {
          current_readahead_size = *reinterpret_cast<size_t*>(arg);
          ASSERT_GT(current_readahead_size, 0);
        });

    SyncPoint::GetInstance()->EnableProcessing();

    ReadOptions ro;
    if (is_adaptive_readahead) {
      ro.adaptive_readahead = true;
    }

    ASSERT_OK(options.statistics->Reset());

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }
    ASSERT_EQ(num_keys, total_keys);

    // For index and data blocks.
    if (is_adaptive_readahead) {
      ASSERT_EQ(readahead_carry_over_count, 2 * (num_sst_files - 1));
    } else {
      ASSERT_GT(buff_prefetch_count, 0);
      ASSERT_EQ(readahead_carry_over_count, 0);
    }

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  Close();
}

// This test verifies the functionality of ReadOptions.adaptive_readahead when
// async_io is enabled.
TEST_P(PrefetchTest, DBIterLevelReadAheadWithAsyncIO) {
  const int kNumKeys = 1000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  bool is_adaptive_readahead = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  int total_keys = 0;
  for (int j = 0; j < 5; j++) {
    for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
      ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
      total_keys++;
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);
  int buff_async_prefetch_count = 0;
  int readahead_carry_over_count = 0;
  int num_sst_files = NumTableFilesAtLevel(2);
  size_t current_readahead_size = 0;

  // Test - Iterate over the keys sequentially.
  {
    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::PrefetchAsyncInternal:Start",
        [&](void*) { buff_async_prefetch_count++; });

    // The callback checks, since reads are sequential, readahead_size doesn't
    // start from 8KB when iterator moves to next file and its called
    // num_sst_files-1 times (excluding for first file).
    SyncPoint::GetInstance()->SetCallBack(
        "BlockPrefetcher::SetReadaheadState", [&](void* arg) {
          readahead_carry_over_count++;
          size_t readahead_size = *reinterpret_cast<size_t*>(arg);
          if (readahead_carry_over_count) {
            ASSERT_GT(readahead_size, 8 * 1024);
          }
        });

    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::TryReadFromCache", [&](void* arg) {
          current_readahead_size = *reinterpret_cast<size_t*>(arg);
          ASSERT_GT(current_readahead_size, 0);
        });

    SyncPoint::GetInstance()->EnableProcessing();

    ReadOptions ro;
    if (is_adaptive_readahead) {
      ro.adaptive_readahead = true;
    }
    ro.async_io = true;

    ASSERT_OK(options.statistics->Reset());

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }
    ASSERT_EQ(num_keys, total_keys);

    // For index and data blocks.
    if (is_adaptive_readahead) {
      ASSERT_EQ(readahead_carry_over_count, 2 * (num_sst_files - 1));
    } else {
      ASSERT_EQ(readahead_carry_over_count, 0);
    }
    ASSERT_GT(buff_async_prefetch_count, 0);

    // Check stats to make sure async prefetch is done.
    {
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      if (ro.async_io) {
        ASSERT_GT(async_read_bytes.count, 0);
      } else {
        ASSERT_EQ(async_read_bytes.count, 0);
      }
    }

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  Close();
}
#endif  //! ROCKSDB_LITE

class PrefetchTest1 : public DBTestBase,
                      public ::testing::WithParamInterface<bool> {
 public:
  PrefetchTest1() : DBTestBase("prefetch_test1", true) {}

  void SetGenericOptions(Env* env, bool use_direct_io, Options& options) {
    options = CurrentOptions();
    options.write_buffer_size = 1024;
    options.create_if_missing = true;
    options.compression = kNoCompression;
    options.env = env;
    options.disable_auto_compactions = true;
    if (use_direct_io) {
      options.use_direct_reads = true;
      options.use_direct_io_for_flush_and_compaction = true;
    }
  }

  void SetBlockBasedTableOptions(BlockBasedTableOptions& table_options) {
    table_options.no_block_cache = true;
    table_options.cache_index_and_filter_blocks = false;
    table_options.metadata_block_size = 1024;
    table_options.index_type =
        BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  }
};

INSTANTIATE_TEST_CASE_P(PrefetchTest1, PrefetchTest1, ::testing::Bool());

#ifndef ROCKSDB_LITE
// This test verifies the functionality of ReadOptions.adaptive_readahead when
// reads are not sequential.
TEST_P(PrefetchTest1, NonSequentialReadsWithAdaptiveReadahead) {
  const int kNumKeys = 1000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options;
  SetGenericOptions(env.get(), GetParam(), options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (GetParam() && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int j = 0; j < 5; j++) {
    for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
      ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);

  int buff_prefetch_count = 0;
  int set_readahead = 0;
  size_t readahead_size = 0;

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->SetCallBack(
      "BlockPrefetcher::SetReadaheadState",
      [&](void* /*arg*/) { set_readahead++; });
  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::TryReadFromCache",
      [&](void* arg) { readahead_size = *reinterpret_cast<size_t*>(arg); });

  SyncPoint::GetInstance()->EnableProcessing();

  {
    // Iterate until prefetch is done.
    ReadOptions ro;
    ro.adaptive_readahead = true;
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());

    while (iter->Valid() && buff_prefetch_count == 0) {
      iter->Next();
    }

    ASSERT_EQ(readahead_size, 8 * 1024);
    ASSERT_EQ(buff_prefetch_count, 1);
    ASSERT_EQ(set_readahead, 0);
    buff_prefetch_count = 0;

    // Move to last file and check readahead size fallbacks to 8KB. So next
    // readahead size after prefetch should be 8 * 1024;
    iter->Seek(BuildKey(4004));
    ASSERT_TRUE(iter->Valid());

    while (iter->Valid() && buff_prefetch_count == 0) {
      iter->Next();
    }

    ASSERT_EQ(readahead_size, 8 * 1024);
    ASSERT_EQ(set_readahead, 0);
    ASSERT_EQ(buff_prefetch_count, 1);
  }
  Close();
}
#endif  //! ROCKSDB_LITE

// This test verifies the functionality of adaptive_readaheadsize with cache and
// if block is found in cache, decrease the readahead_size if
// - its enabled internally by RocksDB (implicit_auto_readahead_) and,
// - readahead_size is greater than 0 and,
// - the block would have called prefetch API if not found in cache for
//   which conditions are:
//   - few/no bytes are in buffer and,
//   - block is sequential with the previous read and,
//   - num_file_reads_ + 1 (including this read) >
//   num_file_reads_for_auto_readahead_
TEST_P(PrefetchTest1, DecreaseReadAheadIfInCache) {
  const int kNumKeys = 2000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options;
  SetGenericOptions(env.get(), GetParam(), options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);  // 8MB
  table_options.block_cache = cache;
  table_options.no_block_cache = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (GetParam() && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  int buff_prefetch_count = 0;
  size_t current_readahead_size = 0;
  size_t expected_current_readahead_size = 8 * 1024;
  size_t decrease_readahead_size = 8 * 1024;

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::TryReadFromCache", [&](void* arg) {
        current_readahead_size = *reinterpret_cast<size_t*>(arg);
      });

  SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions ro;
  ro.adaptive_readahead = true;
  {
    /*
     * Reseek keys from sequential Data Blocks within same partitioned
     * index. After 2 sequential reads it will prefetch the data block.
     * Data Block size is nearly 4076 so readahead will fetch 8 * 1024 data
     * more initially (2 more data blocks).
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    // Warm up the cache
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    buff_prefetch_count = 0;
  }

  {
    ASSERT_OK(options.statistics->Reset());
    // After caching, blocks will be read from cache (Sequential blocks)
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    iter->Seek(
        BuildKey(0));  // In cache so it will decrease the readahead_size.
    ASSERT_TRUE(iter->Valid());
    expected_current_readahead_size = std::max(
        decrease_readahead_size,
        (expected_current_readahead_size >= decrease_readahead_size
             ? (expected_current_readahead_size - decrease_readahead_size)
             : 0));

    iter->Seek(BuildKey(1000));  // Won't prefetch the block.
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(current_readahead_size, expected_current_readahead_size);

    iter->Seek(BuildKey(1004));  // Prefetch the block.
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(current_readahead_size, expected_current_readahead_size);
    expected_current_readahead_size *= 2;

    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());

    // Eligible to Prefetch data (not in buffer) but block is in cache so no
    // prefetch will happen and will result in decrease in readahead_size.
    // readahead_size will be 8 * 1024
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    expected_current_readahead_size = std::max(
        decrease_readahead_size,
        (expected_current_readahead_size >= decrease_readahead_size
             ? (expected_current_readahead_size - decrease_readahead_size)
             : 0));

    // 1016 is the same block as 1015. So no change in readahead_size.
    iter->Seek(BuildKey(1016));
    ASSERT_TRUE(iter->Valid());

    // Prefetch data (not in buffer) but found in cache. So decrease
    // readahead_size. Since it will 0 after decrementing so readahead_size will
    // be set to initial value.
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    expected_current_readahead_size = std::max(
        decrease_readahead_size,
        (expected_current_readahead_size >= decrease_readahead_size
             ? (expected_current_readahead_size - decrease_readahead_size)
             : 0));

    // Prefetch next sequential data.
    iter->Seek(BuildKey(1022));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(current_readahead_size, expected_current_readahead_size);
    ASSERT_EQ(buff_prefetch_count, 2);

    buff_prefetch_count = 0;
  }
  Close();
}

// This test verifies the basic functionality of seek parallelization for
// async_io.
TEST_P(PrefetchTest1, SeekParallelizationTest) {
  const int kNumKeys = 2000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options;
  SetGenericOptions(env.get(), GetParam(), options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (GetParam() && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  int buff_prefetch_count = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::PrefetchAsyncInternal:Start",
      [&](void*) { buff_prefetch_count++; });

  SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  {
    ASSERT_OK(options.statistics->Reset());
    // Each block contains around 4 keys.
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    iter->Seek(BuildKey(0));  // Prefetch data because of seek parallelization.
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    // New data block. Since num_file_reads in FilePrefetch after this read is
    // 2, it won't go for prefetching.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    // Prefetch data.
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    ASSERT_EQ(buff_prefetch_count, 2);

    // Check stats to make sure async prefetch is done.
    {
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      ASSERT_GT(async_read_bytes.count, 0);
      ASSERT_GT(get_perf_context()->number_async_seek, 0);
    }

    buff_prefetch_count = 0;
  }
  Close();
}

extern "C" bool RocksDbIOUringEnable() { return true; }

namespace {
#ifndef ROCKSDB_LITE
#ifdef GFLAGS
const int kMaxArgCount = 100;
const size_t kArgBufferSize = 100000;

void RunIOTracerParserTool(std::string trace_file) {
  std::vector<std::string> params = {"./io_tracer_parser",
                                     "-io_trace_file=" + trace_file};

  char arg_buffer[kArgBufferSize];
  char* argv[kMaxArgCount];
  int argc = 0;
  int cursor = 0;
  for (const auto& arg : params) {
    ASSERT_LE(cursor + arg.size() + 1, kArgBufferSize);
    ASSERT_LE(argc + 1, kMaxArgCount);

    snprintf(arg_buffer + cursor, arg.size() + 1, "%s", arg.c_str());

    argv[argc++] = arg_buffer + cursor;
    cursor += static_cast<int>(arg.size()) + 1;
  }
  ASSERT_EQ(0, ROCKSDB_NAMESPACE::io_tracer_parser(argc, argv));
}
#endif  // GFLAGS
#endif  // ROCKSDB_LITE
}  // namespace

// Tests the default implementation of ReadAsync API with PosixFileSystem during
// prefetching.
TEST_P(PrefetchTest, ReadAsyncWithPosixFS) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  const int kNumKeys = 1000;
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  int total_keys = 0;
  // Write the keys.
  {
    WriteBatch batch;
    Random rnd(309);
    for (int j = 0; j < 5; j++) {
      for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
        ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
        total_keys++;
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);
  }

  int buff_prefetch_count = 0;
  bool read_async_called = false;
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  if (std::get<1>(GetParam())) {
    ro.readahead_size = 16 * 1024;
  }

  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::PrefetchAsyncInternal:Start",
      [&](void*) { buff_prefetch_count++; });

  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Read the keys.
  {
    ASSERT_OK(options.statistics->Reset());
    get_perf_context()->Reset();

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }

    ASSERT_EQ(num_keys, total_keys);
    ASSERT_GT(buff_prefetch_count, 0);

    // Check stats to make sure async prefetch is done.
    {
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      HistogramData prefetched_bytes_discarded;
      options.statistics->histogramData(PREFETCHED_BYTES_DISCARDED,
                                        &prefetched_bytes_discarded);

      // Not all platforms support iouring. In that case, ReadAsync in posix
      // won't submit async requests.
      if (read_async_called) {
        ASSERT_GT(async_read_bytes.count, 0);
      } else {
        ASSERT_EQ(async_read_bytes.count, 0);
      }
      ASSERT_GT(prefetched_bytes_discarded.count, 0);
    }
    ASSERT_EQ(get_perf_context()->number_async_seek, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Close();
}

// This test verifies implementation of seek parallelization with
// PosixFileSystem during prefetching.
TEST_P(PrefetchTest, MultipleSeekWithPosixFS) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  const int kNumKeys = 1000;
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  int total_keys = 0;
  // Write the keys.
  {
    WriteBatch batch;
    Random rnd(309);
    for (int j = 0; j < 5; j++) {
      for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
        ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
        total_keys++;
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);
  }

  int num_keys_first_batch = 0;
  int num_keys_second_batch = 0;
  // Calculate number of keys without async_io for correctness validation.
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    // First Seek.
    iter->Seek(BuildKey(450));
    while (iter->Valid() && num_keys_first_batch < 100) {
      ASSERT_OK(iter->status());
      num_keys_first_batch++;
      iter->Next();
    }
    ASSERT_OK(iter->status());

    iter->Seek(BuildKey(942));
    while (iter->Valid()) {
      ASSERT_OK(iter->status());
      num_keys_second_batch++;
      iter->Next();
    }
    ASSERT_OK(iter->status());
  }

  int buff_prefetch_count = 0;
  bool read_async_called = false;
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  if (std::get<1>(GetParam())) {
    ro.readahead_size = 16 * 1024;
  }

  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::PrefetchAsyncInternal:Start",
      [&](void*) { buff_prefetch_count++; });

  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Read the keys using seek.
  {
    ASSERT_OK(options.statistics->Reset());
    get_perf_context()->Reset();

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    // First Seek.
    {
      iter->Seek(BuildKey(450));
      while (iter->Valid() && num_keys < 100) {
        ASSERT_OK(iter->status());
        num_keys++;
        iter->Next();
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(num_keys, num_keys_first_batch);
      // Check stats to make sure async prefetch is done.
      {
        HistogramData async_read_bytes;
        options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);

        // Not all platforms support iouring. In that case, ReadAsync in posix
        // won't submit async requests.
        if (read_async_called) {
          ASSERT_GT(async_read_bytes.count, 0);
          ASSERT_GT(get_perf_context()->number_async_seek, 0);
        } else {
          ASSERT_EQ(async_read_bytes.count, 0);
          ASSERT_EQ(get_perf_context()->number_async_seek, 0);
        }
      }
    }

    // Second Seek.
    {
      num_keys = 0;
      ASSERT_OK(options.statistics->Reset());
      get_perf_context()->Reset();

      iter->Seek(BuildKey(942));
      while (iter->Valid()) {
        ASSERT_OK(iter->status());
        num_keys++;
        iter->Next();
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(num_keys, num_keys_second_batch);

      ASSERT_GT(buff_prefetch_count, 0);

      // Check stats to make sure async prefetch is done.
      {
        HistogramData async_read_bytes;
        options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
        HistogramData prefetched_bytes_discarded;
        options.statistics->histogramData(PREFETCHED_BYTES_DISCARDED,
                                          &prefetched_bytes_discarded);

        // Not all platforms support iouring. In that case, ReadAsync in posix
        // won't submit async requests.
        if (read_async_called) {
          ASSERT_GT(async_read_bytes.count, 0);
          ASSERT_GT(get_perf_context()->number_async_seek, 0);
        } else {
          ASSERT_EQ(async_read_bytes.count, 0);
          ASSERT_EQ(get_perf_context()->number_async_seek, 0);
        }
        ASSERT_GT(prefetched_bytes_discarded.count, 0);
      }
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// This test verifies implementation of seek parallelization with
// PosixFileSystem during prefetching.
TEST_P(PrefetchTest, SeekParallelizationTestWithPosix) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  const int kNumKeys = 2000;
  // Set options
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  int buff_prefetch_count = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::PrefetchAsyncInternal:Start",
      [&](void*) { buff_prefetch_count++; });

  bool read_async_called = false;
  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  if (std::get<1>(GetParam())) {
    ro.readahead_size = 16 * 1024;
  }

  {
    ASSERT_OK(options.statistics->Reset());
    // Each block contains around 4 keys.
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    iter->Seek(BuildKey(0));  // Prefetch data because of seek parallelization.
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    // New data block. Since num_file_reads in FilePrefetch after this read is
    // 2, it won't go for prefetching.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    // Prefetch data.
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    // Check stats to make sure async prefetch is done.
    {
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      // Not all platforms support iouring. In that case, ReadAsync in posix
      // won't submit async requests.
      if (read_async_called) {
        ASSERT_GT(async_read_bytes.count, 0);
        ASSERT_GT(get_perf_context()->number_async_seek, 0);
        if (std::get<1>(GetParam())) {
          ASSERT_EQ(buff_prefetch_count, 1);
        } else {
          ASSERT_EQ(buff_prefetch_count, 2);
        }
      } else {
        ASSERT_EQ(async_read_bytes.count, 0);
        ASSERT_EQ(get_perf_context()->number_async_seek, 0);
        ASSERT_EQ(buff_prefetch_count, 1);
      }
    }

    buff_prefetch_count = 0;
  }
  Close();
}

#ifndef ROCKSDB_LITE
#ifdef GFLAGS
// This test verifies io_tracing with PosixFileSystem during prefetching.
TEST_P(PrefetchTest, TraceReadAsyncWithCallbackWrapper) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  const int kNumKeys = 1000;
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  int total_keys = 0;
  // Write the keys.
  {
    WriteBatch batch;
    Random rnd(309);
    for (int j = 0; j < 5; j++) {
      for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
        ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
        total_keys++;
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);
  }

  int buff_prefetch_count = 0;
  bool read_async_called = false;
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  if (std::get<1>(GetParam())) {
    ro.readahead_size = 16 * 1024;
  }

  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::PrefetchAsyncInternal:Start",
      [&](void*) { buff_prefetch_count++; });

  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Read the keys.
  {
    // Start io_tracing.
    WriteOptions write_opt;
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    std::string trace_file_path = dbname_ + "/io_trace_file";

    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_file_path, &trace_writer));
    ASSERT_OK(db_->StartIOTrace(trace_opt, std::move(trace_writer)));
    ASSERT_OK(options.statistics->Reset());

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }

    // End the tracing.
    ASSERT_OK(db_->EndIOTrace());
    ASSERT_OK(env_->FileExists(trace_file_path));

    ASSERT_EQ(num_keys, total_keys);
    ASSERT_GT(buff_prefetch_count, 0);

    // Check stats to make sure async prefetch is done.
    {
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      // Not all platforms support iouring. In that case, ReadAsync in posix
      // won't submit async requests.
      if (read_async_called) {
        ASSERT_GT(async_read_bytes.count, 0);
      } else {
        ASSERT_EQ(async_read_bytes.count, 0);
      }
    }

    // Check the file to see if ReadAsync is logged.
    RunIOTracerParserTool(trace_file_path);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Close();
}
#endif  // GFLAGS

class FilePrefetchBufferTest : public testing::Test {
 public:
  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    test_dir_ = test::PerThreadDBPath("file_prefetch_buffer_test");
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));
    stats_ = CreateDBStatistics();
  }

  void TearDown() override { EXPECT_OK(DestroyDir(env_, test_dir_)); }

  void Write(const std::string& fname, const std::string& content) {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(fs_->NewWritableFile(Path(fname), FileOptions(), &f, nullptr));
    ASSERT_OK(f->Append(content, IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  void Read(const std::string& fname, const FileOptions& opts,
            std::unique_ptr<RandomAccessFileReader>* reader) {
    std::string fpath = Path(fname);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(fpath, opts, &f, nullptr));
    reader->reset(new RandomAccessFileReader(
        std::move(f), fpath, env_->GetSystemClock().get(),
        /*io_tracer=*/nullptr, stats_.get()));
  }

  void AssertResult(const std::string& content,
                    const std::vector<FSReadRequest>& reqs) {
    for (const auto& r : reqs) {
      ASSERT_OK(r.status);
      ASSERT_EQ(r.len, r.result.size());
      ASSERT_EQ(content.substr(r.offset, r.len), r.result.ToString());
    }
  }

  FileSystem* fs() { return fs_.get(); }
  Statistics* stats() { return stats_.get(); }

 private:
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string test_dir_;
  std::shared_ptr<Statistics> stats_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }
};

TEST_F(FilePrefetchBufferTest, SeekWithBlockCacheHit) {
  std::string fname = "seek-with-block-cache-hit";
  Random rand(0);
  std::string content = rand.RandomString(32768);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  FilePrefetchBuffer fpb(16384, 16384, true, false, false, 0, 0, fs());
  Slice result;
  // Simulate a seek of 4096 bytes at offset 0. Due to the readahead settings,
  // it will do two reads of 4096+8192 and 8192
  Status s = fpb.PrefetchAsync(IOOptions(), r.get(), 0, 4096, &result);
  // Platforms that don't have IO uring may not support async IO
  ASSERT_TRUE(s.IsTryAgain() || s.IsNotSupported());
  // Simulate a block cache hit
  fpb.UpdateReadPattern(0, 4096, false);
  // Now read some data that straddles the two prefetch buffers - offset 8192 to
  // 16384
  ASSERT_TRUE(fpb.TryReadFromCacheAsync(IOOptions(), r.get(), 8192, 8192,
                                        &result, &s, Env::IOPriority::IO_LOW));
}

TEST_F(FilePrefetchBufferTest, NoSyncWithAsyncIO) {
  std::string fname = "seek-with-block-cache-hit";
  Random rand(0);
  std::string content = rand.RandomString(32768);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  FilePrefetchBuffer fpb(
      /*readahead_size=*/8192, /*max_readahead_size=*/16384, /*enable=*/true,
      /*track_min_offset=*/false, /*implicit_auto_readahead=*/false,
      /*num_file_reads=*/0, /*num_file_reads_for_auto_readahead=*/0, fs());

  int read_async_called = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::ReadAsync",
      [&](void* /*arg*/) { read_async_called++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Slice async_result;
  // Simulate a seek of 4000 bytes at offset 3000. Due to the readahead
  // settings, it will do two reads of 4000+4096 and 4096
  Status s = fpb.PrefetchAsync(IOOptions(), r.get(), 3000, 4000, &async_result);
  // Platforms that don't have IO uring may not support async IO
  ASSERT_TRUE(s.IsTryAgain() || s.IsNotSupported());

  ASSERT_TRUE(fpb.TryReadFromCacheAsync(IOOptions(), r.get(), /*offset=*/3000,
                                        /*length=*/4000, &async_result, &s,
                                        Env::IOPriority::IO_LOW));
  // No sync call should be made.
  HistogramData sst_read_micros;
  stats()->histogramData(SST_READ_MICROS, &sst_read_micros);
  ASSERT_EQ(sst_read_micros.count, 0);

  // Number of async calls should be.
  ASSERT_EQ(read_async_called, 2);
  // Length should be 4000.
  ASSERT_EQ(async_result.size(), 4000);
  // Data correctness.
  Slice result(content.c_str() + 3000, 4000);
  ASSERT_EQ(result.size(), 4000);
  ASSERT_EQ(result, async_result);
}

#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
