//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "test_util/sync_point.h"

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
};

INSTANTIATE_TEST_CASE_P(PrefetchTest, PrefetchTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()));

std::string BuildKey(int num, std::string postfix = "") {
  return "my_key_" + std::to_string(num) + postfix;
}

TEST_P(PrefetchTest, Basic) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());
  const int kNumKeys = 1100;
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options = CurrentOptions();
  options.write_buffer_size = 1024;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.env = env.get();
  if (use_direct_io) {
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
  }

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
TEST_P(PrefetchTest, ConfigureAutoMaxReadaheadSize) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options = CurrentOptions();
  options.write_buffer_size = 1024;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.env = env.get();
  options.disable_auto_compactions = true;
  if (use_direct_io) {
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
  }
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  table_options.cache_index_and_filter_blocks = false;
  table_options.metadata_block_size = 1024;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
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
          // BlockBasedTable::kInitAutoReadaheadSize. So readahead_size remains
          // equal to max_auto_readahead_size.
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
#endif  // !ROCKSDB_LITE

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

  Options options = CurrentOptions();
  options.write_buffer_size = 1024;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.env = env.get();

  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  table_options.cache_index_and_filter_blocks = false;
  table_options.metadata_block_size = 1024;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  if (use_direct_io) {
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
  }

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
    iter->Seek(BuildKey(1000));
    iter->Seek(BuildKey(1004));  // Prefetch Data
    iter->Seek(BuildKey(1008));
    iter->Seek(BuildKey(1011));
    iter->Seek(BuildKey(1015));  // Prefetch Data
    iter->Seek(BuildKey(1019));
    // Missed 2 blocks but they are already in buffer so no reset.
    iter->Seek(BuildKey(103));   // Already in buffer.
    iter->Seek(BuildKey(1033));  // Prefetch Data
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
    iter->Seek(BuildKey(1008));
    iter->Seek(BuildKey(1019));
    iter->Seek(BuildKey(1033));
    iter->Seek(BuildKey(1048));
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
    iter->Seek(BuildKey(1));
    iter->Seek(BuildKey(10));
    iter->Seek(BuildKey(100));
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
    iter->Seek(BuildKey(1000));
    iter->Seek(BuildKey(1004));  // This iteration will prefetch buffer
    iter->Seek(BuildKey(1008));
    iter->Seek(
        BuildKey(996));  // Reseek won't prefetch any data and
                         // readahead_size will be initiallized to 8*1024.
    iter->Seek(BuildKey(992));
    iter->Seek(BuildKey(989));
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
    iter->Seek(BuildKey(1015));
    iter->Seek(BuildKey(1019));  // Prefetch Data
    iter->Seek(BuildKey(1022));
    iter->Seek(BuildKey(1026));
    iter->Seek(BuildKey(103));  // Prefetch Data
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
    iter->Seek(BuildKey(1167));
    iter->Seek(BuildKey(1334));  // This iteration will prefetch buffer
    iter->Seek(BuildKey(1499));
    iter->Seek(BuildKey(1667));
    iter->Seek(BuildKey(1847));
    iter->Seek(BuildKey(1999));
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

  Options options = CurrentOptions();
  options.write_buffer_size = 1024;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.env = env.get();

  BlockBasedTableOptions table_options;
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);  // 8MB
  table_options.block_cache = cache;
  table_options.cache_index_and_filter_blocks = false;
  table_options.metadata_block_size = 1024;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  if (use_direct_io) {
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
  }

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
    iter->Seek(BuildKey(1015));
    iter->Seek(BuildKey(1019));
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
    iter->Seek(BuildKey(1000));
    iter->Seek(BuildKey(1004));  // Prefetch data (not in cache).
    // Missed one sequential block but next is in already in buffer so readahead
    // will not be reset.
    iter->Seek(BuildKey(1011));
    // Prefetch data but blocks are in cache so no prefetch and reset.
    iter->Seek(BuildKey(1015));
    iter->Seek(BuildKey(1019));
    iter->Seek(BuildKey(1022));
    // Prefetch data with readahead_size = 4 blocks.
    iter->Seek(BuildKey(1026));
    iter->Seek(BuildKey(103));
    iter->Seek(BuildKey(1033));
    iter->Seek(BuildKey(1037));

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

class PrefetchTest1
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  PrefetchTest1() : DBTestBase("prefetch_test1", true) {}
};

INSTANTIATE_TEST_CASE_P(PrefetchTest1, PrefetchTest1,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()));

#ifndef ROCKSDB_LITE
TEST_P(PrefetchTest1, DBIterLevelReadAhead) {
  const int kNumKeys = 1000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool is_adaptive_readahead = std::get<1>(GetParam());
  Options options = CurrentOptions();
  options.write_buffer_size = 1024;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.env = env.get();
  if (std::get<0>(GetParam())) {
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
  }
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  table_options.cache_index_and_filter_blocks = false;
  table_options.metadata_block_size = 1024;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (std::get<0>(GetParam()) &&
      (s.IsNotSupported() || s.IsInvalidArgument())) {
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
            // ASSERT_GE(readahead_size, current_readahead_size);
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
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      num_keys++;
    }

    ASSERT_GT(buff_prefetch_count, 0);
    buff_prefetch_count = 0;
    // For index and data blocks.
    if (is_adaptive_readahead) {
      ASSERT_EQ(readahead_carry_over_count, 2 * (num_sst_files - 1));
    } else {
      ASSERT_EQ(readahead_carry_over_count, 0);
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  Close();
}
#endif  //! ROCKSDB_LITE

class PrefetchTest2 : public DBTestBase,
                      public ::testing::WithParamInterface<bool> {
 public:
  PrefetchTest2() : DBTestBase("prefetch_test2", true) {}
};

INSTANTIATE_TEST_CASE_P(PrefetchTest2, PrefetchTest2, ::testing::Bool());

#ifndef ROCKSDB_LITE
TEST_P(PrefetchTest2, NonSequentialReads) {
  const int kNumKeys = 1000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options = CurrentOptions();
  options.write_buffer_size = 1024;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.env = env.get();
  if (GetParam()) {
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
  }
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  table_options.cache_index_and_filter_blocks = false;
  table_options.metadata_block_size = 1024;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
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

TEST_P(PrefetchTest2, DecreaseReadAheadIfInCache) {
  const int kNumKeys = 2000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options = CurrentOptions();
  options.write_buffer_size = 1024;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.env = env.get();
  if (GetParam()) {
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
  }
  BlockBasedTableOptions table_options;
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);  // 8MB
  table_options.block_cache = cache;
  table_options.cache_index_and_filter_blocks = false;
  table_options.metadata_block_size = 1024;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
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
    iter->Seek(BuildKey(1015));
    iter->Seek(BuildKey(1019));
    buff_prefetch_count = 0;
  }
  {
    // After caching, blocks will be read from cache (Sequential blocks)
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    iter->Seek(BuildKey(0));
    iter->Seek(BuildKey(1000));
    iter->Seek(BuildKey(1004));  // Prefetch data (not in cache).
    ASSERT_EQ(current_readahead_size, expected_current_readahead_size);

    // Missed one sequential block but 1011 is already in buffer so
    // readahead will not be reset.
    iter->Seek(BuildKey(1011));
    ASSERT_EQ(current_readahead_size, expected_current_readahead_size);

    // Eligible to Prefetch data (not in buffer) but block is in cache so no
    // prefetch will happen and will result in decrease in readahead_size.
    // readahead_size will be 8 * 1024
    iter->Seek(BuildKey(1015));
    expected_current_readahead_size -= decrease_readahead_size;

    // 1016 is the same block as 1015. So no change in readahead_size.
    iter->Seek(BuildKey(1016));

    // Prefetch data (not in buffer) but found in cache. So decrease
    // readahead_size. Since it will 0 after decrementing so readahead_size will
    // be set to initial value.
    iter->Seek(BuildKey(1019));
    expected_current_readahead_size = std::max(
        decrease_readahead_size,
        (expected_current_readahead_size >= decrease_readahead_size
             ? (expected_current_readahead_size - decrease_readahead_size)
             : 0));

    // Prefetch next sequential data.
    iter->Seek(BuildKey(1022));
    ASSERT_EQ(current_readahead_size, expected_current_readahead_size);
    ASSERT_EQ(buff_prefetch_count, 2);
    buff_prefetch_count = 0;
  }
  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
