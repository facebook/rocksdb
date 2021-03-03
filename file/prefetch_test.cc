//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

class MockFS;

class MockRandomAccessFile : public FSRandomAccessFileWrapper {
 public:
  MockRandomAccessFile(std::unique_ptr<FSRandomAccessFile>& file,
                       bool support_prefetch, std::atomic_int& prefetch_count)
      : FSRandomAccessFileWrapper(file.get()),
        file_(std::move(file)),
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
  std::unique_ptr<FSRandomAccessFile> file_;
  const bool support_prefetch_;
  std::atomic_int& prefetch_count_;
};

class MockFS : public FileSystemWrapper {
 public:
  explicit MockFS(const std::shared_ptr<FileSystem>& wrapped,
                  bool support_prefetch)
      : FileSystemWrapper(wrapped), support_prefetch_(support_prefetch) {}

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

 private:
  const bool support_prefetch_;
  std::atomic_int prefetch_count_{0};
};

class PrefetchTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  PrefetchTest() : DBTestBase("/prefetch_test", true) {}
};

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

INSTANTIATE_TEST_CASE_P(PrefetchTest, PrefetchTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()));
#endif  // !ROCKSDB_LITE

class PrefetchTest1 : public DBTestBase,
                      public ::testing::WithParamInterface<bool> {
 public:
  PrefetchTest1() : DBTestBase("/prefetch_test1", true) {}
};

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
