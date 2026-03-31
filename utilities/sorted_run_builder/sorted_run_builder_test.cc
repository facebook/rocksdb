//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/sorted_run_builder.h"

#include <algorithm>
#include <string>
#include <thread>
#include <vector>

#include "port/stack_trace.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class SortedRunBuilderTest : public testing::Test {
 protected:
  std::string temp_dir_;
  std::string db_dir_;

  void SetUp() override {
    temp_dir_ = test::PerThreadDBPath("sorted_run_builder_test_tmp");
    db_dir_ = test::PerThreadDBPath("sorted_run_builder_test_db");
    ASSERT_OK(DestroyDB(temp_dir_, Options()));
    ASSERT_OK(DestroyDB(db_dir_, Options()));
  }

  void TearDown() override {
    EXPECT_OK(DestroyDB(temp_dir_, Options()));
    EXPECT_OK(DestroyDB(db_dir_, Options()));
  }

  std::string Key(int i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key%08d", i);
    return std::string(buf);
  }

  std::string Value(int i) {
    char buf[64];
    snprintf(buf, sizeof(buf), "value%08d", i);
    return std::string(buf);
  }
};

TEST_F(SortedRunBuilderTest, BasicSortCorrectness) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;
  opts.write_buffer_size = 4 * 1024;  // small to trigger flushes
  opts.target_file_size_bytes = 4 * 1024;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  // Add keys in reverse order
  const int kNumKeys = 100;
  for (int i = kNumKeys - 1; i >= 0; i--) {
    ASSERT_OK(builder->Add(Key(i), Value(i)));
  }

  ASSERT_OK(builder->Finish());

  // Verify output files exist
  auto files = builder->GetOutputFiles();
  ASSERT_GT(files.size(), 0);

  // Verify sorted iteration
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(builder->NewIterator(ro));
  iter->SeekToFirst();
  int count = 0;
  std::string prev_key;
  while (iter->Valid()) {
    std::string key = iter->key().ToString();
    std::string val = iter->value().ToString();
    ASSERT_EQ(key, Key(count));
    ASSERT_EQ(val, Value(count));
    if (!prev_key.empty()) {
      ASSERT_LT(prev_key, key);
    }
    prev_key = key;
    count++;
    iter->Next();
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, kNumKeys);
}

TEST_F(SortedRunBuilderTest, EmptyBuilder) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  ASSERT_OK(builder->Finish());

  auto files = builder->GetOutputFiles();
  ASSERT_EQ(files.size(), 0);
  ASSERT_EQ(builder->GetNumEntries(), 0);
}

TEST_F(SortedRunBuilderTest, WriteBatchPath) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  // Add via WriteBatch
  WriteBatch batch;
  for (int i = 50; i >= 0; i--) {
    ASSERT_OK(batch.Put(Key(i), Value(i)));
  }
  ASSERT_OK(builder->AddBatch(&batch));

  WriteBatch batch2;
  for (int i = 100; i > 50; i--) {
    ASSERT_OK(batch2.Put(Key(i), Value(i)));
  }
  ASSERT_OK(builder->AddBatch(&batch2));

  ASSERT_OK(builder->Finish());

  // Verify sorted output
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(builder->NewIterator(ro));
  iter->SeekToFirst();
  int count = 0;
  while (iter->Valid()) {
    ASSERT_EQ(iter->key().ToString(), Key(count));
    count++;
    iter->Next();
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 101);
}

TEST_F(SortedRunBuilderTest, ConcurrentWrites) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;
  opts.write_buffer_size = 4 * 1024;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  const int kNumThreads = 4;
  const int kKeysPerThread = 250;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);

  for (int t = 0; t < kNumThreads; t++) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kKeysPerThread; i++) {
        int key_idx = t * kKeysPerThread + i;
        EXPECT_OK(builder->Add(Key(key_idx), Value(key_idx)));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  ASSERT_OK(builder->Finish());

  // Verify all keys present and sorted
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(builder->NewIterator(ro));
  iter->SeekToFirst();
  int count = 0;
  std::string prev_key;
  while (iter->Valid()) {
    std::string key = iter->key().ToString();
    if (!prev_key.empty()) {
      ASSERT_LT(prev_key, key);
    }
    prev_key = key;
    count++;
    iter->Next();
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, kNumThreads * kKeysPerThread);
}

TEST_F(SortedRunBuilderTest, IngestIntoTargetDB) {
  // Sort data using SortedRunBuilder
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;
  opts.write_buffer_size = 4 * 1024;
  opts.target_file_size_bytes = 4 * 1024;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  const int kNumKeys = 200;
  for (int i = kNumKeys - 1; i >= 0; i--) {
    ASSERT_OK(builder->Add(Key(i), Value(i)));
  }

  ASSERT_OK(builder->Finish());
  auto files = builder->GetOutputFiles();
  ASSERT_GT(files.size(), 0);

  // Open a target DB and ingest the sorted files
  Options db_opts;
  db_opts.create_if_missing = true;
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(db_opts, db_dir_, &db));

  IngestExternalFileOptions ingest_opts;
  ingest_opts.allow_db_generated_files = true;
  ingest_opts.snapshot_consistency = false;
  ingest_opts.allow_blocking_flush = false;
  ASSERT_OK(db->IngestExternalFile(files, ingest_opts));

  // Verify data in target DB
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro));
  iter->SeekToFirst();
  int count = 0;
  while (iter->Valid()) {
    ASSERT_EQ(iter->key().ToString(), Key(count));
    ASSERT_EQ(iter->value().ToString(), Value(count));
    count++;
    iter->Next();
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, kNumKeys);
}

TEST_F(SortedRunBuilderTest, LargeRandomDataset) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;
  opts.write_buffer_size = 64 * 1024;
  opts.target_file_size_bytes = 64 * 1024;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  // Generate random keys
  const int kNumKeys = 10000;
  std::vector<std::string> keys;
  keys.reserve(kNumKeys);
  for (int i = 0; i < kNumKeys; i++) {
    keys.push_back(Key(i));
  }

  // Shuffle and insert
  RandomShuffle(keys.begin(), keys.end(), 42);
  for (const auto& key : keys) {
    ASSERT_OK(builder->Add(key, "val_" + key));
  }

  ASSERT_OK(builder->Finish());

  // Sort keys for comparison
  std::sort(keys.begin(), keys.end());

  // Verify output matches sorted order
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(builder->NewIterator(ro));
  iter->SeekToFirst();
  int idx = 0;
  while (iter->Valid()) {
    ASSERT_EQ(iter->key().ToString(), keys[idx]);
    idx++;
    iter->Next();
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(idx, kNumKeys);
}

TEST_F(SortedRunBuilderTest, NumEntriesAndDataSize) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  ASSERT_EQ(builder->GetNumEntries(), 0);
  ASSERT_EQ(builder->GetDataSize(), 0);

  ASSERT_OK(builder->Add("key1", "value1"));
  ASSERT_OK(builder->Add("key2", "value2"));

  // Before Finish(), counts reflect all Add() calls
  ASSERT_EQ(builder->GetNumEntries(), 2);
  ASSERT_GT(builder->GetDataSize(), 0);

  ASSERT_OK(builder->Finish());

  // After Finish(), stats come from SST metadata (exact, post-dedup)
  ASSERT_EQ(builder->GetNumEntries(), 2);
  ASSERT_GT(builder->GetDataSize(), 0);
}

TEST_F(SortedRunBuilderTest, NumEntriesAfterDedup) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  // Add duplicate keys — pre-Finish count includes duplicates
  ASSERT_OK(builder->Add("key1", "old_value"));
  ASSERT_OK(builder->Add("key1", "new_value"));
  ASSERT_OK(builder->Add("key2", "value2"));
  ASSERT_EQ(builder->GetNumEntries(), 3);

  ASSERT_OK(builder->Finish());

  // After Finish(), duplicates are resolved — only 2 unique keys remain
  ASSERT_EQ(builder->GetNumEntries(), 2);
}

TEST_F(SortedRunBuilderTest, ErrorAddAfterFinish) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  ASSERT_OK(builder->Add("key1", "value1"));
  ASSERT_OK(builder->Finish());

  // Add after Finish should fail
  Status s = builder->Add("key2", "value2");
  ASSERT_TRUE(s.IsInvalidArgument());

  // AddBatch after Finish should fail
  WriteBatch batch;
  ASSERT_OK(batch.Put("key3", "value3"));
  s = builder->AddBatch(&batch);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Double Finish should fail
  s = builder->Finish();
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_F(SortedRunBuilderTest, ErrorIteratorBeforeFinish) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  ASSERT_OK(builder->Add("key1", "value1"));

  ReadOptions ro;
  std::unique_ptr<Iterator> iter(builder->NewIterator(ro));
  // Iterator should report error
  ASSERT_TRUE(!iter->Valid());
  ASSERT_TRUE(iter->status().IsInvalidArgument());
}

TEST_F(SortedRunBuilderTest, ErrorEmptyTempDir) {
  SortedRunBuilderOptions opts;
  // temp_dir left empty

  std::unique_ptr<SortedRunBuilder> builder;
  Status s = SortedRunBuilder::Create(opts, &builder);
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_F(SortedRunBuilderTest, CleanupRemovesFiles) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;
  opts.keep_temp_db = true;  // Keep so we can test explicit Cleanup()

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  ASSERT_OK(builder->Add("key1", "value1"));
  ASSERT_OK(builder->Finish());

  auto files = builder->GetOutputFiles();
  ASSERT_GT(files.size(), 0);

  // Cleanup should remove the temp DB
  ASSERT_OK(builder->Cleanup());

  // Verify directory is cleaned up (DestroyDB removes all DB files)
  Env* env = Env::Default();
  std::vector<std::string> children;
  // After DestroyDB, the directory should be empty or not exist
  Status s = env->GetChildren(temp_dir_, &children);
  if (s.ok()) {
    // Filter out . and ..
    int real_files = 0;
    for (const auto& child : children) {
      if (child != "." && child != "..") {
        real_files++;
      }
    }
    ASSERT_EQ(real_files, 0);
  }
}

TEST_F(SortedRunBuilderTest, DuplicateKeys) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  // Add duplicate keys — later value should win (RocksDB semantics)
  ASSERT_OK(builder->Add("key1", "old_value"));
  ASSERT_OK(builder->Add("key1", "new_value"));
  ASSERT_OK(builder->Add("key2", "value2"));

  ASSERT_OK(builder->Finish());

  ReadOptions ro;
  std::unique_ptr<Iterator> iter(builder->NewIterator(ro));
  iter->SeekToFirst();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "key1");
  ASSERT_EQ(iter->value().ToString(), "new_value");

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "key2");
  ASSERT_EQ(iter->value().ToString(), "value2");

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(SortedRunBuilderTest, CustomComparator) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;
  opts.comparator = ReverseBytewiseComparator();

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  ASSERT_OK(builder->Add("apple", "v1"));
  ASSERT_OK(builder->Add("zebra", "v2"));
  ASSERT_OK(builder->Add("mango", "v3"));

  ASSERT_OK(builder->Finish());

  // Reverse comparator: zebra > mango > apple
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(builder->NewIterator(ro));
  iter->SeekToFirst();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "zebra");
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "mango");
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "apple");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(SortedRunBuilderTest, SeekAndPrev) {
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;

  std::unique_ptr<SortedRunBuilder> builder;
  ASSERT_OK(SortedRunBuilder::Create(opts, &builder));

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(builder->Add(Key(i), Value(i)));
  }
  ASSERT_OK(builder->Finish());

  ReadOptions ro;
  std::unique_ptr<Iterator> iter(builder->NewIterator(ro));

  // Seek to middle
  iter->Seek(Key(5));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(5));

  // Prev
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(4));

  // SeekToLast
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(9));
  ASSERT_OK(iter->status());
}

TEST_F(SortedRunBuilderTest, DestructionWithoutFinish) {
  // Verify no crash/leak when builder is destroyed without calling Finish()
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;

  {
    std::unique_ptr<SortedRunBuilder> builder;
    ASSERT_OK(SortedRunBuilder::Create(opts, &builder));
    ASSERT_OK(builder->Add("key1", "value1"));
    // Intentionally not calling Finish() — destructor should clean up
  }

  // Verify temp dir is cleaned up
  Env* env = Env::Default();
  std::vector<std::string> children;
  Status s = env->GetChildren(temp_dir_, &children);
  if (s.ok()) {
    int real_files = 0;
    for (const auto& child : children) {
      if (child != "." && child != "..") {
        real_files++;
      }
    }
    ASSERT_EQ(real_files, 0);
  }
}

TEST_F(SortedRunBuilderTest, InvalidOptions) {
  std::unique_ptr<SortedRunBuilder> builder;

  // max_compaction_threads <= 0
  SortedRunBuilderOptions opts;
  opts.temp_dir = temp_dir_;
  opts.max_compaction_threads = 0;
  ASSERT_TRUE(SortedRunBuilder::Create(opts, &builder).IsInvalidArgument());

  // write_buffer_size == 0
  opts.max_compaction_threads = 4;
  opts.write_buffer_size = 0;
  ASSERT_TRUE(SortedRunBuilder::Create(opts, &builder).IsInvalidArgument());

  // max_write_buffer_number < 2
  opts.write_buffer_size = 64 * 1024 * 1024;
  opts.max_write_buffer_number = 1;
  ASSERT_TRUE(SortedRunBuilder::Create(opts, &builder).IsInvalidArgument());

  // target_file_size_bytes == 0
  opts.max_write_buffer_number = 4;
  opts.target_file_size_bytes = 0;
  ASSERT_TRUE(SortedRunBuilder::Create(opts, &builder).IsInvalidArgument());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
