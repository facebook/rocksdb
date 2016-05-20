//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Introduction of SyncPoint effectively disabled building and running this test
// in Release build.
// which is a pity, it is a good test
#include <fcntl.h>
#include <algorithm>
#include <set>
#include <thread>
#include <unordered_set>
#include <utility>
#ifndef OS_WIN
#include <unistd.h>
#endif
#ifdef OS_SOLARIS
#include <alloca.h>
#endif

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "memtable/hash_linklist_rep.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/experimental.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/thread_status.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "table/block_based_table_factory.h"
#include "table/mock_table.h"
#include "table/plain_table_factory.h"
#include "table/scoped_arena_iterator.h"
#include "util/compression.h"
#include "util/file_reader_writer.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/mock_env.h"
#include "util/mutexlock.h"
#include "util/rate_limiter.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/thread_status_util.h"
#include "util/xfunc.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

class DBTest : public DBTestBase {
 public:
  DBTest() : DBTestBase("/db_test") {}
};

class DBTestWithParam
    : public DBTest,
      public testing::WithParamInterface<std::tuple<uint32_t, bool>> {
 public:
  DBTestWithParam() {
    max_subcompactions_ = std::get<0>(GetParam());
    exclusive_manual_compaction_ = std::get<1>(GetParam());
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  uint32_t max_subcompactions_;
  bool exclusive_manual_compaction_;
};

TEST_F(DBTest, MockEnvTest) {
  unique_ptr<MockEnv> env{new MockEnv(Env::Default())};
  Options options;
  options.create_if_missing = true;
  options.env = env.get();
  DB* db;

  const Slice keys[] = {Slice("aaa"), Slice("bbb"), Slice("ccc")};
  const Slice vals[] = {Slice("foo"), Slice("bar"), Slice("baz")};

  ASSERT_OK(DB::Open(options, "/dir/db", &db));
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), keys[i], vals[i]));
  }

  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }

  Iterator* iterator = db->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;

// TEST_FlushMemTable() is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
  DBImpl* dbi = reinterpret_cast<DBImpl*>(db);
  ASSERT_OK(dbi->TEST_FlushMemTable());

  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }
#endif  // ROCKSDB_LITE

  delete db;
}

// NewMemEnv returns nullptr in ROCKSDB_LITE since class InMemoryEnv isn't
// defined.
#ifndef ROCKSDB_LITE
TEST_F(DBTest, MemEnvTest) {
  unique_ptr<Env> env{NewMemEnv(Env::Default())};
  Options options;
  options.create_if_missing = true;
  options.env = env.get();
  DB* db;

  const Slice keys[] = {Slice("aaa"), Slice("bbb"), Slice("ccc")};
  const Slice vals[] = {Slice("foo"), Slice("bar"), Slice("baz")};

  ASSERT_OK(DB::Open(options, "/dir/db", &db));
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), keys[i], vals[i]));
  }

  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }

  Iterator* iterator = db->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  delete iterator;

  DBImpl* dbi = reinterpret_cast<DBImpl*>(db);
  ASSERT_OK(dbi->TEST_FlushMemTable());

  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }

  delete db;

  options.create_if_missing = false;
  ASSERT_OK(DB::Open(options, "/dir/db", &db));
  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }
  delete db;
}
#endif  // ROCKSDB_LITE

TEST_F(DBTest, WriteEmptyBatch) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  WriteOptions wo;
  wo.sync = true;
  wo.disableWAL = false;
  WriteBatch empty_batch;
  ASSERT_OK(dbfull()->Write(wo, &empty_batch));

  // make sure we can re-open it.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
  ASSERT_EQ("bar", Get(1, "foo"));
}

#ifndef ROCKSDB_LITE
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
  ASSERT_TRUE(db_->SyncWAL().IsNotSupported());
}

TEST_F(DBTest, CompactedDB) {
  const uint64_t kFileSize = 1 << 20;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = kFileSize;
  options.target_file_size_base = kFileSize;
  options.max_bytes_for_level_base = 1 << 30;
  options.compression = kNoCompression;
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
  ASSERT_OK(Put("eee", DummyString(kFileSize / 2, 'e')));
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
  ASSERT_OK(Put("fff", DummyString(kFileSize / 2, 'f')));
  ASSERT_OK(Put("hhh", DummyString(kFileSize / 2, 'h')));
  ASSERT_OK(Put("iii", DummyString(kFileSize / 2, 'i')));
  ASSERT_OK(Put("jjj", DummyString(kFileSize / 2, 'j')));
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
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
  std::vector<Status> status_list = dbfull()->MultiGet(
      ReadOptions(),
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

  Reopen(options);
  // Add a key
  ASSERT_OK(Put("fff", DummyString(kFileSize / 2, 'f')));
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
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
#endif  // ROCKSDB_LITE

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

TEST_F(DBTest, PutSingleDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo2", "v2"));
    ASSERT_EQ("v2", Get(1, "foo2"));
    ASSERT_OK(SingleDelete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
    // Skip HashCuckooRep as it does not support single delete. FIFO and
    // universal compaction do not apply to the test case. Skip MergePut
    // because single delete does not get removed when it encounters a merge.
  } while (ChangeOptions(kSkipHashCuckoo | kSkipFIFOCompaction |
                         kSkipUniversalCompaction | kSkipMergePut));
}

TEST_F(DBTest, ReadFromPersistedTier) {
  do {
    Random rnd(301);
    Options options = CurrentOptions();
    for (int disableWAL = 0; disableWAL <= 1; ++disableWAL) {
      CreateAndReopenWithCF({"pikachu"}, options);
      WriteOptions wopt;
      wopt.disableWAL = (disableWAL == 1);
      // 1st round: put but not flush
      ASSERT_OK(db_->Put(wopt, handles_[1], "foo", "first"));
      ASSERT_OK(db_->Put(wopt, handles_[1], "bar", "one"));
      ASSERT_EQ("first", Get(1, "foo"));
      ASSERT_EQ("one", Get(1, "bar"));

      // Read directly from persited data.
      ReadOptions ropt;
      ropt.read_tier = kPersistedTier;
      std::string value;
      if (wopt.disableWAL) {
        // as data has not yet being flushed, we expect not found.
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "foo", &value).IsNotFound());
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).IsNotFound());
      } else {
        ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
        ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
      }

      // Multiget
      std::vector<ColumnFamilyHandle*> multiget_cfs;
      multiget_cfs.push_back(handles_[1]);
      multiget_cfs.push_back(handles_[1]);
      std::vector<Slice> multiget_keys;
      multiget_keys.push_back("foo");
      multiget_keys.push_back("bar");
      std::vector<std::string> multiget_values;
      auto statuses =
          db_->MultiGet(ropt, multiget_cfs, multiget_keys, &multiget_values);
      if (wopt.disableWAL) {
        ASSERT_TRUE(statuses[0].IsNotFound());
        ASSERT_TRUE(statuses[1].IsNotFound());
      } else {
        ASSERT_OK(statuses[0]);
        ASSERT_OK(statuses[1]);
      }

      // 2nd round: flush and put a new value in memtable.
      ASSERT_OK(Flush(1));
      ASSERT_OK(db_->Put(wopt, handles_[1], "rocksdb", "hello"));

      // once the data has been flushed, we are able to get the
      // data when kPersistedTier is used.
      ASSERT_TRUE(db_->Get(ropt, handles_[1], "foo", &value).ok());
      ASSERT_EQ(value, "first");
      ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).ok());
      ASSERT_EQ(value, "one");
      if (wopt.disableWAL) {
        ASSERT_TRUE(
            db_->Get(ropt, handles_[1], "rocksdb", &value).IsNotFound());
      } else {
        ASSERT_OK(db_->Get(ropt, handles_[1], "rocksdb", &value));
        ASSERT_EQ(value, "hello");
      }

      // Expect same result in multiget
      multiget_cfs.push_back(handles_[1]);
      multiget_keys.push_back("rocksdb");
      statuses =
          db_->MultiGet(ropt, multiget_cfs, multiget_keys, &multiget_values);
      ASSERT_TRUE(statuses[0].ok());
      ASSERT_EQ("first", multiget_values[0]);
      ASSERT_TRUE(statuses[1].ok());
      ASSERT_EQ("one", multiget_values[1]);
      if (wopt.disableWAL) {
        ASSERT_TRUE(statuses[2].IsNotFound());
      } else {
        ASSERT_OK(statuses[2]);
      }

      // 3rd round: delete and flush
      ASSERT_OK(db_->Delete(wopt, handles_[1], "foo"));
      Flush(1);
      ASSERT_OK(db_->Delete(wopt, handles_[1], "bar"));

      ASSERT_TRUE(db_->Get(ropt, handles_[1], "foo", &value).IsNotFound());
      if (wopt.disableWAL) {
        // Still expect finding the value as its delete has not yet being
        // flushed.
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).ok());
        ASSERT_EQ(value, "one");
      } else {
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).IsNotFound());
      }
      ASSERT_TRUE(db_->Get(ropt, handles_[1], "rocksdb", &value).ok());
      ASSERT_EQ(value, "hello");

      statuses =
          db_->MultiGet(ropt, multiget_cfs, multiget_keys, &multiget_values);
      ASSERT_TRUE(statuses[0].IsNotFound());
      if (wopt.disableWAL) {
        ASSERT_TRUE(statuses[1].ok());
        ASSERT_EQ("one", multiget_values[1]);
      } else {
        ASSERT_TRUE(statuses[1].IsNotFound());
      }
      ASSERT_TRUE(statuses[2].ok());
      ASSERT_EQ("hello", multiget_values[2]);
      if (wopt.disableWAL == 0) {
        DestroyAndReopen(options);
      }
    }
  } while (ChangeOptions(kSkipHashCuckoo));
}

TEST_F(DBTest, SingleDeleteFlush) {
  // Test to check whether flushing preserves a single delete hidden
  // behind a put.
  do {
    Random rnd(301);

    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    // Put values on second level (so that they will not be in the same
    // compaction as the other operations.
    Put(1, "foo", "first");
    Put(1, "bar", "one");
    ASSERT_OK(Flush(1));
    MoveFilesToLevel(2, 1);

    // (Single) delete hidden by a put
    SingleDelete(1, "foo");
    Put(1, "foo", "second");
    Delete(1, "bar");
    Put(1, "bar", "two");
    ASSERT_OK(Flush(1));

    SingleDelete(1, "foo");
    Delete(1, "bar");
    ASSERT_OK(Flush(1));

    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);

    ASSERT_EQ("NOT_FOUND", Get(1, "bar"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
    // Skip HashCuckooRep as it does not support single delete. FIFO and
    // universal compaction do not apply to the test case. Skip MergePut
    // because merges cannot be combined with single deletions.
  } while (ChangeOptions(kSkipHashCuckoo | kSkipFIFOCompaction |
                         kSkipUniversalCompaction | kSkipMergePut));
}

TEST_F(DBTest, SingleDeletePutFlush) {
  // Single deletes that encounter the matching put in a flush should get
  // removed.
  do {
    Random rnd(301);

    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    Put(1, "foo", Slice());
    Put(1, "a", Slice());
    SingleDelete(1, "a");
    ASSERT_OK(Flush(1));

    ASSERT_EQ("[ ]", AllEntriesFor("a", 1));
    // Skip HashCuckooRep as it does not support single delete. FIFO and
    // universal compaction do not apply to the test case. Skip MergePut
    // because merges cannot be combined with single deletions.
  } while (ChangeOptions(kSkipHashCuckoo | kSkipFIFOCompaction |
                         kSkipUniversalCompaction | kSkipMergePut));
}

TEST_F(DBTest, EmptyFlush) {
  // It is possible to produce empty flushes when using single deletes. Tests
  // whether empty flushes cause issues.
  do {
    Random rnd(301);

    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    Put(1, "a", Slice());
    SingleDelete(1, "a");
    ASSERT_OK(Flush(1));

    ASSERT_EQ("[ ]", AllEntriesFor("a", 1));
    // Skip HashCuckooRep as it does not support single delete. FIFO and
    // universal compaction do not apply to the test case. Skip MergePut
    // because merges cannot be combined with single deletions.
  } while (ChangeOptions(kSkipHashCuckoo | kSkipFIFOCompaction |
                         kSkipUniversalCompaction | kSkipMergePut));
}

// Disable because not all platform can run it.
// It requires more than 9GB memory to run it, With single allocation
// of more than 3GB.
TEST_F(DBTest, DISABLED_VeryLargeValue) {
  const size_t kValueSize = 3221225472u;  // 3GB value
  const size_t kKeySize = 8388608u;       // 8MB key
  std::string raw(kValueSize, 'v');
  std::string key1(kKeySize, 'c');
  std::string key2(kKeySize, 'd');

  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  options.paranoid_checks = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("boo", "v1"));
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put(key1, raw));
  raw[0] = 'w';
  ASSERT_OK(Put(key2, raw));
  dbfull()->TEST_WaitForFlushMemTable();

  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  std::string value;
  Status s = db_->Get(ReadOptions(), key1, &value);
  ASSERT_OK(s);
  ASSERT_EQ(kValueSize, value.size());
  ASSERT_EQ('v', value[0]);

  s = db_->Get(ReadOptions(), key2, &value);
  ASSERT_OK(s);
  ASSERT_EQ(kValueSize, value.size());
  ASSERT_EQ('w', value[0]);

  // Compact all files.
  Flush();
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  // Check DB is not in read-only state.
  ASSERT_OK(Put("boo", "v1"));

  s = db_->Get(ReadOptions(), key1, &value);
  ASSERT_OK(s);
  ASSERT_EQ(kValueSize, value.size());
  ASSERT_EQ('v', value[0]);

  s = db_->Get(ReadOptions(), key2, &value);
  ASSERT_OK(s);
  ASSERT_EQ(kValueSize, value.size());
  ASSERT_EQ('w', value[0]);
}

TEST_F(DBTest, GetFromImmutableLayer) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));

    // Block sync calls
    env_->delay_sstable_sync_.store(true, std::memory_order_release);
    Put(1, "k1", std::string(100000, 'x'));  // Fill memtable
    Put(1, "k2", std::string(100000, 'y'));  // Trigger flush
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

#ifndef ROCKSDB_LITE
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
        // Unsupported case.
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
#endif  // ROCKSDB_LITE

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

#ifndef ROCKSDB_LITE
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
    options.disableDataSync = true;
    CreateAndReopenWithCF({"pikachu"}, options);
    // Arrange for the following to happen:
    //   * sstable A in level 0
    //   * nothing in level 1
    //   * sstable B in level 2
    // Then do enough Get() calls to arrange for an automatic compaction
    // of sstable A.  A bug would cause the compaction to be marked as
    // occurring at level 1 (instead of the correct level 0).

    // Step 1: First place sstables in levels 0 and 2
    Put(1, "a", "begin");
    Put(1, "z", "end");
    ASSERT_OK(Flush(1));
    dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);
    dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]);
    Put(1, "a", "begin");
    Put(1, "z", "end");
    ASSERT_OK(Flush(1));
    ASSERT_GT(NumTableFilesAtLevel(0, 1), 0);
    ASSERT_GT(NumTableFilesAtLevel(2, 1), 0);

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
    dbfull()->TEST_WaitForCompact();

    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);  // XXX
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipFIFOCompaction));
}
#endif  // ROCKSDB_LITE

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
    options.max_write_buffer_number_to_maintain = -1;
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));
    ASSERT_OK(Flush(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, FlushEmptyColumnFamily) {
  // Block flush thread and disable compaction thread
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  Options options = CurrentOptions();
  // disable compaction
  options.disable_auto_compactions = true;
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.max_write_buffer_number = 2;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number_to_maintain = 1;
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

TEST_F(DBTest, FLUSH) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    SetPerfLevel(kEnableTime);
    ;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    // this will now also flush the last 2 writes
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    perf_context.Reset();
    Get(1, "foo");
    ASSERT_TRUE((int)perf_context.get_from_output_files_time > 0);

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
    ASSERT_TRUE((int)perf_context.get_from_output_files_time > 0);

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

#ifndef ROCKSDB_LITE
TEST_F(DBTest, FlushSchedule) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_stop_writes_trigger = 1 << 10;
  options.level0_slowdown_writes_trigger = 1 << 10;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number_to_maintain = 1;
  options.max_write_buffer_number = 2;
  options.write_buffer_size = 120 * 1024;
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<std::thread> threads;

  std::atomic<int> thread_num(0);
  // each column family will have 5 thread, each thread generating 2 memtables.
  // each column family should end up with 10 table files
  std::function<void()> fill_memtable_func = [&]() {
    int a = thread_num.fetch_add(1);
    Random rnd(a);
    WriteOptions wo;
    // this should fill up 2 memtables
    for (int k = 0; k < 5000; ++k) {
      ASSERT_OK(db_->Put(wo, handles_[a & 1], RandomString(&rnd, 13), ""));
    }
  };

  for (int i = 0; i < 10; ++i) {
    threads.emplace_back(fill_memtable_func);
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
#endif  // ROCKSDB_LITE

TEST_F(DBTest, ManifestRollOver) {
  do {
    Options options;
    options.max_manifest_file_size = 10;  // 10 bytes
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

namespace {
class KeepFilter : public CompactionFilter {
 public:
  virtual bool Filter(int level, const Slice& key, const Slice& value,
                      std::string* new_value,
                      bool* value_changed) const override {
    return false;
  }

  virtual const char* Name() const override { return "KeepFilter"; }
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

class DelayFilter : public CompactionFilter {
 public:
  explicit DelayFilter(DBTestBase* d) : db_test(d) {}
  virtual bool Filter(int level, const Slice& key, const Slice& value,
                      std::string* new_value,
                      bool* value_changed) const override {
    db_test->env_->addon_time_.fetch_add(1000);
    return true;
  }

  virtual const char* Name() const override { return "DelayFilter"; }

 private:
  DBTestBase* db_test;
};

class DelayFilterFactory : public CompactionFilterFactory {
 public:
  explicit DelayFilterFactory(DBTestBase* d) : db_test(d) {}
  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    return std::unique_ptr<CompactionFilter>(new DelayFilter(db_test));
  }

  virtual const char* Name() const override { return "DelayFilterFactory"; }

 private:
  DBTestBase* db_test;
};
}  // namespace

#ifndef ROCKSDB_LITE

static std::string CompressibleString(Random* rnd, int len) {
  std::string r;
  test::CompressibleString(rnd, 0.8, len, &r);
  return r;
}
#endif  // ROCKSDB_LITE

TEST_F(DBTest, FailMoreDbPaths) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 10000000);
  options.db_paths.emplace_back(dbname_ + "_2", 1000000);
  options.db_paths.emplace_back(dbname_ + "_3", 1000000);
  options.db_paths.emplace_back(dbname_ + "_4", 1000000);
  options.db_paths.emplace_back(dbname_ + "_5", 1000000);
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
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

#ifndef ROCKSDB_LITE
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

namespace {
void MinLevelHelper(DBTest* self, Options& options) {
  Random rnd(301);

  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    std::vector<std::string> values;
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      values.push_back(DBTestBase::RandomString(&rnd, 10000));
      ASSERT_OK(self->Put(DBTestBase::Key(i), values[i]));
    }
    self->dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(self->NumTableFilesAtLevel(0), num + 1);
  }

  // generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < 12; i++) {
    values.push_back(DBTestBase::RandomString(&rnd, 10000));
    ASSERT_OK(self->Put(DBTestBase::Key(i), values[i]));
  }
  self->dbfull()->TEST_WaitForCompact();

  ASSERT_EQ(self->NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(self->NumTableFilesAtLevel(1), 1);
}

// returns false if the calling-Test should be skipped
bool MinLevelToCompress(CompressionType& type, Options& options, int wbits,
                        int lev, int strategy) {
  fprintf(stderr,
          "Test with compression options : window_bits = %d, level =  %d, "
          "strategy = %d}\n",
          wbits, lev, strategy);
  options.write_buffer_size = 100 << 10;  // 100KB
  options.arena_block_size = 4096;
  options.num_levels = 3;
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
    Options options = CurrentOptions();
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
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
#endif  // ROCKSDB_LITE

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

#ifndef ROCKSDB_LITE
static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
            (unsigned long long)(val), (unsigned long long)(low),
            (unsigned long long)(high));
  }
  return result;
}

TEST_F(DBTest, ApproximateSizesMemTable) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;  // Large write buffer
  options.compression = kNoCompression;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const int N = 128;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
  }

  uint64_t size;
  std::string start = Key(50);
  std::string end = Key(60);
  Range r(start, end);
  db_->GetApproximateSizes(&r, 1, &size, true);
  ASSERT_GT(size, 6000);
  ASSERT_LT(size, 204800);
  // Zero if not including mem table
  db_->GetApproximateSizes(&r, 1, &size, false);
  ASSERT_EQ(size, 0);

  start = Key(500);
  end = Key(600);
  r = Range(start, end);
  db_->GetApproximateSizes(&r, 1, &size, true);
  ASSERT_EQ(size, 0);

  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(1000 + i), RandomString(&rnd, 1024)));
  }

  start = Key(500);
  end = Key(600);
  r = Range(start, end);
  db_->GetApproximateSizes(&r, 1, &size, true);
  ASSERT_EQ(size, 0);

  start = Key(100);
  end = Key(1020);
  r = Range(start, end);
  db_->GetApproximateSizes(&r, 1, &size, true);
  ASSERT_GT(size, 6000);

  options.max_write_buffer_number = 8;
  options.min_write_buffer_number_to_merge = 5;
  options.write_buffer_size = 1024 * N;  // Not very large
  DestroyAndReopen(options);

  int keys[N * 3];
  for (int i = 0; i < N; i++) {
    keys[i * 3] = i * 5;
    keys[i * 3 + 1] = i * 5 + 1;
    keys[i * 3 + 2] = i * 5 + 2;
  }
  std::random_shuffle(std::begin(keys), std::end(keys));

  for (int i = 0; i < N * 3; i++) {
    ASSERT_OK(Put(Key(keys[i] + 1000), RandomString(&rnd, 1024)));
  }

  start = Key(100);
  end = Key(300);
  r = Range(start, end);
  db_->GetApproximateSizes(&r, 1, &size, true);
  ASSERT_EQ(size, 0);

  start = Key(1050);
  end = Key(1080);
  r = Range(start, end);
  db_->GetApproximateSizes(&r, 1, &size, true);
  ASSERT_GT(size, 6000);

  start = Key(2100);
  end = Key(2300);
  r = Range(start, end);
  db_->GetApproximateSizes(&r, 1, &size, true);
  ASSERT_EQ(size, 0);

  start = Key(1050);
  end = Key(1080);
  r = Range(start, end);
  uint64_t size_with_mt, size_without_mt;
  db_->GetApproximateSizes(&r, 1, &size_with_mt, true);
  ASSERT_GT(size_with_mt, 6000);
  db_->GetApproximateSizes(&r, 1, &size_without_mt, false);
  ASSERT_EQ(size_without_mt, 0);

  Flush();

  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(i + 1000), RandomString(&rnd, 1024)));
  }

  start = Key(1050);
  end = Key(1080);
  r = Range(start, end);
  db_->GetApproximateSizes(&r, 1, &size_with_mt, true);
  db_->GetApproximateSizes(&r, 1, &size_without_mt, false);
  ASSERT_GT(size_with_mt, size_without_mt);
  ASSERT_GT(size_without_mt, 6000);
}

TEST_F(DBTest, ApproximateSizes) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 100000000;  // Large write buffer
    options.compression = kNoCompression;
    options.create_if_missing = true;
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
#endif  // ROCKSDB_LITE

#ifndef ROCKSDB_LITE
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

    env_->addon_time_.fetch_add(1);

    const Snapshot* s2 = db_->GetSnapshot();
    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    Put(0, "foo", "0v3");
    Put(1, "foo", "1v3");

    {
      ManagedSnapshot s3(db_);
      ASSERT_EQ(3U, GetNumSnapshots());
      ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());

      Put(0, "foo", "0v4");
      Put(1, "foo", "1v4");
      ASSERT_EQ("0v1", Get(0, "foo", s1));
      ASSERT_EQ("1v1", Get(1, "foo", s1));
      ASSERT_EQ("0v2", Get(0, "foo", s2));
      ASSERT_EQ("1v2", Get(1, "foo", s2));
      ASSERT_EQ("0v3", Get(0, "foo", s3.snapshot()));
      ASSERT_EQ("1v3", Get(1, "foo", s3.snapshot()));
      ASSERT_EQ("0v4", Get(0, "foo"));
      ASSERT_EQ("1v4", Get(1, "foo"));
    }

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
#endif  // ROCKSDB_LITE

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
    // before any compaction or flush is triggered.
    ASSERT_EQ(AllEntriesFor("foo", 1),
              "[ sixth, fifth, fourth, third, second, first ]");
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
    ASSERT_EQ("first", Get(1, "foo", snapshot1));

    // After a flush, "second", "third" and "fifth" should
    // be removed
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth, first ]");

    // after we release the snapshot1, only two values left
    db_->ReleaseSnapshot(snapshot1);
    FillLevels("a", "z", 1);
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);

    // We have only one valid snapshot snapshot2. Since snapshot1 is
    // not valid anymore, "first" should be removed by a compaction.
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth ]");

    // after we release the snapshot2, only one value should be left
    db_->ReleaseSnapshot(snapshot2);
    FillLevels("a", "z", 1);
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth ]");
    // skip HashCuckooRep as it does not support snapshot
  } while (ChangeOptions(kSkipHashCuckoo | kSkipFIFOCompaction));
}

TEST_F(DBTest, UnremovableSingleDelete) {
  // If we compact:
  //
  // Put(A, v1) Snapshot SingleDelete(A) Put(A, v2)
  //
  // We do not want to end up with:
  //
  // Put(A, v1) Snapshot Put(A, v2)
  //
  // Because a subsequent SingleDelete(A) would delete the Put(A, v2)
  // but not Put(A, v1), so Get(A) would return v1.
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    Put(1, "foo", "first");
    const Snapshot* snapshot = db_->GetSnapshot();
    SingleDelete(1, "foo");
    Put(1, "foo", "second");
    ASSERT_OK(Flush(1));

    ASSERT_EQ("first", Get(1, "foo", snapshot));
    ASSERT_EQ("second", Get(1, "foo"));

    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ("[ second, SDEL, first ]", AllEntriesFor("foo", 1));

    SingleDelete(1, "foo");

    ASSERT_EQ("first", Get(1, "foo", snapshot));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));

    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);

    ASSERT_EQ("first", Get(1, "foo", snapshot));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
    db_->ReleaseSnapshot(snapshot);
    // Skip HashCuckooRep as it does not support single delete.  FIFO and
    // universal compaction do not apply to the test case.  Skip MergePut
    // because single delete does not get removed when it encounters a merge.
  } while (ChangeOptions(kSkipHashCuckoo | kSkipFIFOCompaction |
                         kSkipUniversalCompaction | kSkipMergePut));
}

#ifndef ROCKSDB_LITE
TEST_F(DBTest, DeletionMarkers1) {
  Options options = CurrentOptions();
  options.max_background_flushes = 0;
  CreateAndReopenWithCF({"pikachu"}, options);
  Put(1, "foo", "v1");
  ASSERT_OK(Flush(1));
  const int last = 2;
  MoveFilesToLevel(last, 1);
  // foo => v1 is now in last level
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put(1, "a", "begin");
  Put(1, "z", "end");
  Flush(1);
  MoveFilesToLevel(last - 1, 1);
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last - 1, 1), 1);

  Delete(1, "foo");
  Put(1, "foo", "v2");
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, DEL, v1 ]");
  ASSERT_OK(Flush(1));  // Moves to level last-2
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");
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
  CreateAndReopenWithCF({"pikachu"}, options);
  Put(1, "foo", "v1");
  ASSERT_OK(Flush(1));
  const int last = 2;
  MoveFilesToLevel(last, 1);
  // foo => v1 is now in last level
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put(1, "a", "begin");
  Put(1, "z", "end");
  Flush(1);
  MoveFilesToLevel(last - 1, 1);
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
    CreateAndReopenWithCF({"pikachu"}, options);

    // Fill levels 1 and 2 to disable the pushing of new memtables to levels >
    // 0.
    ASSERT_OK(Put(1, "100", "v100"));
    ASSERT_OK(Put(1, "999", "v999"));
    Flush(1);
    MoveFilesToLevel(2, 1);
    ASSERT_OK(Delete(1, "100"));
    ASSERT_OK(Delete(1, "999"));
    Flush(1);
    MoveFilesToLevel(1, 1);
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
#endif  // ROCKSDB_LITE

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
    Status s = TryReopenWithColumnFamilies(
        {"default", "pikachu"}, std::vector<Options>({options, new_options}));
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
      ToNumber(*s);  // Check format
      ToNumber(l);   // Check format
    }
    virtual void FindShortSuccessor(std::string* key) const override {
      ToNumber(*key);  // Check format
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
    new_options.write_buffer_size = 4096;  // Compact more often
    new_options.arena_block_size = 4096;
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
        snprintf(buf, sizeof(buf), "[%d]", i * 10);
        ASSERT_OK(Put(1, buf, buf));
      }
      Compact(1, "[0]", "[1000000]");
    }
  } while (ChangeCompactOptions());
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
  DestroyAndReopen(options);
  ASSERT_TRUE(db_ != nullptr);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "a", "123"));
  ASSERT_OK(Put(1, "b", "234"));
  Flush(1);
  MoveFilesToLevel(3, 1);
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

#ifndef ROCKSDB_LITE
TEST_F(DBTest, SnapshotFiles) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 100000000;  // Large write buffer
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

    for (size_t i = 0; i < files.size(); i++) {
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
            size = manifest_size;  // copy only valid MANIFEST data
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
    for (size_t i = 0; i < newfiles.size(); i++) {
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
#endif

TEST_F(DBTest, CompactOnFlush) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
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

    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2 ]");

    // Case 2: Delete followed by another delete
    Delete(1, "foo");
    Delete(1, "foo");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, DEL, v2 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v2 ]");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 3: Put followed by a delete
    Put(1, "foo", "v3");
    Delete(1, "foo");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v3 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL ]");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 4: Put followed by another Put
    Put(1, "foo", "v4");
    Put(1, "foo", "v5");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5, v4 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");

    // clear database
    Delete(1, "foo");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
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
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
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

#ifndef ROCKSDB_LITE
TEST_F(DBTest, SharedWriteBuffer) {
  Options options = CurrentOptions();
  options.db_write_buffer_size = 100000;  // this is the real limit
  options.write_buffer_size = 500000;     // this is never hit
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  // Trigger a flush on CF "nikitich"
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
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(1));
  }

  // "dobrynia": 20KB
  // Flush 'dobrynia'
  ASSERT_OK(Put(3, Key(2), DummyString(40000)));
  ASSERT_OK(Put(2, Key(2), DummyString(70000)));
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(1));
  }

  // "nikitich" still has data of 80KB
  // Inserting Data in "dobrynia" triggers "nikitich" flushing.
  ASSERT_OK(Put(3, Key(2), DummyString(40000)));
  ASSERT_OK(Put(2, Key(2), DummyString(40000)));
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // "dobrynia" still has 40KB
  ASSERT_OK(Put(1, Key(2), DummyString(20000)));
  ASSERT_OK(Put(0, Key(1), DummyString(10000)));
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[0]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  // This should triggers no flush
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // "default": 10KB, "pikachu": 20KB, "dobrynia": 40KB
  ASSERT_OK(Put(1, Key(2), DummyString(40000)));
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  dbfull()->TEST_WaitForFlushMemTable(handles_[0]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
  dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  // This should triggers flush of "pikachu"
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // "default": 10KB, "dobrynia": 40KB
  // Some remaining writes so 'default', 'dobrynia' and 'nikitich' flush on
  // closure.
  ASSERT_OK(Put(3, Key(1), DummyString(1)));
  ReopenWithColumnFamilies({"default", "pikachu", "dobrynia", "nikitich"},
                           options);
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(3));
  }
}
#endif  // ROCKSDB_LITE

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

#ifndef ROCKSDB_LITE
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
          ASSERT_EQ(5, sscanf(values[i].c_str(), "%d.%d.%d.%d.%d", &k, &w, &c,
                              &cf, &u))
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
#endif  // ROCKSDB_LITE

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
    std::sort(expected_db.begin(), expected_db.end());

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

    HistogramData hist_data = {0, 0, 0, 0, 0};
    options.statistics->histogramData(DB_WRITE, &hist_data);
    ASSERT_GT(hist_data.average, 0.0);
  } while (ChangeOptions(kSkipNoSeekToLast));
}

namespace {
typedef std::map<std::string, std::string> KVMap;
}

class ModelDB : public DB {
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
  using DB::Delete;
  virtual Status Delete(const WriteOptions& o, ColumnFamilyHandle* cf,
                        const Slice& key) override {
    WriteBatch batch;
    batch.Delete(cf, key);
    return Write(o, &batch);
  }
  using DB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& o, ColumnFamilyHandle* cf,
                              const Slice& key) override {
    WriteBatch batch;
    batch.SingleDelete(cf, key);
    return Write(o, &batch);
  }
  using DB::Merge;
  virtual Status Merge(const WriteOptions& o, ColumnFamilyHandle* cf,
                       const Slice& k, const Slice& v) override {
    WriteBatch batch;
    batch.Merge(cf, k, v);
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

#ifndef ROCKSDB_LITE
  using DB::AddFile;
  virtual Status AddFile(ColumnFamilyHandle* column_family,
                         const ExternalSstFileInfo* file_path,
                         bool move_file) override {
    return Status::NotSupported("Not implemented.");
  }
  virtual Status AddFile(ColumnFamilyHandle* column_family,
                         const std::string& file_path,
                         bool move_file) override {
    return Status::NotSupported("Not implemented.");
  }

  using DB::GetPropertiesOfAllTables;
  virtual Status GetPropertiesOfAllTables(
      ColumnFamilyHandle* column_family,
      TablePropertiesCollection* props) override {
    return Status();
  }

  virtual Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      TablePropertiesCollection* props) override {
    return Status();
  }
#endif  // ROCKSDB_LITE

  using DB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) override {
    if (value_found != nullptr) {
      *value_found = false;
    }
    return true;  // Not Supported directly
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
        // (*map_)[key.ToString()] = value.ToString();
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
  using DB::GetAggregatedIntProperty;
  virtual bool GetAggregatedIntProperty(const Slice& property,
                                        uint64_t* value) override {
    return false;
  }
  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(ColumnFamilyHandle* column_family,
                                   const Range* range, int n, uint64_t* sizes,
                                   bool include_memtable) override {
    for (int i = 0; i < n; i++) {
      sizes[i] = 0;
    }
  }
  using DB::CompactRange;
  virtual Status CompactRange(const CompactRangeOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice* start, const Slice* end) override {
    return Status::NotSupported("Not supported operation.");
  }

  using DB::CompactFiles;
  virtual Status CompactFiles(const CompactionOptions& compact_options,
                              ColumnFamilyHandle* column_family,
                              const std::vector<std::string>& input_file_names,
                              const int output_level,
                              const int output_path_id = -1) override {
    return Status::NotSupported("Not supported operation.");
  }

  Status PauseBackgroundWork() override {
    return Status::NotSupported("Not supported operation.");
  }

  Status ContinueBackgroundWork() override {
    return Status::NotSupported("Not supported operation.");
  }

  Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override {
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

  using DB::GetDBOptions;
  virtual const DBOptions& GetDBOptions() const override { return options_; }

  using DB::Flush;
  virtual Status Flush(const rocksdb::FlushOptions& options,
                       ColumnFamilyHandle* column_family) override {
    Status ret;
    return ret;
  }

  virtual Status SyncWAL() override { return Status::OK(); }

#ifndef ROCKSDB_LITE
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

  virtual Status GetUpdatesSince(
      rocksdb::SequenceNumber, unique_ptr<rocksdb::TransactionLogIterator>*,
      const TransactionLogIterator::ReadOptions& read_options =
          TransactionLogIterator::ReadOptions()) override {
    return Status::NotSupported("Not supported in Model DB");
  }

  virtual void GetColumnFamilyMetaData(
      ColumnFamilyHandle* column_family,
      ColumnFamilyMetaData* metadata) override {}
#endif  // ROCKSDB_LITE

  virtual Status GetDbIdentity(std::string& identity) const override {
    return Status::OK();
  }

  virtual SequenceNumber GetLatestSequenceNumber() const override { return 0; }

  virtual ColumnFamilyHandle* DefaultColumnFamily() const override {
    return nullptr;
  }

 private:
  class ModelIter : public Iterator {
   public:
    ModelIter(const KVMap* map, bool owned)
        : map_(map), owned_(owned), iter_(map_->end()) {}
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
               ? 1  // Short sometimes to encourage collisions
               : (rnd->OneIn(100) ? rnd->Skewed(10) : rnd->Uniform(10)));
  } while (len < minimum);
  return test::RandomKey(rnd, len);
}

static bool CompareIterators(int step, DB* model, DB* db,
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
       ok && miter->Valid() && dbiter->Valid(); miter->Next(), dbiter->Next()) {
    count++;
    if (miter->key().compare(dbiter->key()) != 0) {
      fprintf(stderr, "step %d: Key mismatch: '%s' vs. '%s'\n", step,
              EscapeString(miter->key()).c_str(),
              EscapeString(dbiter->key()).c_str());
      ok = false;
      break;
    }

    if (miter->value().compare(dbiter->value()) != 0) {
      fprintf(stderr, "step %d: Value mismatch for key '%s': '%s' vs. '%s'\n",
              step, EscapeString(miter->key()).c_str(),
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

class DBTestRandomized : public DBTest,
                         public ::testing::WithParamInterface<int> {
 public:
  virtual void SetUp() override { option_config_ = GetParam(); }

  static std::vector<int> GenerateOptionConfigs() {
    std::vector<int> option_configs;
    // skip cuckoo hash as it does not support snapshot.
    for (int option_config = kDefault; option_config < kEnd; ++option_config) {
      if (!ShouldSkipOptions(option_config, kSkipDeletesFilterFirst |
                                                kSkipNoSeekToLast |
                                                kSkipHashCuckoo)) {
        option_configs.push_back(option_config);
      }
    }
    option_configs.push_back(kBlockBasedTableWithIndexRestartInterval);
    return option_configs;
  }
};

INSTANTIATE_TEST_CASE_P(
    DBTestRandomized, DBTestRandomized,
    ::testing::ValuesIn(DBTestRandomized::GenerateOptionConfigs()));

TEST_P(DBTestRandomized, Randomized) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  DestroyAndReopen(options);

  Random rnd(test::RandomSeed() + GetParam());
  ModelDB model(options);
  const int N = 10000;
  const Snapshot* model_snap = nullptr;
  const Snapshot* db_snap = nullptr;
  std::string k, v;
  for (int step = 0; step < N; step++) {
    // TODO(sanjay): Test Get() works
    int p = rnd.Uniform(100);
    int minimum = 0;
    if (option_config_ == kHashSkipList || option_config_ == kHashLinkList ||
        option_config_ == kHashCuckoo ||
        option_config_ == kPlainTableFirstBytePrefix ||
        option_config_ == kBlockBasedTableWithWholeKeyHashIndex ||
        option_config_ == kBlockBasedTableWithPrefixHashIndex) {
      minimum = 1;
    }
    if (p < 45) {  // Put
      k = RandomKey(&rnd, minimum);
      v = RandomString(&rnd,
                       rnd.OneIn(20) ? 100 + rnd.Uniform(100) : rnd.Uniform(8));
      ASSERT_OK(model.Put(WriteOptions(), k, v));
      ASSERT_OK(db_->Put(WriteOptions(), k, v));
    } else if (p < 90) {  // Delete
      k = RandomKey(&rnd, minimum);
      ASSERT_OK(model.Delete(WriteOptions(), k));
      ASSERT_OK(db_->Delete(WriteOptions(), k));
    } else {  // Multi-element batch
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

      Reopen(options);
      ASSERT_TRUE(CompareIterators(step, &model, db_, nullptr, nullptr));

      model_snap = model.GetSnapshot();
      db_snap = db_->GetSnapshot();
    }
  }
  if (model_snap != nullptr) model.ReleaseSnapshot(model_snap);
  if (db_snap != nullptr) db_->ReleaseSnapshot(db_snap);
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
    ASSERT_EQ(static_cast<int>(s.size()), 2);
    ASSERT_TRUE(s[0].IsNotFound() && s[1].IsNotFound());
  } while (ChangeCompactOptions());
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

#ifndef ROCKSDB_LITE
TEST_P(DBTestWithParam, FIFOCompactionTest) {
  for (int iter = 0; iter < 2; ++iter) {
    // first iteration -- auto compaction
    // second iteration -- manual compaction
    Options options;
    options.compaction_style = kCompactionStyleFIFO;
    options.write_buffer_size = 100 << 10;  // 100KB
    options.arena_block_size = 4096;
    options.compaction_options_fifo.max_table_files_size = 500 << 10;  // 500KB
    options.compression = kNoCompression;
    options.create_if_missing = true;
    options.max_subcompactions = max_subcompactions_;
    if (iter == 1) {
      options.disable_auto_compactions = true;
    }
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    Random rnd(301);
    for (int i = 0; i < 6; ++i) {
      for (int j = 0; j < 110; ++j) {
        ASSERT_OK(Put(ToString(i * 100 + j), RandomString(&rnd, 980)));
      }
      // flush should happen here
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    if (iter == 0) {
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    } else {
      CompactRangeOptions cro;
      cro.exclusive_manual_compaction = exclusive_manual_compaction_;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    }
    // only 5 files should survive
    ASSERT_EQ(NumTableFilesAtLevel(0), 5);
    for (int i = 0; i < 50; ++i) {
      // these keys should be deleted in previous compaction
      ASSERT_EQ("NOT_FOUND", Get(ToString(i)));
    }
  }
}
#endif  // ROCKSDB_LITE

// verify that we correctly deprecated timeout_hint_us
TEST_F(DBTest, SimpleWriteTimeoutTest) {
  WriteOptions write_opt;
  write_opt.timeout_hint_us = 0;
  ASSERT_OK(Put(Key(1), Key(1) + std::string(100, 'v'), write_opt));
  write_opt.timeout_hint_us = 10;
  ASSERT_NOK(Put(Key(1), Key(1) + std::string(100, 'v'), write_opt));
}

#ifndef ROCKSDB_LITE
/*
 * This test is not reliable enough as it heavily depends on disk behavior.
 */
TEST_F(DBTest, RateLimitingTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 1 << 20;  // 1MB
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
    ASSERT_OK(
        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
  }
  uint64_t elapsed = env_->NowMicros() - start;
  double raw_rate = env_->bytes_written_ * 1000000.0 / elapsed;
  Close();

  // # rate limiting with 0.7 x threshold
  options.rate_limiter.reset(
      NewGenericRateLimiter(static_cast<int64_t>(0.7 * raw_rate)));
  env_->bytes_written_ = 0;
  DestroyAndReopen(options);

  start = env_->NowMicros();
  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(
        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
  }
  elapsed = env_->NowMicros() - start;
  Close();
  ASSERT_EQ(options.rate_limiter->GetTotalBytesThrough(), env_->bytes_written_);
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
    ASSERT_OK(
        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
  }
  elapsed = env_->NowMicros() - start;
  Close();
  ASSERT_EQ(options.rate_limiter->GetTotalBytesThrough(), env_->bytes_written_);
  ratio = env_->bytes_written_ * 1000000 / elapsed / raw_rate;
  fprintf(stderr, "write rate ratio = %.2lf, expected 0.5\n", ratio);
  ASSERT_LT(ratio, 0.6);
}

TEST_F(DBTest, TableOptionsSanitizeTest) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  ASSERT_EQ(db_->GetOptions().allow_mmap_reads, false);

  options.table_factory.reset(new PlainTableFactory());
  options.prefix_extractor.reset(NewNoopTransform());
  Destroy(options);
  ASSERT_TRUE(!TryReopen(options).IsNotSupported());

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

// On Windows you can have either memory mapped file or a file
// with unbuffered access. So this asserts and does not make
// sense to run
#ifndef OS_WIN
TEST_F(DBTest, MmapAndBufferOptions) {
  Options options = CurrentOptions();

  options.allow_os_buffer = false;
  options.allow_mmap_reads = true;
  ASSERT_NOK(TryReopen(options));

  // All other combinations are acceptable
  options.allow_os_buffer = true;
  ASSERT_OK(TryReopen(options));

  options.allow_os_buffer = false;
  options.allow_mmap_reads = false;
  ASSERT_OK(TryReopen(options));

  options.allow_os_buffer = true;
  ASSERT_OK(TryReopen(options));
}
#endif

TEST_F(DBTest, ConcurrentMemtableNotSupported) {
  Options options = CurrentOptions();
  options.allow_concurrent_memtable_write = true;
  options.soft_pending_compaction_bytes_limit = 0;
  options.hard_pending_compaction_bytes_limit = 100;
  options.create_if_missing = true;

  DestroyDB(dbname_, options);
  options.memtable_factory.reset(NewHashLinkListRepFactory(4, 0, 3, true, 4));
  ASSERT_NOK(TryReopen(options));

  options.memtable_factory.reset(new SkipListFactory);
  ASSERT_OK(TryReopen(options));

  ColumnFamilyOptions cf_options(options);
  cf_options.memtable_factory.reset(
      NewHashLinkListRepFactory(4, 0, 3, true, 4));
  ColumnFamilyHandle* handle;
  ASSERT_NOK(db_->CreateColumnFamily(cf_options, "name", &handle));
}

#endif  // ROCKSDB_LITE

TEST_F(DBTest, SanitizeNumThreads) {
  for (int attempt = 0; attempt < 2; attempt++) {
    const size_t kTotalTasks = 8;
    test::SleepingBackgroundTask sleeping_tasks[kTotalTasks];

    Options options = CurrentOptions();
    if (attempt == 0) {
      options.max_background_compactions = 3;
      options.max_background_flushes = 2;
    }
    options.create_if_missing = true;
    DestroyAndReopen(options);

    for (size_t i = 0; i < kTotalTasks; i++) {
      // Insert 5 tasks to low priority queue and 5 tasks to high priority queue
      env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                     &sleeping_tasks[i],
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
    options.num_levels = 10;
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

#ifndef ROCKSDB_LITE
TEST_F(DBTest, DynamicMemtableOptions) {
  const uint64_t k64KB = 1 << 16;
  const uint64_t k128KB = 1 << 17;
  const uint64_t k5KB = 5 * 1024;
  const int kNumPutsBeforeWaitForFlush = 64;
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.max_background_compactions = 1;
  options.write_buffer_size = k64KB;
  options.arena_block_size = 16 * 1024;
  options.max_write_buffer_number = 2;
  // Don't trigger compact/slowdown/stop
  options.level0_file_num_compaction_trigger = 1024;
  options.level0_slowdown_writes_trigger = 1024;
  options.level0_stop_writes_trigger = 1024;
  DestroyAndReopen(options);

  auto gen_l0_kb = [this, kNumPutsBeforeWaitForFlush](int size) {
    Random rnd(301);
    for (int i = 0; i < size; i++) {
      ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));

      // The following condition prevents a race condition between flush jobs
      // acquiring work and this thread filling up multiple memtables. Without
      // this, the flush might produce less files than expected because
      // multiple memtables are flushed into a single L0 file. This race
      // condition affects assertion (A).
      if (i % kNumPutsBeforeWaitForFlush == kNumPutsBeforeWaitForFlush - 1) {
        dbfull()->TEST_WaitForFlushMemTable();
      }
    }
    dbfull()->TEST_WaitForFlushMemTable();
  };

  // Test write_buffer_size
  gen_l0_kb(64);
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  ASSERT_LT(SizeAtLevel(0), k64KB + k5KB);
  ASSERT_GT(SizeAtLevel(0), k64KB - k5KB * 2);

  // Clean up L0
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  // Increase buffer size
  ASSERT_OK(dbfull()->SetOptions({
      {"write_buffer_size", "131072"},
  }));

  // The existing memtable is still 64KB in size, after it becomes immutable,
  // the next memtable will be 128KB in size. Write 256KB total, we should
  // have a 64KB L0 file, a 128KB L0 file, and a memtable with 64KB data
  gen_l0_kb(256);
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);  // (A)
  ASSERT_LT(SizeAtLevel(0), k128KB + k64KB + 2 * k5KB);
  ASSERT_GT(SizeAtLevel(0), k128KB + k64KB - 4 * k5KB);

  // Test max_write_buffer_number
  // Block compaction thread, which will also block the flushes because
  // max_background_flushes == 0, so flushes are getting executed by the
  // compaction thread
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  // Start from scratch and disable compaction/flush. Flush can only happen
  // during compaction but trigger is pretty high
  options.max_background_flushes = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Put until writes are stopped, bounded by 256 puts. We should see stop at
  // ~128KB
  int count = 0;
  Random rnd(301);

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DelayWrite:Wait",
      [&](void* arg) { sleeping_task_low.WakeUp(); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  while (!sleeping_task_low.WokenUp() && count < 256) {
    ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024), WriteOptions()));
    count++;
  }
  ASSERT_GT(static_cast<double>(count), 128 * 0.8);
  ASSERT_LT(static_cast<double>(count), 128 * 1.2);

  sleeping_task_low.WaitUntilDone();

  // Increase
  ASSERT_OK(dbfull()->SetOptions({
      {"max_write_buffer_number", "8"},
  }));
  // Clean up memtable and L0
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  sleeping_task_low.Reset();
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  count = 0;
  while (!sleeping_task_low.WokenUp() && count < 1024) {
    ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024), WriteOptions()));
    count++;
  }
// Windows fails this test. Will tune in the future and figure out
// approp number
#ifndef OS_WIN
  ASSERT_GT(static_cast<double>(count), 512 * 0.8);
  ASSERT_LT(static_cast<double>(count), 512 * 1.2);
#endif
  sleeping_task_low.WaitUntilDone();

  // Decrease
  ASSERT_OK(dbfull()->SetOptions({
      {"max_write_buffer_number", "4"},
  }));
  // Clean up memtable and L0
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  sleeping_task_low.Reset();
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  count = 0;
  while (!sleeping_task_low.WokenUp() && count < 1024) {
    ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024), WriteOptions()));
    count++;
  }
// Windows fails this test. Will tune in the future and figure out
// approp number
#ifndef OS_WIN
  ASSERT_GT(static_cast<double>(count), 256 * 0.8);
  ASSERT_LT(static_cast<double>(count), 266 * 1.2);
#endif
  sleeping_task_low.WaitUntilDone();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}
#endif  // ROCKSDB_LITE

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
      ASSERT_EQ(thread_type_counts[ThreadStatus::HIGH_PRIORITY] +
                    thread_type_counts[ThreadStatus::LOW_PRIORITY],
                kHighPriCounts[test] + kLowPriCounts[test]);
      // Verify the number of high-priority threads
      ASSERT_EQ(thread_type_counts[ThreadStatus::HIGH_PRIORITY],
                kHighPriCounts[test]);
      // Verify the number of low-priority threads
      ASSERT_EQ(thread_type_counts[ThreadStatus::LOW_PRIORITY],
                kLowPriCounts[test]);
    }
    if (i == 0) {
      // repeat the test with multiple column families
      CreateAndReopenWithCF({"pikachu", "about-to-remove"}, options);
      env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(handles_,
                                                                     true);
    }
  }
  db_->DropColumnFamily(handles_[2]);
  delete handles_[2];
  handles_.erase(handles_.begin() + 2);
  env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(handles_,
                                                                 true);
  Close();
  env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(handles_,
                                                                 true);
}

TEST_F(DBTest, DisableThreadStatus) {
  Options options;
  options.env = env_;
  options.enable_thread_tracking = false;
  TryReopen(options);
  CreateAndReopenWithCF({"pikachu", "about-to-remove"}, options);
  // Verify non of the column family info exists
  env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(handles_,
                                                                 false);
}

TEST_F(DBTest, ThreadStatusFlush) {
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  options.enable_thread_tracking = true;
  options = CurrentOptions(options);

  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"FlushJob::FlushJob()", "DBTest::ThreadStatusFlush:1"},
      {"DBTest::ThreadStatusFlush:2", "FlushJob::WriteLevel0Table"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu"}, options);
  VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 0);

  ASSERT_OK(Put(1, "foo", "v1"));
  ASSERT_EQ("v1", Get(1, "foo"));
  VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 0);

  uint64_t num_running_flushes = 0;
  db_->GetIntProperty(DB::Properties::kNumRunningFlushes, &num_running_flushes);
  ASSERT_EQ(num_running_flushes, 0);

  Put(1, "k1", std::string(100000, 'x'));  // Fill memtable
  Put(1, "k2", std::string(100000, 'y'));  // Trigger flush

  // The first sync point is to make sure there's one flush job
  // running when we perform VerifyOperationCount().
  TEST_SYNC_POINT("DBTest::ThreadStatusFlush:1");
  VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 1);
  db_->GetIntProperty(DB::Properties::kNumRunningFlushes, &num_running_flushes);
  ASSERT_EQ(num_running_flushes, 1);
  // This second sync point is to ensure the flush job will not
  // be completed until we already perform VerifyOperationCount().
  TEST_SYNC_POINT("DBTest::ThreadStatusFlush:2");
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBTestWithParam, ThreadStatusSingleCompaction) {
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
  options.max_subcompactions = max_subcompactions_;

  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DBTest::ThreadStatusSingleCompaction:0", "DBImpl::BGWorkCompaction"},
      {"CompactionJob::Run():Start", "DBTest::ThreadStatusSingleCompaction:1"},
      {"DBTest::ThreadStatusSingleCompaction:2", "CompactionJob::Run():End"},
  });
  for (int tests = 0; tests < 2; ++tests) {
    DestroyAndReopen(options);
    rocksdb::SyncPoint::GetInstance()->ClearTrace();
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();

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
    uint64_t num_running_compactions = 0;
    db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                        &num_running_compactions);
    ASSERT_EQ(num_running_compactions, 0);
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
    db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                        &num_running_compactions);
    ASSERT_EQ(num_running_compactions, 1);
    // TODO(yhchiang): adding assert to verify each compaction stage.
    TEST_SYNC_POINT("DBTest::ThreadStatusSingleCompaction:2");

    // repeat the test with disabling thread tracking.
    options.enable_thread_tracking = false;
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_P(DBTestWithParam, PreShutdownManualCompaction) {
  Options options = CurrentOptions();
  options.max_background_flushes = 0;
  options.max_subcompactions = max_subcompactions_;
  CreateAndReopenWithCF({"pikachu"}, options);

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
    ASSERT_EQ("1,0,2", FilesPerLevel(1));
    CancelAllBackgroundWork(db_);
    db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr);
    ASSERT_EQ("1,0,2", FilesPerLevel(1));

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

TEST_F(DBTest, PreShutdownFlush) {
  Options options = CurrentOptions();
  options.max_background_flushes = 0;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(1, "key", "value"));
  CancelAllBackgroundWork(db_);
  Status s =
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr);
  ASSERT_TRUE(s.IsShutdownInProgress());
}

TEST_P(DBTestWithParam, PreShutdownMultipleCompaction) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 40;
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
  options.max_subcompactions = max_subcompactions_;

  TryReopen(options);
  Random rnd(301);

  std::vector<ThreadStatus> thread_list;
  // Delay both flush and compaction
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"FlushJob::FlushJob()", "CompactionJob::Run():Start"},
       {"CompactionJob::Run():Start",
        "DBTest::PreShutdownMultipleCompaction:Preshutdown"},
       {"CompactionJob::Run():Start",
        "DBTest::PreShutdownMultipleCompaction:VerifyCompaction"},
       {"DBTest::PreShutdownMultipleCompaction:Preshutdown",
        "CompactionJob::Run():End"},
       {"CompactionJob::Run():End",
        "DBTest::PreShutdownMultipleCompaction:VerifyPreshutdown"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Make rocksdb busy
  int key = 0;
  // check how many threads are doing compaction using GetThreadList
  int operation_count[ThreadStatus::NUM_OP_TYPES] = {0};
  for (int file = 0; file < 16 * kNumL0Files; ++file) {
    for (int k = 0; k < kEntriesPerBuffer; ++k) {
      ASSERT_OK(Put(ToString(key++), RandomString(&rnd, kTestValueSize)));
    }

    Status s = env_->GetThreadList(&thread_list);
    for (auto thread : thread_list) {
      operation_count[thread.operation_type]++;
    }

    // Speed up the test
    if (operation_count[ThreadStatus::OP_FLUSH] > 1 &&
        operation_count[ThreadStatus::OP_COMPACTION] >
            0.6 * options.max_background_compactions) {
      break;
    }
    if (file == 15 * kNumL0Files) {
      TEST_SYNC_POINT("DBTest::PreShutdownMultipleCompaction:Preshutdown");
    }
  }

  TEST_SYNC_POINT("DBTest::PreShutdownMultipleCompaction:Preshutdown");
  ASSERT_GE(operation_count[ThreadStatus::OP_COMPACTION], 1);
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

TEST_P(DBTestWithParam, PreShutdownCompactionMiddle) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 40;
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
  options.max_subcompactions = max_subcompactions_;

  TryReopen(options);
  Random rnd(301);

  std::vector<ThreadStatus> thread_list;
  // Delay both flush and compaction
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBTest::PreShutdownCompactionMiddle:Preshutdown",
        "CompactionJob::Run():Inprogress"},
       {"CompactionJob::Run():Start",
        "DBTest::PreShutdownCompactionMiddle:VerifyCompaction"},
       {"CompactionJob::Run():Inprogress", "CompactionJob::Run():End"},
       {"CompactionJob::Run():End",
        "DBTest::PreShutdownCompactionMiddle:VerifyPreshutdown"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Make rocksdb busy
  int key = 0;
  // check how many threads are doing compaction using GetThreadList
  int operation_count[ThreadStatus::NUM_OP_TYPES] = {0};
  for (int file = 0; file < 16 * kNumL0Files; ++file) {
    for (int k = 0; k < kEntriesPerBuffer; ++k) {
      ASSERT_OK(Put(ToString(key++), RandomString(&rnd, kTestValueSize)));
    }

    Status s = env_->GetThreadList(&thread_list);
    for (auto thread : thread_list) {
      operation_count[thread.operation_type]++;
    }

    // Speed up the test
    if (operation_count[ThreadStatus::OP_FLUSH] > 1 &&
        operation_count[ThreadStatus::OP_COMPACTION] >
            0.6 * options.max_background_compactions) {
      break;
    }
    if (file == 15 * kNumL0Files) {
      TEST_SYNC_POINT("DBTest::PreShutdownCompactionMiddle:VerifyCompaction");
    }
  }

  ASSERT_GE(operation_count[ThreadStatus::OP_COMPACTION], 1);
  CancelAllBackgroundWork(db_);
  TEST_SYNC_POINT("DBTest::PreShutdownCompactionMiddle:Preshutdown");
  TEST_SYNC_POINT("DBTest::PreShutdownCompactionMiddle:VerifyPreshutdown");
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

#ifndef ROCKSDB_LITE
TEST_F(DBTest, FlushOnDestroy) {
  WriteOptions wo;
  wo.disableWAL = true;
  ASSERT_OK(Put("foo", "v1", wo));
  CancelAllBackgroundWork(db_);
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

  OnFileDeletionListener* listener = new OnFileDeletionListener();
  options.listeners.emplace_back(listener);

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
    listener->SetExpectedFileName(dbname_ + file.name);
    ASSERT_OK(dbfull()->DeleteFile(file.name));
  }
  listener->VerifyMatchedCount(cf_meta.levels[4].files.size());

  int num_keys = 0;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    num_keys++;
  }
  ASSERT_OK(iter->status());
  ASSERT_GT(SizeAtLevel(0) + SizeAtLevel(3), num_keys * 4000U);
}

TEST_F(DBTest, DynamicLevelCompressionPerLevel2) {
  if (!Snappy_Supported() || !LZ4_Supported() || !Zlib_Supported()) {
    return;
  }
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
  options.soft_pending_compaction_bytes_limit = 1024 * 1024;

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
          ASSERT_TRUE(compaction->output_compression() == kLZ4Compression);
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

    if (i % 25 == 0) {
      dbfull()->TEST_WaitForFlushMemTable();
    }
  }

  Flush();
  dbfull()->TEST_WaitForFlushMemTable();
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
          ASSERT_TRUE(compaction->output_compression() == kZlibCompression);
          num_zlib.fetch_add(1);
        } else {
          ASSERT_TRUE(compaction->output_compression() == kLZ4Compression);
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
  options.soft_pending_compaction_bytes_limit = 1024 * 1024;
  options.write_buffer_size = k64KB;
  options.arena_block_size = 4 * k4KB;
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
  ASSERT_OK(dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "2"},
                                  {"target_file_size_base", ToString(k32KB)}}));

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
  ASSERT_OK(
      dbfull()->SetOptions({{"max_bytes_for_level_base", ToString(k1MB)}}));

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
  ASSERT_OK(
      dbfull()->SetOptions({{"max_bytes_for_level_multiplier", "2"},
                            {"max_bytes_for_level_base", ToString(k128KB)}}));

  // writing 20 x 64KB = 10 x 128KB
  // (L1 + L2 + L3) = (1 + 2 + 4) * 128KB
  for (int i = 0; i < 20; ++i) {
    gen_l0_kb(i, 64, 32);
  }
  dbfull()->TEST_WaitForCompact();
  uint64_t total_size = SizeAtLevel(1) + SizeAtLevel(2) + SizeAtLevel(3);
  ASSERT_TRUE(total_size < k128KB * 7 * 1.5);

  // Test level0_stop_writes_trigger.
  // Clean up memtable and L0. Block compaction threads. If continue to write
  // and flush memtables. We should see put stop after 8 memtable flushes
  // since level0_stop_writes_trigger = 8
  dbfull()->TEST_FlushMemTable(true);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  // Block compaction
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  int count = 0;
  Random rnd(301);
  WriteOptions wo;
  while (count < 64) {
    ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024), wo));
    dbfull()->TEST_FlushMemTable(true);
    count++;
    if (dbfull()->TEST_write_controler().IsStopped()) {
      sleeping_task_low.WakeUp();
      break;
    }
  }
  // Stop trigger = 8
  ASSERT_EQ(count, 8);
  // Unblock
  sleeping_task_low.WaitUntilDone();

  // Now reduce level0_stop_writes_trigger to 6. Clear up memtables and L0.
  // Block compaction thread again. Perform the put and memtable flushes
  // until we see the stop after 6 memtable flushes.
  ASSERT_OK(dbfull()->SetOptions({{"level0_stop_writes_trigger", "6"}}));
  dbfull()->TEST_FlushMemTable(true);
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  // Block compaction again
  sleeping_task_low.Reset();
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();
  count = 0;
  while (count < 64) {
    ASSERT_OK(Put(Key(count), RandomString(&rnd, 1024), wo));
    dbfull()->TEST_FlushMemTable(true);
    count++;
    if (dbfull()->TEST_write_controler().IsStopped()) {
      sleeping_task_low.WakeUp();
      break;
    }
  }
  ASSERT_EQ(count, 6);
  // Unblock
  sleeping_task_low.WaitUntilDone();

  // Test disable_auto_compactions
  // Compaction thread is unblocked but auto compaction is disabled. Write
  // 4 L0 files and compaction should be triggered. If auto compaction is
  // disabled, then TEST_WaitForCompact will be waiting for nothing. Number of
  // L0 files do not change after the call.
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "true"}}));
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
    // Wait for compaction so that put won't stop
    dbfull()->TEST_FlushMemTable(true);
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(NumTableFilesAtLevel(0), 4);

  // Enable auto compaction and perform the same test, # of L0 files should be
  // reduced after compaction.
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
  dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
    // Wait for compaction so that put won't stop
    dbfull()->TEST_FlushMemTable(true);
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_LT(NumTableFilesAtLevel(0), 4);
}
#endif  // ROCKSDB_LITE

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

  const int kCDTKeysPerBuffer = 4;
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

#ifndef ROCKSDB_LITE
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

  ASSERT_OK(dbfull()->SetOptions({{"max_sequential_skip_in_iterations", "4"}}));
  // Clear memtable and make new option effective
  dbfull()->TEST_FlushMemTable(true);
  // Trigger reseek
  assert_reseek_count(200, 1);

  ASSERT_OK(
      dbfull()->SetOptions({{"max_sequential_skip_in_iterations", "16"}}));
  // Clear memtable and make new option effective
  dbfull()->TEST_FlushMemTable(true);
  // No reseek
  assert_reseek_count(300, 1);

  MutableCFOptions mutable_cf_options;
  CreateAndReopenWithCF({"pikachu"}, options);
  // Test soft_pending_compaction_bytes_limit,
  // hard_pending_compaction_bytes_limit
  ASSERT_OK(dbfull()->SetOptions(
      handles_[1], {{"soft_pending_compaction_bytes_limit", "200"},
                    {"hard_pending_compaction_bytes_limit", "300"}}));
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[1],
                                                     &mutable_cf_options));
  ASSERT_EQ(200, mutable_cf_options.soft_pending_compaction_bytes_limit);
  ASSERT_EQ(300, mutable_cf_options.hard_pending_compaction_bytes_limit);
  // Test report_bg_io_stats
  ASSERT_OK(
      dbfull()->SetOptions(handles_[1], {{"report_bg_io_stats", "true"}}));
  // sanity check
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[1],
                                                     &mutable_cf_options));
  ASSERT_EQ(true, mutable_cf_options.report_bg_io_stats);
  // Test min_partial_merge_operands
  ASSERT_OK(
      dbfull()->SetOptions(handles_[1], {{"min_partial_merge_operands", "4"}}));
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[1],
                                                     &mutable_cf_options));
  ASSERT_EQ(4, mutable_cf_options.min_partial_merge_operands);
  // Test compression
  // sanity check
  ASSERT_OK(dbfull()->SetOptions({{"compression", "kNoCompression"}}));
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[0],
                                                     &mutable_cf_options));
  ASSERT_EQ(CompressionType::kNoCompression, mutable_cf_options.compression);
  ASSERT_OK(dbfull()->SetOptions({{"compression", "kSnappyCompression"}}));
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[0],
                                                     &mutable_cf_options));
  ASSERT_EQ(CompressionType::kSnappyCompression,
            mutable_cf_options.compression);
  // Test paranoid_file_checks already done in db_block_cache_test
  ASSERT_OK(
      dbfull()->SetOptions(handles_[1], {{"paranoid_file_checks", "true"}}));
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[1],
                                                     &mutable_cf_options));
  ASSERT_EQ(true, mutable_cf_options.report_bg_io_stats);
}
#endif  // ROCKSDB_LITE

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
  // iter 4 -- xpress
  CompressionType compressions[] = {kZlibCompression, kBZip2Compression,
                                    kLZ4Compression, kLZ4HCCompression,
                                    kXpressCompression};
  for (auto comp : compressions) {
    if (!CompressionTypeSupported(comp)) {
      continue;
    }
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
      options.compression = comp;
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

TEST_F(DBTest, MutexWaitStatsDisabledByDefault) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  CreateAndReopenWithCF({"pikachu"}, options);
  const uint64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
                                       kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  ASSERT_EQ(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), 0);
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

TEST_F(DBTest, MutexWaitStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.statistics->stats_level_ = StatsLevel::kAll;
  CreateAndReopenWithCF({"pikachu"}, options);
  const uint64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
                                       kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  ASSERT_GE(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), kMutexWaitDelay);
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

TEST_F(DBTest, CloseSpeedup) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_write_buffer_number = 16;

  // Block background threads
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

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

class DelayedMergeOperator : public MergeOperator {
 private:
  DBTest* db_test_;

 public:
  explicit DelayedMergeOperator(DBTest* d) : db_test_(d) {}
  virtual bool FullMerge(const Slice& key, const Slice* existing_value,
                         const std::deque<std::string>& operand_list,
                         std::string* new_value,
                         Logger* logger) const override {
    db_test_->env_->addon_time_.fetch_add(1000);
    *new_value = "";
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
  this->env_->addon_time_.store(0);
  this->env_->time_elapse_only_sleep_ = true;
  this->env_->no_sleep_ = true;
  Options options = CurrentOptions();
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

  ASSERT_EQ(1000000, TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME));

  ReadOptions read_options;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ++count;
  }

  ASSERT_EQ(1, count);
  ASSERT_EQ(2000000, TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME));
#if ROCKSDB_USING_THREAD_STATUS
  ASSERT_GT(TestGetTickerCount(options, FLUSH_WRITE_BYTES), 0);
#endif  // ROCKSDB_USING_THREAD_STATUS
  this->env_->time_elapse_only_sleep_ = false;
}

#ifndef ROCKSDB_LITE
TEST_P(DBTestWithParam, MergeCompactionTimeTest) {
  SetPerfLevel(kEnableTime);
  Options options = CurrentOptions();
  options.compaction_filter_factory = std::make_shared<KeepFilterFactory>();
  options.statistics = rocksdb::CreateDBStatistics();
  options.merge_operator.reset(new DelayedMergeOperator(this));
  options.compaction_style = kCompactionStyleUniversal;
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  for (int i = 0; i < 1000; i++) {
    ASSERT_OK(db_->Merge(WriteOptions(), "foo", "TEST"));
    ASSERT_OK(Flush());
  }
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();

  ASSERT_NE(TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME), 0);
}

TEST_P(DBTestWithParam, FilterCompactionTimeTest) {
  Options options = CurrentOptions();
  options.compaction_filter_factory =
      std::make_shared<DelayFilterFactory>(this);
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  // put some data
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      Put(ToString(table * 100 + i), "val");
    }
    Flush();
  }

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ(0U, CountLiveFiles());

  Reopen(options);

  Iterator* itr = db_->NewIterator(ReadOptions());
  itr->SeekToFirst();
  ASSERT_NE(TestGetTickerCount(options, FILTER_OPERATION_TOTAL_TIME), 0);
  delete itr;
}
#endif  // ROCKSDB_LITE

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

#ifndef ROCKSDB_LITE
TEST_F(DBTest, EmptyCompactedDB) {
  Options options = CurrentOptions();
  options.max_open_files = -1;
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  Status s = Put("new", "value");
  ASSERT_TRUE(s.IsNotSupported());
  Close();
}
#endif  // ROCKSDB_LITE

#ifndef ROCKSDB_LITE
TEST_F(DBTest, SuggestCompactRangeTest) {
  class CompactionFilterFactoryGetContext : public CompactionFilterFactory {
   public:
    virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context& context) override {
      saved_context = context;
      std::unique_ptr<CompactionFilter> empty_filter;
      return empty_filter;
    }
    const char* Name() const override {
      return "CompactionFilterFactoryGetContext";
    }
    static bool IsManual(CompactionFilterFactory* compaction_filter_factory) {
      return reinterpret_cast<CompactionFilterFactoryGetContext*>(
                 compaction_filter_factory)
          ->saved_context.is_manual_compaction;
    }
    CompactionFilter::Context saved_context;
  };

  Options options = CurrentOptions();
  options.memtable_factory.reset(
      new SpecialSkipListFactory(DBTestBase::kNumKeysByGenerateNewRandomFile));
  options.compaction_style = kCompactionStyleLevel;
  options.compaction_filter_factory.reset(
      new CompactionFilterFactoryGetContext());
  options.write_buffer_size = 200 << 10;
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 4;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 450 << 10;
  options.target_file_size_base = 98 << 10;
  options.max_grandparent_overlap_factor = 1 << 20;  // inf

  Reopen(options);

  Random rnd(301);

  for (int num = 0; num < 3; num++) {
    GenerateNewRandomFile(&rnd);
  }

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("0,4", FilesPerLevel(0));
  ASSERT_TRUE(!CompactionFilterFactoryGetContext::IsManual(
      options.compaction_filter_factory.get()));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("1,4", FilesPerLevel(0));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("2,4", FilesPerLevel(0));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("3,4", FilesPerLevel(0));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("0,4,4", FilesPerLevel(0));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("1,4,4", FilesPerLevel(0));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("2,4,4", FilesPerLevel(0));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("3,4,4", FilesPerLevel(0));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("0,4,8", FilesPerLevel(0));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ("1,4,8", FilesPerLevel(0));

  // compact it three times
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(experimental::SuggestCompactRange(db_, nullptr, nullptr));
    dbfull()->TEST_WaitForCompact();
  }

  // All files are compacted
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // nonoverlapping with the file on level 0
  Slice start("a"), end("b");
  ASSERT_OK(experimental::SuggestCompactRange(db_, &start, &end));
  dbfull()->TEST_WaitForCompact();

  // should not compact the level 0 file
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  start = Slice("j");
  end = Slice("m");
  ASSERT_OK(experimental::SuggestCompactRange(db_, &start, &end));
  dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(CompactionFilterFactoryGetContext::IsManual(
      options.compaction_filter_factory.get()));

  // now it should compact the level 0 file
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
}

TEST_F(DBTest, PromoteL0) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  DestroyAndReopen(options);

  // non overlapping ranges
  std::vector<std::pair<int32_t, int32_t>> ranges = {
      {81, 160}, {0, 80}, {161, 240}, {241, 320}};

  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::map<int32_t, std::string> values;
  for (const auto& range : ranges) {
    for (int32_t j = range.first; j < range.second; j++) {
      values[j] = RandomString(&rnd, value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
    ASSERT_OK(Flush());
  }

  int32_t level0_files = NumTableFilesAtLevel(0, 0);
  ASSERT_EQ(level0_files, ranges.size());
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 0);  // No files in L1

  // Promote L0 level to L2.
  ASSERT_OK(experimental::PromoteL0(db_, db_->DefaultColumnFamily(), 2));
  // We expect that all the files were trivially moved from L0 to L2
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2, 0), level0_files);

  for (const auto& kv : values) {
    ASSERT_EQ(Get(Key(kv.first)), kv.second);
  }
}

TEST_F(DBTest, PromoteL0Failure) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  DestroyAndReopen(options);

  // Produce two L0 files with overlapping ranges.
  ASSERT_OK(Put(Key(0), ""));
  ASSERT_OK(Put(Key(3), ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), ""));
  ASSERT_OK(Flush());

  Status status;
  // Fails because L0 has overlapping files.
  status = experimental::PromoteL0(db_, db_->DefaultColumnFamily());
  ASSERT_TRUE(status.IsInvalidArgument());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Now there is a file in L1.
  ASSERT_GE(NumTableFilesAtLevel(1, 0), 1);

  ASSERT_OK(Put(Key(5), ""));
  ASSERT_OK(Flush());
  // Fails because L1 is non-empty.
  status = experimental::PromoteL0(db_, db_->DefaultColumnFamily());
  ASSERT_TRUE(status.IsInvalidArgument());
}
#endif  // ROCKSDB_LITE

// Github issue #596
TEST_F(DBTest, HugeNumberOfLevels) {
  Options options = CurrentOptions();
  options.write_buffer_size = 2 * 1024 * 1024;         // 2MB
  options.max_bytes_for_level_base = 2 * 1024 * 1024;  // 2MB
  options.num_levels = 12;
  options.max_background_compactions = 10;
  options.max_bytes_for_level_multiplier = 2;
  options.level_compaction_dynamic_level_bytes = true;
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 300000; ++i) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
  }

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
}

TEST_F(DBTest, AutomaticConflictsWithManualCompaction) {
  Options options = CurrentOptions();
  options.write_buffer_size = 2 * 1024 * 1024;         // 2MB
  options.max_bytes_for_level_base = 2 * 1024 * 1024;  // 2MB
  options.num_levels = 12;
  options.max_background_compactions = 10;
  options.max_bytes_for_level_multiplier = 2;
  options.level_compaction_dynamic_level_bytes = true;
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 300000; ++i) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
  }

  std::atomic<int> callback_count(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction()::Conflict",
      [&](void* arg) { callback_count.fetch_add(1); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  CompactRangeOptions croptions;
  croptions.exclusive_manual_compaction = false;
  ASSERT_OK(db_->CompactRange(croptions, nullptr, nullptr));
  ASSERT_GE(callback_count.load(), 1);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  for (int i = 0; i < 300000; ++i) {
    ASSERT_NE("NOT_FOUND", Get(Key(i)));
  }
}

// Github issue #595
// Large write batch with column families
TEST_F(DBTest, LargeBatchWithColumnFamilies) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  CreateAndReopenWithCF({"pikachu"}, options);
  int64_t j = 0;
  for (int i = 0; i < 5; i++) {
    for (int pass = 1; pass <= 3; pass++) {
      WriteBatch batch;
      size_t write_size = 1024 * 1024 * (5 + i);
      fprintf(stderr, "prepare: %" ROCKSDB_PRIszt " MB, pass:%d\n",
              (write_size / 1024 / 1024), pass);
      for (;;) {
        std::string data(3000, j++ % 127 + 20);
        data += ToString(j);
        batch.Put(handles_[0], Slice(data), Slice(data));
        if (batch.GetDataSize() > write_size) {
          break;
        }
      }
      fprintf(stderr, "write: %" ROCKSDB_PRIszt " MB\n",
              (batch.GetDataSize() / 1024 / 1024));
      ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
      fprintf(stderr, "done\n");
    }
  }
  // make sure we can re-open it.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
}

// Make sure that Flushes can proceed in parallel with CompactRange()
TEST_F(DBTest, FlushesInParallelWithCompactRange) {
  // iter == 0 -- leveled
  // iter == 1 -- leveled, but throw in a flush between two levels compacting
  // iter == 2 -- universal
  for (int iter = 0; iter < 3; ++iter) {
    Options options = CurrentOptions();
    if (iter < 2) {
      options.compaction_style = kCompactionStyleLevel;
    } else {
      options.compaction_style = kCompactionStyleUniversal;
    }
    options.write_buffer_size = 110 << 10;
    options.level0_file_num_compaction_trigger = 4;
    options.num_levels = 4;
    options.compression = kNoCompression;
    options.max_bytes_for_level_base = 450 << 10;
    options.target_file_size_base = 98 << 10;
    options.max_write_buffer_number = 2;

    DestroyAndReopen(options);

    Random rnd(301);
    for (int num = 0; num < 14; num++) {
      GenerateNewRandomFile(&rnd);
    }

    if (iter == 1) {
      rocksdb::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::RunManualCompaction()::1",
            "DBTest::FlushesInParallelWithCompactRange:1"},
           {"DBTest::FlushesInParallelWithCompactRange:2",
            "DBImpl::RunManualCompaction()::2"}});
    } else {
      rocksdb::SyncPoint::GetInstance()->LoadDependency(
          {{"CompactionJob::Run():Start",
            "DBTest::FlushesInParallelWithCompactRange:1"},
           {"DBTest::FlushesInParallelWithCompactRange:2",
            "CompactionJob::Run():End"}});
    }
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();

    std::vector<std::thread> threads;
    threads.emplace_back([&]() { Compact("a", "z"); });

    TEST_SYNC_POINT("DBTest::FlushesInParallelWithCompactRange:1");

    // this has to start a flush. if flushes are blocked, this will try to
    // create
    // 3 memtables, and that will fail because max_write_buffer_number is 2
    for (int num = 0; num < 3; num++) {
      GenerateNewRandomFile(&rnd, /* nowait */ true);
    }

    TEST_SYNC_POINT("DBTest::FlushesInParallelWithCompactRange:2");

    for (auto& t : threads) {
      t.join();
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBTest, DelayedWriteRate) {
  const int kEntriesPerMemTable = 100;
  const int kTotalFlushes = 20;

  Options options = CurrentOptions();
  env_->SetBackgroundThreads(1, Env::LOW);
  options.env = env_;
  env_->no_sleep_ = true;
  options.write_buffer_size = 100000000;
  options.max_write_buffer_number = 256;
  options.max_background_compactions = 1;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 3;
  options.level0_stop_writes_trigger = 999999;
  options.delayed_write_rate = 20000000;  // Start with 200MB/s
  options.memtable_factory.reset(
      new SpecialSkipListFactory(kEntriesPerMemTable));

  CreateAndReopenWithCF({"pikachu"}, options);

  // Block compactions
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  for (int i = 0; i < 3; i++) {
    Put(Key(i), std::string(10000, 'x'));
    Flush();
  }

  // These writes will be slowed down to 1KB/s
  uint64_t estimated_sleep_time = 0;
  Random rnd(301);
  Put("", "");
  uint64_t cur_rate = options.delayed_write_rate;
  for (int i = 0; i < kTotalFlushes; i++) {
    uint64_t size_memtable = 0;
    for (int j = 0; j < kEntriesPerMemTable; j++) {
      auto rand_num = rnd.Uniform(20);
      // Spread the size range to more.
      size_t entry_size = rand_num * rand_num * rand_num;
      WriteOptions wo;
      Put(Key(i), std::string(entry_size, 'x'), wo);
      size_memtable += entry_size + 18;
      // Occasionally sleep a while
      if (rnd.Uniform(20) == 6) {
        env_->SleepForMicroseconds(2666);
      }
    }
    dbfull()->TEST_WaitForFlushMemTable();
    estimated_sleep_time += size_memtable * 1000000u / cur_rate;
    // Slow down twice. One for memtable switch and one for flush finishes.
    cur_rate = static_cast<uint64_t>(static_cast<double>(cur_rate) /
                                     kSlowdownRatio / kSlowdownRatio);
  }
  // Estimate the total sleep time fall into the rough range.
  ASSERT_GT(env_->addon_time_.load(),
            static_cast<int64_t>(estimated_sleep_time / 2));
  ASSERT_LT(env_->addon_time_.load(),
            static_cast<int64_t>(estimated_sleep_time * 2));

  env_->no_sleep_ = false;
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBTest, HardLimit) {
  Options options = CurrentOptions();
  options.env = env_;
  env_->SetBackgroundThreads(1, Env::LOW);
  options.max_write_buffer_number = 256;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 * 1024;
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 999999;
  options.level0_stop_writes_trigger = 999999;
  options.hard_pending_compaction_bytes_limit = 800 << 10;
  options.max_bytes_for_level_base = 10000000000u;
  options.max_background_compactions = 1;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));

  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  CreateAndReopenWithCF({"pikachu"}, options);

  std::atomic<int> callback_count(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack("DBImpl::DelayWrite:Wait",
                                                 [&](void* arg) {
                                                   callback_count.fetch_add(1);
                                                   sleeping_task_low.WakeUp();
                                                 });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  int key_idx = 0;
  for (int num = 0; num < 5; num++) {
    GenerateNewFile(&rnd, &key_idx, true);
    dbfull()->TEST_WaitForFlushMemTable();
  }

  ASSERT_EQ(0, callback_count.load());

  for (int num = 0; num < 5; num++) {
    GenerateNewFile(&rnd, &key_idx, true);
    dbfull()->TEST_WaitForFlushMemTable();
  }
  ASSERT_GE(callback_count.load(), 1);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  sleeping_task_low.WaitUntilDone();
}

#ifndef ROCKSDB_LITE
TEST_F(DBTest, SoftLimit) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  options.max_write_buffer_number = 256;
  options.level0_file_num_compaction_trigger = 1;
  options.level0_slowdown_writes_trigger = 3;
  options.level0_stop_writes_trigger = 999999;
  options.delayed_write_rate = 20000;  // About 200KB/s limited rate
  options.soft_pending_compaction_bytes_limit = 160000;
  options.target_file_size_base = 99999999;  // All into one file
  options.max_bytes_for_level_base = 50000;
  options.max_bytes_for_level_multiplier = 10;
  options.max_background_compactions = 1;
  options.compression = kNoCompression;

  Reopen(options);

  // Generating 360KB in Level 3
  for (int i = 0; i < 72; i++) {
    Put(Key(i), std::string(5000, 'x'));
    if (i % 10 == 0) {
      Flush();
    }
  }
  dbfull()->TEST_WaitForCompact();
  MoveFilesToLevel(3);

  // Generating 360KB in Level 2
  for (int i = 0; i < 72; i++) {
    Put(Key(i), std::string(5000, 'x'));
    if (i % 10 == 0) {
      Flush();
    }
  }
  dbfull()->TEST_WaitForCompact();
  MoveFilesToLevel(2);

  Put(Key(0), "");

  test::SleepingBackgroundTask sleeping_task_low;
  // Block compactions
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();

  // Create 3 L0 files, making score of L0 to be 3.
  for (int i = 0; i < 3; i++) {
    Put(Key(i), std::string(5000, 'x'));
    Put(Key(100 - i), std::string(5000, 'x'));
    // Flush the file. File size is around 30KB.
    Flush();
  }
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
  sleeping_task_low.Reset();
  dbfull()->TEST_WaitForCompact();

  // Now there is one L1 file but doesn't trigger soft_rate_limit
  // The L1 file size is around 30KB.
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  // Only allow one compactin going through.
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void* arg) {
        // Schedule a sleeping task.
        sleeping_task_low.Reset();
        env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                       &sleeping_task_low, Env::Priority::LOW);
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();
  // Create 3 L0 files, making score of L0 to be 3
  for (int i = 0; i < 3; i++) {
    Put(Key(10 + i), std::string(5000, 'x'));
    Put(Key(90 - i), std::string(5000, 'x'));
    // Flush the file. File size is around 30KB.
    Flush();
  }

  // Wake up sleep task to enable compaction to run and waits
  // for it to go to sleep state again to make sure one compaction
  // goes through.
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilSleeping();

  // Now there is one L1 file (around 60KB) which exceeds 50KB base by 10KB
  // Given level multiplier 10, estimated pending compaction is around 100KB
  // doesn't trigger soft_pending_compaction_bytes_limit
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  // Create 3 L0 files, making score of L0 to be 3, higher than L0.
  for (int i = 0; i < 3; i++) {
    Put(Key(20 + i), std::string(5000, 'x'));
    Put(Key(80 - i), std::string(5000, 'x'));
    // Flush the file. File size is around 30KB.
    Flush();
  }
  // Wake up sleep task to enable compaction to run and waits
  // for it to go to sleep state again to make sure one compaction
  // goes through.
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilSleeping();

  // Now there is one L1 file (around 90KB) which exceeds 50KB base by 40KB
  // L2 size is 360KB, so the estimated level fanout 4, estimated pending
  // compaction is around 200KB
  // triggerring soft_pending_compaction_bytes_limit
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilSleeping();

  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  // shrink level base so L2 will hit soft limit easier.
  ASSERT_OK(dbfull()->SetOptions({
      {"max_bytes_for_level_base", "5000"},
  }));

  Put("", "");
  Flush();
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());

  sleeping_task_low.WaitUntilSleeping();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBTest, LastWriteBufferDelay) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;
  options.max_write_buffer_number = 4;
  options.delayed_write_rate = 20000;
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  int kNumKeysPerMemtable = 3;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(kNumKeysPerMemtable));

  Reopen(options);
  test::SleepingBackgroundTask sleeping_task;
  // Block flushes
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);
  sleeping_task.WaitUntilSleeping();

  // Create 3 L0 files, making score of L0 to be 3.
  for (int i = 0; i < 3; i++) {
    // Fill one mem table
    for (int j = 0; j < kNumKeysPerMemtable; j++) {
      Put(Key(j), "");
    }
    ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  }
  // Inserting a new entry would create a new mem table, triggering slow down.
  Put(Key(0), "");
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());

  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
}
#endif  // ROCKSDB_LITE

TEST_F(DBTest, FailWhenCompressionNotSupportedTest) {
  CompressionType compressions[] = {kZlibCompression, kBZip2Compression,
                                    kLZ4Compression, kLZ4HCCompression,
                                    kXpressCompression};
  for (auto comp : compressions) {
    if (!CompressionTypeSupported(comp)) {
      // not supported, we should fail the Open()
      Options options = CurrentOptions();
      options.compression = comp;
      ASSERT_TRUE(!TryReopen(options).ok());
      // Try if CreateColumnFamily also fails
      options.compression = kNoCompression;
      ASSERT_OK(TryReopen(options));
      ColumnFamilyOptions cf_options(options);
      cf_options.compression = comp;
      ColumnFamilyHandle* handle;
      ASSERT_TRUE(!db_->CreateColumnFamily(cf_options, "name", &handle).ok());
    }
  }
}

#ifndef ROCKSDB_LITE
TEST_F(DBTest, RowCache) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();
  options.row_cache = NewLRUCache(8192);
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 0);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 0);
  ASSERT_EQ(Get("foo"), "bar");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 0);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 1);
  ASSERT_EQ(Get("foo"), "bar");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 1);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 1);
}
#endif  // ROCKSDB_LITE

TEST_F(DBTest, DeletingOldWalAfterDrop) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"Test:AllowFlushes", "DBImpl::BGWorkFlush"},
       {"DBImpl::BGWorkFlush:done", "Test:WaitForFlush"}});
  rocksdb::SyncPoint::GetInstance()->ClearTrace();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  Options options = CurrentOptions();
  options.max_total_wal_size = 8192;
  options.compression = kNoCompression;
  options.write_buffer_size = 1 << 20;
  options.level0_file_num_compaction_trigger = (1 << 30);
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  CreateColumnFamilies({"cf1", "cf2"}, options);
  ASSERT_OK(Put(0, "key1", DummyString(8192)));
  ASSERT_OK(Put(0, "key2", DummyString(8192)));
  // the oldest wal should now be getting_flushed
  ASSERT_OK(db_->DropColumnFamily(handles_[0]));
  // all flushes should now do nothing because their CF is dropped
  TEST_SYNC_POINT("Test:AllowFlushes");
  TEST_SYNC_POINT("Test:WaitForFlush");
  uint64_t lognum1 = dbfull()->TEST_LogfileNumber();
  ASSERT_OK(Put(1, "key3", DummyString(8192)));
  ASSERT_OK(Put(1, "key4", DummyString(8192)));
  // new wal should have been created
  uint64_t lognum2 = dbfull()->TEST_LogfileNumber();
  EXPECT_GT(lognum2, lognum1);
}

TEST_F(DBTest, UnsupportedManualSync) {
  DestroyAndReopen(CurrentOptions());
  env_->is_wal_sync_thread_safe_.store(false);
  Status s = db_->SyncWAL();
  ASSERT_TRUE(s.IsNotSupported());
}

INSTANTIATE_TEST_CASE_P(DBTestWithParam, DBTestWithParam,
                        ::testing::Combine(::testing::Values(1, 4),
                                           ::testing::Bool()));

TEST_F(DBTest, PauseBackgroundWorkTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000;  // Small write buffer
  Reopen(options);

  std::vector<std::thread> threads;
  std::atomic<bool> done(false);
  db_->PauseBackgroundWork();
  threads.emplace_back([&]() {
    Random rnd(301);
    for (int i = 0; i < 10000; ++i) {
      Put(RandomString(&rnd, 10), RandomString(&rnd, 10));
    }
    done.store(true);
  });
  env_->SleepForMicroseconds(200000);
  // make sure the thread is not done
  ASSERT_EQ(false, done.load());
  db_->ContinueBackgroundWork();
  for (auto& t : threads) {
    t.join();
  }
  // now it's done
  ASSERT_EQ(true, done.load());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
