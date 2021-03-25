//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <assert.h>

#include <iostream>
#include <memory>

#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/write_batch_internal.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

bool use_compression;

class MergeTest : public testing::Test {};

size_t num_merge_operator_calls;
void resetNumMergeOperatorCalls() { num_merge_operator_calls = 0; }

size_t num_partial_merge_calls;
void resetNumPartialMergeCalls() { num_partial_merge_calls = 0; }

class CountMergeOperator : public AssociativeMergeOperator {
 public:
  CountMergeOperator() {
    mergeOperator_ = MergeOperators::CreateUInt64AddOperator();
  }

  bool Merge(const Slice& key, const Slice* existing_value, const Slice& value,
             std::string* new_value, Logger* logger) const override {
    assert(new_value->empty());
    ++num_merge_operator_calls;
    if (existing_value == nullptr) {
      new_value->assign(value.data(), value.size());
      return true;
    }

    return mergeOperator_->PartialMerge(key, *existing_value, value, new_value,
                                        logger);
  }

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* logger) const override {
    assert(new_value->empty());
    ++num_partial_merge_calls;
    return mergeOperator_->PartialMergeMulti(key, operand_list, new_value,
                                             logger);
  }

  const char* Name() const override { return "UInt64AddOperator"; }

 private:
  std::shared_ptr<MergeOperator> mergeOperator_;
};

class EnvMergeTest : public EnvWrapper {
 public:
  EnvMergeTest() : EnvWrapper(Env::Default()) {}
  //  ~EnvMergeTest() override {}

  uint64_t NowNanos() override {
    ++now_nanos_count_;
    return target()->NowNanos();
  }

  static uint64_t now_nanos_count_;

  static std::unique_ptr<EnvMergeTest> singleton_;

  static EnvMergeTest* GetInstance() {
    if (nullptr == singleton_) singleton_.reset(new EnvMergeTest);
    return singleton_.get();
  }
};

uint64_t EnvMergeTest::now_nanos_count_{0};
std::unique_ptr<EnvMergeTest> EnvMergeTest::singleton_;

std::shared_ptr<DB> OpenDb(const std::string& dbname, const bool ttl = false,
                           const size_t max_successive_merges = 0) {
  DB* db;
  Options options;
  options.create_if_missing = true;
  options.merge_operator = std::make_shared<CountMergeOperator>();
  options.max_successive_merges = max_successive_merges;
  options.env = EnvMergeTest::GetInstance();
  EXPECT_OK(DestroyDB(dbname, Options()));
  Status s;
// DBWithTTL is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
  if (ttl) {
    DBWithTTL* db_with_ttl;
    s = DBWithTTL::Open(options, dbname, &db_with_ttl);
    db = db_with_ttl;
  } else {
    s = DB::Open(options, dbname, &db);
  }
#else
  assert(!ttl);
  s = DB::Open(options, dbname, &db);
#endif  // !ROCKSDB_LITE
  EXPECT_OK(s);
  assert(s.ok());
  return std::shared_ptr<DB>(db);
}

// Imagine we are maintaining a set of uint64 counters.
// Each counter has a distinct name. And we would like
// to support four high level operations:
// set, add, get and remove
// This is a quick implementation without a Merge operation.
class Counters {
 protected:
  std::shared_ptr<DB> db_;

  WriteOptions put_option_;
  ReadOptions get_option_;
  WriteOptions delete_option_;

  uint64_t default_;

 public:
  explicit Counters(std::shared_ptr<DB> db, uint64_t defaultCount = 0)
      : db_(db),
        put_option_(),
        get_option_(),
        delete_option_(),
        default_(defaultCount) {
    assert(db_);
  }

  virtual ~Counters() {}

  // public interface of Counters.
  // All four functions return false
  // if the underlying level db operation failed.

  // mapped to a levedb Put
  bool set(const std::string& key, uint64_t value) {
    // just treat the internal rep of int64 as the string
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    Slice slice(buf, sizeof(value));
    auto s = db_->Put(put_option_, key, slice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // mapped to a rocksdb Delete
  bool remove(const std::string& key) {
    auto s = db_->Delete(delete_option_, key);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // mapped to a rocksdb Get
  bool get(const std::string& key, uint64_t* value) {
    std::string str;
    auto s = db_->Get(get_option_, key, &str);

    if (s.IsNotFound()) {
      // return default value if not found;
      *value = default_;
      return true;
    } else if (s.ok()) {
      // deserialization
      if (str.size() != sizeof(uint64_t)) {
        std::cerr << "value corruption\n";
        return false;
      }
      *value = DecodeFixed64(&str[0]);
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }

  // 'add' is implemented as get -> modify -> set
  // An alternative is a single merge operation, see MergeBasedCounters
  virtual bool add(const std::string& key, uint64_t value) {
    uint64_t base = default_;
    return get(key, &base) && set(key, base + value);
  }

  // convenience functions for testing
  void assert_set(const std::string& key, uint64_t value) {
    assert(set(key, value));
  }

  void assert_remove(const std::string& key) { assert(remove(key)); }

  uint64_t assert_get(const std::string& key) {
    uint64_t value = default_;
    int result = get(key, &value);
    assert(result);
    if (result == 0) exit(1);  // Disable unused variable warning.
    return value;
  }

  void assert_add(const std::string& key, uint64_t value) {
    int result = add(key, value);
    assert(result);
    if (result == 0) exit(1);  // Disable unused variable warning.
  }
};

// Implement 'add' directly with the new Merge operation
class MergeBasedCounters : public Counters {
 private:
  WriteOptions merge_option_;  // for merge

 public:
  explicit MergeBasedCounters(std::shared_ptr<DB> db, uint64_t defaultCount = 0)
      : Counters(db, defaultCount), merge_option_() {}

  // mapped to a rocksdb Merge operation
  bool add(const std::string& key, uint64_t value) override {
    char encoded[sizeof(uint64_t)];
    EncodeFixed64(encoded, value);
    Slice slice(encoded, sizeof(uint64_t));
    auto s = db_->Merge(merge_option_, key, slice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << s.ToString() << std::endl;
      return false;
    }
  }
};

void dumpDb(DB* db) {
  auto it = std::unique_ptr<Iterator>(db->NewIterator(ReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    // uint64_t value = DecodeFixed64(it->value().data());
    // std::cout << it->key().ToString() << ": " << value << std::endl;
  }
  assert(it->status().ok());  // Check for any errors found during the scan
}

void testCounters(Counters& counters, DB* db, bool test_compaction) {
  FlushOptions o;
  o.wait = true;

  counters.assert_set("a", 1);

  if (test_compaction) {
    ASSERT_OK(db->Flush(o));
  }

  ASSERT_EQ(counters.assert_get("a"), 1);

  counters.assert_remove("b");

  // defaut value is 0 if non-existent
  ASSERT_EQ(counters.assert_get("b"), 0);

  counters.assert_add("a", 2);

  if (test_compaction) {
    ASSERT_OK(db->Flush(o));
  }

  // 1+2 = 3
  ASSERT_EQ(counters.assert_get("a"), 3);

  dumpDb(db);

  // 1+...+49 = ?
  uint64_t sum = 0;
  for (int i = 1; i < 50; i++) {
    counters.assert_add("b", i);
    sum += i;
  }
  ASSERT_EQ(counters.assert_get("b"), sum);

  dumpDb(db);

  if (test_compaction) {
    ASSERT_OK(db->Flush(o));

    ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    dumpDb(db);

    ASSERT_EQ(counters.assert_get("a"), 3);
    ASSERT_EQ(counters.assert_get("b"), sum);
  }
}

void testCountersWithFlushAndCompaction(Counters& counters, DB* db) {
  ASSERT_OK(db->Put({}, "1", "1"));
  ASSERT_OK(db->Flush(FlushOptions()));

  std::atomic<int> cnt{0};
  const auto get_thread_id = [&cnt]() {
    thread_local int thread_id{cnt++};
    return thread_id;
  };
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:BeforeWriterWaiting", [&](void* /*arg*/) {
        int thread_id = get_thread_id();
        if (1 == thread_id) {
          TEST_SYNC_POINT(
              "testCountersWithFlushAndCompaction::bg_compact_thread:0");
        } else if (2 == thread_id) {
          TEST_SYNC_POINT(
              "testCountersWithFlushAndCompaction::bg_flush_thread:0");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void* /*arg*/) {
        int thread_id = get_thread_id();
        if (0 == thread_id) {
          TEST_SYNC_POINT(
              "testCountersWithFlushAndCompaction::set_options_thread:0");
          TEST_SYNC_POINT(
              "testCountersWithFlushAndCompaction::set_options_thread:1");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WakeUpAndDone", [&](void* arg) {
        auto* mutex = reinterpret_cast<InstrumentedMutex*>(arg);
        mutex->AssertHeld();
        int thread_id = get_thread_id();
        ASSERT_EQ(2, thread_id);
        mutex->Unlock();
        TEST_SYNC_POINT(
            "testCountersWithFlushAndCompaction::bg_flush_thread:1");
        TEST_SYNC_POINT(
            "testCountersWithFlushAndCompaction::bg_flush_thread:2");
        mutex->Lock();
      });
  SyncPoint::GetInstance()->LoadDependency({
      {"testCountersWithFlushAndCompaction::set_options_thread:0",
       "testCountersWithCompactionAndFlush:BeforeCompact"},
      {"testCountersWithFlushAndCompaction::bg_compact_thread:0",
       "testCountersWithFlushAndCompaction:BeforeIncCounters"},
      {"testCountersWithFlushAndCompaction::bg_flush_thread:0",
       "testCountersWithFlushAndCompaction::set_options_thread:1"},
      {"testCountersWithFlushAndCompaction::bg_flush_thread:1",
       "testCountersWithFlushAndCompaction:BeforeVerification"},
      {"testCountersWithFlushAndCompaction:AfterGet",
       "testCountersWithFlushAndCompaction::bg_flush_thread:2"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread set_options_thread([&]() {
    ASSERT_OK(reinterpret_cast<DBImpl*>(db)->SetOptions(
        {{"disable_auto_compactions", "false"}}));
  });
  TEST_SYNC_POINT("testCountersWithCompactionAndFlush:BeforeCompact");
  port::Thread compact_thread([&]() {
    ASSERT_OK(reinterpret_cast<DBImpl*>(db)->CompactRange(
        CompactRangeOptions(), db->DefaultColumnFamily(), nullptr, nullptr));
  });

  TEST_SYNC_POINT("testCountersWithFlushAndCompaction:BeforeIncCounters");
  counters.add("test-key", 1);

  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db->Flush(flush_opts));

  TEST_SYNC_POINT("testCountersWithFlushAndCompaction:BeforeVerification");
  std::string expected;
  PutFixed64(&expected, 1);
  std::string actual;
  Status s = db->Get(ReadOptions(), "test-key", &actual);
  TEST_SYNC_POINT("testCountersWithFlushAndCompaction:AfterGet");
  set_options_thread.join();
  compact_thread.join();
  ASSERT_OK(s);
  ASSERT_EQ(expected, actual);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

void testSuccessiveMerge(Counters& counters, size_t max_num_merges,
                         size_t num_merges) {
  counters.assert_remove("z");
  uint64_t sum = 0;

  for (size_t i = 1; i <= num_merges; ++i) {
    resetNumMergeOperatorCalls();
    counters.assert_add("z", i);
    sum += i;

    if (i % (max_num_merges + 1) == 0) {
      ASSERT_EQ(num_merge_operator_calls, max_num_merges + 1);
    } else {
      ASSERT_EQ(num_merge_operator_calls, 0);
    }

    resetNumMergeOperatorCalls();
    ASSERT_EQ(counters.assert_get("z"), sum);
    ASSERT_EQ(num_merge_operator_calls, i % (max_num_merges + 1));
  }
}

void testPartialMerge(Counters* counters, DB* db, size_t max_merge,
                      size_t min_merge, size_t count) {
  FlushOptions o;
  o.wait = true;

  // Test case 1: partial merge should be called when the number of merge
  //              operands exceeds the threshold.
  uint64_t tmp_sum = 0;
  resetNumPartialMergeCalls();
  for (size_t i = 1; i <= count; i++) {
    counters->assert_add("b", i);
    tmp_sum += i;
  }
  ASSERT_OK(db->Flush(o));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(tmp_sum, counters->assert_get("b"));
  if (count > max_merge) {
    // in this case, FullMerge should be called instead.
    ASSERT_EQ(num_partial_merge_calls, 0U);
  } else {
    // if count >= min_merge, then partial merge should be called once.
    ASSERT_EQ((count >= min_merge), (num_partial_merge_calls == 1));
  }

  // Test case 2: partial merge should not be called when a put is found.
  resetNumPartialMergeCalls();
  tmp_sum = 0;
  ASSERT_OK(db->Put(ROCKSDB_NAMESPACE::WriteOptions(), "c", "10"));
  for (size_t i = 1; i <= count; i++) {
    counters->assert_add("c", i);
    tmp_sum += i;
  }
  ASSERT_OK(db->Flush(o));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(tmp_sum, counters->assert_get("c"));
  ASSERT_EQ(num_partial_merge_calls, 0U);
  ASSERT_EQ(EnvMergeTest::now_nanos_count_, 0U);
}

void testSingleBatchSuccessiveMerge(DB* db, size_t max_num_merges,
                                    size_t num_merges) {
  ASSERT_GT(num_merges, max_num_merges);

  Slice key("BatchSuccessiveMerge");
  uint64_t merge_value = 1;
  char buf[sizeof(merge_value)];
  EncodeFixed64(buf, merge_value);
  Slice merge_value_slice(buf, sizeof(merge_value));

  // Create the batch
  WriteBatch batch;
  for (size_t i = 0; i < num_merges; ++i) {
    ASSERT_OK(batch.Merge(key, merge_value_slice));
  }

  // Apply to memtable and count the number of merges
  resetNumMergeOperatorCalls();
  ASSERT_OK(db->Write(WriteOptions(), &batch));
  ASSERT_EQ(
      num_merge_operator_calls,
      static_cast<size_t>(num_merges - (num_merges % (max_num_merges + 1))));

  // Get the value
  resetNumMergeOperatorCalls();
  std::string get_value_str;
  ASSERT_OK(db->Get(ReadOptions(), key, &get_value_str));
  assert(get_value_str.size() == sizeof(uint64_t));
  uint64_t get_value = DecodeFixed64(&get_value_str[0]);
  ASSERT_EQ(get_value, num_merges * merge_value);
  ASSERT_EQ(num_merge_operator_calls,
            static_cast<size_t>((num_merges % (max_num_merges + 1))));
}

void runTest(const std::string& dbname, const bool use_ttl = false) {
  {
    auto db = OpenDb(dbname, use_ttl);

    {
      Counters counters(db, 0);
      testCounters(counters, db.get(), true);
    }

    {
      MergeBasedCounters counters(db, 0);
      testCounters(counters, db.get(), use_compression);
    }
  }

  ASSERT_OK(DestroyDB(dbname, Options()));

  {
    size_t max_merge = 5;
    auto db = OpenDb(dbname, use_ttl, max_merge);
    MergeBasedCounters counters(db, 0);
    testCounters(counters, db.get(), use_compression);
    testSuccessiveMerge(counters, max_merge, max_merge * 2);
    testSingleBatchSuccessiveMerge(db.get(), 5, 7);
    ASSERT_OK(db->Close());
    ASSERT_OK(DestroyDB(dbname, Options()));
  }

  {
    size_t max_merge = 100;
    // Min merge is hard-coded to 2.
    uint32_t min_merge = 2;
    for (uint32_t count = min_merge - 1; count <= min_merge + 1; count++) {
      auto db = OpenDb(dbname, use_ttl, max_merge);
      MergeBasedCounters counters(db, 0);
      testPartialMerge(&counters, db.get(), max_merge, min_merge, count);
      ASSERT_OK(db->Close());
      ASSERT_OK(DestroyDB(dbname, Options()));
    }
    {
      auto db = OpenDb(dbname, use_ttl, max_merge);
      MergeBasedCounters counters(db, 0);
      testPartialMerge(&counters, db.get(), max_merge, min_merge,
                       min_merge * 10);
      ASSERT_OK(db->Close());
      ASSERT_OK(DestroyDB(dbname, Options()));
    }
  }

  {
    {
      auto db = OpenDb(dbname);
      MergeBasedCounters counters(db, 0);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
      ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    DB* reopen_db;
    ASSERT_OK(DB::Open(Options(), dbname, &reopen_db));
    std::string value;
    ASSERT_NOK(reopen_db->Get(ReadOptions(), "test-key", &value));
    delete reopen_db;
    ASSERT_OK(DestroyDB(dbname, Options()));
  }

  /* Temporary remove this test
  {
    std::cout << "Test merge-operator not set after reopen (recovery case)\n";
    {
      auto db = OpenDb(dbname);
      MergeBasedCounters counters(db, 0);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
      counters.add("test-key", 1);
    }

    DB* reopen_db;
    ASSERT_TRUE(DB::Open(Options(), dbname, &reopen_db).IsInvalidArgument());
  }
  */
}

TEST_F(MergeTest, MergeDbTest) {
  runTest(test::PerThreadDBPath("merge_testdb"));
}

#ifndef ROCKSDB_LITE
TEST_F(MergeTest, MergeDbTtlTest) {
  runTest(test::PerThreadDBPath("merge_testdbttl"),
          true);  // Run test on TTL database
}

TEST_F(MergeTest, MergeWithCompactionAndFlush) {
  const std::string dbname =
      test::PerThreadDBPath("merge_with_compaction_and_flush");
  {
    auto db = OpenDb(dbname);
    {
      MergeBasedCounters counters(db, 0);
      testCountersWithFlushAndCompaction(counters, db.get());
    }
  }
  ASSERT_OK(DestroyDB(dbname, Options()));
}
#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::use_compression = false;
  if (argc > 1) {
    ROCKSDB_NAMESPACE::use_compression = true;
  }

  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
